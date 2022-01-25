#===============================================================================
# Script Name: 1_Hypack Data Parser_Phantom.R
#
# Script Function: This script is designed to unpack the Hypack .LOG files, and 
# to extract various pieces of the data that are contained within the logs. The 
# script locates files with the .LOG extension, reads them in and extract the 
# timestamps, device (sensor) names, and sensor data for positions, depths, 
# heading, altitude, speed and slant range (from camera). The script searches 
# for duplicate timestamp records for data sources that update faster than 1 Hz, 
# and then removes extra records to preserved a common 1 Hz time series for all 
# sensors.
# 
# The script design at this point is to merge all records from the .LOG files in 
# the working directory into one 'master file', and to then use transect start/
# end time to 'trim' the master file time series to the periods of interest. 
# Specifically, data is trimmed to transect start/end times based on timestamps 
# provided in an accompanying Dive Log file, which is assumed to be an MS Excel 
# file with start/end times for each transect. 
#
# Relevant data are then extracted, one parameter at a time, from the trimmed 
# master file. In some cases, the script will search for the preferred data 
# source first (i.e. CTD depth, rather than onboard depth sensor) and will fall 
# back to extracting the secondary source as required, while also writing a data 
# flag to alert the user.
#
# In the particular case of position data records, the script will convert the 
# projected UTM coordinates that are stored in the Hypack .LOG file into decimal 
# degrees.
#
# All data is merged into separate data frame for each transect, and is written 
# out as .CSV files.
#
# Script Author: Ben Snow
# Script Date: Aug 27, 2019
# R Version: 3.5.1
#
################################################################################
#                                           CHANGE LOG
################################################################################
#
# May 12, 2020: Padded transect start and end times by 5 minutes on either side, 
#               as per request from J.Nephin and S. Jeffery
# May 24, 2020: Changed out of range values for Tritech PA500 altimeter 
#               (MiniZeus Slant Range) and ROWETech DVL (Altitude) to -9999, 
#               instead of N/A. Note that Phantom ROV speed (from the DVL) be 
#               default reads -9999 when out of range.
# June 2, 2020: Both Phantom heading and ship heading are now exported by this 
#               script; previously it was only the phantom's heading. Also, 
#               changed out-of range data values for speed from -99.9999 to 
#               -9999, to maintain consistency with Altitude and Slant Range 
#               Calculations.
# Apr 21, 2021: Updated device read in values, Cyclops HPR records removed, 
#               switched to RogueCam. Updated the initial read in loop to read 
#               an extra column now reads up to column X6 (previously was only 
#               to X5). This allows for appropriate parsing of the HPR devices, 
#               which includes data up to column X6.
# Apr 27, 2021: Added new section to create non-clipped data records, this is to 
#               allow plotting of certain variables during the descent/ascent 
#               phase of each dive.
# Nov 18, 2021: Tested GitHub functionality with RStudio
# Jan 2022: Started develop branch, made a number of changes:
#          - raw files read once with readLines then parsed
#          - uses function for reading raw files (faster than loop)
#          - processes sensor data prior to expanding to a 1Hz freq dataset
#          - added feet to meters conversion of secondary depth source
#          - fills in rov position gaps with ship gps, added source field 
#          - negative no data values classed as NA instead of -999
#          - gives option for transect padding, default to 2 min
#          - removes any time overlap between transects caused by padding
#          - option to keep all data, incl outside of transect start/stop times
#          - merges padded start/end times with processed sensor data
#          - attempted to make code more explicit, removed use of column order
#          - also removed all get() functions
################################################################################



#===============================================================================
# Packages and session options

# Check for the presence of packages shown below, install if needed
packages <- c("lubridate", "sp", "readxl", "readr", "purrr",
              "dplyr", "stringr", "rgdal", "zoo")
new_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
if(length(new_packages)) install.packages(new_packages)

# Set the number of sig figs high, to facilitate inspection of UTM data records
options(digits = 12)

# Load required packages
lapply(packages, require, character.only = TRUE)



#===============================================================================
# Select options

# Choose the number of minutes for padding start and end transect times
padtime <- 2

# Should hypack sensor data be clipped to on-transect times only?
# TRUE for on-transect, FALSE for all data including water column
onlyTransect <- TRUE


#===============================================================================
# STEP 1 - SET PATHS AND MAKE EXPORT DIRECTORY

# Enter Project folder
project_folder <- "Pac2021-054_phantom"

# Directory where Hypack .RAW files are stored
# Path must start from your working directory, check with getwd(), or full paths
hypack_path <- file.path(project_folder, "Data/Raw")

# Directory where dive log csv file is stored
# Path must start from your working directory, check with getwd(), or full paths
divelog_path <- file.path(project_folder, "Data/Dive_Logs/Dive_Log.csv")

# Create directory for saving both clipped and unclipped .CSV files
save_dir <- file.path(project_folder, "Data/Initial_Processed_Data")
dir.create(save_dir, recursive = TRUE) # Will warn if already exists



#===============================================================================
# STEP 2 - SET HYPACK RAW FILE DATA STREAM SOURCES

# Device types to extract data from
device_types <- c("POS","EC1","HCP","GYR","DFT")

# Set column names for position, depth, heading, draft and heave data sources. 
# Must match the names as listed in hardware devices. If a device is not present, 
# write NULL. MAKE SURE DEVICE NAMES MATCH .RAW FILES 
ship_heading_pref <- "Hemisphere_GPS" # DEV 0 
GPS_pref <- "Hemisphere_GPS" # DEV 0
depth_pref <- "RBR_CTD_Depth" # DEV 2
pos_pref <- "USBL_4370_Wide" # DEV 4
phantom_heading_pref <- "ROV_Heading_Depth_UTurns" # Dev 5
speed_pref <- "ROWETech_DVL" # Dev 6
altitude_pref <- "ROWETech_DVL" # Dev 6
slant_pref <- "Tritech_Slant_Range" # Dev 8
rogue_cam_pref <- "MiniZeus_ROV_IMU_Pitch_Roll" # Dev 10

# Set names for secondary hardware devices, for cases were primary device may 
# be malfunctioning, NULL if there is no secondary
pos_secondary <- "USBL_1000-21884" # Dev 7 "USBL_300-564"
depth_secondary <- "ROV_Heading_Depth_UTurns" # Dev 5



#===============================================================================
# STEP 3 - READ IN RAW DATA

# Lists Hypack files, apply function to extract data on list of files, combine
# extracted data into a single dataframe for entire cruise.


# Function to extract data from hypack file
extractHypack <- function( hfile ){
  
  # Read in lines from hypack files
  hlines <- readLines(hfile)
  
  # Extract device info 
  # Question: Could grep OFF offset info as well, could that be important?
  dev <- hlines[grepl("DEV", hlines)]
  dev <- gsub("\"", "", dev) %>% strsplit(., " ") %>% 
    do.call(rbind, .) %>% as.data.frame()
  # Only need columns 2 and 4
  dev <- dev[,c("V2","V4")] 
  
  # Extract UTM Zone
  # From the 3rd column of line 5
  longitude <- hlines[5] %>% strsplit(., " ") %>% 
    unlist() %>% .[3] %>% as.numeric()
  # Get UTM zone based on longitude
  zone <- NA
  zone[longitude == -123] <- 10
  zone[longitude == -129] <- 9
  zone[longitude == -135] <- 8
  # Check
  if(is.na(zone)) warning("Longitude does not match UTM zone 8, 9  or 10", 
                          call.= FALSE)
  
  # Extract start day
  # from the 3rd column of line 9
  day <- hlines[9] %>% strsplit(., " ") %>% 
    unlist() %>% .[3] 
  
  # Extract data stream
  # Start after EOH line
  startline <- which(grepl("EOH", hlines)) + 1
  ds_lines <- hlines[startline:length(hlines)]
  # Only include lines with device_types set in STEP 2
  ds_lines <- ds_lines[grepl(paste0(device_types, collapse = "|"), ds_lines)]
  # rbind lines together in dataframe, fills in blanks with NA
  ds_list <-  ds_lines %>% strsplit(., " ") 
  ds <- map(ds_list, ~ c(X=.)) %>% bind_rows(.) %>% as.data.frame()
  # Format columns
  ds$X3 <- as.integer(ds$X3)
  ds$X4 <- as.numeric(ds$X4) 
  ds$X5 <- as.numeric(ds$X5)
  ds$X6 <- as.numeric(ds$X6)
  # Add datetime field
  # Date formated as m/d/y in raw files
  ds$Datetime <- ymd_hms(mdy(day) + seconds(ds$X3))
  # Adjust datetime for second day if needed
  if ( any(ds$X3 < ds$X3[1]) ){
    ds$Datetime[ds$X3 < ds$X3[1]] <- ymd_hms((mdy(day)+1) + 
                                               seconds(ds$X3[ds$X3 < ds$X3[1]]))
  }
  
  # Merge device and data stream tables.
  alldat <- merge(dev, ds, by.x = "V2", by.y = "X2")
  names(alldat)[1:3] <- c("ID", "Device", "Device_type")
  # Add UTM zone field
  alldat$Zone <- zone
  
  # Return
  return(alldat)
}

# List of hypack files
input_files <- list.files(pattern = ".RAW", path = hypack_path, full.names = T)

# Apply function across list of input files
all_list <- lapply( input_files, FUN=extractHypack )
dat <- do.call("rbind", all_list)




#===============================================================================
# STEP 4 - PROCESS SENSOR DATA

# Split sensor data into individual dataframes, process, then merge together, so
# there is a field for each data type (e.g. depth, heading, etc.) in the final
# data frame.


#=============#
#    Depth    #
#=============#
# device type == 'EC1' device type
# primary device == depth_pref
# secondary device == depth_secondary
depth_data <- dat[dat$Device_type == "EC1" & dat$Device == depth_pref, 
                  c("X4", "Datetime")]
names(depth_data)[1] <- "Depth"
depth_data2 <- dat[dat$Device_type == "EC1" & dat$Device == depth_secondary, 
                   c("X4", "Datetime")]
names(depth_data2)[1] <- "Depth2"
# Merge depths together with datetime
depths <- merge(depth_data, depth_data2, by="Datetime", all=T)
# Assign NA to all negative values
depths$Depth[ depths$Depth < 0 ] <- NA
depths$Depth2[ depths$Depth2 < 0 ] <- NA
# Convert Depth 2 from feet to meters
# Question: This was not done in previous version, should depth_secondary be in meters? 
depths$Depth2 <- depths$Depth2 * 3.28084
# Remove duplicated
depths <- depths[!duplicated(depths$Datetime),]
# Plot, look for tight relationship between depth sensors
plot(depths$Depth, depths$Depth2)
abline(a=0, b=1, col="red")
# Use depth_secondary to fill in gaps in depth_pref where possible
depths$Depth <- ifelse( is.na(depths$Depth), depths$Depth2, depths$Depth )
depths <- depths[c("Datetime","Depth")]
# Summary
summary(depths)


#==============#
#   Position   #
#==============#

# Ships Position
# device type == 'POS' device type
# primary device == GPS_pref
ship_GPS_data <- dat[dat$Device_type == "POS" & dat$Device == GPS_pref, 
                     c("X4", "X5", "Datetime", "Zone")]
names(ship_GPS_data)[1:2] <- c("Ship_Easting","Ship_Northing")
# Remove duplicated
ship_gps <- ship_GPS_data[!duplicated(ship_GPS_data$Datetime),]
# Summary
summary(ship_gps)

# ROV position
# device type == 'POS' device type
# primary device == pos_pref
# secondary device == pos_secondary
position_data <- dat[dat$Device_type == "POS" & dat$Device == pos_pref, 
                     c("X4", "X5", "Datetime", "Zone")]
names(position_data)[1:2] <- c("Beacon_Easting","Beacon_Northing")
position_data2 <- dat[dat$Device_type == "POS" & dat$Device == pos_secondary, 
                      c("X4", "X5", "Datetime", "Zone")]
names(position_data2)[1:2] <- c("Beacon_Easting2","Beacon_Northing2")
# Sort primary by Datetime
position_data <- position_data[order(position_data$Datetime),]
# Calculate gap time in seconds between primary position records 
position_data$gaps <- c( difftime(tail(position_data$Datetime, -1), 
                                        head(position_data$Datetime, -1)), 0 )

# Merge primary, secondary and ship positions together using datetime and zone
pos_list <- list(position_data, position_data2, ship_gps)
positions <- Reduce(function(x, y) merge(x, y, by=c("Datetime","Zone"), all=TRUE), 
                    pos_list, accumulate=FALSE)
# Remove duplicated and order
positions <- positions[!duplicated(positions$Datetime),]
positions <- positions[order(positions$Datetime),]
# Plot, look for tight relationship between position sensor data
# Primary v secondary
plot(positions$Beacon_Easting, positions$Beacon_Easting2)
abline(a=0, b=1, col="red")
# Primary v ship
plot(positions$Beacon_Easting, positions$Ship_Easting, asp=1)
abline(a=0, b=1, col="red")

# Check for NA in first Beacon_Easting (signal a gap at the start in the primary)
# If there is a gap at the start, measure it in minutes
if( is.na(positions$Beacon_Easting[1]) ){
  # First Beacon_Easting position
  f <- min(which(!is.na(positions$Beacon_Easting)))
  positions$gaps[1] <- difftime(position_data$Datetime[f], 
                                position_data$Datetime[1])
}
# Fill in gap values to non-primary records
positions$gaps <- na.locf(positions$gaps, fromLast = FALSE)
# Replace primary with secondary when primary is NA for more than 60 seconds
positions$Beacon_Northing <-ifelse( positions$gaps > 60 & 
                                      is.na(positions$Beacon_Northing), 
                                     positions$Beacon_Northing2, 
                                     positions$Beacon_Northing )
positions$Beacon_Easting <- ifelse( positions$gaps > 60 & 
                                      is.na(positions$Beacon_Easting), 
                                    positions$Beacon_Easting2, 
                                    positions$Beacon_Easting )
# Then, replace remaining primary gaps over 60 with ship
positions$Beacon_Northing <-ifelse( positions$gaps > 60 & 
                                      is.na(positions$Beacon_Northing), 
                                    positions$Ship_Northing, 
                                    positions$Beacon_Northing )
positions$Beacon_Easting <- ifelse( positions$gaps > 60 & 
                                      is.na(positions$Beacon_Easting), 
                                    positions$Ship_Easting, 
                                    positions$Beacon_Easting )
# Add source field
positions$PositionSource <- "Primary"
positions$PositionSource[is.na(positions$Beacon_Easting)] <- ""
positions$PositionSource[positions$Beacon_Easting == 
                           positions$Beacon_Easting2] <- "Secondary"
positions$PositionSource[positions$Beacon_Easting == 
                           positions$Ship_Easting] <- "Ships"
# Subset
pos <- positions[c("Datetime","Zone", "PositionSource",
                   "Beacon_Easting","Beacon_Northing")]
# Summary
summary(pos)


#=============#
#   Heading   #
#=============#
# device type == 'GYR' device type
# primary device == phantom_heading_pref
# secondary device == ship_heading_pref
rov_heading_data <- dat[dat$Device_type == "GYR" & dat$Device == phantom_heading_pref, 
                        c("X4", "Datetime")]
names(rov_heading_data)[1] <- "rov_heading"
ship_heading_data <- dat[dat$Device_type == "GYR" & dat$Device == ship_heading_pref, 
                         c("X4", "Datetime")]
names(ship_heading_data)[1] <- "ship_heading"
# Merge headings together with datetime
headings <- merge(rov_heading_data, ship_heading_data, by="Datetime", all=T)
# Assign NA to all negative values
headings$rov_heading[ headings$rov_heading < 0 ] <- NA
headings$ship_heading[ headings$ship_heading < 0 ] <- NA
# Remove duplicated
headings <- headings[!duplicated(headings$Datetime),]
# Summary
summary(headings)


#=============#
#   Altitude  #
#=============#
# device type == 'DFT' device type
# primary device == altitude_pref
altitude_data <- dat[dat$Device_type == "DFT" & dat$Device == altitude_pref, 
                     c("X4", "Datetime")]
names(altitude_data)[1] <- "altitude"
# Assign NA to all negative values
altitude_data$altitude[ altitude_data$altitude < 0 ] <- NA
# Remove duplicated
altitude <- altitude_data[!duplicated(altitude_data$Datetime),]
# Summary
summary(altitude)


#=============#
#    Slant    #
#=============#
# device type == 'EC1' device type
# primary device == slant_pref
slant_data <- dat[dat$Device_type == "EC1" & dat$Device == slant_pref, 
                  c("X4", "Datetime")]
names(slant_data)[1] <- "slant_range"
# Assign NA to all negative values
slant_data$slant_range[ altitude_data$slant_range < 0 ] <- NA
# Remove duplicated
slant <- slant_data[!duplicated(slant_data$Datetime),]
# Summary
summary(slant)


#=============#
#    Speed    #
#=============#
# device type == 'HCP' device type
# primary device == speed_pref
speed_data <- dat[dat$Device_type == "HCP" & dat$Device == speed_pref, 
                  c("X4", "Datetime")]
names(speed_data)[1] <- "speed"
# Assign NA to all negative values
speed_data$speed[ speed_data$speed < 0 ] <- NA
# Remove duplicated
speed <- speed_data[!duplicated(speed_data$Datetime),]
# Summary
summary(speed)


#===============#
#   Pitch/roll  #
#===============#
# device type == 'HCP' device type
# primary device == rogue_cam_pref
cam_data <- dat[dat$Device_type == "HCP" & dat$Device == rogue_cam_pref, 
                c("X5", "X6", "Datetime")] # X4 was all zeros
# Question: How to determine which is pitch and roll?
names(cam_data)[1:2] <- c("roll","pitch")
# Remove duplicated
pitchroll <- cam_data[!duplicated(cam_data$Datetime),]
# Summary
summary(pitchroll)


#=============#
#   Combine   #
#=============#
# Merge all together based on datetime
df_list <- list(pos, depths, headings, altitude, slant, speed, pitchroll)
sdat <- Reduce(function(x, y) merge(x, y, by="Datetime", all=TRUE), 
               df_list, accumulate=FALSE)
summary(sdat)

# Sort by Datetime
sdat <- sdat[order(sdat$Datetime),]



#===============================================================================
# STEP 5 - READ IN DIVE LOG AND MERGE WITH SENSOR DATA

# Read in the start and end time from the Dive Log
dlog <- read.csv(divelog_path)

# Check for all required fields
dnames <- c("Dive_Name", "Transect_Name", "Start_UTC", "End_UTC")
if( any(!dnames %in% names(dlog)) ) {
  stop( "Missing fields in dive log: ", 
        paste(dnames[!dnames %in% names(dlog)], collapse = ", "))
} else {
  # Remove all but required fields
  dlog <- dlog[dnames]
}

# Set datetime format
dlog$Start_UTC <- mdy_hm(dlog$Start_UTC)
dlog$End_UTC <- mdy_hm(dlog$End_UTC)

# Pad out the transect start/end times by 'padtime' minutes each to ensure overlap with 
# transect annotations
dlog$Start_UTC_pad <- dlog$Start_UTC - minutes(padtime)
dlog$End_UTC_pad <- dlog$End_UTC + minutes(padtime)

# Crop padded times so there is no overlap between transects
for (i in 2:nrow(dlog)){
  if( dlog$End_UTC_pad[i-1] > dlog$Start_UTC_pad[i] ){
    # Replace end times with non-padded times
    dlog$End_UTC_pad[i-1] <- dlog$End_UTC[i-1]
    # Set start time to previous endtime 
    dlog$Start_UTC_pad[i] <- dlog$End_UTC[i-1]
  }
}

# Generate a second-by-second sequence of datetimes from the start to finish 
# of the on-transect portion of each dive. 
slog <- NULL
for(i in 1:nrow(dlog)){
  name <- dlog$Transect_Name[i]
  tmp <- data.frame(
    Transect_Name = name, 
    Datetime = seq(dlog$Start_UTC_pad[i],dlog$End_UTC_pad[i], 1)
  )
  # Bind each transect to the previous
  slog <- rbind(slog, tmp)
}
# Save for use in later processing scripts
save(slog, file=file.path(save_dir, "Dive_Times_1Hz.RData"))


# Merge the hypack processed data with the 1 Hz  dive log sequence
# Removes senor data that is not on-transect if onlyTransect == TRUE
if( onlyTransect ){
  alldat <- merge(slog, sdat, by = "Datetime", all.x=T)
} else {
  alldat <- merge(slog, sdat, by = "Datetime", all=T)
}

# Save for use in later processing scripts
save(alldat, file=file.path(save_dir, "SensorData_onTransects.RData"))


#===============================================================================
# STEP 6 - WRITE CLIPPED .CSV FILES FOR EACH DIVE

# Loop through the Dive Names in the all_data frames, write one .CSV 
# for the clipped (on transect data) for each dive.
for(j in unique(alldat$Transect_Name)){
  to_write <- filter(alldat, Transect_Name == j)
  write.csv(to_write, file = file.path(save_dir, paste0(j,".csv")), 
            quote = F, row.names = F)
}

# If onlyTransect is FALSE, then export all data as one additional CSV
if ( !onlyTransect ){
  write.csv(alldat, file = file.path(save_dir,"Alldata_OnOffTransects.csv"), 
            quote = F, row.names = F)
}



# To do
# - Only export a single csv for on-transect
# - Makes sense to do the interpolation to fill in gaps at this point before 
#   converting to lat and long with NA values (not sure why step 13 is necessary now)





# 
# ######################STEP 13 - CONVERT THE POSITION DATA TO DECIMAL DEGREES####################################
# 
# #In order to convert the data that is recorded by Hypack as UTM values, the position_all data frame needs to be converted to
# #a SpatialPointsDataFrame, which is class in the package(sp), loaded by package(rgdal) at the start of this script. 
# #SpatialPointsDataFrames cannot contain NA values, so these value must first be selected for and set aside, since some of these rows with NA
# #values are still of interest, they are re-inserted into the data when the SpatialPointsDataFrame object is converted back to a regular data.frame
# #SpatialPointsDataFrames also are restricted to a single coordinate reference system (CRS). Since different dives could conceivably by from different
# #UTM zones (i.e. different CRS), the loop below splits the position data into seperate transects and load the appropriate CRS that corresponds to the UTM
# #Zone that was set in Hypack for each particular dive.
# 
# #Empty data frames to fill with the ship and beacon values converted into decimal degree values for latitude and longitude.
# ship_geographic <- data.frame()
# main_beacon_geographic <- data.frame()
# secondary_beacon_geographic <- data.frame()
# 
# #Strip NA values, generate spatial points DFs for each transect, transform to Lat/Longs, then save as a dataframe again.
# for(i in unique(position_all$Transect_Name))
# {
#   main_beacon_no_NA <- filter(position_all, !is.na(Main_Beacon_Easting) & position_all$Transect_Name == i)
#   ship_no_NA <- filter(position_all, !is.na(Ship_Easting) & position_all$Transect_Name == i)
#   
#   main_beacon_coordinates <- main_beacon_no_NA[, c("Main_Beacon_Easting","Main_Beacon_Northing")] # UTM coordinates for the AAE wide transponder
#   ship_coordinates <- ship_no_NA[, c("Ship_Easting","Ship_Northing")] # UTM coordinates for the Ship GPS
#   
#   main_beacon_data <- main_beacon_no_NA[, c("date_time","Transect_Name","device","Gaps")] # data to keep
#   ship_data <- as.data.frame(ship_no_NA[, c("date_time")]) # data to keep
#   
#   zone_num <- unique(position_all$zone[position_all$Transect_Name == i]) #Find the unique zone number value associate with each dive number, this might include an NA value.
#   zone_num <- zone_num[!is.na(zone_num)] #Control for NA values; drop index values that may be NA, keeping only integer zone number values (8,9 or 10).
#   crs <- CRS(paste0("+proj=utm +zone=", zone_num ," +datum=WGS84")) #proj4string of coordinates.
#   
#   #Assemble the spatial data points DF.
#   
#   main_beacon_spatial <- SpatialPointsDataFrame(coords = main_beacon_coordinates, data = main_beacon_data, proj4string = crs)
#   ship_spatial <- SpatialPointsDataFrame(coords = ship_coordinates, data = ship_data, proj4string = crs)
#   
#   #Transform the UTMs coordinates into lat/longs, formatted as decimal degrees. Then turn it back into a regular DF.
#   
#   main_beacon_spatial <- spTransform(main_beacon_spatial, CRS("+proj=longlat +datum=WGS84"))
#   ship_spatial <- spTransform(ship_spatial, CRS("+proj=longlat +datum=WGS84"))
#   main_beacon_spatial <- as.data.frame(main_beacon_spatial)
#   main_beacon_spatial <- main_beacon_spatial[,c(1:3,5:6)]
#   ship_spatial <- as.data.frame(ship_spatial)
#  
#   
#   #Add rows to empty data frames initialzed before this loop.
#   main_beacon_geographic <- bind_rows(main_beacon_geographic, main_beacon_spatial)
#   ship_geographic <- bind_rows(ship_geographic, ship_spatial)
#   
#   #Check to see if a secondary beacon was used during the transect before trying to generate the SP Data Frame object, if not (length = 0), skip trying
#   #to process data from the secondary beacon. 
#   current_transect <- filter(position_all, Transect_Name == i)
#   if(length(which(!is.na(current_transect$Secondary_Beacon_Easting))) != 0)
#   {
#     secondary_beacon_no_NA <- filter(position_all, !is.na(Secondary_Beacon_Easting) & position_all$Transect_Name == i)
#     secondary_beacon_coordinates <- secondary_beacon_no_NA[, c("Secondary_Beacon_Easting","Secondary_Beacon_Northing")] # UTM coordinates for the AAE wide transponder
#     secondary_beacon_data <- secondary_beacon_no_NA[, c("date_time")] # data to keep
#     secondary_beacon_spatial <- SpatialPointsDataFrame(coords = secondary_beacon_coordinates, data = secondary_beacon_data, proj4string = crs)
#     secondary_beacon_spatial <- spTransform(secondary_beacon_spatial, CRS("+proj=longlat +datum=WGS84"))
#     secondary_beacon_spatial <- as.data.frame(secondary_beacon_spatial)
#     secondary_beacon_geographic <- bind_rows(secondary_beacon_geographic, secondary_beacon_spatial)
#   }
#   
# }
# 
# #Rename the columns.
# 
# names(main_beacon_geographic) <- c("date_time","Transect_Name","device","Beacon_Long","Beacon_Lat")
# names(ship_geographic) <- c("date_time","Ship_Long","Ship_Lat")
# 
# 
# #Check to see if secondary beacon has any data; secondary beacon may not have been used. 
# #If that is the case, fill the entries in the beacon dataframe containing Lat/Long values with 'NULL' entries.
# if(exists("secondary_beacon_spatial")) 
# {
#   names(secondary_beacon_geographic) <- c("date_time","Transect_Name","device","Beacon_Long","Beacon_Lat")
# } else {
#   secondary_beacon_geographic <- full_seq
#   secondary_beacon_geographic$Secondary_Beacon_Long <- rep("NULL",length(full_seq$date_time))
#   secondary_beacon_geographic$Secondary_Beacon_Lat <- rep("NULL", length(full_seq$date_time))
#   secondary_beacon_geographic <- secondary_beacon_geographic[, c(2:4)]
# }  
# 
# #Slot the Long/Lat positions back in the position_all DF, and remove the northing and eastings
# 
# position_all <- left_join(position_all, main_beacon_geographic, by = "date_time")
# position_all$Main_Beacon_Easting <- position_all$Main_Beacon_Long
# position_all$Main_Beacon_Northing <- position_all$Main_Beacon_Lat
# position_all <- left_join(position_all, secondary_beacon_geographic, by = "date_time")
# position_all <- left_join(position_all, ship_geographic, by = "date_time")
# 
# 
# position_all <- position_all[,c(1:3,12:17,8)]
# names(position_all) <- c("date_time","Transect_Name","device","Main_Beacon_Long","Main_Beacon_Lat",
#                          "Secondary_Beacon_Long","Secondary_Beacon_Lat","Ship_Long","Ship_Lat","Gaps")
# 
# #Round all Lat/Long values to the 5th decimal, equivalent to ~ 1m precision.
# 
# position_all$Main_Beacon_Lat <- round(position_all$Main_Beacon_Lat, digits = 5)
# position_all$Main_Beacon_Long <- round(position_all$Main_Beacon_Long, digits = 5)
# if(exists("secondary_beacon_spatial")) #As above, secondary beacon may not have been used. Skip next step if so.
#   {
#   position_all$Secondary_Beacon_Lat <- round(position_all$Secondary_Beacon_Lat, digits = 5)
#   position_all$Secondary_Beacon_Long <- round(position_all$Secondary_Beacon_Long, digits = 5)
#   }
# position_all$Ship_Long <- round(position_all$Ship_Long, digits = 5)
# position_all$Ship_Lat <- round(position_all$Ship_Lat, digits = 5)
# 
#
