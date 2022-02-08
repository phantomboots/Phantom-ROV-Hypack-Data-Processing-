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
# Specifically, data is trimmed to transect start/end times based on divelog. 
#
# The script will search for the preferred data source first (i.e. CTD depth, 
# rather than onboard depth sensor) and will fall back to extracting the 
# secondary source as required, while also writing a data flag.
#
# Script Author: Ben Snow, adapted by Jessica Nephin
# Script Date: Aug 27, 2019, adapted in Jan 2022
# R Version: 3.5.1, version 4.0.2


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
#          - merges padded start/end times with processed sensor data
#          - option to save off transect data too
#          - attempted to make code more explicit, removed use of column order
#          - also removed all get() functions
#          - exports all data together instead of by transect
#          - Saves data processing log with warnings, errors and data summaries
#          - Using terra instead of rgdal, accepts NA values
#          - Added before or after dive phase field, used before and after 
#            of descent or ascent in case there are multiple transects in a dive
################################################################################


#===============================================================================
# Packages and session options

# Check for the presence of packages shown below, install if needed
packages <- c("lubridate", "purrr", "dplyr", "zoo", "future.apply", "terra")
new_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
if(length(new_packages)) install.packages(new_packages)

# Load required packages
lapply(packages, require, character.only = TRUE)

# Set the number of sig figs high, to facilitate inspection of UTM data records
options(digits = 12)

# Set multisession so future_lapply runs in parallel
plan(multisession)


#===============================================================================
# Select options

# Choose the number of minutes for padding start and end transect times
padtime <- 2

# Should hypack sensor data be clipped to on-transect times only?
# TRUE for on-transect only
# FALSE for exporting additional off-transect sensor data too
# Question: How to incorporate this into later processing code?
onlyTransect <- TRUE


#===============================================================================
# STEP 1 - SET PATHS AND MAKE EXPORT DIRECTORY

# Set working directory
# Use getwd() if you are already in the right directory
# The project folder and needs to be in the working directory
wdir <- getwd() 

# Enter Project folder
project_folder <- "Pac2021-054_phantom"

# Directory where Hypack .RAW files are stored
hypack_path <- file.path(wdir, project_folder, "Data/Raw")

# Directory where dive log csv file is stored
divelog_path <- file.path(wdir, project_folder, "Data/Dive_Logs/Dive_Log.csv")

# Create directory for saving .CSV files
save_dir <- file.path(wdir, project_folder, "Data/1.Hypack_Processed_Data")
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
# STEP 3 - START LOG FILE

# Sink output to file
rout <- file( file.path(wdir,project_folder,"Data","1.Hypack_Data_Parser.log"), 
              open="wt" )
sink( rout, split = TRUE ) # display output on console and send to log file
sink( rout, type = "message" ) # send warnings to log file
options(warn=1) # print warnings as they occur

# Start the timer
sTime <- Sys.time( )

# Messages
message("Parsing ", project_folder, " project data on ", Sys.Date(), "\n")
message("Padding start and end of transects by ", padtime, " minutes\n")
message("Only export on-transect data? ", onlyTransect, "\n")

# Print device types and names
cat("Device types:\n")
cat(paste0(device_types, collapse = ", "), "\n")
cat("\nDevice names:\n")
cat("ship_heading_pref = '", ship_heading_pref, "'\n", sep="")
cat("GPS_pref = '", GPS_pref, "'\n", sep="")
cat("depth_pref = '", depth_pref, "'\n", sep="")
cat("pos_pref = '", pos_pref, "'\n", sep="")
cat("phantom_heading_pref = '", phantom_heading_pref, "'\n", sep="")
cat("speed_pref = '", speed_pref, "'\n", sep="")
cat("altitude_pref = '", altitude_pref, "'\n", sep="")
cat("slant_pref = '", slant_pref, "'\n", sep="")
cat("rogue_cam_pref = '", rogue_cam_pref, "'\n", sep="")
cat("pos_secondary = '", pos_secondary, "'\n", sep="")
cat("depth_secondary = '", depth_secondary, "'\n\n", sep="")



#===============================================================================
# STEP 4 - READ IN RAW DATA

# Lists Hypack files, apply function to extract data on list of files, combine
# extracted data into a single dataframe for entire cruise

# Function to extract data from hypack file
extractHypack <- function( hfile ){
  # Packages
  packages <- c("lubridate","dplyr","purrr")
  lapply(packages, require, character.only = TRUE)
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
  # Merge device and data stream tables
  alldat <- merge(dev, ds, by.x = "V2", by.y = "X2")
  names(alldat)[1:3] <- c("ID", "Device", "Device_type")
  # Add UTM zone field
  alldat$Zone <- zone
  # Return
  return(alldat)
}

# List of hypack files
hypack_files <- list.files(pattern = ".RAW", path = hypack_path, full.names = T)
# Apply function across list of input files
all_list <- future_lapply(hypack_files, FUN=extractHypack, future.seed=42)
dat <- do.call("rbind", all_list)



#===============================================================================
# STEP 5 - PROCESS SENSOR DATA

# Split sensor data into individual dataframes, process, then merge together, so
# there is a field for each data type (e.g. depth, heading, etc.) in the final
# data frame.


#=============#
#    Depth    #
#=============#
# Message
message("\nExtracting depth")
# device type == 'EC1' device type
# primary device == depth_pref
# secondary device == depth_secondary
depth_data <- dat[dat$Device_type == "EC1" & dat$Device == depth_pref, 
                  c("X4", "Datetime")]
names(depth_data)[1] <- "Depth_m"
depth_data2 <- dat[dat$Device_type == "EC1" & dat$Device == depth_secondary, 
                   c("X4", "Datetime")]
names(depth_data2)[1] <- "Depth2"
# Merge depths together with datetime
depths <- merge(depth_data, depth_data2, by="Datetime", all=T)
# Assign NA to all negative values
depths$Depth_m[ depths$Depth_m < 0 ] <- NA
depths$Depth2[ depths$Depth2 < 0 ] <- NA
# Convert Depth 2 from feet to meters
# Question: This was not done in previous version, should depth_secondary be in meters? 
depths$Depth2 <- depths$Depth2 * 3.28084
# Remove duplicated
depths <- depths[!duplicated(depths$Datetime),]
# Plot, look for tight relationship between depth sensors
plot(depths$Depth_m, depths$Depth2)
abline(a=0, b=1, col="red")
# Use depth_secondary to fill in gaps in depth_pref where possible
depths$Depth <- ifelse( is.na(depths$Depth_m), depths$Depth2, depths$Depth_m )
# Depth source
depths$Depth_Source <- paste0("Hypack_", depth_pref)
depths$Depth_Source[depths$Depth_m == depths$Depth2] <- 
  paste0("Hypack_", depth_secondary)
table(depths$Depth_Source)
# Subset
depths <- depths[c("Datetime","Depth_m", "Depth_Source")]
# Summary
print(summary(depths))


#==============#
#   Position   #
#==============#
# Message
message("\nExtracting the ROV and ship position")
# Ships Position
# device type == 'POS' device type
# primary device == GPS_pref
ship_GPS_data <- dat[dat$Device_type == "POS" & dat$Device == GPS_pref, 
                     c("X4", "X5", "Datetime", "Zone")]
names(ship_GPS_data)[1:2] <- c("Ship_Easting","Ship_Northing")
# Remove duplicated
ship_gps <- ship_GPS_data[!duplicated(ship_GPS_data$Datetime),]

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
position_data$Beacon_Gaps <- as.numeric(
  c( difftime(tail(position_data$Datetime, -1), 
              head(position_data$Datetime, -1)), 0 ))

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

# Check for NA in first Beacon_Easting (a gap at the start in the primary)
# If there is a gap at the start, measure it in minutes
if( is.na(positions$Beacon_Easting[1]) ){
  # First Beacon_Easting position
  f <- min(which(!is.na(positions$Beacon_Easting)))
  positions$Beacon_Gaps[1] <- as.numeric(difftime(positions$Datetime[f], 
                                                 positions$Datetime[1]))
}
# Expand gap values
positions$Beacon_Gaps <- na.locf(positions$Beacon_Gaps, fromLast = FALSE)
# Warn: filling in gap values with non-primary data sources
if( any(positions$Beacon_Gaps > 60 & is.na(positions$Beacon_Northing)) ){
  warning("Gaps greater than 60 seconds exist in the primary position sensor.\n", 
          "Attempting to fill using secondary source then ship position.")
}
# Replace primary with secondary when primary is NA for more than 60 seconds
positions$Beacon_Northing <-ifelse( positions$Beacon_Gaps > 60 & 
                                      is.na(positions$Beacon_Northing), 
                                     positions$Beacon_Northing2, 
                                     positions$Beacon_Northing )
positions$Beacon_Easting <- ifelse( positions$Beacon_Gaps > 60 & 
                                      is.na(positions$Beacon_Easting), 
                                    positions$Beacon_Easting2, 
                                    positions$Beacon_Easting )
# Then, replace remaining primary gaps over 60 with ship
positions$Beacon_Northing <-ifelse( positions$Beacon_Gaps > 60 & 
                                      is.na(positions$Beacon_Northing), 
                                    positions$Ship_Northing, 
                                    positions$Beacon_Northing )
positions$Beacon_Easting <- ifelse( positions$Beacon_Gaps > 60 & 
                                      is.na(positions$Beacon_Easting), 
                                    positions$Ship_Easting, 
                                    positions$Beacon_Easting )
# Add source field
positions$Beacon_Source <- "Primary"
positions$Beacon_Source[is.na(positions$Beacon_Easting)] <- NA
positions$Beacon_Source[positions$Beacon_Easting == 
                         positions$Beacon_Easting2] <- "Secondary"
positions$Beacon_Source[positions$Beacon_Easting == 
                         positions$Ship_Easting] <- "Ship"

# Convert UTM to lat/lon
# Empty lon and lat columns to fill
positions$Beacon_Longitude <- NA
positions$Beacon_Latitude <- NA
positions$Ship_Longitude <- NA
positions$Ship_Latitude <- NA
# Loop through UTM zones
for (z in unique(positions$Zone) ){
  # Subset by zone
  tmp <- filter(positions, Zone == z)
  # Convert to vector layer
  vb <- terra::vect(tmp, geom=c("Beacon_Easting", "Beacon_Northing"),
                    crs=paste0("+proj=utm +zone=", z," +datum=WGS84 +units=m"))
  vs <- terra::vect(tmp, geom=c("Ship_Easting", "Ship_Northing"),
                    crs=paste0("+proj=utm +zone=", z," +datum=WGS84 +units=m"))
  # Project to lat/lon
  pb <- terra::project(vb, "+proj=longlat +datum=WGS84")
  ps <- terra::project(vs, "+proj=longlat +datum=WGS84")
  # Replace with lon and lat values for zone == z rows
  positions[positions$Zone == z,"Beacon_Longitude"] <- geom(pb)[, "x"]
  positions[positions$Zone == z,"Beacon_Latitude"] <- geom(pb)[, "y"]
  positions[positions$Zone == z,"Ship_Longitude"] <- geom(ps)[, "x"]
  positions[positions$Zone == z,"Ship_Latitude"] <- geom(ps)[, "y"]
}
# Replace NaN with NA
positions[is.na(positions)] <- NA
# Subset
pos <- positions[c("Datetime", "Beacon_Source", "Beacon_Gaps",
                   "Beacon_Longitude","Beacon_Latitude", 
                   "Ship_Longitude","Ship_Latitude")]
# Summary
print(summary(pos))


#=============#
#   Heading   #
#=============#
# Message
message("\nExtracting the ROV and ship heading")
# device type == 'GYR' device type
# primary device == phantom_heading_pref
# secondary device == ship_heading_pref
rov_heading_data <- dat[dat$Device_type == "GYR" & dat$Device == phantom_heading_pref, 
                        c("X4", "Datetime")]
names(rov_heading_data)[1] <- "ROV_heading"
ship_heading_data <- dat[dat$Device_type == "GYR" & dat$Device == ship_heading_pref, 
                         c("X4", "Datetime")]
names(ship_heading_data)[1] <- "Ship_heading"
# Merge headings together with datetime
headings <- merge(rov_heading_data, ship_heading_data, by="Datetime", all=T)
# Assign NA to all negative values
headings$ROV_heading[ headings$ROV_heading < 0 ] <- NA
headings$Ship_heading[ headings$Ship_heading < 0 ] <- NA
# Remove duplicated
headings <- headings[!duplicated(headings$Datetime),]
# Summary
print(summary(headings))


#=============#
#   Altitude  #
#=============#
# Message
message("\nExtracting altitude")
# device type == 'DFT' device type
# primary device == altitude_pref
altitude_data <- dat[dat$Device_type == "DFT" & dat$Device == altitude_pref, 
                     c("X4", "Datetime")]
names(altitude_data)[1] <- "Altitude_m"
# Assign NA to all negative values
altitude_data$Altitude_m[ altitude_data$Altitude_m < 0 ] <- NA
# Remove duplicated
altitude <- altitude_data[!duplicated(altitude_data$Datetime),]
# Summary
print(summary(altitude))


#=============#
#    Slant    #
#=============#
# Message
message("\nExtracting slant range")
# device type == 'EC1' device type
# primary device == slant_pref
slant_data <- dat[dat$Device_type == "EC1" & dat$Device == slant_pref, 
                  c("X4", "Datetime")]
names(slant_data)[1] <- "Slant_range_m"
# Assign NA to all negative values
slant_data$Slant_range_m[ altitude_data$Slant_range_m < 0 ] <- NA
# Remove duplicated
slant <- slant_data[!duplicated(slant_data$Datetime),]
# Summary
print(summary(slant))


#=============#
#    Speed    #
#=============#
# Message
message("\nExtracting ROV speed")
# device type == 'HCP' device type
# primary device == speed_pref
speed_data <- dat[dat$Device_type == "HCP" & dat$Device == speed_pref, 
                  c("X4", "Datetime")]
names(speed_data)[1] <- "Speed_kts"
# Assign NA to all negative values
speed_data$Speed_kts[ speed_data$Speed_kts < 0 ] <- NA
# Remove duplicated
speed <- speed_data[!duplicated(speed_data$Datetime),]
# Check for high speeds
hist(speed$Speed_kts, breaks=30)
# Summary
print(summary(speed))


#===============#
#   Pitch/roll  #
#===============#
# Message
message("\nExtracting Rogue pitch and roll")
# device type == 'HCP' device type
# primary device == rogue_cam_pref
cam_data <- dat[dat$Device_type == "HCP" & dat$Device == rogue_cam_pref, 
                c("X5", "X6", "Datetime")] # X4 was all zeros
# Question: Confirm which is pitch and roll?
names(cam_data)[1:2] <- c("Rogue_roll","Rogue_pitch")
# Remove duplicated
pitchroll <- cam_data[!duplicated(cam_data$Datetime),]
# Summary
print(summary(pitchroll))


#=============#
#   Combine   #
#=============#
# Message
message("\nCombining all sensor data")
# Merge all together based on datetime
df_list <- list(pos, depths, headings, altitude, slant, speed, pitchroll)
sdat <- Reduce(function(x, y) merge(x, y, by="Datetime", all=TRUE), 
               df_list, accumulate=FALSE)

# Sort by Datetime
sdat <- sdat[order(sdat$Datetime),]



#===============================================================================
# STEP 6 - EXTRACT PLANNED LINES FROM RAW HYPACK FILES

# Planned line file coordinates are logged in each Hypack .RAW file, and have 
# the line designations 'PTS' and 'LNN'. The exact line number will vary 
# depending on how many devices are present in the Hypack Hardware file.

# Function to extract planned lines data from hypack file
extractPlannedLines <- function( hfile ){
  # Read in lines from hypack files
  hlines <- readLines(hfile)
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
  # Extract planned lines info
  plines <- hlines[grepl("PTS", hlines)]
  PlannedStart <- as.numeric(strsplit(plines[1], " ")[[1]][-1])
  PlannedEnd <- as.numeric(strsplit(plines[2], " ")[[1]][-1])
  lname <- sub("LNN", "", hlines[grepl("LNN", hlines)])
  pldf <- data.frame(lname, t(PlannedStart), t(PlannedEnd), zone)
  names(pldf) <- c("Name", "Start_x", "Start_y", "End_x", "End_y", "Zone")
  # Return
  return(pldf)
}

# Apply function across list of input files
pl_list <- future_lapply(hypack_files, FUN=extractPlannedLines, future.seed=42)
pl <- do.call("rbind", pl_list)

# Remove any duplicates
pl <- pl[!duplicated(pl),]
#Remove any lines that NA values for their names
pl <- pl[!is.na(pl$Name),]

# Convert UTM to lat/lon
# Empty lon and lat columns to fill
pl$Start_Longitude <- NA
pl$Start_Latitude <- NA
pl$End_Longitude <- NA
pl$End_Latitude <- NA
# Loop through UTM zones
for (z in unique(pl$Zone) ){
  # Subset by zone
  tmp <- filter(pl, Zone == z)
  # Convert to vector layer
  sv <- terra::vect(tmp, geom=c("Start_x", "Start_y"),
                    crs=paste0("+proj=utm +zone=", z," +datum=WGS84 +units=m"))
  ev <- terra::vect(tmp, geom=c("End_x", "End_y"),
                    crs=paste0("+proj=utm +zone=", z," +datum=WGS84 +units=m"))
  # Project to lat/lon
  sll <- terra::project(sv, "+proj=longlat +datum=WGS84")
  ell <- terra::project(ev, "+proj=longlat +datum=WGS84")
  # Replace with lon and lat values for zone == z rows
  pl[pl$Zone == z,"Start_Longitude"] <- geom(sll)[, "x"]
  pl[pl$Zone == z,"Start_Latitude"] <- geom(sll)[, "y"]
  pl[pl$Zone == z,"End_Longitude"] <- geom(ell)[, "x"]
  pl[pl$Zone == z,"End_Latitude"] <- geom(ell)[, "y"]
}
# Replace NaN with NA
pl[is.na(pl)] <- NA
# Subset
plannedTransects <- pl[c("Name", "Start_Longitude", "Start_Latitude",
                         "End_Longitude","End_Latitude")]

# Message
message( "\n", nrow(pl), " planned transects found in Hypack for ", 
         project_folder, "\n")


#===============================================================================
# STEP 7 - READ IN DIVE LOG AND MERGE WITH SENSOR DATA

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
    Datetime = seq(dlog$Start_UTC_pad[i],dlog$End_UTC_pad[i], 1),
    Dive_Phase = "On_transect"
  )
  # Bind each transect to the previous
  slog <- rbind(slog, tmp)
}
# Change dive phase to before or after if before start or after end time
for (n in unique(slog$Transect_Name)){
  # Rows for transect n
  ind <- which(slog$Transect_Name == n)
  # Before start
  st <- slog$Datetime[slog$Transect_Name == n] < 
    dlog$Start_UTC[dlog$Transect_Name == n]
  slog$Dive_Phase[ind[st]] <- "Before_transect"
  # After end
  en <- slog$Datetime[slog$Transect_Name == n] > 
    dlog$End_UTC[dlog$Transect_Name == n]
  slog$Dive_Phase[ind[en]] <- "After_transect"
}

# Save for use in later processing scripts
save(slog, file=file.path(save_dir, "Dive_Times_1Hz.RData"))

# Merge the hypack processed data with the 1 Hz  dive log sequence
# Removes senor data that is not on-transect if onlyTransect == TRUE
ondat <- merge(slog, sdat, by = "Datetime", all.x=T)
if( !onlyTransect ) {
  offdat <- merge(slog, sdat, by = "Datetime", all=T)
  offdat <- offdat[is.na(offdat$Transect_Name),]
}
# Message
cat("\n", length(unique(ondat$Transect_Name)), "transects: \n")
cat(paste0(unique(ondat$Transect_Name), collapse = "\n"), "\n")
# Check
# Map each transect
for (i in unique(ondat$Transect_Name)){
  tmp <- ondat[ondat$Transect_Name == i,]
  plot(tmp$Ship_Longitude,tmp$Ship_Latitude, asp=1, main=i, pch=16, cex=.5)
  points(tmp$Beacon_Longitude,tmp$Beacon_Latitude, col="blue", pch=16, cex=.5)
}

# Summary
message("\nSummary of expanded 1Hz on-transect hypack data", "\n")
print(summary(ondat))


#===============================================================================
# STEP 8 - EXPORT DATA

# Planned transects
# Save as RData for use in later processing scripts
save(plannedTransects, file=file.path(save_dir,"Hypack_PlannedTransects.RData"))

# Transects only
# Save as RData for use in later processing scripts
save(ondat, file=file.path(save_dir, "HypackData_onTransect.RData"))
# Save as CSV
write.csv(ondat, file = file.path(save_dir, "HypackData_onTransect.csv"), 
          quote = F, row.names = F)

# Export off transect data as well if onlyTransect is FALSE
if( !onlyTransect ) {
  # Save as RData for use in later processing scripts
  save(offdat, file=file.path(save_dir, "HypackData_offTransect.RData"))
  # Save as CSV
  write.csv(offdat, file = file.path(save_dir, "HypackData_offTransect.csv"), 
            quote = F, row.names = F)
}


#===============================================================================
# Print end of file message and elapsed time
cat( "\nFinished: ", sep="" )
print( Sys.time( ) - sTime )

# Stop sinking output
sink( type = "message" )
sink( )
closeAllConnections()




