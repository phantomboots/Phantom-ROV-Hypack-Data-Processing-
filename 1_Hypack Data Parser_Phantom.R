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
# Jan 17, 2022: Started develop branch, JN to review and edit as needed
################################################################################



#===============================================================================
# Set up

# Check for the presence of packages shown below, install if needed
packages <- c("lubridate", "sp", "readxl", "readr",
              "dplyr", "stringr", "rgdal", "zoo")
new_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
if(length(new_packages)) install.packages(new_packages)

# Set the number of sig figs high, to facilitate inspection of UTM data records
options(digits = 12)

# Load required packages
lapply(packages, require, character.only = TRUE)



#===============================================================================
# STEP 1 - SET PATHS AND MAKE EXPORT DIRECTORY

# Enter Project folder
# All files should be found within a 'Data' folder within the project folder
project_folder <- "Test"

# Assign path to Hypack .RAW files
# Path must start from your working directory, check with getwd(), or full paths
hypack_path <- paste0(project_folder, "/Data/Raw")

# Assign path to dive log csv file
# Path must start from your working directory, check with getwd(), or full paths
divelog_path <- paste0(project_folder,"/Data/Dive_Logs/Dive_Log.csv")

# Create directory for saving both clipped and unclipped .CSV files
save_dir <- paste0(project_folder, "/Data/Initial_Processed_Data")
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
  dat <- merge(dev, ds, by.x = "V2", by.y = "X2")
  names(dat)[1:3] <- c("ID", "Device", "Device_type")
  # Add UTM zone field
  dat$Zone <- zone
  
  # Return
  return(dat)
}

# List of hypack files
input_files <- list.files(pattern = ".RAW", path = hypack_path, full.names = T)

# Apply function across list of input files
all_list <- lapply( input_files, FUN=extractHypack )
all_input <- do.call("rbind", all_list)




#===============================================================================
# STEP 4 - MOVE SENSOR DATA TO INDIVIDUAL FIELDS


# Split sensor data into individual dataframes

# Depth 
# device type == 'EC1' device type
# primary device == depth_pref
# secondary device == depth_secondary
depth_data <- dat[dat$Device_type == "EC1" & dat$Device == depth_pref, 
                  c("X4", "Datetime")]
names(depth_data)[1] <- "Depth"
depth_data2 <- dat[dat$Device_type == "EC1" & dat$Device == depth_secondary, 
                   c("X4", "Datetime")]
names(depth_data2)[1] <- "Depth2"

# Heading
# device type == 'GYR' device type
# primary device == phantom_heading_pref
# secondary device == ship_heading_pref
rov_heading_data <- dat[dat$Device_type == "GYR" & dat$Device == phantom_heading_pref, 
                        c("X4", "Datetime")]
names(rov_heading_data)[1] <- "rov_heading"
ship_heading_data <- dat[dat$Device_type == "GYR" & dat$Device == ship_heading_pref, 
                         c("X4", "Datetime")]
names(ship_heading_data)[1] <- "ship_heading"

# Altitude
# device type == 'DFT' device type
# primary device == altitude_pref
altitude_data <- dat[dat$Device_type == "DFT" & dat$Device == altitude_pref, 
                     c("X4", "Datetime")]
names(altitude_data)[1] <- "altitude"

# Slant range
# device type == 'EC1' device type
# primary device == slant_pref
slant_data <- dat[dat$Device_type == "EC1" & dat$Device == slant_pref, 
                  c("X4", "Datetime")]
names(slant_data)[1] <- "slant_range"

# Speed
# device type == 'HCP' device type
# primary device == speed_pref
speed_data <- dat[dat$Device_type == "HCP" & dat$Device == speed_pref, 
                  c("X4", "Datetime")]
names(speed_data)[1] <- "speed"

# Position
# device type == 'POS' device type
# primary device == pos_pref
# secondary device == pos_secondary
position_data <- dat[dat$Device_type == "POS" & dat$Device == pos_pref, 
                     c("X4", "X5", "Datetime", "Zone")]
position_data2 <- dat[dat$Device_type == "POS" & dat$Device == pos_secondary, 
                      c("X4", "X5", "Datetime", "Zone")]
names(position_data)[1:2] <- c("Beacon_Easting","Beacon_Northing")
names(position_data2)[1:2] <- c("Beacon_Easting","Beacon_Northing")

# Ship GPS
# device type == 'POS' device type
# primary device == GPS_pref
ship_GPS_data <- dat[dat$Device_type == "POS" & dat$Device == GPS_pref, 
                     c("X4", "X5", "Datetime", "Zone")]
names(ship_GPS_data)[1:2] <- c("Ship_Easting","Ship_Northing")

# Camera pitch and roll
# device type == 'HCP' device type
# primary device == rogue_cam_pref
cam_data <- dat[dat$Device_type == "HCP" & dat$Device == rogue_cam_pref, 
                  c("X5", "X6", "Datetime")] # X4 was all zeros
# Question: How to determine which is pitch and roll?
names(cam_data)[1:2] <- c("roll","pitch")

# Merge all together based on datetime

# Make plots to check the similarity of primary and secondary data streams




#===============================================================================
# STEP 5 - READ IN DIVE LOG AND TRIM THE FILE TO TRANSECT START AND END TIMES

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

# Pad out the transect start/end times by 2 minutes each to ensure overlap with 
# transect annotations
dlog$Start_UTC_pad <- dlog$Start_UTC - minutes(2)
dlog$End_UTC_pad <- dlog$End_UTC + minutes(2)

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
save(slog, file=file.path(project_folder, "Data", "Dive_Times_1Hz.RData"))

# Merge the hypack processed data with the 1 Hz  dive log sequence
#dives_full <- merge(slog, all_input, by = "Datetime", all.x=T)

# Note - don't think the tables should be merged at this time
# would be better to cast the all_input data into the sensor types first





#===============================================================================
# Extract depth


depth_data <- depth_data[, c("date_time","Transect_Name","device","X4")]
names(depth_data) <- c("date_time","Transect_Name","device","Depth_m")
depth_data2 <- depth_data2[, c("date_time","Transect_Name","device","X4")]
names(depth_data2) <- c("date_time","Transect_Name","device","Depth_m")

#Where its available, insert the depth from the primary depth data source (the CTD). If not, 
#label these rows as NA. Remove duplicates, just to be safe.

depth_all <- left_join(full_seq, depth_data, by = "date_time")
depth_all <- depth_all[!duplicated(depth_all$date_time),]
depth_all <- depth_all[, c(2:5)] #Keep only the relevant columns

#Insert the secondary depth data source in a seperate column, adjacent to the primary depth source.

depth_all <- left_join(depth_all, depth_data2, by = "date_time")
depth_all <- depth_all[!duplicated(depth_all$date_time),]
depth_all <- depth_all[, c(1,3:7)] #Keep only the relevant columns

#Loop through the elements of depth_all, where the there are no depth records from the CTD, fill in with depth records from the Deep Ocean Engineering depth sensor.
#if there are no records from either devices, the depth_all value will remains as NA.

for(k in 1:length(depth_all$date_time))
{
  if(is.na(depth_all$Depth_m.x[k]))
  {depth_all$Depth_m.x[k] <- depth_all$Depth_m.y[k]
  depth_all$device.x[k] <- depth_all$device.y[k]
  }
}

#Keep only required columns, and rename them.

depth_all <- depth_all[,c(1,4,2,3)]
names(depth_all) <- c("date_time","Dive_Name","device","Depth_m")


################################STEP 6 - EXTRACT HEADING DATA###################################################

#Select rows in the first column (X1) that have a 'GYR' identifier. These should heading values from 
#the ship's GPS source, as well as the onboard compass. 

phantom_heading_data <- filter(dives_full, X2 == "GYR" & device == phantom_heading_pref) 
ship_heading_data <- filter(dives_full, X2 == "GYR" & device == ship_heading_pref)

#First, get the BOOTS heading data.
phantom_heading_data <- left_join(full_seq, phantom_heading_data, by = "date_time")
phantom_heading_data <- phantom_heading_data[!duplicated(phantom_heading_data$date_time),]
phantom_heading_data <- phantom_heading_data[, c(2,1,10,7)]
names(phantom_heading_data) <- c("date_time","Dive_Name","device","Phantom_heading")

#Next, get the ship heading data
ship_heading_data <- left_join(full_seq, ship_heading_data, by = "date_time")
ship_heading_data <- ship_heading_data[!duplicated(ship_heading_data$date_time),]
ship_heading_data <- ship_heading_data[, c(2,1,10,7)]
names(ship_heading_data) <- c("date_time","Dive_Name","device","Ship_heading")


#############################STEP 7 - SEARCH FOR ALTITUDE, EXTRACT IT IF ITS PRESENT############################

#The Phantom's altitude is read in as 'draft' data source, and is output by the ROWETech DVL. Select altitude device ID (DFT), then
#select the prefered altitude device. No substitute device, so those rows that are missing data will retain NA values.

if(which(dives_full$X2 == "DFT") != 0) #Check to see if any altitude data is present, before proceeding further.
{
altitude_data <- filter(dives_full, X2 == "DFT" & device == altitude_pref)
altitude_data <- left_join(full_seq, altitude_data, by = "date_time")
altitude_data <- altitude_data[!duplicated(altitude_data$date_time),]
altitude_data <- altitude_data[, c(2,1,10,7)]
names(altitude_data) <- c("date_time","Transect_Name","device","altitude_m")
}

#If the altitude is 0 m, this indicates an out of range reading. Substitute in -9999 for these cases.
altitude_data$altitude_m[altitude_data$altitude_m == 0] <- -9999

#####################################STEP 8 - EXTRACT MINIZEUS SLANT RANGE######################################

#Phantom slant range altitude is read in as a depth data source, through the Tritech Altimeter. Select depth device ID (EC1), then
#select the prefered slant_range device. No substitute device, so those rows that are missing data will retain NA values.
#Remove values of 0 and 9.99, which are errors or out of range.


slant_data <- filter(dives_full, X2 == "EC1" & device == slant_pref)
slant_data <- filter(slant_data, X4 > 0 & X4 < 9.99) 
slant_data <- left_join(full_seq, slant_data, by = "date_time")
slant_data <- slant_data[!duplicated(slant_data$date_time),]
slant_data <- slant_data[, c(2,1,10,7)]
names(slant_data) <- c("date_time","Transect_Name","device","slant_range_m")


###################STEP 9 - SEARCH FOR SPEED DATA; EXTRACT IT IF IT'S PRESENT################################

#Search for rows in the first column (X1) that have a 'HCP' identifier. These are heave, pitch and roll values
#Only interested in the heave values have been used as place holders for speed, since Hypack does not have a
#speed variable. This can include heave from a DVL, as well a from onboard IMU devices (i.e. Cyclops HPR sensor).
#Here, we are extracting only the speed data from the ROWETECH DVL.

if(which(dives_full$X2 == "HCP") != 0)
{
  speed_data <- filter(dives_full, X2 == "HCP" & device == speed_pref)
  speed_data <- filter(speed_data, X4 > 0 & X4 < 9.99) 
  speed_data <- left_join(full_seq, speed_data, by = "date_time")
  speed_data <- speed_data[!duplicated(speed_data$date_time),]
  speed_data <- speed_data[, c(2,1,3,10,7)]
  names(speed_data) <- c("date_time","Transect_Name","device","speed_kts")
}

#If the speed is -99.9999 m or 0, this indicates an out of range reading. Substitute in -9999 for these cases.
speed_data$speed_kts[slant_data$speed_kts == -99.999] <- -9999
speed_data$speed_kts[slant_data$speed_kts == 0] <- -9999

################################STEP 10 - EXTRACT POSITION DATA FOR BEACONS#######################################

#Select rows in the first column (X1) that have a 'POS' identifier. Keep only the preffered and secondary POS devices
#If the prefferd source is unavailable, use the secondary source to fill larger gaps. If there is a gap of only 1 or 2 seconds
#ignore it. Focus on gaps larger then 60 secs; fill these in with the secondary position source (secondary Phantom beacon) 
#where possible. If  there is no secondary position source, leave as NA and deal with this in further post-processings steps.

position_data <- filter(dives_full, X2 == "POS" & device == pos_pref)
position_data2 <- filter(dives_full, X2 == "POS" & device == pos_secondary)


position_data <- position_data[, c(2,1,9,6,7,10)]
names(position_data) <- c("date_time","Transect_Name", "device","Beacon_Easting","Beacon_Northing","zone")
position_data2 <- position_data2[, c(2,1,9,6,7,10)]
names(position_data2) <- c("date_time","Transect_Name","device","Beacon_Easting","Beacon_Northing","zone")

#Where its availalble, insert the position from the primary position data source (the CTD). If not, 
#label these rows as NA. Remove duplicated just to be safe.

position_all <- left_join(full_seq, position_data, by = "date_time")
position_all <- position_all[!duplicated(position_all$date_time),]

#Insert the secondary position data source in a seperate column, adjacent to the primary depth source.

position_all <- left_join(position_all, position_data2, by = "date_time")
position_all <- position_all[!duplicated(position_all$date_time),]

#Find sequences where the primary transponder's position is missing for more than 60 seconds.

gaps <- rle(is.na(position_all[,5])) #Search for NA values in column 5, the easting values for the primary beacon.
gaps$values <- gaps$values & gaps$lengths >= 60 #This line searches for more than 60 NA values in a row.
position_all$gaps <- inverse.rle(gaps) #Put the TRUE/FALSE indices back into the data frame.

#Loop through the elements of position_all, where the there are no position records from the primary transponder for large GAP period,
#fill in with depth records from the Ship"s GPS. Do the same for the zone field, so that zone numbers from the secondary transponder fill in 
#any blanks in the primary transponder recorders where needed.

for(k in 1:length(position_all$date_time))
{
  if(position_all$gaps[k] == T) #If there is no Lat, there will be no Long either, so just have to search for one of the two
  {position_all$Easting.x[k] <- position_all$Easting.y[k]
  position_all$Northing.x[k] <- position_all$Northing.y[k]
  position_all$device.x[k] <- position_all$device.y[k]
  position_all$zone.x[k] <- position_all$zone.y[k] #Fill in zone numbers where required.
  }
}

position_all <- position_all[,c(2,1,4:6,10,11,13,7)]
names(position_all) <- c("date_time","Transect_Name","device","Main_Beacon_Easting","Main_Beacon_Northing",
                         "Secondary_Beacon_Easting","Secondary_Beacon_Northing","Gaps","zone")

####################################STEP 11 - EXTRACT SHIP GPS POSITION#############################################

ship_GPS_data <- filter(dives_full, X2 == "POS" & device == GPS_pref)
ship_GPS_data <- left_join(full_seq, ship_GPS_data, by = "date_time")
ship_GPS_data <- ship_GPS_data[!duplicated(ship_GPS_data$date_time),]
ship_GPS_data <- ship_GPS_data[, c(2,1,10,7:8,11)]
names(ship_GPS_data) <- c("date_time","Transect_Name","device","Ship_Easting","Ship_Northing","zone")

#Join to the position_all data frame
position_all <- left_join(position_all, ship_GPS_data, by = "date_time")
position_all <- position_all[,c(1:7,12,13,8,14)]

#Rename columns. NOTE, we are now using the zone column from the Ship GPS data set, which should be the most comprehensive.
#OK to do this, since the Ship and the ROV are always going to be in the same UTM zone as each other.
names(position_all) <- c("date_time","Transect_Name","device","Main_Beacon_Easting","Main_Beacon_Northing",
                         "Secondary_Beacon_Easting","Secondary_Beacon_Northing","Ship_Easting","Ship_Northing","Gaps","zone")

###################STEP 12 - SEARCH FOR ROGUE CAM PITCH AND ROLL DATA; EXTRACT IT IF IT'S PRESENT################################

#Search for rows in the first column (X1) that have a 'HCP' identifier. These are heave, pitch and roll values
#This can include heave from a DVL, as well a from onboard IMU devices (i.e. Rogue Cam pitch and roll sensor).
#Here, we are extracting only the speed data from the ROWETECH DVL.

if(which(dives_full$X2 == "HCP") != 0)
{
  rogue_data <- filter(dives_full, Device_type == "HCP" & Device == rogue_cam_pref)
  rogue_data <- left_join(slog, rogue_data, by = "Datetime")
  rogue_data <- rogue_data[!duplicated(rogue_data$Datetime),]
  rogue_data <- rogue_data[, c(2,1,10,7,8)]
  names(rogue_data) <- c("date_time","Transect_Name","device","roll","pitch")
}


######################STEP 13 - CONVERT THE POSITION DATA TO DECIMAL DEGREES####################################

#In order to convert the data that is recorded by Hypack as UTM values, the position_all data frame needs to be converted to
#a SpatialPointsDataFrame, which is class in the package(sp), loaded by package(rgdal) at the start of this script. 
#SpatialPointsDataFrames cannot contain NA values, so these value must first be selected for and set aside, since some of these rows with NA
#values are still of interest, they are re-inserted into the data when the SpatialPointsDataFrame object is converted back to a regular data.frame
#SpatialPointsDataFrames also are restricted to a single coordinate reference system (CRS). Since different dives could conceivably by from different
#UTM zones (i.e. different CRS), the loop below splits the position data into seperate transects and load the appropriate CRS that corresponds to the UTM
#Zone that was set in Hypack for each particular dive.

#Empty data frames to fill with the ship and beacon values converted into decimal degree values for latitude and longitude.
ship_geographic <- data.frame()
main_beacon_geographic <- data.frame()
secondary_beacon_geographic <- data.frame()

#Strip NA values, generate spatial points DFs for each transect, transform to Lat/Longs, then save as a dataframe again.
for(i in unique(position_all$Transect_Name))
{
  main_beacon_no_NA <- filter(position_all, !is.na(Main_Beacon_Easting) & position_all$Transect_Name == i)
  ship_no_NA <- filter(position_all, !is.na(Ship_Easting) & position_all$Transect_Name == i)
  
  main_beacon_coordinates <- main_beacon_no_NA[, c("Main_Beacon_Easting","Main_Beacon_Northing")] # UTM coordinates for the AAE wide transponder
  ship_coordinates <- ship_no_NA[, c("Ship_Easting","Ship_Northing")] # UTM coordinates for the Ship GPS
  
  main_beacon_data <- main_beacon_no_NA[, c("date_time","Transect_Name","device","Gaps")] # data to keep
  ship_data <- as.data.frame(ship_no_NA[, c("date_time")]) # data to keep
  
  zone_num <- unique(position_all$zone[position_all$Transect_Name == i]) #Find the unique zone number value associate with each dive number, this might include an NA value.
  zone_num <- zone_num[!is.na(zone_num)] #Control for NA values; drop index values that may be NA, keeping only integer zone number values (8,9 or 10).
  crs <- CRS(paste0("+proj=utm +zone=", zone_num ," +datum=WGS84")) #proj4string of coordinates.
  
  #Assemble the spatial data points DF.
  
  main_beacon_spatial <- SpatialPointsDataFrame(coords = main_beacon_coordinates, data = main_beacon_data, proj4string = crs)
  ship_spatial <- SpatialPointsDataFrame(coords = ship_coordinates, data = ship_data, proj4string = crs)
  
  #Transform the UTMs coordinates into lat/longs, formatted as decimal degrees. Then turn it back into a regular DF.
  
  main_beacon_spatial <- spTransform(main_beacon_spatial, CRS("+proj=longlat +datum=WGS84"))
  ship_spatial <- spTransform(ship_spatial, CRS("+proj=longlat +datum=WGS84"))
  main_beacon_spatial <- as.data.frame(main_beacon_spatial)
  main_beacon_spatial <- main_beacon_spatial[,c(1:3,5:6)]
  ship_spatial <- as.data.frame(ship_spatial)
 
  
  #Add rows to empty data frames initialzed before this loop.
  main_beacon_geographic <- bind_rows(main_beacon_geographic, main_beacon_spatial)
  ship_geographic <- bind_rows(ship_geographic, ship_spatial)
  
  #Check to see if a secondary beacon was used during the transect before trying to generate the SP Data Frame object, if not (length = 0), skip trying
  #to process data from the secondary beacon. 
  current_transect <- filter(position_all, Transect_Name == i)
  if(length(which(!is.na(current_transect$Secondary_Beacon_Easting))) != 0)
  {
    secondary_beacon_no_NA <- filter(position_all, !is.na(Secondary_Beacon_Easting) & position_all$Transect_Name == i)
    secondary_beacon_coordinates <- secondary_beacon_no_NA[, c("Secondary_Beacon_Easting","Secondary_Beacon_Northing")] # UTM coordinates for the AAE wide transponder
    secondary_beacon_data <- secondary_beacon_no_NA[, c("date_time")] # data to keep
    secondary_beacon_spatial <- SpatialPointsDataFrame(coords = secondary_beacon_coordinates, data = secondary_beacon_data, proj4string = crs)
    secondary_beacon_spatial <- spTransform(secondary_beacon_spatial, CRS("+proj=longlat +datum=WGS84"))
    secondary_beacon_spatial <- as.data.frame(secondary_beacon_spatial)
    secondary_beacon_geographic <- bind_rows(secondary_beacon_geographic, secondary_beacon_spatial)
  }
  
}

#Rename the columns.

names(main_beacon_geographic) <- c("date_time","Transect_Name","device","Beacon_Long","Beacon_Lat")
names(ship_geographic) <- c("date_time","Ship_Long","Ship_Lat")


#Check to see if secondary beacon has any data; secondary beacon may not have been used. 
#If that is the case, fill the entries in the beacon dataframe containing Lat/Long values with 'NULL' entries.
if(exists("secondary_beacon_spatial")) 
{
  names(secondary_beacon_geographic) <- c("date_time","Transect_Name","device","Beacon_Long","Beacon_Lat")
} else {
  secondary_beacon_geographic <- full_seq
  secondary_beacon_geographic$Secondary_Beacon_Long <- rep("NULL",length(full_seq$date_time))
  secondary_beacon_geographic$Secondary_Beacon_Lat <- rep("NULL", length(full_seq$date_time))
  secondary_beacon_geographic <- secondary_beacon_geographic[, c(2:4)]
}  

#Slot the Long/Lat positions back in the position_all DF, and remove the northing and eastings

position_all <- left_join(position_all, main_beacon_geographic, by = "date_time")
position_all$Main_Beacon_Easting <- position_all$Main_Beacon_Long
position_all$Main_Beacon_Northing <- position_all$Main_Beacon_Lat
position_all <- left_join(position_all, secondary_beacon_geographic, by = "date_time")
position_all <- left_join(position_all, ship_geographic, by = "date_time")


position_all <- position_all[,c(1:3,12:17,8)]
names(position_all) <- c("date_time","Transect_Name","device","Main_Beacon_Long","Main_Beacon_Lat",
                         "Secondary_Beacon_Long","Secondary_Beacon_Lat","Ship_Long","Ship_Lat","Gaps")

#Round all Lat/Long values to the 5th decimal, equivalent to ~ 1m precision.

position_all$Main_Beacon_Lat <- round(position_all$Main_Beacon_Lat, digits = 5)
position_all$Main_Beacon_Long <- round(position_all$Main_Beacon_Long, digits = 5)
if(exists("secondary_beacon_spatial")) #As above, secondary beacon may not have been used. Skip next step if so.
  {
  position_all$Secondary_Beacon_Lat <- round(position_all$Secondary_Beacon_Lat, digits = 5)
  position_all$Secondary_Beacon_Long <- round(position_all$Secondary_Beacon_Long, digits = 5)
  }
position_all$Ship_Long <- round(position_all$Ship_Long, digits = 5)
position_all$Ship_Lat <- round(position_all$Ship_Lat, digits = 5)



#####################################STEP 14 - ASSEMBLE ALL DATA TO A SINGLE DATA FRAME###########################

#Join position data with depth, heading, altitude, slant range and RogueCam pitch and roll data records for the full period of record (descent, on transect, ascent)

all_data <- position_all
all_data$Depth_m <- depth_all$Depth_m
all_data$Phantom_heading <- phantom_heading_data$Phantom_heading
all_data$Ship_heading <- ship_heading_data$Ship_heading
all_data$Speed_kts <- speed_data$speed_kts
all_data$Altitude_m <- altitude_data$altitude_m
all_data$Slant_Range_m <- slant_data$slant_range_m
all_data$Rogue_pitch <- rogue_data$pitch
all_data$Rogue_roll <- rogue_data$roll

#Add the data source for each depth records, and the column listing gaps in position

all_data$Depth_Source <- depth_all$device
all_data$Position_Source <- position_all$device

#Re-order columns to the following order: date_time, Transect_Name, Main Beacon Long/Lat, Secondary Beacon Long/Lat, Ship Lat/Long,
#Depth_m, Phantom Heading, Ship Heading, Phantom Speed, Phantom Altitude, MiniZeus Slant Range, Rogue Pitch, Rogue Roll, Depth Data Source
#Position Source, Gaps. Device column is dropped.

all_data <- all_data[,c(1:2,4:9,11:20,10)]


#######################################STEP 15 - WRITE CLIPPED .CSV FILES FOR EACH DIVE##################################


#Loop through the Dive Names in the all_data frames, write one .CSV for the clipped (on transect data) for each dive.

for(j in unique(all_data$Transect_Name))
{
  to_write <- filter(all_data, Transect_Name == j)
  write.csv(to_write, file = paste0(save_dir,"/",j,".csv"), quote = F, row.names = F)
}

