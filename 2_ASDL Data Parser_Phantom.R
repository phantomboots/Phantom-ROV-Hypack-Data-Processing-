#===============================================================================
# Script Name: 2_ASDL Data Processing_Phantom
#
# Script Function: Reads in the data files created by Advanced Serial Data 
#   logger, parses each file and fills in missing date_time values. Writes one 
#   master file for the whole cruise. Create files with the suffix '_MasterLog', 
#   for use in later scripts Processes records for:
#   - Phantom's Onboard Heading/Depth/Umbilical turns
#   - Hemisphere GPS position 
#   - ROWETech DVL
#   - Tritech Altimeter (Slant Range)
#   - Video Overlay string
#   - MiniZeus Zoom/Focus/Aperture
#   - RBR CTD 
#   - MiniZeus zoom, aperture and focus
#   - MiniZeus pitch and roll
#   - TrackMan data 
#
# NOTE: In 2021, the RogueCam pitch and roll was only read into Hypack, due 
#   to a device hardware limitation. For this reason, there is no RogueCam 
#   data processed by this script.
#
# Script Author: Ben Snow, adapted by Jessica Nephin
# Script Date: Sep 4, 2019, adapted in Jan 2022
# R Version: 3.5.1, version 4.0.2


################################################################################
#                                   CHANGE LOG                                 #
################################################################################
#
# June 6, 2020: Converted out of range values of -99.999 or 0 in the DVL Bottom 
#               velocity values to -9999, to match out of range values used for
#               Altitude and Slant Range.
# June 6, 2020: Set out of range values of for Altitude and Slant Range to -9999.
# Apr 21, 2021: Removed ASDL data processing for Cyclops camera pitch and roll. 
#               Added in processing of pitch/roll from MiniZeus and MuxCan IMUs
# Apr 23, 2021: Set options(digits = 12), to make sure that Lat/Long with many 
#               sig figures are displayed as expected. Added in explicit number 
#               of columns to read for Hemisphere_position, ROWETech DVL and RBR 
#               log files. read_delim scans the first rows to determine the 
#               appropriate number of columns for the parsed data, these devices
#               can have variable numbers of columns when parsed from comma 
#               delimited format. Setting number of columns explicitly via 
#               col_names = paste0("X", seq_len(...)), where ... is the 
#               explicitly stated number of columns, will control for this.
# Jan 2021: - Created a function to load and process the datetime
#           - Function catches errors and keeps going
#           - Removed the need to convert from datetime to numeric back to dt
#           - Applies that function to each file type
#           - Use NA as nodata instead of -999
#           - Removed need for measurements package, added future.apply
#           - Fixed if exists statements
#           - Saves data processing log with warnings, errors and data summaries
#           - Added manual calc of rov lat/lon from trackman
#           - Fixed error in '2b_Manual Beacon Position Calculator.R' code that 
#             used ship position instead of calculated position
################################################################################



#===============================================================================
# Packages and session options

# Check for the presence of packages shown below, install missing
packages <- c("lubridate","dplyr","stringr","imputeTS","purrr", 
              "future.apply","geosphere")
new_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
if(length(new_packages)) install.packages(new_packages)

# Force display of long digit numbers
options(digits = 12)

# Load required packages
lapply(packages, require, character.only = TRUE)

# Set multisession so future_lapply runs in parallel
plan(multisession)


#===============================================================================
# STEP 1 - SET PATHS AND OFFSETS, MAKE EXPORT DIRECTORY

# Enter Project folder name
project_folder <- "Pac2021-054_phantom"

# Directory where the ASDL files are stored
# Path must start from your working directory, check with getwd(), or full paths
ASDL_dir <- file.path(project_folder, "Data/Advanced_Serial_Data_Logger")

# Set the directory for saving of the master files.
# Path must start from your working directory, check with getwd(), or full paths
save_dir <- file.path(project_folder, "Data/Advanced_Serial_Data_Logger/Full_Cruise")
dir.create(save_dir, recursive = TRUE) # Will warn if already exists

# Offset values for IMUs located in the Phantom's subsea can and on the MiniZeus
# An offset of zero means is measure the expect 90 degrees when perpendicular 
# to the seafloor.
zeus_pitch_offset <- 0 
zeus_roll_offset <- 6
rov_pitch_offset <- -33
rov_roll_offset <- -1

# Set the maximum distance (in meters) that can occur between the ROV and ship
# Will differ depending on the ROV
# Question - what is this distance for phantom and boots?
max_dist <- 200


#===============================================================================
# STEP 2 - START LOG FILE

# Sink output to file
rout <- file( file.path(save_dir, "ASDL_Data_Parser.log" ), open="wt" )
sink( rout, split = TRUE ) # display output on console and send to log file
sink( rout, type = "message" ) # send warnings to log file
options(warn=1) # print warnings as they occur

# Start the timer
sTime <- Sys.time( )

# Message
message("Parsing ", project_folder, " project data on ", Sys.Date(), "\n")


#===============================================================================
# STEP 3 - READ AND PROCESS ASDL DATA BY TYPE

# Function to read in and process ASDL
# If a file fails to be read or processed, continue to next file
# Return error messages
readASDL <- function( afile, type ){
  tryCatch({
    # Packages
    packages <- c("lubridate","dplyr","stringr","imputeTS","purrr")
    lapply(packages, require, character.only = TRUE)
    # Read in lines from ASDL files
    alines <- readLines(afile, skipNul=FALSE)
    # If 'pos' crop to first GPZDA row, first row with datetime and 
    # subset GPZDA and GPGGA rows only
    if ( type == "pos" ){
      alines <- alines[min(grep("GPZDA",alines)):length(alines)]
      alines <- alines[grep("GPZDA|GPGGA",alines)]
    } else {
      # Count the number of columns (commas) in each row
      colcount <- str_count(alines,",")
      # Remove lines without the correct number of columns
      # Filters out rows with nulls or errors
      alines <- alines[colcount == median(colcount)]
    }
    # Bind lines together in dataframe, fills in blanks with NA
    ds_list <-  alines %>% strsplit(., ",") 
    ds <- map(ds_list, ~ c(X=.)) %>% bind_rows(.) %>% as.data.frame()
    # Make a copy of X1 prior to datetime formatting
    dtype <- ds$X1 # Copy first column
    # Keep only relevant columns based on type
    if( type == "rov" ) ds <- ds[,c("X1","X2","X3")]
    if( type == "tritech" ) ds <- ds[,c("X1","X4")]
    if( type == "minizeus" ) ds <- ds[,c("X1","X3","X5","X7")]
    if( type == "rbr" ) ds <- ds[,paste0("X", 1:10)]
    if( type == "pos" ) ds <- ds[,c("X1","X2","X3","X4","X5")]
    if( type == "head" ) ds <- ds[,c("X1","X2")]
    if( type == "track" ) ds <- ds[,paste0("X", c(1:2,4:18))]
    if( type == "dvl" ) ds <- ds[,paste0("X", c(1,5:9))]
    if( type == "sound" ) ds <- ds[,c("X1","X4")]
    if( type == "imu" ) ds <- ds[,paste0("X", 1:5)]
    # Extract datetime in X1
    if( type == "rbr" ){
      ds$X1 <- sub(".*[>]","", ds$X1)
      ds$X1 <- sub("[.].*","", ds$X1)
      ds$X1 <- ymd_hms(ds$X1)
    } else if( type == "pos" ){
      date_str <- paste0(ds$X5, ds$X4, ds$X3)
      date_str[dtype == "$GPGGA"] <- "19990909" # place holder to be removed
      time_str <- sub("[.].*","",ds$X2)
      ds$X1 <- suppressWarnings(ymd_hms(paste(date_str, time_str, sep = " ")))
      ds$X1[dtype == "$GPGGA"] <- NA # remove place holder datetimes
    } else {
      date_str <- str_extract(ds$X1, "\\d{8}")
      time_str <- str_extract(ds$X1, "\\d{2}\\:\\d{2}\\.\\d{2}")
      time_str <- gsub("\\.",":",time_str)
      ds$X1 <- suppressWarnings(ymd_hms(paste(date_str, time_str, sep = " ")))
    }
    # Interpolate the datetime series to fill gaps
    ds$X1 <- floor_date(as_datetime(na_interpolation(as.numeric(ds$X1))), "second")
    # For position, reformat lat/lon
    if( type == "pos" ){
      # Subset rows and columns GPS position
      ds <- ds[dtype == "$GPGGA", c("X1","X3","X5")]
      # Reformat to decimal degrees with 7 decimal points
      ds$X3 <- round(as.numeric(substring(ds$X3, 1, 2)) + 
                       (as.numeric(substring(ds$X3, 3, nchar(ds$X3)))/60), 7)
      ds$X5 <- -1 * round(as.numeric(substring(ds$X5, 1, 3)) + 
                            (as.numeric(substring(ds$X5, 4, nchar(ds$X5)))/60), 7)
    } 
    # If heading, subset GPHDT and HEHDT rows only
    if ( type == "head" ){
      ds <- ds[grep("GPHDT|HEHDT",dtype),]
    }
    # Remove duplicate time stamps
    ds <- ds[!duplicated(ds$X1),]
    # Set all columns except X1 as numeric
    if( ncol(ds) > 2 ){
      ds[,-1] <- apply(ds[,-1], 2, as.numeric)
    } else {
      ds[,2] <- as.numeric(ds[,2])
    }
    # Remove NA values by checking second row
    ds <- ds[!is.na(ds[,2]),]
    # Return
    return(ds)
  },
  error=function(cond) {
    message(paste("File caused an error:", sub(".*[/]","",afile)))
    message(conditionMessage(cond))
    return(NULL)
  })    
}

#======================#
#    Depth & Heading   #
#======================#

# List files
ROV_files <- list.files(pattern = "^ROV", path = ASDL_dir, full.names = T)
# Run if files exist
if( length(ROV_files) > 0 ){
  # Message
  message("\nCreating 'ROV_Heading_Depth_MasterLog.csv'", "\n")
  # Apply function in parallel
  rovlist <- future_lapply(ROV_files, FUN=readASDL, type="rov")
  # Bind into dataframe
  ROV_all <- do.call("rbind", rovlist)
  # Rename
  names(ROV_all) <- c("Datetime","Depth_m","ROV_heading")
  # Summary
  print(summary(ROV_all))
  # Write
  write.csv(ROV_all, file.path(save_dir,"ROV_Heading_Depth_MasterLog.csv"),
            quote = F, row.names = F)
}

#======================#
#      Slant range     #
#======================#

# List files
Tritech_files <- list.files(pattern = "^Tritech", path = ASDL_dir, full.names = T)
# Run if files exist
if( length(Tritech_files) > 0 ){
  # Message
  message("\nCreating 'Tritech_SlantRange_MasterLog.csv'", "\n")
  # Apply function in parallel
  trilist <- future_lapply(Tritech_files, FUN=readASDL, type="tritech")
  # Bind into dataframe
  Tritech_all <- do.call("rbind", trilist)
  # Rename
  names(Tritech_all) <- c("Datetime","Slant_range_m")
  # Set values of 9.99 or 0 or less (out of range values) to NA
  # Question: Can slant range be less than zero?
  Tritech_all$slant_range_m[Tritech_all$slant_range_m <= 0] <- NA
  Tritech_all$slant_range_m[Tritech_all$slant_range_m == 9.99] <- NA
  # Summary
  print(summary(Tritech_all))
  # Write
  write.csv(Tritech_all, file.path(save_dir,"Tritech_SlantRange_MasterLog.csv"),
            quote = F, row.names = F)
}

#======================#
#      MiniZeus ZFA    #
#======================#

# List files
ZFA_files <- list.files(pattern = "^MiniZeus", path = ASDL_dir, full.names = T)
# Run if files exist
if( length(ZFA_files) > 0 ){
  # Message
  message("\nCreating 'MiniZeus_ZFA_MasterLog.csv'", "\n")
  # Apply function in parallel
  zfalist <- future_lapply(ZFA_files, FUN=readASDL, type="minizeus")
  # Bind into data frame
  ZFA_all <- do.call("rbind", zfalist)
  # Rename
  names(ZFA_all) <- c("Datetime","zoom_percent","focus_percent","aperture_percent")
  # Summary
  print(summary(ZFA_all))
  # Write
  write.csv(ZFA_all, file.path(save_dir,"MiniZeus_ZFA_MasterLog.csv"),
            quote = F, row.names = F)
}

#======================#
#        RBR CTD       #
#======================#

# List files
RBR_files <- list.files(pattern = "^RBR", path = ASDL_dir, full.names = T)
# Run if files exist
if( length(RBR_files) > 0 ){
  # Message
  message("\nCreating 'RBR_CTD_MasterLog.csv'", "\n")
  # Apply function in parallel
  rbrlist <- future_lapply(RBR_files, FUN=readASDL, type="rbr")
  # Bind into data frame
  RBR_all <- do.call("rbind", rbrlist)
  # Rename
  names(RBR_all) <- c("Datetime","Conductivity_mS/cm","Temp_C","Pressure_dbar",
                      "Dissolved_02_sat_%","Sea_Pressure_dbar", "Depth_m",
                      "Salinity_PSU","Sound_Speed_m/s","Specific_Cond_uS/cm")
  # Summary
  print(summary(RBR_all))
  # Check
  plot(RBR_all$Depth_m, RBR_all$Pressure_dbar)
  # Write
  write.csv(RBR_all, file.path(save_dir,"RBR_CTD_MasterLog.csv"),
            quote = F, row.names = F)
}

#=========================#
#   HEMISPHERE POSITION   #
#=========================#

# Ship GPS backup
# The heading data from this source is not used in subsequently, so was removed 
# from processing to simplify

# List files
GPS_files <- list.files(pattern = "Hemisphere_position", 
                        path = ASDL_dir, full.names = T)
# Run if files exist
if( length(GPS_files) > 0 ){
  # Message
  message("\nCreating 'Hemisphere_GPS_MasterLog.csv'", "\n")
  # Apply function in parallel
  gpslist <- future_lapply(GPS_files, FUN=readASDL, type="pos")
  # Bind into data frame
  GPS_all <- do.call("rbind", gpslist)
  # Rename
  names(GPS_all) <- c("Datetime","Latitude","Longitude")
  # Summary
  print(summary(GPS_all))
  # Check
  plot(GPS_all$Longitude, GPS_all$Latitude, asp=1)
  # Write
  write.csv(GPS_all, file.path(save_dir,"Hemisphere_GPS_MasterLog.csv"),
            quote = F, row.names = F)
}

#========================#
#   HEMISPHERE HEADING   #
#========================#

# Question: In previous version GPROT were removed prior to datetime interp, but
# that seems to be needed for the datetime for some files. Change to only filter
# out after time interp. Is there a reason that shouldn't be done?

# List files
head_files <- list.files(pattern = "Hemisphere_heading", 
                         path = ASDL_dir, full.names = T)
# Run if files exist
if( length(head_files) > 0 ){
  # Message
  message("\nCreating 'Hemisphere_Heading_MasterLog.csv'", "\n")
  # Apply function in parallel
  headlist <- future_lapply(head_files, FUN=readASDL, type="head")
  # Bind into data frame
  heading_all <- do.call("rbind", headlist)
  # Rename
  names(heading_all) <- c("Datetime","Ship_heading")
  # Summary
  print(summary(heading_all))
  # Write
  write.csv(heading_all, file.path(save_dir,"Hemisphere_Heading_MasterLog.csv"),
            quote = F, row.names = F)
}

#===============#
#   TRACKMAN   #
#===============#
# Question: Is this used in subsequent processing?

# List files
track_files <- list.files(pattern = "^TrackMan", path = ASDL_dir, full.names = T)
# Run if files exist
if( length(track_files) > 0 ){
  # Message
  message("\nCreating 'TrackMan_Beacons_MasterLog.csv'", "\n")
  # Apply function in parallel
  tracklist <- future_lapply(track_files, FUN=readASDL, type="track")
  # Bind into data frame
  Track_all <- do.call("rbind", tracklist)
  # Rename
  names(Track_all) <- c("Datetime","Beacon_ID","Phase_Counts_A",
                        "Phase_Counts_B","Phase_Counts_C","Quality_Factor",
                        "Error_Code", "Target_Slant_Range_m","Depression_Angle",
                        "Target_Bearing","DistanceX_m","DistanceY_m","DistanceZ_m",
                        "Ship_Heading","TSS_Pitch","TSS_Roll","Temp_C")
  # Summary
  print(summary(Track_all))
  # Write
  write.csv(Track_all, file.path(save_dir,"TrackMan_Beacons_MasterLog.csv"),
            quote = F, row.names = F)
}

#===================#
#    ROWETech DVL   #
#===================#
# Question: Previously looked for -99.999 and 0 for out-of-bounds data
# but there are other negative values, should all negatives be nodata?

# List files
DVL_files <- list.files(pattern = "DVL", path = ASDL_dir, full.names = T)
# Run if files exist
if( length(DVL_files) > 0 ){
  # Message
  message("\nCreating 'ROWETECH_DVL_MasterLog.csv'", "\n")
  # Apply function in parallel
  dvllist <- future_lapply(DVL_files, FUN=readASDL, type="dvl")
  # Bind into data frame
  DVL_all <- do.call("rbind", dvllist)
  # Convert units
  DVL_all[,paste0("X", 5:9)] <- DVL_all[,paste0("X", 5:9)] / 1000
  # Set zeros and negative values to NA
  DVL_all[,-1] <- apply(DVL_all[,-1], 2, function(x) ifelse(x <= 0, NA, x))
  # Rename
  names(DVL_all) <- c("Datetime","Bottom_X_Velocity_ms","Bottom_Y_Velocity_ms",
                        "Bottom_Z_Velocity_ms","Bottom_3D_Velocity_ms","Altitude_m")
  # Summary
  print(summary(DVL_all))
  # Write
  write.csv(DVL_all, file.path(save_dir,"ROWETECH_DVL_MasterLog.csv"),
            quote = F, row.names = F)
}

#===================#
#   VECTOR SOUNDER  #
#===================#
# Question: Is this used in subsequent processing? 

# List files
sound_files <- list.files(pattern = "Vector_12", 
                         path = ASDL_dir, full.names = T)
# Run if files exist
if( length(sound_files) > 0 ){
  # Message
  message("\nCreating 'Vector_12Khz_Sounder_MasterLog.csv'", "\n")
  # Apply function in parallel
  soundlist <- future_lapply(sound_files, FUN=readASDL, type="sound")
  # Bind into data frame
  sound_all <- do.call("rbind", soundlist)
  # Rename
  names(sound_all) <- c("Datetime","Bottom_Depth_m")
  # Summary
  print(summary(sound_all))
  # Write
  write.csv(sound_all, file.path(save_dir,"Vector_12Khz_Sounder_MasterLog.csv"),
            quote = F, row.names = F)
}

#===================#
#    MINIZEUS IMU   #
#===================#

# List files
IMU_files <- list.files(pattern = "^Zeus_Cans", 
                          path = ASDL_dir, full.names = T)
# Run if files exist
if( length(IMU_files) > 0 ){
  # Message
  message("\nCreating 'Zeus_ROV_IMU_MasterLog.csv'", "\n")
  # Apply function in parallel
  imulist <- future_lapply(IMU_files, FUN=readASDL, type="imu")
  # Bind into data frame
  IMU_all <- do.call("rbind", imulist)
  # Rename
  names(IMU_all) <- c("Datetime","Zeus_Pitch","Zeus_Roll","ROV_Pitch","ROV_Roll")
  # Apply the offsets to the IMU pitch and roll values
  IMU_all$ROV_Pitch <- IMU_all$ROV_Pitch + rov_pitch_offset
  IMU_all$ROV_Roll <- IMU_all$ROV_Roll + rov_roll_offset
  IMU_all$Zeus_Pitch <- IMU_all$Zeus_Pitch + zeus_pitch_offset
  IMU_all$Zeus_Roll <- IMU_all$Zeus_Roll + zeus_roll_offset
  # Calculate difference
  IMU_all$MiniZeus_Pitch_Minus_ROV_Pitch <- IMU_all$Zeus_Pitch - IMU_all$ROV_Pitch
  IMU_all$MiniZeus_Roll_Minus_ROV_Roll <- IMU_all$Zeus_Roll - IMU_all$ROV_Roll
  # Summary
  print(summary(IMU_all))
  # Write
  write.csv(IMU_all, file.path(save_dir,"Zeus_ROV_IMU_MasterLog.csv"),
            quote = F, row.names = F)
}


#===============================================================================
# STEP 4 - CALCULATE ROV POSITION FROM TRACKMAN 

# Question - When is this needed?
# Computes the coordinates of each beacon using the X, Y distance and bearing 
# from the hydrophone specified in the TrackMan master file and converts these 
# to new Lat/Long positions for the vehicle.

# Message
message("\nCreating 'Manual_Beacon_Tracking_MasterLog.csv'", "\n")

# Read in data if not already loaded in workspace
if( !exists("Track_all") ){
  Track_all <- read.csv(file.path(save_dir,"TrackMan_Beacons_MasterLog.csv"))
  Track_all$Datetime <- ymd_hms(Track_all$Datetime)
}
if( !exists("GPS_all") ){
  GPS_all <- read.csv(file.path(save_dir,"Hemisphere_GPS_MasterLog.csv"))
  GPS_all$Datetime <- ymd_hms(GPS_all$Datetime)
}

# Filter out TrackMan readings where no DistanceX or Distance
TrackMan <- filter(Track_all, DistanceX_m != 0 & Error_Code != 18)
# Join the Lat/Long positions to the TrackMan DF
New_Tracking <- left_join(TrackMan, GPS_all, by = "Datetime")
# Calculate distance to ship from DistanceX and DistanceY variables
New_Tracking$Distance <- sqrt(abs(New_Tracking$DistanceX_m)^2 + 
                                abs(New_Tracking$DistanceY_m)^2)
# Remove records with NA coordinates 
New_Tracking <- New_Tracking[!is.na(New_Tracking$Latitude),]
# Remove records with distance out of range 
message( "Removing ", length(which(New_Tracking$Distance > max_dist)), 
         " records with distance greater than ", max_dist, " m")
New_Tracking <- New_Tracking[New_Tracking$Distance < max_dist,]
# Check
hist(New_Tracking$Distance, breaks = 30)

# Generate new points with X,Y distance and bearing from existing Lat/Longs
Beacon_Coords <- destPoint(p=New_Tracking[c("Longitude","Latitude")], 
                           b=New_Tracking$Target_Bearing, 
                           d=New_Tracking$Distance)
# Slot the new Beacon tracking points back into New_Tracking DF
New_Tracking$Beacon_Longitude <- Beacon_Coords[,1]
New_Tracking$Beacon_Latitude <- Beacon_Coords[,2]
# Check
plot(New_Tracking$Beacon_Longitude, New_Tracking$Beacon_Latitude, asp=1)
# Drop unnecessary columns
New_Tracking <- New_Tracking[c("Datetime","Beacon_Longitude","Beacon_Latitude")]
# Summary
print(summary(New_Tracking))
# Write
write.csv(New_Tracking, file.path(save_dir,"Manual_Beacon_Tracking_MasterLog.csv"),
          quote = F, row.names = F)




#===============================================================================
# Print end of file message and elapsed time
cat( "\nFinished: ", sep="" )
print( Sys.time( ) - sTime )

# Stop sinking output
sink( type = "message" )
sink( )
closeAllConnections()
  