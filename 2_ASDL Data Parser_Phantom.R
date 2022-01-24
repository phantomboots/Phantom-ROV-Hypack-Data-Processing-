#===============================================================================
# Script Name: ASDL Data Processing_Phantom
# Script Function: Reads in the data files created by Advanced Serial Data 
#   logger, parses each file and fills in missing date_time values. Disseminates 
#   data to 1Hz and writes one master file for the whole cruise. Create files 
#   with the suffix '_MasterLog', for use in later steps. Processes records for 
#   Phantom's Onboard Heading/Depth/Umbilical turns, Hemisphere GPS position and 
#   heading, ROWETech DVL, Tritech Altimeter (Slant Range), Video Overlay string, 
#   MiniZeus Zoom/Focus/Aperture, RBR CTD, MiniZeus and ROV pitch and roll and
#   TrackMan data records. Each step is proceeded by and if() statement that 
#   checks if data is present before attempting to process the records from a 
#   particular device.
#
# NOTE: In 2021, the RogueCam pitch and roll was only read into Hypack, due 
#   to a device hardware limitation. For this reason, there is no RogueCam 
#   data processed by this script.
#
# Script Author: Ben Snow
# Script Date: Sep 4, 2019
# R Version: 3.5.1

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
#           - Removed the need to convert from datetime to numeric back to dt
#           - Applies that function to each file type
#           - Use NA as nodata instead of -999
################################################################################

# Notes - 
# - maybe combine all the data into one dataframe with columns as data types
# - then #3 uses this data to fill in hypack data, maybe do that here? and save
#   3 just for interpolation


#===============================================================================
# Packages and session options

# Check for the presence of packages shown below, install missing
packages <- c("lubridate","dplyr","stringr","imputeTS","purrr", "future.apply")
new_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
if(length(new_packages)) install.packages(new_packages)

# Force display of long digit numbers
options(digits = 12)

# Load required packages
lapply(packages, require, character.only = TRUE)

# Set multisession so future_lapply runs in parallel
plan(multisession)



# measurements - Facilitates converting data from the Hemisphere GPS from 
#                Deg/Decimal mins to Decimal degrees.


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


#===============================================================================
# STEP 2 - READ AND PROCESS ASDL DATA BY TYPE

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
    # For "pos" reformat lat/lon
    if( type == "pos" ){
      # Subset rows and columns GPS position
      ds <- ds[dtype == "$GPGGA", c("X1","X3","X5")]
      # Reformat to decimal degrees with 7 decimal points
      ds$X3 <- round(as.numeric(substring(ds$X3, 1, 2)) + 
                       (as.numeric(substring(ds$X3, 3, nchar(ds$X3)))/60), 7)
      ds$X5 <- -1 * round(as.numeric(substring(ds$X5, 1, 3)) + 
                            (as.numeric(substring(ds$X5, 4, nchar(ds$X5)))/60), 7)
      # If heading subset GPHDT and HEHDT rows only
    } else if ( type == "head" ){
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
# Run if ROV_files exist
if( length(ROV_files) > 0 ){
  # Apply function in parallel
  zfalist <- future_lapply(ROV_files, FUN=readASDL, type="rov")
  # Bind into dataframe
  ROV_all <- do.call("rbind", rovlist)
  # Rename
  names(ROV_all) <- c("Datetime","Depth_m","Phantom_heading")
  # Summary
  summary(ROV_all)
  #   write.csv(ROV_all, paste(save_dir,"ROV_Heading_Depth_MasterLog.csv", sep ="/"), 
  #             quote = F, row.names = F)
}

#======================#
#      Slant range     #
#======================#

# List files
Tritech_files <- list.files(pattern = "^Tritech", path = ASDL_dir, full.names = T)
# Run if slant_files exist
if( length(Tritech_files) > 0 ){
  # Apply function in parallel
  zfalist <- future_lapply(Tritech_files, FUN=readASDL, type="tritech")
  # Bind into dataframe
  Tritech_all <- do.call("rbind", trilist)
  # Rename
  names(Tritech_all) <- c("Datetime","slant_range_m")
  # Set values of 9.99 or 0 or less (out of range values) to NA
  # Question: Can slant range be less than zero?
  Tritech_all$slant_range_m[Tritech_all$slant_range_m <= 0] <- NA
  Tritech_all$slant_range_m[Tritech_all$slant_range_m == 9.99] <- NA
  # Summary
  summary(Tritech_all)
  #   write.csv(Tritech_all, paste(save_dir,"Tritech_SlantRange_MasterLog.csv", sep ="/"), 
  #             quote = F, row.names = F)
}

#======================#
#      MiniZeus ZFA    #
#======================#

# List files
ZFA_files <- list.files(pattern = "^MiniZeus", path = ASDL_dir, full.names = T)
# Run if slant_files exist
if( length(ZFA_files) > 0 ){
  # Apply function in parallel
  zfalist <- future_lapply(ZFA_files, FUN=readASDL, type="minizeus")
  # Bind into data frame
  ZFA_all <- do.call("rbind", zfalist)
  # Rename
  names(ZFA_all) <- c("Datetime","zoom_percent","focus_percent","aperture_percent")
  # Summary
  summary(ZFA_all)
  #   write.csv(ZFA_all, paste(save_dir,"MiniZeus_ZFA_MasterLog.csv", sep = "/"), 
  # quote = F, row.names = F)
}

#======================#
#        RBR CTD       #
#======================#

# List files
RBR_files <- list.files(pattern = "^RBR", path = ASDL_dir, full.names = T)
# Run if slant_files exist
if( length(RBR_files) > 0 ){
  # Apply function in parallel
  rbrlist <- future_lapply(RBR_files, FUN=readASDL, type="rbr")
  # Bind into data frame
  RBR_all <- do.call("rbind", rbrlist)
  # Rename
  names(RBR_all) <- c("Datetime","Conductivity_mS/cm","Temp_C","Pressure_dbar",
                      "Dissolved_02_sat_%","Sea_Pressure_dbar", "Depth_m",
                      "Salinity_PSU","Sound_Speed_m/s","Specific_Cond_uS/cm")
  # Summary
  summary(RBR_all)
  #   write.csv(RBR_all, paste(save_dir,"RBR_CTD_MasterLog.csv", sep = "/"), 
  # quote = F, row.names = F)
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
# Run if slant_files exist
if( length(GPS_files) > 0 ){
  # Apply function in parallel
  gpslist <- future_lapply(GPS_files, FUN=readASDL, type="pos")
  # Bind into data frame
  GPS_all <- do.call("rbind", gpslist)
  # Rename
  names(GPS_all) <- c("Datetime","Latitude","Longitude")
  # Summary
  summary(GPS_all)
  # Check
  plot(GPS_all$Longitude, GPS_all$Latitude, asp=1)
  #   write.csv(GPS_all, paste(save_dir,"Hemisphere_GPS_MasterLog.csv", sep = "/"), 
  # quote = F, row.names = F)
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
# Run if slant_files exist
if( length(head_files) > 0 ){
  # Apply function in parallel
  headlist <- future_lapply(head_files, FUN=readASDL, type="head")
  # Bind into data frame
  heading_all <- do.call("rbind", headlist)
  # Rename
  names(heading_all) <- c("Datetime","Ship_heading")
  # Summary
  summary(heading_all)
  #   write.csv(GPS_all, paste(save_dir,"Hemisphere_Heading_MasterLog.csv", sep = "/"), 
  # quote = F, row.names = F)
}

#===============#
#   TRACKMAN   #
#===============#
# Question: Is this used in subsequent processing?

# List files
track_files <- list.files(pattern = "^TrackMan", path = ASDL_dir, full.names = T)
# Run if slant_files exist
if( length(track_files) > 0 ){
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
  summary(Track_all)
  #   write.csv(GPS_all, paste(save_dir,"TrackMan_Beacons_MasterLog.csv", sep = "/"), 
  # quote = F, row.names = F)
}

#===================#
#    ROWETech DVL   #
#===================#
# Question: Previously looked for -99.999 and 0 for out-of-bounds data
# but there are other negative values, should all negatives be nodata?

# List files
DVL_files <- list.files(pattern = "DVL", path = ASDL_dir, full.names = T)
# Run if slant_files exist
if( length(DVL_files) > 0 ){
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
  summary(DVL_all)
  #   write.csv(GPS_all, paste(save_dir,"ROWETECH_DVL_MasterLog.csv", sep = "/"), 
  # quote = F, row.names = F)
}



#- start here

#############################################STEP 11 - READ IN VECTOR 12 KHZ SOUNDER DATA#############################################


Vector_12_files <- list.files(ASDL_dir, pattern = "Vector_12", full.names = T)

if(length(Vector_12_files != 0))
{
  
  for(i in 1:length(Vector_12_files))
  {
    name <- as.character(i)
    assign(name, read_csv(Vector_12_files[i], col_names = F, col_types = cols(X1 = "c", X2 = "c", X3 = "c", X4 = "c",
                                                                           X5 = "c", X7 = "c")))
    if(name == "1")
    {Vector_12_all <- get(name)
    }else Vector_12_all <- bind_rows(Vector_12_all, get(name))
    rm(list = c(i))
  }
  
  
  #Located date stamp values and time stamp values. Replace period in timestamp value with a colon. Parse date_time.
  
  Vector_12_all$date <- str_extract(Vector_12_all$X1, "\\d{8}")
  Vector_12_all$time <- str_extract(Vector_12_all$X1, "\\d{2}\\:\\d{2}\\.\\d{2}")
  Vector_12_all$time <- gsub("\\.",":",Vector_12_all$time)
  Vector_12_all$date_time <- ymd_hms(paste(Vector_12_all$date, Vector_12_all$time, sep = " "))
  
  #Extract altimeter slant range.
  
  Vector_12_all$Bottom_Depth_m <- as.numeric(Vector_12_all$X4)
  
  #Impute the time series, before filtering out any values
  
  full <- na_interpolation(as.numeric(Vector_12_all$date_time))
  Vector_12_all$date_time <- as.integer(full) #Put it back into the data frame as an integer, for filtering purposes.
  
  #Remove duplicate values, convert the date_time back to a POSIXct object.
  
  Vector_12_all <- Vector_12_all[!duplicated(Vector_12_all$date_time),]
  Vector_12_all$date_time <- as.POSIXct(Vector_12_all$date_time, origin = "1970-01-01", tz = "UTC") #Standard R origin value
  
  #Remove any Bottom Depth NA values.
  
  Vector_12_all <- filter(Vector_12_all, !is.na(Bottom_Depth_m))
  
  #Drop unused columns and write .CSV MasterLog for the altitude_m
  
  Vector_12_all <- Vector_12_all[,c(10,11)]
  write.csv(Vector_12_all, paste(save_dir,"Vector_12Khz_Sounder_MasterLog.csv", sep = "/"), quote = F, row.names = F)
}

#############################################STEP 13 - READ IN ROV AND MINIZEUS IMUS#############################################


IMU_files <- list.files(ASDL_dir, pattern = "Zeus_Cans", full.names = T)

if(length(IMU_files != 0))

  
  for(i in 1:length(IMU_files))
  {
    name <- as.character(i)
    assign(name, read_csv(IMU_files[i], col_names = F, col_types = cols(X1 = "c", X2 = "c", X3 = "c", X4 = "c",
                                                                              X5 = "c", X6 = "c", X7 = "c")))
    if(name == "1")
    {IMU_all <- get(name)
    }else IMU_all <- bind_rows(IMU_all, get(name))
    rm(list = c(i))
  }
  
  
  #Located date stamp values and time stamp values. Replace period in timestamp value with a colon. Parse date_time.
  
  IMU_all$date <- str_extract(IMU_all$X1, "\\d{8}")
  IMU_all$time <- str_extract(IMU_all$X1, "\\d{2}\\:\\d{2}\\.\\d{2}")
  IMU_all$time <- gsub("\\.",":",IMU_all$time)
  IMU_all$date_time <- ymd_hms(paste(IMU_all$date, IMU_all$time, sep = " "))
  
  
  #Impute the time series, before filtering out any values
  
  full <- na_interpolation(as.numeric(IMU_all$date_time))
  IMU_all$date_time <- as.integer(full) #Put it back into the data frame as an integer, for filtering purposes.
  
  #Remove duplicate values, convert the date_time back to a POSIXct object.
  
  IMU_all <- IMU_all[!duplicated(IMU_all$date_time),]
  IMU_all$date_time <- as.POSIXct(IMU_all$date_time, origin = "1970-01-01", tz = "UTC") #Standard R origin value

  #Drop unused columns and write .CSV MasterLog for the pitch and roll values.
  
  IMU_all <- IMU_all[,c(10,2:7)]
  
  #Re-convert all pitch/roll columns to numeric values

  IMU_all[,2:7] <- lapply(IMU_all[,2:7], as.numeric)
  
  #Rename the columns
  names(IMU_all) <- c("date_time","Zeus_Pitch","Zeus_Roll","ROV_Pitch","ROV_Roll","MiniZeus_Pitch_Minus_ROV_Pitch","MiniZeus_Roll_Minus_ROV_Roll")
  
  #Apply the offsets to the IMU pitch and roll values
  
  IMU_all$ROV_Pitch <- IMU_all$ROV_Pitch + rov_pitch_offset
  IMU_all$ROV_Roll <- IMU_all$ROV_Roll + rov_roll_offset
  IMU_all$Zeus_Pitch <- IMU_all$Zeus_Pitch + zeus_pitch_offset
  IMU_all$Zeus_Roll <- IMU_all$Zeus_Roll + zeus_roll_offset
  
  #Calculate difference
  IMU_all$MiniZeus_Pitch_Minus_ROV_Pitch <- IMU_all$Zeus_Pitch - IMU_all$ROV_Pitch
  IMU_all$MiniZeus_Roll_Minus_ROV_Roll <- IMU_all$Zeus_Roll - IMU_all$ROV_Roll
  
  
  #Write Master Log file.

  write.csv(IMU_all, paste(save_dir,"Zeus_ROV_IMU_MasterLog.csv", sep = "/"), quote = F, row.names = F)

