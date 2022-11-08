#===============================================================================
# Script Name: 2_ASDL Data Processing_Phantom
#
# Script Function: Reads in the data files created by Advanced Serial Data 
#   logger, parses each file and fills in missing date_time values. Writes one 
#   master file for the whole cruise. Create files with the suffix '_MasterLog', 
#   for use in later scripts Processes records for:
#   - BOOTS string
#   - COM33 Ship heading
#   - Hemisphere GPS position 
#   - Tritech Altimeter (Slant Range)
#   - Imagenex Altimeter
#   - MiniZeus Zoom/Focus/Aperture
#   - SBE CTD
#   - MiniZeus zoom, aperture and focus
#   - TrackMan data 
#
#
# Script Author: Ben Snow, adapted by Jessica Nephin
# Script Date: Sep 4, 2019, adapted in Nov 2022
# R Version: 4.0.2
#
# Note: 
# Written to work with the 2021 BOOTS survey 


#===============================================================================
# Packages and session options

# Check for the presence of packages shown below, install missing
packages <- c("lubridate","dplyr","stringr","imputeTS","purrr", 
              "future.apply","geosphere")
new_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
if(length(new_packages)) install.packages(new_packages)

# Load required packages
lapply(packages, require, character.only = TRUE)

# Force display of long digit numbers
options(digits = 12)

# Set multisession so future_lapply runs in parallel
plan(multisession)


#===============================================================================
# STEP 1 - SET PATHS AND OFFSETS, MAKE EXPORT DIRECTORY

# Set working directory
# Use getwd() if you are already in the right directory
# The project folder and needs to be in the working directory
wdir <- getwd() 

# Enter Project folder name
project_folder <- "Pac2021-036_boots"

# Directory where the ASDL files are stored
# Path must start from your working directory, check with getwd(), or full paths
ASDL_dir <- file.path(wdir, project_folder, "ASDL_Backup")

# Set the directory for saving of the master files.
# Path must start from your working directory, check with getwd(), or full paths
save_dir <- file.path(wdir, project_folder, "2.ASDL_Processed_Data")
dir.create(save_dir, recursive = TRUE) # Will warn if already exists

# Beacon ID number for ROV beacon? (not clump beacon)
ROV_beacon <- 1



#===============================================================================
# STEP 2 - START LOG FILE

# Sink output to file
rout <- file( file.path(wdir,project_folder,"2.ASDL_Data_Parser.log" ), 
              open="wt" )
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
    # If <> messages exist add <> to the start of all lines
    if (any(grep("^<", alines))){
      ind <- 1:length(alines)
      ind <- ind[!ind %in% grep("^<", alines)]
      alines[ind] <- paste0("<>",alines[ind])
    } 
    # Convert SBE backup delim to comma
    if (type == "sbe") alines <- gsub("  +", ",", alines)
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
    ds <-  alines %>% strsplit(., ",")  %>%
      map(., ~ c(X=.)) %>% bind_rows(.) %>% as.data.frame()
    # Make a copy of X1 prior to datetime formatting
    dtype <- ds$X1 # Copy first column
    # Keep only relevant columns based on type
    if( type == "tritech" ) ds <- data.frame(X1=ds$X, X2=gsub(".*>|m","", ds$X))
    if( type == "minizeus" ) ds <- ds[,c("X1","X3","X5","X7")]
    if( type == "pos" ) ds <- ds[,c("X1","X3","X4","X5")]
    if( type == "sbe" ) ds <- ds[,c("X1",paste0("X", 3:10))]
    if( type == "head" ) ds <- ds[,c("X1","X2")]
    if( type == "track" ) ds <- ds[,paste0("X", c(1:2,4:18))]
    if( type == "boots" ) ds <- ds[,paste0("X", c(1,3:10))]
    if( type == "image" ) ds <- ds[,c("X1","X4")]
    # Extract datetime in X1
    if( type == "pos" ){
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
    # Remove NA values 
    ds <- na.omit(ds)
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
  Tritech_all$Slant_range_m[Tritech_all$Slant_range_m <= 0] <- NA
  Tritech_all$Slant_range_m[Tritech_all$Slant_range_m >= 50] <- NA
  # Summary
  print(summary(Tritech_all))
  # Write
  write.csv(Tritech_all, file.path(save_dir,"Tritech_SlantRange_MasterLog.csv"),
            quote = F, row.names = F)
} else {
  stop("\nNo Tritech files were found!\n")
}

#======================#
#      Seabird CTD     #
#======================#

# List files
SBE_files <- list.files(pattern = "^SBE25", path = ASDL_dir, full.names = T)
# Run if files exist
if( length(SBE_files) > 0 ){
  # Message
  message("\nCreating 'CTD_MasterLog.csv'", "\n")
  # Apply function in parallel
  sbelist <- future_lapply(SBE_files, FUN=readASDL, type="sbe")
  # Bind into data frame
  SBE_all <- do.call("rbind", sbelist)
  # Rename
  names(SBE_all) <- c("Datetime", "Conductivity_mS_cm","Temperature_C",
                      "Depth_m","Pressure_dbar","DO_Sat_mlL","SBE43_02Conc", 
                      "Salinity_PSU","Sound_Speed_m_s")
  # Summary
  print(summary(SBE_all))
  # Check
  plot(SBE_all$Depth_m, SBE_all$Pressure_dbar)
  # Write
  write.csv(SBE_all, file.path(save_dir,"CTD_MasterLog.csv"),
            quote = F, row.names = F)
} else {
  stop("\nNo SBE files were found!\n")
}



#=========================#
#   HEMISPHERE POSITION   #
#=========================#

# List files
GPS_files <- list.files(pattern = "Hemisphere", path = ASDL_dir, full.names = T)
GPS_files <- GPS_files[grep("GPS", GPS_files, ignore.case = T)]

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
} else {
  stop("\nNo hemisphere position files were found!\n")
}


#========================#
#       SHIP HEADING     #
#========================#

# List files
head_files <- list.files(pattern = "Ship_Heading", path = ASDL_dir, full.names = T)

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
} else {
  stop("\nNo hemisphere heading files were found!\n")
}

#===============#
#   TRACKMAN   #
#===============#

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
} else {
  stop("\nNo TrackMan files were found!\n")
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
  names(ZFA_all) <- c("Datetime","MiniZeus_zoom_percent",
                      "MiniZeus_focus_percent","MiniZeus_aperture_percent")
  # Summary
  print(summary(ZFA_all))
  # Write
  write.csv(ZFA_all, file.path(save_dir,"MiniZeus_ZFA_MasterLog.csv"),
            quote = F, row.names = F)
} else {
  stop("\nNo MiniZeus files were found!\n")
}


#======================#
#      BOOTS string    #
#======================#

# List files
BOOTS_files <- list.files(pattern = "^BOOTS_String", path = ASDL_dir, full.names = T)
# Run if files exist
if( length(BOOTS_files) > 0 ){
  # Message
  message("\nCreating 'BOOTS_files_MasterLog.csv'", "\n")
  # Apply function in parallel
  bootslist <- future_lapply(BOOTS_files, FUN=readASDL, type="boots")
  # Bind into data frame
  BOOTS_all <- do.call("rbind", bootslist)
  # Rename
  names(BOOTS_all) <- c("Datetime","Longitude", "Latitude", "ROV_heading",
                        "Depth_m", "ROV_pitch", "ROV_roll", 
                        "MiniZeus_pan", "MiniZeus_tilt")
  # Summary
  print(summary(BOOTS_all))
  # Write
  write.csv(BOOTS_all, file.path(save_dir,"BOOTS_string_MasterLog.csv"),
            quote = F, row.names = F)
} else {
  stop("\nNo BOOTS string files were found!\n")
}


#======================#
#  Imagenex Altimeter  #
#======================#

# List files
Imagenex_files <- list.files(pattern = "^Imagenex", path = ASDL_dir, full.names = T)
# Run if files Imagenex_files
if( length(Imagenex_files) > 0 ){
  # Message
  message("\nCreating 'Imagenex_Atlimeter_MasterLog.csv'", "\n")
  # Apply function in parallel
  imalist <- future_lapply(Imagenex_files, FUN=readASDL, type="image")
  # Bind into dataframe
  Imagenex_all <- do.call("rbind", imalist)
  # Rename
  names(Imagenex_all) <- c("Datetime","Altitude_m")
  # Set values of 0 or less to NA
  Imagenex_all$Altitude_m[Imagenex_all$Altitude_m <= 0] <- NA
  # Check for most common low alititude value and set as NA
  # They are likely to be when BOOTs is sitting on the deck
  deckval <- sort(table(Imagenex_all$Altitude_m[Imagenex_all$Altitude_m < 1]), 
                  decreasing = T)[1]
  Imagenex_all$Altitude_m[Imagenex_all$Altitude_m == names(deckval)] <- NA
  
  # Summary
  print(summary(Imagenex_all))
  # Write
  write.csv(Imagenex_all, file.path(save_dir,"Imagenex_Atlimeter_MasterLog.csv"),
            quote = F, row.names = F)
} else {
  stop("\nNo Imagenex files were found!\n")
}




#===============================================================================
# Print end of file message and elapsed time
cat( "\nFinished: ", sep="" )
print( Sys.time( ) - sTime )

# Stop sinking output
sink( type = "message" )
sink( )
closeAllConnections()
  