#===============================================================================
# Script Name: 3_QAQC_Interpolate__Smooth.R
# Function: This script reads in the .CSV files created from "1_Hypack Data 
#           Parser.R', "2_ASDL Data Parser ****.R'. It combined hypack, ASDL and
#           CTD data outputs, filling in any gaps with the preferred and existing
#           backup data sources. Several quality control checks are completed. 
#           Lat/Longs for ship and vehicle beacon position are filtered and
#           interpolated. Filters are applied to x,y positions to remove 
#           potentially erroneous fixes. ROV beacon positions are then smoothed.
#
# Script Author: Ben Snow, adapted by Jessica Nephin
# Script Date: Sep 9, 2019 adapted in Nov 2022
# R Version: version 4.0.2



#===============================================================================
# Packages and session options

# Check if necessary packages are present, install as required.
packages <- c("lubridate","dplyr","stringr","imputeTS","geosphere","readxl")
new_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
if(length(new_packages)) install.packages(new_packages)

# Load required packages
lapply(packages, require, character.only = TRUE)

# Force display of long digit numbers
options(digits = 12)



#===============================================================================
# STEP 1 - SET PATHS AND SELECT OPTIONS, MAKE EXPORT DIRECTORY

# Set working directory
# Use getwd() if you are already in the right directory
# The project folder and needs to be in the working directory
wdir <- getwd() 

# Project folder
project_folder <- "Pac2021-036_boots"

# ROV type
# phantom or boots
rov <- "boots"

# CTD type
# RBR and SBE
ctd <- "SBE"

# Directory where Hypack processed data are stored
hypack_path <- file.path(wdir, project_folder, "1.Hypack_Processed_Data")

# Directory where CTD files are stored
CTD_path <- file.path(wdir, project_folder, "SBE25")

# Directory with ASDL master files
ASDL_path <- file.path(wdir, project_folder, "2.ASDL_Processed_Data")

# Export directories 
final_dir <- file.path(wdir, project_folder, "3.Final_Processed_Data")
dir.create(final_dir, recursive = TRUE) # Will warn if already exists
fig_dir <- file.path(wdir, project_folder, "3.Figures")
dir.create(fig_dir, recursive = TRUE) # Will warn if already exists

# Specify offsets for ship GPS source if the offsets were not set, or not set
# correctly in hypack. Offset to the port side are positive values for
# 'GPS_abeam' and offset towards the bow are positive for 'GPS_along'.
offsets <- FALSE # FALSE is no offsets need to be applied, default
# GPS_abeam <- 0
# GPS_along <- 2

# Do you want to use manual trackman ROV position as a backup ROV position?
# TRUE for yes, FALSE for no
trackman <- FALSE

# Set the value to use for the 'window' size (in seconds) of the running median 
# smoothing of the beacon position. Must be odd number! Typically from 31 to 301
smooth_window <- 301

# Set the LOESS span values. This is a parameter that described the proportion 
# of the total data set to use when weighting (e.g. span = 0.05 would use 5% 
# of the total data set as the local weighting window). Typically from .02 to .1 
loess_span = 0.05

# Set the maximum distance (in meters) that can occur between the ROV and ship
# Will differ depending on the ROV
# Question - what is this distance for phantom (200m) and boots (500m)?
max_dist <- 500

# Slant range and altimeter cut-offs. 
# What is are the maximum possible values in meters?
slant_max <- 50
alititude_max <- 20


#===============================================================================
# STEP 2 - START LOG FILE

# Sink output to file
rout <- file( file.path(wdir,project_folder,"3.QAQC_Processing.log" ), 
              open="wt" )
sink( rout, split = TRUE ) # display output on console and send to log file
sink( rout, type = "message" ) # send warnings to log file
options(warn=1) # print warnings as they occur

# Start the timer
sTime <- Sys.time( )

# Messages
message("QAQC ", project_folder, " project data on ", Sys.Date(), "\n\n")
if ( offsets ) message( "GPS abeam distance = ", GPS_abeam)
if ( offsets ) message( "GPS along distance = ", GPS_along)
message( "Smoothing window = ", smooth_window)
message( "loess span = ", loess_span)
message( "Maximum allowable distance between ship and ROV = ", max_dist, "\n")


#===============================================================================
# STEP 3 - READ IN THE HYPACK AND ASDL PROCESSED DATA


# List of all potentially existing ASDL files to load
asdl_files <- c('Hemisphere_GPS','Hemisphere_Heading','Manual_Beacon_Tracking',  
                'Tritech_SlantRange','BOOTS_string','Imagenex_Atlimeter', 'CTD', 
                'MiniZeus_ZFA','ROWETECH_DVL','ROV_Heading_Depth','Zeus_ROV_IMU')

# Load ASDL files
for( f in asdl_files){
  filepath <- file.path(ASDL_path,paste0(f, "_MasterLog.csv"))
  if( file.exists(filepath) ){
    tmp <- read.csv(filepath)
    tmp$Datetime <- ymd_hms(tmp$Datetime)
    tmp$ID <- paste0(f, ' ASDL') 
  } else {
    # Empty dataframe if ASDL file does not exist
    tmp <- data.frame(Datetime=NA)
  }
  name <- paste0(f, '_Master')
  assign(name, tmp)
}


# Load transect data processed at a 1Hz from Hypack (named: dat)
load(file=file.path(hypack_path, "HypackData_bySecond.RData"))

# Is hypack data by transect or by dive?
# Look for 'off-transect' values
if( any(dat$Dive_Phase == 'Off_transect') ) {
  grp <- "Dive_Name"
} else {
  grp <- "Transect_Name"
}
message('Hypack data processed by: ', grp, '\n')


#===============================================================================
# STEP 4 - LOAD CTD DATA

# For RBR CTD data
if (ctd == 'RBR'){
  # Read in RBR data from .xlsx files
  # Create blank data frame to fill in the loop below.
  CTD_Data <- data.frame()
  # Read in all RBR .xlsx files in the directory, merge into one larger dataframe
  RBR_files <- list.files(path=CTD_path, pattern = ".xlsx", full.names = T)
  for(i in RBR_files){
    # High max guess range, sometimes there are many rows missing at beginning
    tmp <- read_xlsx(i, sheet = "Data", skip = 1, guess_max = 10000)
    CTD_Data <- bind_rows(CTD_Data, tmp)
  }
  # Rename
  names(CTD_Data) <- c("Datetime", "Conductivity_mS_cm", "Temperature_C",
                       "Pressure_dbar", "DO_Sat_percent", "Sea_Pressure_dbar",
                       "Depth_m", "Salinity_PSU", "Sound_Speed_m_s",
                       "Specific_Cond_uS_cm", "Density_kg_m3", "DO_conc_mgL")
  # Remove duplicate datetimes
  CTD_Data <- CTD_Data[!duplicated(CTD_Data$Datetime),]
  # Add source ID
  CTD_Data$ID <- "RBR CTD XLS"
  # Summary 
  message("RBR data summary from merged excel files:")
  print(summary(CTD_Data))
  # Check to see if the RBR data is from year 2000 (default in system)
  if ( any(year(CTD_Data$Datetime) == 2000) ){
    warning("RBR year is incorrect!! No RBR CTD data with match up.")
  }
}

# For SBE CTD data
if (ctd == 'SBE'){
  # Read in SBE data from .asc files
  # Create blank data frame to fill in the loop below.
  CTD_Data <- data.frame()
  # Read in all SBE .asc files in the directory, merge into one larger dataframe
  SBE_files <- list.files(path=CTD_path, pattern = ".asc", full.names = T)
  for(i in SBE_files){
    tmp <- read.csv(i)
    CTD_Data <- bind_rows(CTD_Data, tmp)
  }
  # Datetime
  CTD_Data$Datetime <- mdy_hms(paste(CTD_Data$mm.dd.yyy, CTD_Data$hh.mm.ss))
  # Re-order
  CTD_Data <- CTD_Data[c(15,5:12)]
  # Rename
  names(CTD_Data) <- c("Datetime", "Depth_m","Temperature_C","Conductivity_mS_cm",
                       "Density_kg_m3","Salinity_PSU","Sound_Speed_m_s",
                       "DO_Sat_mlL","SBE43_02Conc")
  # Remove duplicate datetimes
  CTD_Data <- CTD_Data[!duplicated(CTD_Data$Datetime),]
  # Add source ID
  CTD_Data$ID <- "SBE CTD ASC"
}



#===============================================================================
# STEP 5 - APPLY OFFSETS TO HYPACK BEACON POSITION DATA

# Calculate the bearing and distance for offset calculations using GPS_abeam and
# GPS_along offsets. Offset for the antenna to the port side are (+) for 
# 'GPS_abeam' and offsets towards the bow are (+) for 'GPS_along'. 
# For trig purposes, abeam = opposite and along = adjacent.

if (offsets){
  # Compute length of hypotenuse to determine offset distance, in meters.
  # Offset dist will be 0 if GPS_abeam and GPS_along are 0
  offset_dist <- sqrt((GPS_abeam^2) + (GPS_along^2))
  
  # Calculate the angle from the center of the ship to the antenna location
  # Absolute value
  offset_angle <- atan(GPS_abeam/GPS_along)
  offset_angle <- abs(offset_angle * (180/pi)) #Convert to degrees.
  
  # Set bearing when the GPS either along=0 or abeam == 0 or both
  # GPS antenna dead center on the ship
  if(GPS_abeam == 0 & GPS_along == 0) {
    bearing <- 0
    # GPS antenna along keel line, forward of the center of ship
  } else if (GPS_abeam == 0 & GPS_along > 0) {
    bearing <- dat$Ship_heading
    # GPS antenna along keel line, aft of the center of ship
  } else if (GPS_abeam == 0 & GPS_along < 0) {
    bearing <- dat$Ship_heading - 180
    # GPS antenna centered fore/aft, but port of the keel line
  } else if (GPS_abeam > 0 & GPS_along == 0) {
    bearing <- dat$Ship_heading - 90
    # GPS antenna centered fore/aft, but starboard of the keel line
  } else if (GPS_abeam < 0 & GPS_along == 0) {
    bearing <- dat$Ship_heading + 90
  }
  
  # Calculate bearing with offset angle when along and abeam are not == 0
  # When GPS is starboard and aft, abeam (-) and along (-)
  if (GPS_abeam < 0 & GPS_along < 0){
    # Subtract offset angle
    bearing <- dat$Ship_heading - offset_angle - 180
    # When GPS is port and aft, abeam (+) and along (-)
  } else if (GPS_abeam > 0 & GPS_along < 0){
    # Add offset angle
    bearing <- dat$Ship_heading + offset_angle + 180
    # When GPS is starboard and forward, abeam (-) and along (+)
  } else if (GPS_abeam < 0 & GPS_along > 0){
    # Add offset angle
    bearing <- dat$Ship_heading + offset_angle
    # When GPS is port and forward, abeam (+) and along (+)
  } else if (GPS_abeam > 0 & GPS_along > 0){
    # Subtract offset angle
    bearing <- dat$Ship_heading - offset_angle
  }
  
  # If bearing is negative, add to 360
  bearing[bearing < 0 & !is.na(bearing)] <- bearing[bearing < 0 & !is.na(bearing)] + 360
  # If bearing is greater than 360, subtract 360 from bearing
  bearing[bearing > 360 & !is.na(bearing)] <- bearing[bearing > 360 & !is.na(bearing)] - 360
  
  # Calculate new lat/lon using bearing direction and offset distance
  offset_calc <- destPoint(p=dat[c("Beacon_Longitude",
                                   "Beacon_Latitude")],
                           b=bearing,
                           d=offset_dist)
  # Add new offset long/lat to dat
  dat$Beacon_Longitude <- offset_calc[,1]
  dat$Beacon_Latitude <- offset_calc[,2]
} 


#===============================================================================
# STEP 6 - USE ASDL AND RBR TO FILL IN HYPACK DATA GAPS 

# Function to fill gaps
# Arguments:
# 'tofill' is the dataframe with gaps that need filling
# 'forfilling' is the dataframe to attempt to fill the gaps with
# 'sourcefields' are the names of the columns that you want filled
# 'fillfields' are the names of the columns to fill sourcefields with
# 'type' differentiates between position and other data types
fillgaps <- function(tofill, forfilling, type="", sourcefields, fillfields ){
  # Check if fields are present in the data
  if( any(!sourcefields %in% names(tofill))  ){
    stop("'sourcefields' were not found in 'tofill' dataframe", call.=F)
  }
  if( any(!fillfields %in% names(forfilling)) ){
    stop("'fillfields' were not found in 'forfilling' dataframe", call.=F)
  }
  # Use the first field in sourcefields to find gaps
  field <- sourcefields[1]
  # Find indices with remaining ROV position gaps in 'tofill' DF
  # for position data only fill large gaps, greater than 60 seconds
  if( type == "pos"){
    gapstofill <- which(is.na(tofill[[field]]) & tofill$Beacon_Gaps > 60)
  } else {
    gapstofill <- which(is.na(tofill[[field]]))
  }
  # Find matching indices in 'forfilling' DF by datetime
  fillingrows <- match(tofill$Datetime[gapstofill], forfilling$Datetime)
  # If there are gaps to fill and matches found
  if( length(gapstofill) > 0 && any(!is.na(fillingrows)) ){
    # Fill gaps
    tofill[gapstofill, sourcefields] <- forfilling[fillingrows, fillfields]
  }
  # Remaining gaps
  if( type == "pos"){
    stillgaps <- which(is.na(tofill[[field]]) & tofill$Beacon_Gaps > 60)
  } else {
    stillgaps <- which(is.na(tofill[[field]]))
  }
  # Message
  message( length(gapstofill), " gaps detected in ", field, "\n",
           length(stillgaps), " gaps remain after filling with ", 
           fillfields[1], "\n\n")
  # Return
  return(tofill)
}


#===============#
#    POSITION   #
#===============#
# Compare hypack and manual tracking backup beacon positions by dive
tmp <- merge(dat, Manual_Beacon_Tracking_Master, by="Datetime")
# Map each dive
for (i in unique(dat$Dive_Name)){
  ot <- dat[dat$Dive_Name == i,]
  mt <- tmp[tmp$Dive_Name == i,]
  plot(ot$Ship_Longitude, ot$Ship_Latitude, 
       asp=1, main=i, pch=16, cex=.5, col="#009E73", 
       xlab = "Longitude", ylab="Latitude")
  points(ot$Beacon_Longitude, ot$Beacon_Latitude, 
         pch=16, cex=.5, col="#0072B2")
  points(mt$Beacon_Longitude.y, mt$Beacon_Latitude.y, 
         pch=16, cex=.5, col="#D55E00")
  legend("bottom", horiz=T, bty = "n", legend = c("Ship", "Hypack", "Manual"),
         col = c("#009E73","#0072B2","#D55E00"), pch=16)
}

# Fill gaps in Beacon long and lat with TrackMan manual ROV GPS
if ( trackman ){
  message( "Filling position with TrackMan manual ROV GPS:")
  dat <- fillgaps(tofill=dat,
                  forfilling=Manual_Beacon_Tracking_Master,
                  type = "pos",
                  sourcefields=c("Beacon_Longitude", "Beacon_Latitude", 
                                 "Beacon_Source"),
                  fillfields=c("Beacon_Longitude", "Beacon_Latitude", "ID") )
}

# Could fill ship position with hemisphere ASDL backup here


#==============#
#    HEADING   #
#==============#
# Check for relationship between tofill and forfilling
tmp <- merge(dat, ROV_Heading_Depth_Master, by="Datetime")
if(nrow(tmp) > 0) plot(tmp$ROV_heading.x, tmp$ROV_heading.y)
# Fill gaps in ROV heading with 'ROV_Heading_Depth_MasterLog.csv'
message( "Filling ROV heading with 'ROV_Heading_Depth_MasterLog.csv':")
dat <- fillgaps(tofill=dat,
                forfilling=ROV_Heading_Depth_Master,
                sourcefields="ROV_heading",
                fillfields="ROV_heading" )
# Check for relationship between tofill and forfilling
tmp <- merge(dat, BOOTS_string_Master, by="Datetime")
if(nrow(tmp) > 0) plot(tmp$ROV_heading.x, tmp$ROV_heading.y)
# Fill gaps in ROV heading with 'BOOTS_string_MasterLog.csv'
message( "Filling ROV heading with 'BOOTS_string_MasterLog.csv':")
dat <- fillgaps(tofill=dat,
                forfilling=BOOTS_string_Master,
                sourcefields="ROV_heading",
                fillfields="ROV_heading" )
# Check for relationship between tofill and forfilling
tmp <- merge(dat, Hemisphere_Heading_Master, by="Datetime")
if(nrow(tmp) > 0) plot(tmp$Ship_heading.x, tmp$Ship_heading.y)
# Fill gaps in ships heading with 'Hemisphere_Heading_MasterLog.csv'
message( "Filling ship heading with 'Hemisphere_Heading_MasterLog.csv':")
dat <- fillgaps(tofill=dat,
                forfilling=Hemisphere_Heading_Master,
                sourcefields="Ship_heading",
                fillfields="Ship_heading" )
# Check
plot(dat$ROV_heading)
plot(dat$Ship_heading)


#==============#
#     DEPTH    #
#==============#
# Check for relationship between tofill and forfilling
tmp <- merge(dat, CTD_Data, by="Datetime")
if(nrow(tmp) > 0) plot(tmp$Depth_m.x, tmp$Depth_m.y)
# Fill gaps in depth with 'CTD_MasterLog.csv'
message( "Filling depth with CTD data:")
dat <- fillgaps(tofill=dat,
                forfilling=CTD_Data,
                sourcefields=c("Depth_m","Depth_Source"),
                fillfields=c("Depth_m","ID"))
# Check for relationship between tofill and forfilling
tmp <- merge(dat, CTD_Master, by="Datetime")
if(nrow(tmp) > 0) plot(tmp$Depth_m.x, tmp$Depth_m.y)
# Fill gaps in depth with 'ROV_Heading_Depth_MasterLog.csv'
message( "Filling depth with CTD ASDL backup:")
dat <- fillgaps(tofill=dat,
                forfilling=CTD_Master,
                sourcefields=c("Depth_m","Depth_Source"),
                fillfields=c("Depth_m","ID"))
# Check for relationship between tofill and forfilling
tmp <- merge(dat, ROV_Heading_Depth_Master, by="Datetime")
# filter out depth error from Master
if(nrow(tmp) > 0) plot(tmp$Depth_m.x, tmp$Depth_m.y)
# Fill remaining gaps in depth with 'ROV_Heading_Depth_MasterLog.csv'
message( "Filling depth with 'ROV_Heading_Depth_MasterLog.csv':")
dat <- fillgaps(tofill=dat,
                forfilling=ROV_Heading_Depth_Master,
                sourcefields=c("Depth_m","Depth_Source"),
                fillfields=c("Depth_m","ID"))
# Check for relationship between tofill and forfilling
tmp <- merge(dat, BOOTS_string_Master, by="Datetime")
# filter out depth error from Master
if(nrow(tmp) > 0) plot(tmp$Depth_m.x, tmp$Depth_m.y)
# Fill remaining gaps in depth with 'ROV_Heading_Depth_MasterLog.csv'
message( "Filling depth with 'BOOTS_string_MasterLog.csv':")
dat <- fillgaps(tofill=dat,
                forfilling=BOOTS_string_Master,
                sourcefields=c("Depth_m","Depth_Source"),
                fillfields=c("Depth_m","ID"))

# Check
plot(dat$Depth_m)


#==================#
#    SLANT RANGE   #
#==================#
# Check for relationship between tofill and forfilling
tmp <- merge(dat, Tritech_SlantRange_Master, by="Datetime")
if(nrow(tmp) > 0) plot(tmp$Slant_range_m.x, tmp$Slant_range_m.y)
# Fill gaps in slant range with 'Tritech_SlantRange_MasterLog.csv'
message( "Filling slant range with Tritech backup:")
dat <- fillgaps(tofill=dat,
                forfilling=Tritech_SlantRange_Master,
                sourcefields="Slant_range_m",
                fillfields="Slant_range_m" )
# Check
plot(dat$Slant_range_m)


#===============#
#    ALTITUDE   #
#===============#
# Check for relationship between tofill and forfilling
tmp <- merge(dat, ROWETECH_DVL_Master, by="Datetime")
if(nrow(tmp) > 0) plot(tmp$Altitude_m.x, tmp$Altitude_m.y)
# Fill gaps in altitude with 'ROWETECH_DVL_MasterLog.csv'
message( "Filling altitude with DVL backup:")
dat <- fillgaps(tofill=dat,
                forfilling=ROWETECH_DVL_Master,
                sourcefields="Altitude_m",
                fillfields="Altitude_m" )
# Check for relationship between tofill and forfilling
tmp <- merge(dat, Imagenex_Atlimeter_Master, by="Datetime")
if(nrow(tmp) > 0) plot(tmp$Altitude_m.x, tmp$Altitude_m.y)
# Fill gaps in altitude with 'Imagenex_Atlimeter_MasterLog.csv'
message( "Filling altitude with Imagenex backup:")
dat <- fillgaps(tofill=dat,
                forfilling=Imagenex_Atlimeter_Master,
                sourcefields="Altitude_m",
                fillfields="Altitude_m" )
# Check
plot(dat$Altitude_m)



#==============#
#     SPEED    #
#==============#
# Check for relationship between tofill and forfilling
tmp <- merge(dat, ROWETECH_DVL_Master, by="Datetime")
if(nrow(tmp) > 0) plot(tmp$Speed_kts.x, tmp$Speed_kts.y)
# Fill gaps in speed with 'ROWETECH_DVL_MasterLog.csv'
message( "Filling speed with DVL backup:")
dat <- fillgaps(tofill=dat,
                forfilling=ROWETECH_DVL_Master,
                sourcefields="Speed_kts",
                fillfields="Speed_kts" )


#===============================================================================
# STEP 6 - ADD ASDL and RBR CTD DATA NOT IN HYPACK DATA 

# Merge MiniZeus fields
dat <- merge(dat, MiniZeus_ZFA_Master[names(MiniZeus_ZFA_Master) != "ID"], 
                 by = "Datetime", all.x=T)
dat <- merge(dat, Zeus_ROV_IMU_Master[names(Zeus_ROV_IMU_Master) != "ID"], 
             by = "Datetime", all.x=T)
dat <- merge(dat, BOOTS_string_Master[!names(BOOTS_string_Master) %in% 
                        c("Longitude","Latitude","Depth_m","ROV_heading","ID")], 
             by = "Datetime", all.x=T)
dat <- merge(dat, CTD_Data[!names(CTD_Data) %in% c("Depth_m","ID")], 
             by = "Datetime", all.x=T)

# Use ASDL data to fill RBR CTD data gaps
# Looks for gaps using the first field => Conductivity_mS_cm   
message( "Filling CTD gaps with ASDL CTD master backup:")
ctdfields <- names(CTD_Master)[!names(CTD_Master) %in% c("Depth_m","ID", "Datetime")]
ctdfields <- ctdfields[ctdfields %in% names(dat)]
dat <- fillgaps(tofill=dat,
                forfilling=CTD_Master,
                sourcefields=ctdfields,
                fillfields=ctdfields)
# Summary
summary(dat)


#===============================================================================
# STEP 8 - INTERPOLATE TO FILL GAPS

# Interpolate function
# Only interpolates if there are more than 2 non-NA values
interpGaps <- function( x, variable ){
  # Check total not missing values
  total_not_missing <- sum(!is.na(x[[variable]]))
  # check there is sufficient data for na_interpolation 
  if(total_not_missing >= 2) {
    # For altitude and slant range over fill 1 row gaps
    if( grepl("altitude|slant", variable, ignore.case = T) ){
      # Interpolate, max gap = 1
      x[[variable]] <- na_interpolation(x[[variable]], 
                                        option = "linear", maxgap=1)
    } else {
      # Interpolate, max gap = INF
      x[[variable]] <- na_interpolation(x[[variable]], option = "linear")
    }
  } else {
    # Don't interpolate if there aren't enough non-NA values
    x[[variable]] <- x[[variable]]
  }
  # Return
  return(x)
}

# Variables to interpolate
# ROV heading, speed, rogue or minizeus camera variables not interpolated
variables <- c("Beacon_Longitude", "Beacon_Latitude", "Ship_Longitude",
               "Ship_Latitude", "Depth_m", "Ship_heading",
               "Altitude_m", "Slant_range_m")

# Interpolate within each dive or transect by variable
for (v in variables){
  dat <- dat %>% group_by(dplyr::across(grp)) %>% 
    group_modify(~interpGaps(.x, variable={{v}})) %>% as.data.frame()
}
# Summary
message("\nSummary after variable gaps were filled with linear interpolation", "\n")
print(summary(dat))


# Set out of bound altitude and slant range to NA
# > 20m for altitude and > 10m for slant range
dat$Altitude_m[dat$Altitude_m > alititude_max] <- NA
dat$Slant_range_m[dat$Slant_range_m > slant_max] <- NA


# Add "interp" label to beacon source
dat$Beacon_Source[is.na(dat$Beacon_Source)] <- "Interpolation"
# Check
message("\nBeacon position sources:")
print(table(dat$Beacon_Source))

# Rename beacon to ROV
names(dat) <- sub("Beacon_L","ROV_L", names(dat))


#===============================================================================
# STEP 9 - REMOVE SHIP AND ROV POSITION OUTLIERS

# Order dataframe by datetime
dat <- dat[order(dat$Datetime),]

# Start while loop to remove outliers from ship positions
alongdist_ship <- 100
counter <- 1
while( any(alongdist_ship > 5) ){
  # Calculate distance between adjacent points in ship track
  alongdist_ship <- c(0, geosphere::distGeo(
    as.matrix(dat[1:(nrow(dat)-1), c("Ship_Longitude","Ship_Latitude")]),
    as.matrix(dat[2:nrow(dat), c("Ship_Longitude","Ship_Latitude")])))
  # Set distance to zero if between dives or transects
  for(i in 2:nrow(dat)){
    if( dat$Dive_Name[i-1] != dat$Dive_Name[i] ) alongdist_ship[i] <- 0
    if( dat$Transect_Name[i-1] != dat$Transect_Name[i] ) alongdist_ship[i] <- 0
  }
  # Check
  message("\nWhile loop #", counter)
  message("Distance between adjacent ship positions", "\n")
  print(summary(alongdist_ship))
  # Check
  message("\nRemoved ", length(which(alongdist_ship > 5)) ,
          " outliers greater than 5 m between along track ship positions")
  # Set long/lat values outside of range to NA
  dat$Ship_Longitude[alongdist_ship > 5] <- NA
  dat$Ship_Latitude[alongdist_ship > 5] <- NA
  # Re-interpolate ship long/lat values now that outliers have been removed
  # Variables to interpolate
  variables <- c("Ship_Longitude", "Ship_Latitude")
  # Interpolate within each dive or transect by variable
  for (v in variables){
    dat <- dat %>% group_by(dplyr::across(grp)) %>% 
      group_modify(~interpGaps(.x, variable={{v}})) %>% as.data.frame()
  }
  # Number of loops counter
  counter <- counter + 1
} # End of while loop



# Start while loop to remove outliers from ROV positions
alongdist_rov <- 100
counter <- 1
while( any(alongdist_rov > 10) ){
  # Calculate distance between adjacent points in ROV track
  alongdist_rov <- c(0, geosphere::distGeo(
    as.matrix(dat[1:(nrow(dat)-1), c("ROV_Longitude","ROV_Latitude")]),
    as.matrix(dat[2:nrow(dat), c("ROV_Longitude","ROV_Latitude")])))
  # Set distance to zero if between dives or transects
  for(i in 2:nrow(dat)){
    if( dat$Dive_Name[i-1] != dat$Dive_Name[i] ) alongdist_rov[i] <- 0
    if( dat$Transect_Name[i-1] != dat$Transect_Name[i] ) alongdist_rov[i] <- 0
  }
  # Check
  message("\nWhile loop #", counter)
  message("Distance between adjacent ROV positions", "\n")
  print(summary(alongdist_rov))
  # Check
  message("\nRemoved ", length(which(alongdist_rov > 10)) ,
          " outliers greater than 10 m between along track ROV positions")
  # Set long/lat values outside of range to NA
  dat$ROV_Longitude[alongdist_rov > 10] <- NA
  dat$ROV_Latitude[alongdist_rov > 10] <- NA
  # Re-interpolate ROV long/lat values now that outliers have been removed
  # Variables to interpolate
  variables <- c("ROV_Longitude", "ROV_Latitude")
  # Interpolate within each dive or transect by variable
  for (v in variables){
    dat <- dat %>% group_by(dplyr::across(grp)) %>% 
      group_modify(~interpGaps(.x, variable={{v}})) %>% as.data.frame()
  }
  # Number of loops counter
  counter <- counter + 1
} # End of while loop


# Calculate cross track distance between ship pos and beacon positions,
# after along track outliers have been removed and interpolated
crossdist <- geosphere::distGeo(
  as.matrix(dat[c("Ship_Longitude","Ship_Latitude")]), 
  as.matrix(dat[c("ROV_Longitude","ROV_Latitude")]))
# Check
message("\nSummary of distance between ship and ROV position", "\n")
print(summary(crossdist))

# Calculate outlier distance for each dive or transect
# Loop through each dive or transect
for (g in unique(dat[[grp]])){
  # Indices for the group
  ind <- which(dat[[grp]] == g)
  # subset by grp
  distance <- crossdist[ind]
  # Look for outliers (+/- 2.5 * Interquartile range)
  outlier_limit <- median(distance) + (2.5*IQR(distance))
  # Check
  message("\n", grp, " ", g, ": Removed ",
          length(which(distance > max_dist | distance > outlier_limit)),
          " outliers greater than ", round(min(c(outlier_limit, max_dist)),1),
          " m between ship and ROV")
  # Set long/lat values outside of range to NA
  rem <- ind[distance > max_dist | distance > outlier_limit]
  dat$ROV_Longitude[rem] <- NA
  dat$ROV_Latitude[rem] <- NA
}

# Re-interpolate ROV long/lat values now that outliers have been removed
# Variables to interpolate
variables <- c("ROV_Longitude", "ROV_Latitude")
# Interpolate within each dive or transect by variable
for (v in variables){
  dat <- dat %>% group_by(dplyr::across(grp)) %>% 
    group_modify(~interpGaps(.x, variable={{v}})) %>% as.data.frame()
}



#===============================================================================
# STEP 10 - APPLY LOESS AND RUNNING MEDIAN SMOOTHING TO ROV POSITION DATA 

# Smoothing function
# Round values to the 6th decimal
smoothCoords <- function( x, variable ){
  # loess smooth
  lname <- paste(variable, "loess", sep="_")
  x[[lname]] <- round(loess(x[[variable]] ~ as.numeric(x[['Datetime']]), 
                            span = loess_span)$fitted, 6)
  # window smooth
  sname <- paste(variable, "smoothed", sep="_")
  x[[sname]] <- round(runmed(x[[variable]], smooth_window), 6)
  # Return
  return(x)
}

# Variables to smooth
variables <- c("ROV_Longitude", "ROV_Latitude")
# Smooth using loess and window method within each dive or transect by variable
for (v in variables){
  dat <- dat %>% group_by(dplyr::across(grp)) %>% 
    group_modify(~smoothCoords(.x, variable={{v}})) %>% as.data.frame()
}

# Round unsmoothed to 6 decimals places
dat[variables] <- round(dat[variables], 6)
# Rename unsmoothed long/lat
names(dat)[names(dat) %in% variables] <- c("ROV_Longitude_unsmoothed", 
                                           "ROV_Latitude_unsmoothed")
# Rename beacon source
names(dat)[names(dat) == "Beacon_Source"] <- "ROV_Source"


# Function to add a scalebar to a base-graphics plot
# Adds a scalebar in meters roughly one fifth of plot width
myScalebar <- function(){
  # Get plot coordinates
  pc <- par("usr") 
  # Convert to mercator to get units in meters
  p1 <- geosphere::mercator(pc[c(1,3)])
  p2 <- geosphere::mercator(pc[c(2,4)])
  # Get 1/5th of width of plot as scale bar distance, 2 sig digits
  # Calculate scale bar in meractor
  dist_scale <- signif((p2[1] - p1[1]) * 0.2, digits=2)
  units_label <- paste(dist_scale, "Meters")
  x1 <- p2[1] - (dist_scale * 1.15)
  x2 <- x1 + dist_scale
  y <- p1[2] + ((p2[2] - p1[2]) * 0.05)
  # Convert scale coorinates back to lat/long
  l1 <- geosphere::mercator(c(x1, y), inverse = T)
  l2 <- geosphere::mercator(c(x2, y), inverse = T)
  # Position scale line between last two major x-axis tick marks
  # and 1/10th of the total y-range above the lower y-axis coordinate
  lines(x=c(l1[1],l2[1]), y=c(l1[2],l2[2]), lwd=4, lend=2)
  # Place the units label at the midpoint of and just below the scale line
  text(x=mean(c(l1[1],l2[1])), y=l1[2], label=units_label, adj=c(0.5, -1))
}

# Check
# Map each transect
for (i in unique(dat$Dive_Name)){
  tmp <- dat[dat$Dive_Name == i,]
  png(filename=file.path(fig_dir, paste0("Dive_", i, ".png")),
      width=12, height=10, units = "in", res = 120)
  par(mar = c(5, 4, 4, 8), xpd = TRUE)
  plot(tmp$Ship_Longitude,tmp$Ship_Latitude, 
       asp=1, main=i, pch=16, cex=.5, col="#009E73", 
       xlab = "Longitude", ylab="Latitude")
  points(tmp$Beacon_Longitude,tmp$Beacon_Latitude, 
         pch=16, cex=.5, col="#0072B2")
  points(tmp$ROV_Longitude_unsmoothed, tmp$ROV_Latitude_unsmoothed, 
         pch=16, cex=.5, col="#D55E00")
  points(tmp$ROV_Longitude_smoothed, tmp$ROV_Latitude_smoothed, 
         pch=16, cex=.4, col="grey20")
  points(tmp$ROV_Longitude_loess, tmp$ROV_Latitude_loess, 
         pch=16, cex=.4, col="grey50")
  myScalebar()
  legend("right", inset = c(-0.17, 0), horiz=F, bty = "n",
         legend = c("Ship", "ROV OG", "ROV unsmooth", "ROV smooth", "ROV loess"),
         col = c("#009E73","#0072B2","#D55E00","grey20","grey50"), pch=16)
  dev.off()
}


#===============================================================================
# STEP 11 - WRITE FINAL PROCESSED DATA TO FILE

# Final dataset
fdat <- dat[,names(dat) != 'Beacon_Gaps']
fdat <- fdat %>% relocate(Datetime)
fdat <- fdat %>% relocate(Transect_Name, .after = Dive_Name)
fdat <- fdat %>% relocate(ROV_Longitude_loess,
                         ROV_Longitude_smoothed,
                         ROV_Latitude_loess,
                         ROV_Latitude_smoothed, .after = ROV_Latitude_unsmoothed)
names(fdat)

# Save as Rdata
save(fdat, file=file.path(final_dir, paste0(project_folder, 
                                            "_SensorData_Georeferenced.RData")))
# Save all transects in one csv
write.csv(fdat, quote = F, row.names = F,
          file = file.path(final_dir, paste0(project_folder, 
                                             "_SensorData_Georeferenced.csv")))

# Export by dive or transect
for (i in unique(fdat[[grp]]) ){
  tmp <- fdat[fdat$Dive_Name == i,]
  write.csv(tmp, quote = F, row.names = F,
            file = file.path(final_dir, 
                             paste0(project_folder,
                                    "_SensorData_Georeferenced_", i, ".csv")))
}


# Write summary file
npdat <- fdat[fdat$Dive_Phase != 'Padded_transect',]
sumdat <- npdat %>% group_by(dplyr::across(grp)) %>% 
  summarise(
    Length_hrs = as.numeric(round(tail(Datetime,1) - head(Datetime,1),2)),
    Start_Longitude = head(ROV_Longitude_unsmoothed,1),
    Start_Latitude =head(ROV_Latitude_unsmoothed,1),
    End_Longitude = tail(ROV_Longitude_unsmoothed,1),
    End_Latitude = tail(ROV_Latitude_unsmoothed,1),
    Start_Depth =head(round(Depth_m),1),
    End_Depth = tail(round(Depth_m),1),
    Max_Depth = max(round(Depth_m))
)
write.csv(sumdat, quote = F, row.names = F,
          file = file.path(final_dir, paste0(project_folder,"_DiveSummaries.csv")))



#===============================================================================
# Print end of file message and elapsed time
cat( "\nFinished: ", sep="" )
print( Sys.time( ) - sTime )

# Stop sinking output
sink( type = "message" )
sink( )
closeAllConnections()


