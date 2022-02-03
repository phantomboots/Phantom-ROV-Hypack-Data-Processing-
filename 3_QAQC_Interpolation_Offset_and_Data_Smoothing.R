#===============================================================================
# Script Name: 3_QAQC_Interpolation__Offset_and_Data_Smoothing.R
# Function: This script reads in the .CSV files created from "1_Hypack Data 
#           Parser_****.R', "2_ASDL Data Processing.R' and"2b_Manual Beacon 
#           Position Calculator.R". It fills in the best position and depth data 
#           source for each dive, and interpolates Lat/Longs for Ship GPS 
#           position and vehicle beacon. Next, distance to GPS track is 
#           calculated for each beacon point, and points outside of median+1.5 *
#           IQRare identified as outliers, and beacon fixes at these time points 
#           are removed. Beacon positions are interpolated a second time (with 
#           outliers removed). Beacon positions are then smoothed using a 
#           rolling median (overlapping medians), using stats::runmed(). 
#           Bandwidth is set in the 'EDIT THESE DATA' portion of these scripts. 
#           Final data for each dive are written to .CSV.
#
# Script Author: Ben Snow
# Script Date: Sep 9, 2019
# R Version: 3.5.1


################################################################################
#                                    CHANGE LOG                                #
################################################################################
#
# May 13, 2020: Based on conversations with J.Nephin and S. Jeffery, decided to 
#               add linear interpolation of the depth data stream. Depth record 
#               completion now checks ASDL to see if additional data records can 
#               be found.
# May 24, 2020: Added a check to see if there were additional records that could 
#               be recovered from ASDL for Altitude and Slant Range. FOR 2019 
#               ONLY, added loop to pull Bottom_Velocity_Y (forward velocity). 
#               For some dives in 2019, the 3D dimensional velocity had been 
#               recorded by Hypack, rather then simply forward velocity. Added 
#               linear interpolation of Slant Range and Altitude, but only for 
#               cases where there is just one NA value in a row. This is done by 
#               setting zoo::na.approx(..., maxgap = 1).
# June 3, 2020: Reworked the initial loop that checks ASDL to replace NA values 
#               from the Hypack read-in. This was previously only working 
#               properly for the positional data streams (it checked that GAPS 
#               == T for all data, not just position)
# June 3, 2020: In some cases, interpolation of a -9999 and a normal Altitude or
#               Slant Range values produced values in the range of -4000 or 
#               -5000. These is now a check against this, and values are reset 
#               to -9999 after interpolation.
# July 2, 2020: Removed the portion of this script that was previously used to 
#               generate .KML and .SHP files, and put it in a new script called
#               "4b_Generate_KML_and_Shapefiles.R"
# Aug 20, 2021: Added if() statement to check if the manual beacon tracking 
#               script has been run, before trying to interpolate from this data 
#               source. If this script has not been run, skip this portion.
# Jan 2022: - Wrote function for filling gaps
#           - Applied function to each ASDL source used for filling gaps
#           - Returns number of gaps detected and filled each time
#           - Fixed speed unit error, used to replace knots with m/s
#           - Check the relationship between variables in tofill and forfilling
#             before filling gaps
#           - Question: what to do when relationship is bad, don't fill?
#             eg. speed_kts
#           - attempted to make code more explicit, removed use of column order
#           - rewrote offset section to fix issue with if statements, need to
#             confirm it is working as intended
#           - Using distGeo instead of distm function for step 7
#           - Only removing upper qauntile outliers (not lower)
#           - Rounding to 6 decimal places
#           - added log file
#           - saves transect maps to file comparing rov and ship positions
#           - exports all transects and each transect NAV files to csv
################################################################################

# -- fix issue with transect P10056.1
# -- add outlier checks after smoothing?

#===============================================================================
# Packages and session options

# Check if necessary packages are present, install as required.
packages <- c("lubridate","dplyr","stringr","imputeTS","geosphere")
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

# Name of Ship used in the survey
ship_name <- "CCGS_Vector"

# Project folder
project_folder <- "Pac2021-054_phantom"

# Directory where Hypack processed data are stored
hypack_path <- file.path(wdir, project_folder, "Data/Initial_Processed_Data")

# Directory with ASDL master files
ASDL_path <- file.path(wdir, project_folder, 
                       "Data/Advanced_Serial_Data_Logger/Full_Cruise")

# Export directories 
final_dir <- file.path(wdir, project_folder, "Data/Secondary_Processed_Data")
dir.create(final_dir, recursive = TRUE) # Will warn if already exists
fig_dir <- file.path(wdir, project_folder, "Data/Figures")
dir.create(fig_dir, recursive = TRUE) # Will warn if already exists

# Specify offsets for ship GPS source. If more than one GPS is used, specify 
# both sources independently. Offset to the port side are positive values for 
# 'GPS_abeam' and offset towards the bow are positive for 'GPS_along'.
# Question: are these the distances from the center of the ship, or from the 
# ROV transponder? 
GPS_abeam <- -4.1
GPS_along <- -12.57

# Set the value to use for the 'window' size (in seconds) of the running median 
# smoothing of the beacon position.
smooth_window <- 31

# Set the LOESS span values. This is a parameter that described the proportion 
# of the total data set to use when weighting (e.g. span = 0.05 would use 5% 
# of the total data set as the local weighting window).
loess_span = 0.05

# Set the maximum distance (in meters) that can occur between the ROV and ship
# Will differ depending on the ROV
# Question - what is this distance for phantom (100m) and boots (1000m)?
max_dist <- 100



#===============================================================================
# STEP 2 - START LOG FILE

# Sink output to file
rout <- file( file.path(save_dir, "QAQC_Interp_Smoothing.log" ), open="wt" )
sink( rout, split = TRUE ) # display output on console and send to log file
sink( rout, type = "message" ) # send warnings to log file
options(warn=1) # print warnings as they occur

# Start the timer
sTime <- Sys.time( )

# Messages
message("QAQC ", project_folder, " project data on ", Sys.Date(), "\n\n")
message( "GPS abeam distance = ", GPS_abeam)
message( "GPS along distance = ", GPS_along)
message( "Smoothing window = ", smooth_window)
message( "loess span = ", loess_span)
message( "Maximum allowable distance between ship and ROV = ", max_dist, "\n\n")


#===============================================================================
# STEP 3 - READ IN THE HYPACK AND ASDL PROCESSED DATA 

# Read all Master Log files
Slant_Range_Master <- read.csv(file.path(ASDL_path,"Tritech_SlantRange_MasterLog.csv"))
Slant_Range_Master$Datetime <- ymd_hms(Slant_Range_Master$Datetime)
Manual_Track_Master <- read.csv(file.path(ASDL_path,"Manual_Beacon_Tracking_MasterLog.csv"))
Manual_Track_Master$Datetime <- ymd_hms(Manual_Track_Master$Datetime)
Manual_Track_Master$ID <- "Manual tracking backup"
Hemisphere_Master <- read.csv(file.path(ASDL_path,"Hemisphere_GPS_MasterLog.csv"))
Hemisphere_Master$Datetime <- ymd_hms(Hemisphere_Master$Datetime)
Hemisphere_Master$ID <- "Ship GPS backup"
Ship_Heading_Master <- read.csv(file.path(ASDL_path,"Hemisphere_Heading_MasterLog.csv"))
Ship_Heading_Master$Datetime <- ymd_hms(Ship_Heading_Master$Datetime)
DVL_Master <- read.csv(file.path(ASDL_path,"ROWETECH_DVL_MasterLog.csv"))
DVL_Master$Datetime <- ymd_hms(DVL_Master$Datetime)
ROV_Heading_Depth_Master <- read.csv(file.path(ASDL_path,"ROV_Heading_Depth_MasterLog.csv"))
ROV_Heading_Depth_Master$Datetime <- ymd_hms(ROV_Heading_Depth_Master$Datetime)
RBR_Master <- read.csv(file.path(ASDL_path,"RBR_CTD_MasterLog.csv"))
RBR_Master$Datetime <- ymd_hms(RBR_Master$Datetime)
RBR_Master$ID <- "RBR CTD backup"
MiniZeus_ZFA_Master <- read.csv(file.path(ASDL_path,"MiniZeus_ZFA_MasterLog.csv"))
MiniZeus_ZFA_Master$Datetime <- ymd_hms(MiniZeus_ZFA_Master$Datetime)
ROV_MiniZeus_IMUS_Master <- read.csv(file.path(ASDL_path,"Zeus_ROV_IMU_MasterLog.csv"))
ROV_MiniZeus_IMUS_Master$Datetime <- ymd_hms(ROV_MiniZeus_IMUS_Master$Datetime)

# Read in transect data processed at a 1Hz from Hypack (named: ondat)
load(file=file.path(hypack_path, "HypackData_onTransect.RData"))
summary(ondat)


#===============================================================================
# STEP 4 - USE ASDL TO FILL IN HYPACK DATA GAPS 

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
# Check for relationship between tofill and forfilling
tmp <- merge(ondat, Manual_Track_Master, by="Datetime")
if(nrow(tmp) > 0) plot(tmp$Beacon_Longitude.x, tmp$Beacon_Longitude.y)
# Fill gaps in Beacon long and lat with TrackMan manual ROV GPS
message( "Filling position with TrackMan manual ROV GPS:")
ondat <- fillgaps(tofill=ondat,
                  forfilling=Manual_Track_Master,
                  type = "pos",
                  sourcefields=c("Beacon_Longitude", "Beacon_Latitude", 
                                 "Beacon_Source"),
                  fillfields=c("Beacon_Longitude", "Beacon_Latitude", "ID") )
# Check for relationship between tofill and forfilling
tmp <- merge(ondat, Hemisphere_Master, by="Datetime")
if(nrow(tmp) > 0) plot(tmp$Beacon_Longitude, tmp$Longitude)
# Fill remaining gaps in Beacon long and lat with ship GPS backup (Hemisphere GPS)
message( "Filling position with ship GPS backup (Hemisphere GPS):")
ondat <- fillgaps(tofill=ondat,
                  forfilling=Hemisphere_Master,
                  type = "pos",
                  sourcefields=c("Beacon_Longitude", "Beacon_Latitude", 
                                 "Beacon_Source"),
                  fillfields=c("Longitude", "Latitude", "ID") )

#==============#
#    HEADING   #
#==============#
# Check for relationship between tofill and forfilling
tmp <- merge(ondat, ROV_Heading_Depth_Master, by="Datetime")
if(nrow(tmp) > 0) plot(tmp$ROV_heading.x, tmp$ROV_heading.y)
# Fill gaps in ROV heading with 'ROV_Heading_Depth_MasterLog.csv'
message( "Filling ROV heading with 'ROV_Heading_Depth_MasterLog.csv':")
ondat <- fillgaps(tofill=ondat,
                  forfilling=ROV_Heading_Depth_Master,
                  sourcefields="ROV_heading",
                  fillfields="ROV_heading" )
# Check for relationship between tofill and forfilling
tmp <- merge(ondat, Ship_Heading_Master, by="Datetime")
if(nrow(tmp) > 0) plot(tmp$Ship_heading.x, tmp$Ship_heading.y)
# Fill gaps in ships heading with 'Hemisphere_Heading_MasterLog.csv'
message( "Filling ship heading with 'Hemisphere_Heading_MasterLog.csv':")
ondat <- fillgaps(tofill=ondat,
                  forfilling=Ship_Heading_Master,
                  sourcefields="Ship_heading",
                  fillfields="Ship_heading" )

#==============#
#     DEPTH    #
#==============#
# Check for relationship between tofill and forfilling
tmp <- merge(ondat, RBR_Master, by="Datetime")
if(nrow(tmp) > 0) plot(tmp$Depth_m.x, tmp$Depth_m.y)
# Fill gaps in depth with 'ROV_Heading_Depth_MasterLog.csv'
message( "Filling depth with RBR CTD backup:")
ondat <- fillgaps(tofill=ondat,
                  forfilling=RBR_Master,
                  sourcefields="Depth_m",
                  fillfields="Depth_m" )
# Check for relationship between tofill and forfilling
tmp <- merge(ondat, ROV_Heading_Depth_Master, by="Datetime")
if(nrow(tmp) > 0) plot(tmp$Depth_m.x, tmp$Depth_m.y)
# Fill remaining gaps in depth with 'ROV_Heading_Depth_MasterLog.csv'
message( "Filling depth with 'ROV_Heading_Depth_MasterLog.csv':")
ondat <- fillgaps(tofill=ondat,
                  forfilling=ROV_Heading_Depth_Master,
                  sourcefields="Depth_m",
                  fillfields="Depth_m" )

#==================#
#    SLANT RANGE   #
#==================#
# Check for relationship between tofill and forfilling
tmp <- merge(ondat, Slant_Range_Master, by="Datetime")
if(nrow(tmp) > 0) plot(tmp$Slant_range_m.x, tmp$Slant_range_m.y)
# Fill gaps in slant range with 'Tritech_SlantRange_MasterLog.csv'
message( "Filling slant range with Tritech backup:")
ondat <- fillgaps(tofill=ondat,
                  forfilling=Slant_Range_Master,
                  sourcefields="Slant_range_m",
                  fillfields="Slant_range_m" )

#===============#
#    ALTITUDE   #
#===============#
# Check for relationship between tofill and forfilling
tmp <- merge(ondat, DVL_Master, by="Datetime")
if(nrow(tmp) > 0) plot(tmp$Altitude_m.x, tmp$Altitude_m.y)
# Question why leave out of range values as -9999, instead of NA? 
# Fill gaps in altitude with 'ROWETECH_DVL_MasterLog.csv'
message( "Filling altitude with DVL backup:")
ondat <- fillgaps(tofill=ondat,
                  forfilling=DVL_Master,
                  sourcefields="Altitude_m",
                  fillfields="Altitude_m" )

#==============#
#     SPEED    #
#==============#
# Check for relationship between tofill and forfilling
tmp <- merge(ondat, DVL_Master, by="Datetime")
if(nrow(tmp) > 0) plot(tmp$Speed_kts.x, tmp$Speed_kts.y)
# Fill gaps in speed with 'ROWETECH_DVL_MasterLog.csv'
message( "Filling speed with DVL backup:")
ondat <- fillgaps(tofill=ondat,
                  forfilling=DVL_Master,
                  sourcefields="Speed_kts",
                  fillfields="Speed_kts" )
  


#===============================================================================
# STEP 5 - ADD ASDL NOT IN HYPACK DATA 

# Merge MiniZeus fields
ondat <- left_join(ondat, MiniZeus_ZFA_Master, by = "Datetime")
ondat <- left_join(ondat, ROV_MiniZeus_IMUS_Master, by = "Datetime")

# Summary
summary(ondat)

# Save data after ASDL was used to fill gaps and extra fields added
save(ondat, file=file.path(hypack_path, "HypackData_wASDL_onTransect.RData"))



#===============================================================================
# STEP 6 - INTERPOLATE TO FILL GAPS

# Load ondat if the first part was run in a previous session
if (!exists("ondat")){
  load(file=file.path(hypack_path, "HypackData_wASDL_onTransect.RData"))
}

# Interpolate function
# Only interpolates if there are more than 2 non-NA values
interpGaps <- function( dat, variable ){
  # Check total not missing values
  total_not_missing <- sum(!is.na(dat[[variable]]))
  # check there is sufficient data for na_interpolation 
  if(total_not_missing >= 2) {
    # For altitude and slant range over fill 1 row gaps
    if( grepl("altitude|slant", variable, ignore.case = T) ){
      # Interpolate, max gap = 1
      dat[[variable]] <- na_interpolation(dat[[variable]], option = "linear", maxgap=1)
    } else {
      # Interpolate, max gap = INF
      dat[[variable]] <- na_interpolation(dat[[variable]], option = "linear")
    }
  }else {
    # Don't interpolate if there aren't enough non-NA values
    dat[[variable]] <- dat[[variable]]
  }
  # Return
  return(dat)
}

# Variables to interpolate
# ROV heading, speed, rogue or minizeus camera variables not interpolated
variables <- c("Beacon_Longitude", "Beacon_Latitude", "Ship_Longitude", 
               "Ship_Latitude", "Depth_m", "Ship_heading", 
               "Altitude_m", "Slant_range_m")

# Interpolate within each transect by variable
for (v in variables){
  ondat <- ondat %>% group_by(Transect_Name) %>% 
    group_modify(~interpGaps(.x, variable={{v}})) %>% as.data.frame()
}
# Summary
message("\nSummary after variable gaps were filled with linear interpolation", "\n")
print(summary(ondat))

# Set out of bound alitude and slant range to NA
# > 20m for altitude and > 10m for slant range
ondat$Altitude_m[ondat$Altitude_m > 20] <- NA
ondat$Slant_range_m[ondat$Slant_range_m > 10] <- NA

# Add "interp" label to beacon source
ondat$Beacon_Source[is.na(ondat$Beacon_Source)] <- "Interpolation"
# Check
message("\nBeacon position sources:", "\n")
print(table(ondat$Beacon_Source))



#===============================================================================
# STEP 7 - APPLY OFFSETS TO POSITIONS DATA

# Calculate the bearing and distance from the center of the ship to the GPS 
# antenna. The bearing and distance are used to calculate new lat and long
# coordinates at the GPS antenna location (offset from the center of the ship).
# Offset for the antenna to the port side are (+) for 'GPS_abeam' and offsets 
# towards the bow are (+) for 'GPS_along'. For trig purposes, abeam = opposite 
# and along = adjacent.

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
  bearing <- ondat$Ship_heading
  # GPS antenna along keel line, aft of the center of ship
} else if (GPS_abeam == 0 & GPS_along < 0) { 
  bearing <- ondat$Ship_heading - 180
  # GPS antenna centered fore/aft, but port of the keel line
} else if (GPS_abeam > 0 & GPS_along == 0) { 
  bearing <- ondat$Ship_heading - 90
  # GPS antenna centered fore/aft, but starboard of the keel line
} else if (GPS_abeam < 0 & GPS_along == 0) { 
  bearing <- ondat$Ship_heading + 90
}

# Calculate bearing with offset angle when along and abeam are not == 0 
# When GPS is starboard and aft, abeam (-) and along (-)
if (GPS_abeam < 0 & GPS_along < 0){
  # Subtract offset angle
  bearing <- ondat$Ship_heading - offset_angle - 180
# When GPS is port and aft, abeam (+) and along (-)
} else if (GPS_abeam > 0 & GPS_along < 0){
  # Add offset angle
  bearing <- ondat$Ship_heading + offset_angle + 180
# When GPS is starboard and forward, abeam (-) and along (+)
} else if (GPS_abeam < 0 & GPS_along > 0){
  # Add offset angle
  bearing <- ondat$Ship_heading + offset_angle 
# When GPS is port and forward, abeam (+) and along (+)
} else if (GPS_abeam > 0 & GPS_along > 0){
  # Subtract offset angle 
  bearing <- ondat$Ship_heading - offset_angle
}

# If bearing is negative, add to 360
bearing[bearing < 0] <- bearing[bearing < 0] + 360
# If bearing is greater than 360, subtract 360 from bearing
bearing[bearing > 360] <- bearing[bearing > 360] - 360

# Apply offsets from GPS to center of ship using bearing and offset_dist
# Question: should this offset only be applied to specific beacon sources? 
# e.g. primary but not ship?
offsets <- destPoint(p=ondat[c("Beacon_Longitude",
                               "Beacon_Latitude")],
                     b=bearing, 
                     d=offset_dist)
# Add new offset values to ondat
ondat$Beacon_Longitude_offset <- offsets[,1]
ondat$Beacon_Latitude_offset <- offsets[,2]



#===============================================================================
# STEP 8 - REMOVE BEACON OUTLIERS

# Calculate cross track distance between GPS track of the Ship and beacon 
# position. Do this by creating a point distance matrix between each 
# interpolated beacon position, and the Ship_GPS point for that same second
crossdist <- geosphere::distGeo(
  as.matrix(ondat[c("Ship_Longitude","Ship_Latitude")]), 
  as.matrix(ondat[c("Beacon_Longitude_offset",
                    "Beacon_Latitude_offset")]))
# Check
message("\nSummary of distance between ship and ROV position", "\n")
print(summary(crossdist))

summary(crossdist)
# Look for outliers (+/- 1.5*Interquartile range)
outlier_limit <- median(crossdist) + (1.5*IQR(crossdist))
# Remove distances greater than the max_dist and upper
crossdist[crossdist > crossdist | crossdist > outlier_limit] <- NA
# Check
message("\nSummary of distance between ship and ROV position", 
        " after outliers removed", "\n")
print(summary(crossdist))
# Set long/lat values outside of range to NA
ondat$Beacon_Longitude_offset[is.na(crossdist)] <- NA
ondat$Beacon_Longitude_offset[is.na(crossdist)] <- NA

# Re-interpolate Beacon long/lat values now that outliers have been removed
# Variables to interpolate
variables <- c("Beacon_Longitude_offset", 
               "Beacon_Latitude_offset")
# Interpolate within each transect by variable
for (v in variables){
  ondat <- ondat %>% group_by(Transect_Name) %>% 
    group_modify(~interpGaps(.x, variable={{v}})) %>% as.data.frame()
}
# Summary
summary(ondat)


#===============================================================================
# STEP 9 - APPLY LOESS AND RUNNING MEDIAN SMOOTHING TO ROV POSITION DATA 

# Smoothing function
# Rounds the values of the smoothed data to 6 decimal places
smoothCoords <- function( dat, variable ){
  # base variable name
  basename <- sub("_offset.*","", variable)
  # loess smooth
  lname <- paste(basename, "smoothed_loess", sep="_")
  dat[[lname]] <- round(loess(dat[[variable]] ~ as.numeric(dat$Datetime), 
                        span = loess_span)$fitted, 6)
  # window smooth
  sname <- paste(basename, "smoothed_window", sep="_")
  dat[[sname]] <- round(runmed(dat[[variable]], smooth_window), 6)
  # Return
  return(dat)
}

# Variables to smooth
variables <- c("Beacon_Longitude_offset", 
               "Beacon_Latitude_offset")
# Smooth using loess and window method within each transect by variable
for (v in variables){
  ondat <- ondat %>% group_by(Transect_Name) %>% 
    group_modify(~smoothCoords(.x, variable={{v}})) %>% as.data.frame()
}

# Check
# Map each transect
for (i in unique(ondat$Transect_Name)){
  tmp <- ondat[ondat$Transect_Name == i,]
  png(filename=file.path(fig_dir, paste0("Transect_", i, ".png")),
      width=10, height=10, units = "in", res = 120)
  plot(tmp$Ship_Longitude,tmp$Ship_Latitude, 
       asp=1, main=i, pch=16, cex=.3, col="#009E73", 
       xlab = "Longitude", ylab="Latitude")
  points(tmp$Beacon_Longitude,tmp$Beacon_Latitude, 
         pch=16, cex=.3, col="#0072B2")
  points(tmp$Beacon_Longitude_offset,
         tmp$Beacon_Latitude_offset, pch=16, cex=.4, col="#D55E00")
  points(tmp$Beacon_Longitude_smoothed_window,
         tmp$Beacon_Latitude_smoothed_window, pch=16, cex=.4, col="grey20")
  points(tmp$Beacon_Longitude_smoothed_loess,
         tmp$Beacon_Latitude_smoothed_loess, pch=16, cex=.4, col="grey50")
  legend("bottom", horiz=T, bty = "n",
         legend = c("Ship", "ROV OG", "ROV offset", "ROV window", "ROV loess"),
         col = c("#009E73","#0072B2","#D55E00","grey20","grey50"), pch=16)
  dev.off()
}



#===============================================================================
# STEP 10 - WRITE FINAL PROCESSED DATA TO FILE


# Fields to include in final processed files
flds <- c("Datetime","Transect_Name", "Dive_Phase", 
          "Beacon_Longitude_smoothed_window", "Beacon_Latitude_smoothed_window",
          "Beacon_Longitude_smoothed_loess", "Beacon_Latitude_smoothed_loess",
          "Beacon_Longitude_offset", "Beacon_Latitude_offset", "Beacon_Source", 
          "Ship_Longitude", "Ship_Latitude", "Depth_m", "Depth_Source",
          "ROV_heading", "Ship_heading", "Speed_kts", "Altitude_m", 
          "Slant_range_m", "Rogue_roll", "Rogue_pitch", "MiniZeus_zoom_percent",
          "MiniZeus_focus_percent", "MiniZeus_aperture_percent", 
          "MiniZeus_pitch", "MiniZeus_roll", "ROV_pitch", "ROV_roll")
# Save all transects in one csv
write.csv(ondat[flds], quote = F, row.names = F,
          file = file.path(final_dir, 
                           paste0(project_folder, "_alltransect_NAV.csv")))


# Export by transect
for (i in unique(ondat$Transect_Name)){
  tmp <- ondat[ondat$Transect_Name == i,]
  write.csv(tmp[flds], quote = F, row.names = F,
            file = file.path(final_dir, 
                             paste0(project_folder, "_", i, "_NAV.csv")))
}




#===============================================================================
# Print end of file message and elapsed time
cat( "\nFinished: ", sep="" )
print( Sys.time( ) - sTime )

# Stop sinking output
sink( type = "message" )
sink( )
closeAllConnections()