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
#           - Question: what to do when relationship is bad, check and don't fill
#             for example depth and heading from ROV_Heading_Depth_Master?
################################################################################



#===============================================================================
# Packages and session options

# Check if necessary packages are present, install as required.
packages <- c("lubridate","readxl","readr","dplyr","stringr",
              "rgdal","zoo","geosphere")
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

# Export directory 
final_dir <- file.path(wdir, project_folder, "Data/Secondary_Processed_Data")
dir.create(final_dir, recursive = TRUE) # Will warn if already exists

# Specify offsets for ship GPS source. If more than one GPS is used, specify 
# both sources independently. Offset to the port side are positive values for 
# 'GPS_abeam' and offset towards the bow are positive for 'GPS_along'.
GPS_abeam <- -4.1
GPS_along <- -12.57

# Set the value to use for the 'window' size (in seconds) of the running median 
# smoothing of the beacon position.
smooth_window <- 31

# Set the LOESS span values. This is a parameter that described the proportion 
# of the total data set to use when weighting (e.g. span = 0.05 would use 5% 
# of the total data set as the local weighting window).
loess_span = 0.05


#===============================================================================
# STEP 2 - READ IN THE HYPACK AND ASDL PROCESSED DATA 

# Read all Master Log files
Slant_Range_Master <- read.csv(file.path(ASDL_path,"Tritech_SlantRange_MasterLog.csv"))
Slant_Range_Master$Datetime <- ymd_hms(Slant_Range_Master$Datetime)
MiniZeus_ZFA_Master <- read.csv(file.path(ASDL_path,"MiniZeus_ZFA_MasterLog.csv"))
MiniZeus_ZFA_Master$Datetime <- ymd_hms(MiniZeus_ZFA_Master$Datetime)
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
ROV_MiniZeus_IMUS_Master <- read.csv(file.path(ASDL_path,"Zeus_ROV_IMU_MasterLog.csv"))
ROV_MiniZeus_IMUS_Master$Datetime <- ymd_hms(ROV_MiniZeus_IMUS_Master$Datetime)
RBR_Master <- read.csv(file.path(ASDL_path,"RBR_CTD_MasterLog.csv"))
RBR_Master$Datetime <- ymd_hms(RBR_Master$Datetime)
RBR_Master$ID <- "RBR CTD backup"

# Read in transect data processed at a 1Hz from Hypack (named: ondat)
load(file=file.path(hypack_path, "HypackData_onTransect.RData"))
summary(ondat)


#===============================================================================
# STEP 3 - USE ASDL TO FILL IN HYPACK DATA GAPS 

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
    gapstofill <- which(is.na(tofill[[field]]) & tofill$BeaconGaps > 60)
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
    stillgaps <- which(is.na(tofill[[field]]) & tofill$BeaconGaps > 60)
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
                                 "BeaconSource"),
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
                                 "BeaconSource"),
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
  



  


############STEP 4 - COERCE THE LAT/LONGS TO A ZOO OBJECT AND INTERPOLATE MISSING POSITIONS##########

#Zoo objects are 'totally ordered observations', and each observation must be unique. Coercing the Lat/Long for the beacon data to a Zoo 
#object allows the use zoo::na.approx function, to interpolate missing records in the data series.
#However, don't want the data stored as a matrix at the end of this process, so it is rebinded back into a data frame at the end of this loop.

for(i in unique(Dives))
{
  name <- get(i)
  Long <- zoo(name$Main_Beacon_Long, order.by = name$date_time)
  Lat <- zoo(name$Main_Beacon_Lat, order.by = name$date_time)
  Long_intepolated <- na.approx(Long, rule = 2) #Rule 2 means interpolate both forwards and backwards in the time series.
  Long_intepolated <- as.matrix(Long_intepolated) #Convert back to a base object class (a matrix) to extract the components of the zoo object.
  Main_Beacon_Long_interp <- Long_intepolated[1:length(Long_intepolated)]
  Lat_intepolated <- na.approx(Lat, rule = 2) #Rule 2 means interpolate both forwards and backwards in the time series.
  Lat_intepolated <- as.matrix(Lat_intepolated) #Convert back to a base object class (a matrix) to extract the components of the zoo object.
  Main_Beacon_Lat_interp <- Lat_intepolated[1:length(Lat_intepolated)]
  assign(i, cbind(name, Main_Beacon_Long_interp, Main_Beacon_Lat_interp))
}

#Coerce the Lat/Longs for the Ship's GPS position to a Zoo object and interpolate, in the same manner as above. Again, re-bind it to a data frame
#at the end of the loop.

for(i in unique(Dives))
{
  name <- get(i)
  Long <- zoo(name$Ship_Long, order.by = name$date_time)
  Lat <- zoo(name$Ship_Lat, order.by = name$date_time)
  Long_intepolated <- na.approx(Long, rule = 2) #Rule 2 means interpolate both forwards and backwards in the time series.
  Long_intepolated <- as.matrix(Long_intepolated) #Convert back to a base object class (a matrix) to extract the components of the zoo object.
  Ship_Long_interp <- Long_intepolated[1:length(Long_intepolated)]
  Lat_intepolated <- na.approx(Lat, rule = 2) #Rule 2 means interpolate both forwards and backwards in the time series.
  Lat_intepolated <- as.matrix(Lat_intepolated) #Convert back to a base object class (a matrix) to extract the components of the zoo object.
  Ship_Lat_interp <- Lat_intepolated[1:length(Lat_intepolated)]
  assign(i, cbind(name, Ship_Long_interp, Ship_Lat_interp))
}

#############################STEP 5 - ADD IN ALL OTHER DATA SOURCES#####################################################

#Loop through all the dives that have been loaded in, and merge all the various data sets into a single data frame for each dive.
#use left_join() to add additional columns onto the data frame, matching them by timestamp.

for(i in unique(Dives))
{
  name <- get(i)
  name <- left_join(name, MiniZeus_ZFA_Master, by = "date_time")
  name <- left_join(name, ROV_MiniZeus_IMUS_Master, by = "date_time")
  name <- name[,c(1:8,20:23,9:19,24:30)]
  names(name) <- c("date_time","Transect_name","Main_Beacon_Long_raw","Main_Beacon_Lat_raw","Secondary_Beacon_Long_Raw","Secondary_Beacon_Lat_Raw",
                    "Ship_Long_raw","Ship_Lat_raw","Main_Beacon_Long_interp","Main_Beacon_Lat_interp","Ship_Long_interp","Ship_Lat_interp","Depth_m",
               "Phantom_heading","Ship_heading","Speed_kts","Altitude_m","Slant_Range_m","Rogue_pitch","Rogue_roll","Best_Depth_Source","Best_Position_Source","Gaps",
               "MiniZeus_zoom_percent","MiniZeus_focus_percent","MiniZeus_aperture_percent","MiniZeus_pitch","MiniZeus_roll","ROV_pitch","ROV_roll")
  name$Best_Position_Source[is.na(name$Best_Position_Source)] <- "Linear Interpolation"  #Flag the instances where linear interpolation was performed
  assign(i, name)
}

############STEP 6 - COERCE HEADING DATA TO A ZOO OBJECT AND INTERPOLATE MISSING DATA##########

# Coerce the Ship's Heading data to a zoo object, and interpolate missing records. The same process as in the previous section.

for(i in unique(Dives))
{
  name <- get(i)
  Heading <- zoo(name$Ship_heading, order.by = name$date_time)
  Heading_intepolated <- na.approx(Heading, rule = 2) #Rule 2 means interpolate both forwards and backwards in the time series.
  Heading_intepolated <- as.matrix(Heading_intepolated) #Convert back to a base object class (a matrix) to extract the components of the zoo object.
  Heading_intepolated <- Heading_intepolated[1:length(Heading_intepolated)]
  assign(i, mutate(name, Ship_heading = Heading_intepolated)) #Replace the column with missing values with the one that were just interpolated
}


############STEP 7 - COERCE THE DEPTH DATA TO A ZOO OBJECT AND INTERPOLATE MISSING DATA##############

# Coerce the depth data to a zoo object, and interpolate missing records. The same process as in the previous section.

for(i in unique(Dives))
{
  name <- get(i)
  Depth <- zoo(name$Depth_m, order.by = name$date_time)
  Depth_interpolated <- na.approx(Depth, rule = 2) #Rule 2 means interpolate both forwards and backwards in the time series.
  Depth_interpolated <- as.matrix(Depth_interpolated) #Convert back to a base object class (a matrix) to extract the components of the zoo object.
  Depth_interpolated <- Depth_interpolated[1:length(Depth_interpolated)]
  assign(i, mutate(name, Depth_m = Depth_interpolated)) #Replace the column with missing values with the one that were just interpolated
}

############STEP 8 - COERCE THE SLANT RANGE AND ALTITUDE DATA TO ZOO OBJECTS AND INTERPOLATE MISSING DATA##############

# Coerce the MiniZeus slant range and ROV altitude data to a zoo object, and interpolate missing records. This differs from the NA interpolation done above,
# since sequential NA values (i.e. more than one in a row) won't be filled. This is done by setting maxgap = 1.
# This process will generate a zoo object with a different length than the dive data frame that it is derived from, so a left_join is performed and the resulting
# vector of the correct lenght is extracted to be re-inserted back into the dive data frame via dplyr::mutate().

for(i in unique(Dives))
{
  name <- get(i)
  if(any(!is.na(name$Altitude_m)) == F) #Check if there is any altitude data, if there is none (i.e, all entries are NA) skip trying to interpolate and move to the next file.
  {
    break
  }
  Altitude <- zoo(name$Altitude_m, order.by = name$date_time)
  Altitude_interpolated <- na.approx(Altitude, rule = 2, maxgap = 1) #Rule 2 means interpolate both forwards and backwards in the time series. Maxgap = 1 means that back to back NA values won't be replaced.
  Altitude_interpolated <- as.matrix(Altitude_interpolated) #Convert back to a base object class (a matrix) to extract the components of the zoo object.
  Altitude_names <- rownames(Altitude_interpolated) #Extract the rownames from the matrix (i.e. the timestamps)
  Altitude_interpolated <- Altitude_interpolated[1:length(Altitude_interpolated)] #Extract the data from the matrix
  Altitude_interpolated <- data.frame(date_time = ymd_hms(Altitude_names), Altitude_m = Altitude_interpolated) #Re-assemble the data into a data frame. Set the date_time to a POSIXct object
  Altitude_interpolated <- left_join(name, Altitude_interpolated, by = "date_time") #Join the interpolated date to the dive data frame. 
  Altitude_interpolated$Altitude_m.y[Altitude_interpolated$Altitude_m.y < 0] <- -9999  #Interpolation may have resulted in some negative values that are not -9999, reset all negative values to -9999
  assign(i, mutate(name, Altitude_m = Altitude_interpolated$Altitude_m.y)) #Replace the column with missing values with the one that were just interpolated
}

for(i in unique(Dives))
{
  name <- get(i)
  if(any(!is.na(name$Slant_Range_m)) == F) #Check if there is any Slant Range data, if there is none (i.e, all entries are NA) skip trying to interpolate and move to the next file.
  {
    break
  }
  Slant <- zoo(name$Slant_Range_m, order.by = name$date_time)
  Slant_interpolated <- na.approx(Slant, rule = 2, maxgap = 1) #Rule 2 means interpolate both forwards and backwards in the time series. Maxgap = 1 means that back to back NA values won't be replaced.
  Slant_interpolated <- as.matrix(Slant_interpolated) #Convert back to a base object class (a matrix) to extract the components of the zoo object.
  Slant_names <- rownames(Slant_interpolated) #Extract the rownames from the matrix (i.e. the timestamps)
  Slant_interpolated <- Slant_interpolated[1:length(Slant_interpolated)] #Extract the data from the matrix
  Slant_interpolated <- data.frame(date_time = ymd_hms(Slant_names), Slant_Range_m = Slant_interpolated) #Re-assemble the data into a data frame. Set the date_time to a POSIXct object
  Slant_interpolated <- left_join(name, Slant_interpolated, by = "date_time") #Join the interpolated date to the dive data frame. 
  Slant_interpolated$Slant_Range_m.y[Slant_interpolated$Slant_Range_m.y < 0] <- -9999  #Interpolation may have resulted in some negative values that are not -9999, reset all negative values to -9999
  assign(i, mutate(name, Slant_Range_m = Slant_interpolated$Slant_Range_m.y)) #Replace the column with missing values with the one that were just interpolated

}

###################################STEP 9 - APPLY OFFSETS TO POSITIONS DATA SOURCES############################

#Calculate angles created by the abeam and along ship centerline offset values, to be used in calculation of offset from center of ship
#for GPS antenna. For trigonometry purposes, abeam = opposite and along = adjacent.

#Compute length of hypotenuse to determine offset distance, in meters.
offset_dist = sqrt((GPS_abeam^2) + (GPS_along^2))

#For cases where the GPS antenna is offset in both abeam and along ship axes, calculate the angle from the center of the ship to the 
#antenna location.

offset_angle <- atan(GPS_abeam/GPS_along)
offset_angle <- offset_angle * (180/pi) #Convert to degrees.

for(i in unique(Dives))
{
  name <- get(i)
  
  if(GPS_abeam == 0 & GPS_along == 0) {   #Case 1: the GPS antenna is dead center on the ship 
    offset_dist = 0 
  } else if (GPS_abeam == 0) {  #Case 2: GPS antenna along keel line, but fore/aft of center of ship. 
    
    name$bearing <- name$Ship_heading - 180 #Subtract 180 if it is astern of the center of the ship
    name$bearing <- name$Ship_heading  #No change to the heading if it is ahead of the center of the ship

  } else if (GPS_along == 0) {  #Case 3: GPS antenna centered fore/aft, but not along keel 
    
    name$bearing <- name$Ship_heading - 90 #Subtract 90 if is to the port of center.
    name$bearing <- name$Ship_heading + 90 #Add 90 if it is to the stbd of center.

  } else if (GPS_along != 0 & GPS_abeam != 0) {   #Case 4: Standard case, GPS offset in both abeam and alongship axes.
    
    name$bearing <- name$Ship_heading + offset_angle
  }
  
  name$bearing[name$bearing > 360] <- name$bearing[name$bearing > 360] - 360 #Reduce integer >360 back to values less than of equal to 360
  name$bearing[name$bearing < 0] <- name$bearing[name$bearing < 0] + 360 #Increase negative values by 360, to get correct bearing between 0 and 360

  #Calculate the offset positions long/lat.
    
    for(k in 1:length(name$date_time))
    {
      start <- cbind(name$Main_Beacon_Long_interp[k], name$Main_Beacon_Lat_interp[k])
      offset <- destPoint(start, name$bearing[k], offset_dist)
      name$offset_long[k] <-  offset[1]
      name$offset_lat[k] <- offset[2]
      
    }
  mutate(name, Main_Beacon_Long_interp = offset_long, Main_Beacon_Lat_interp = offset_lat)
  name <- name[,1:31] #Drop the offset_lat and offset_long columns
  
  assign(i, name)
}

################################################STEP 10 - REMOVE BEACON DATA OUTLIERS#################################

#Calculate cross track distance between GPS track of the Ship and beacon position. Do this by creating a point distance matrix between
#each interpolated beacon position, and the Ship_GPS point for that same second

for(k in unique(Dives))
{
  name <- get(k)
  for(i in 1:length(name$date_time))
  {
    ship <- cbind(name$Ship_Long_interp[i], name$Ship_Lat_interp[i])
    beacon <- cbind(name$Main_Beacon_Long_interp[i], name$Main_Beacon_Lat_interp[i])
    XTE <- distm(ship, beacon)
    name$XTE[i] <- as.numeric(XTE[,1])
  }
  assign(k, name)
}

#Calculate the median XTE value for each dive, and detect outliers as median +/- 1.5*Interquartile range. Set the lat/long values 
#for any indices outside of the values to NA.

for(k in unique(Dives))
{
  name <- get(k)
  upper <- median(name$XTE) + (1.5*IQR(name$XTE))
  lower <- median(name$XTE) - (1.5*IQR(name$XTE))
  name$Main_Beacon_Lat_interp[name$XTE > upper | name$XTE < lower] <- NA
  name$Main_Beacon_Long_interp[name$XTE > upper | name$XTE < lower] <- NA
  assign(k, name)
}

#Coerce to a zoo object and run linear interpolation on the Beacon Lat/Long that have had the outlier values set to NA

for(i in unique(Dives))
{
  name <- get(i)
  Long <- zoo(name$Main_Beacon_Long_interp, order.by = name$date_time)
  Lat <- zoo(name$Main_Beacon_Lat_interp, order.by = name$date_time)
  Long_intepolated <- na.approx(Long, rule = 2) #Rule 2 means interpolate both forwards and backwards in the time series.
  Long_intepolated <- as.matrix(Long_intepolated) #Convert back to a base object class (a matrix) to extract the components of the zoo object.
  name$Main_Beacon_Long_interp <- Long_intepolated[1:length(Long_intepolated)]
  Lat_intepolated <- na.approx(Lat, rule = 2) #Rule 2 means interpolate both forwards and backwards in the time series.
  Lat_intepolated <- as.matrix(Lat_intepolated) #Convert back to a base object class (a matrix) to extract the components of the zoo object.
  name$Main_Beacon_Lat_interp <- Lat_intepolated[1:length(Lat_intepolated)]
  assign(i, name)
}

#############################################STEP 11 - APPLY LOESS AND RUNNING MEDIAN SMOOTHING TO ROV POSITION DATA #############################################

#Smooth the interpolated beacon position with the Loess smoother, with an alpha value of 0.05

for(i in unique(Dives))
{
  name <- get(i)
  loess_lat <- loess(name$Main_Beacon_Lat_interp ~ as.numeric(name$date_time), span = loess_span)
  name$loess_lat <- loess_lat$fitted
  loess_long <- loess(name$Main_Beacon_Long_interp ~ as.numeric(name$date_time), span = loess_span)
  name$loess_long <- loess_long$fitted
  assign(i, name)
}


#Smooth the interpolated beacon position with a running median - set the bandwitdth to ~ 1 min (59 seconds). Must be an odd value.
#Round the values of the smoothed data to 5 decimal places; keep trailing zeros!

for(i in unique(Dives))
{
  name <- get(i)
  name$Main_Beacon_Lat_smooth <- runmed(name$Main_Beacon_Lat_interp, smooth_window)
  name$Main_Beacon_Long_smooth <- runmed(name$Main_Beacon_Long_interp, smooth_window)
  name$Main_Beacon_Lat_smooth <- round(name$Main_Beacon_Lat_smooth, digits = 5)
  name$Main_Beacon_Long_smooth <- round(name$Main_Beacon_Long_smooth, digits = 5)
  assign(i, name)
}

###################################STEP 12 - WRITE FINAL PROCESSED DATA TO FILE####################################################

#Create final processed files, and write to .CSV.

for(i in unique(Dives))
{
  name <- get(i)
  name <- name[,c(1:2,33:34,10,9,35:36,12,11,13:18,21:22,19:20,24:30)]
  names(name) <- c("date_time","Transect_Name","Beacon_Lat_loess","Beacon_Long_loess","Beacon_Lat_interp","Beacon_Long_interp","Beacon_Lat_smoothed",
                   "Beacon_Long_smoothed","Ship_Lat_interp","Ship_Long_interp","Depth_m","Phantom_heading","Ship_Heading","Speed_kts",
                   "Altitude_m","Slant_Range_m","Best_Depth_Source","Best_Position_Source","RogueCam_roll",
                   "RogueCam_pitch","MiniZeus_zoom_percent","MiniZeus_focus_percent","MiniZeus_aperture_percent","MiniZeus_pitch","MiniZeus_roll","ROV_pitch","ROV_roll")
  write.csv(name, file = paste0(final_dir,"/",i), quote = F, row.names = F)
  assign(i, name)
}



