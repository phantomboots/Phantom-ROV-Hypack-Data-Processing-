#=====================================================================================================
# Script Name: ASDL Data Processing_Phantom
# Script Function: Reads in the data files created by Advanced Serial Data logger, parses each file and fills in missing
#                 date_time values. Decimates data to 1Hz and writes one master file for the whole cruise. Create files with 
#                 the suffix '_MasterLog', for use in later steps. Processes records for Phantom's Onboard Heading/Depth/Umbilical turns, Hemisphere GPS position and 
#                 heading, ROWETech DVL, Tritech Altimeter (Slant Range), Video Overlay string, MiniZeus Zoom/Focus/Aperture, RBR CTD, MiniZeus and ROV pitch and roll and
#                 TrackMan data records. Each step is proceeded by and if() statement that checks if data is present before attempting to
#                 process the records from a particular device.
#                 #NOTE: In 2021, the RogueCam pitch and roll was only read into Hypack, due to a device hardware limitation. For this reason, there is no RogueCam data
#                         processed by this script.
#
# Script Author: Ben Snow
# Script Date: Sep 4, 2019
# R Version: 3.5.1
######################################################################################################
#                                            CHANGE LOG                                              #
######################################################################################################
#
# June 6, 2020: Converted out of range values of -99.999 or 0 in the DVL Bottom velocity values to -9999, to match out of range
#               values used for Altitude and Slant Range.
# June 6, 2020: Set out of range values of for Altitude and Slant Range to -9999.
# Apr 21, 2021: Removed ASDL data processing for Cyclops camera pitch and roll. Added in processing of pitch and roll from MiniZeus and Mux Can IMUs
# Apr 23, 2021: Set options(digits = 12), to make sure that Lat/Long with many sig figures are displayed as expected. Added in explicit number of columns to 
#               read for Hemisphere_position, ROWETech DVL and RBR log files. read_delim scans the first rows to determine the appropriate number of columns for the
#               parsed data, these devices can have variable numbers of columns when parsed from comma delimited format. Setting number of columns explicitly via 
#               col_names = paste0("X", seq_len(...)), where ... is the explicitly stated number of columns, will control for this.
#=====================================================================================================

#Check for the presence of packages shown below, install any packages that are missing
packages <- c("lubridate","readr","dplyr","stringr","imputeTS","measurements")
new_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
if(length(new_packages)) install.packages(new_packages)

#Force display of long digit numbers

options(digits = 12)

#Required packages#

require(lubridate)
require(readr)
require(dplyr)
require(stringr)
require(imputeTS) #Intepolation of missing time series values, where required (replaces 'NA' values)
require(measurements) #Facilitates converting data from the Hemisphere GPS from Deg/Decimal mins to Decimal degrees.


########################################STEP 1 - EDIT THESE DATA ##############################################

#Enter Project folder name

project_folder <- "~/Projects/Apr2021_Phantom_Cruise_PAC2021_035"

#Offset values for IMUs located in the Phantom's subsea can and on the MiniZeus

zeus_pitch_offset <- 0 #An offset of zero means is measure the expect 90 degrees when perpendicular to the seafloor.
zeus_roll_offset <- 6
rov_pitch_offset <- -33
rov_roll_offset <- -1



#######################################STEP 2 - SET WORKING DIRECTORY AND GENERATE FOLDERS####################

#Directory where the ASDL files are stored
ASDL_dir <- paste0(project_folder, "/Data/Advanced_Serial_Data_Logger")

#Set the directory for saving of the master files.
save_dir <- paste0(project_folder, "/Data/Advanced_Serial_Data_Logger/Full_Cruise")

#Vector of directories to check for

dirs <- c(ASDL_dir, save_dir)

#Check for and create directories, as needed.

for(i in unique(dirs))
{
  if(dir.exists(i) == FALSE)
  {
    dir.create(i, recursive = TRUE)
  }
}

#Set working directory, wherever the Advanced Serial data logger text files are saved.
setwd(ASDL_dir)

#########################STEP 3 - READ IN THE ROV HEADING, DEPTH AND UTURNS FROM ASDL#########################

#List files and read into one larger file. This is a comma seperate record, but some files have more columns than others, so need to
#read in files without comma delimeters to start. Use the semi-colon as a bogus delimeter.

ROV_files <- list.files(ASDL_dir, pattern = "ROV")

if(length(ROV_files != 0))
{
  
for(i in 1:length(ROV_files))
{
  name <- as.character(i)
  assign(name, read_csv(ROV_files[i], col_names = F, col_types = cols(X1 = "c", X2 ="c", X3 = "c")))
  if(name == "1")
  {ROV_all <- get(name)
  }else ROV_all <- bind_rows(ROV_all, get(name))
  rm(list = c(i))
}


#Locate date stamp values and time stamp values. Replace period in timestamp value with a colon. Parse date_time.

ROV_all$date <- str_extract(ROV_all$X1, "\\d{8}")
ROV_all$time <- str_extract(ROV_all$X1, "\\d{2}\\:\\d{2}\\.\\d{2}")
ROV_all$time <- gsub("\\.",":",ROV_all$time)
ROV_all$date_time <- ymd_hms(paste(ROV_all$date, ROV_all$time, sep = " "))

#Impute the time series, before filtering out any values

full <- na_interpolation(as.numeric(ROV_all$date_time))
full <- as.integer(full)
ROV_all$date_time <- full #Put it back into the data frame.

#Remove duplicate time stamps row. Convert back to a POSIXct object.

ROV_all <- ROV_all[!duplicated(ROV_all$date_time),]
ROV_all$date_time <- as.POSIXct(ROV_all$date_time, origin = "1970-01-01", tz = "UTC") #Standard R origin value

#Keep only the relevant columns, and write to a .CSV file.

ROV_all <- ROV_all[,c(6,2:3)]
names(ROV_all) <- c("date_time","Depth_m","Phantom_heading")
write.csv(ROV_all, paste(save_dir,"ROV_Heading_Depth_MasterLog.csv", sep ="/"), quote = F, row.names = F)

}

########################STEP 4 - READ IN THE TRITECH ALTIMETER SLANT RANGE FROM ASDL########################################

#List files and read into one larger file. This is a comma seperate record, but some files have more columns than others, so need to
#read in files without comma delimeters to start. Use the semi-colon as a bogus delimeter.

Tritech_files <- list.files(ASDL_dir, pattern = "Tritech")

if(length(Tritech_files != 0))
{
  
for(i in 1:length(Tritech_files))
{
  name <- as.character(i)
  assign(name, read_csv(Tritech_files[i], col_names = F, col_types = cols(X1 = "c", X2 = "c", X3 = "c", X4 = "c",
                                                                                          X5 = "c", X7 = "c")))
  if(name == "1")
  {Tritech_all <- get(name)
  }else Tritech_all <- bind_rows(Tritech_all, get(name))
  rm(list = c(i))
}


#Located date stamp values and time stamp values. Replace period in timestamp value with a colon. Parse date_time.

Tritech_all$date <- str_extract(Tritech_all$X1, "\\d{8}")
Tritech_all$time <- str_extract(Tritech_all$X1, "\\d{2}\\:\\d{2}\\.\\d{2}")
Tritech_all$time <- gsub("\\.",":",Tritech_all$time)
Tritech_all$date_time <- ymd_hms(paste(Tritech_all$date, Tritech_all$time, sep = " "))

#Extract altimeter slant range.

Tritech_all$slant_range_m <- as.numeric(Tritech_all$X4)

#Impute the time series, before filtering out any values

full <- na_interpolation(as.numeric(Tritech_all$date_time))
Tritech_all$date_time <- as.integer(full) #Put it back into the data frame as an integer, for filtering purposes.

#Remove duplicate values, convert the date_time back to a POSIXct object.

Tritech_all <- Tritech_all[!duplicated(Tritech_all$date_time),]
Tritech_all$date_time <- as.POSIXct(Tritech_all$date_time, origin = "1970-01-01", tz = "UTC") #Standard R origin value

#Remove NA values.

Tritech_all <- filter(Tritech_all, !is.na(slant_range_m))

#Set values of 9.99 and 0 (out of range values) to -9999

Tritech_all$slant_range_m[Tritech_all$slant_range_m == 0] < -9999
Tritech_all$slant_range_m[Tritech_all$slant_range_m == 9.99] <- -9999

#Drop unused columns and write .CSV MasterLog for the altitude_m

Tritech_all <- Tritech_all[,c(10,11)]
write.csv(Tritech_all, paste(save_dir,"Tritech_SlantRange_MasterLog.csv", sep = "/"), quote = F, row.names = F)
}

########################STEP 5 - READ IN MINIZEUS ZOOM, FOCUS, APERTURE AND CREATE MASTER LOG##################################

#List files and read into one larger file. This is a comma seperate record, but some files have more columns than others, so need to
#read in files without comma delimeters to start. Use the semi-colon as a bogus delimeter.

MiniZeus_ZFA_files <- list.files(ASDL_dir, pattern = "MiniZeus")

if(length(MiniZeus_ZFA_files) != 0)
{
  
for(i in 1:length(MiniZeus_ZFA_files))
{
  name <- as.character(i)
  assign(name, read_csv(MiniZeus_ZFA_files[i], skip = 1, col_names = F, col_types = cols(X1 = "c", X2 = "c", X3 = 'c', X4 = 'c', X5 = 'c',
                                                                               X6 = "c", X7 = "c")))
  if(name == "1")
  {MiniZeus_ZFA_all <- get(name)
  }else MiniZeus_ZFA_all <- bind_rows(MiniZeus_ZFA_all, get(name))
  rm(list = c(i))
}

#Remove and rows with NA values. Choose any column to search for NA values.

MiniZeus_ZFA_all <- filter(MiniZeus_ZFA_all, !is.na(X2))


#Locate date stamp values and time stamp values. Replace period in timestamp value with a colon. Parse date_time.

MiniZeus_ZFA_all$date <- str_extract(MiniZeus_ZFA_all$X1, "\\d{8}")
MiniZeus_ZFA_all$time <- str_extract(MiniZeus_ZFA_all$X1, "\\d{2}\\:\\d{2}\\.\\d{2}")
MiniZeus_ZFA_all$time <- gsub("\\.",":",MiniZeus_ZFA_all$time)
MiniZeus_ZFA_all$date_time <- ymd_hms(paste(MiniZeus_ZFA_all$date, MiniZeus_ZFA_all$time, sep = " "))

#Impute the time series, before filtering out any values

full <- na_interpolation(as.numeric(MiniZeus_ZFA_all$date_time))
MiniZeus_ZFA_all$date_time <- as.integer(full) #Put it back into the data frame as an integer, for filtering purposes.

#Remove duplicate values, convert the date_time back to a POSIXct object.

MiniZeus_ZFA_all <- MiniZeus_ZFA_all[!duplicated(MiniZeus_ZFA_all$date_time),]
MiniZeus_ZFA_all$date_time <- as.POSIXct(MiniZeus_ZFA_all$date_time, origin = "1970-01-01", tz = "UTC") #Standard R origin value

#Drop unused columns, rename kept columns and write .CSV MasterLog for the altitude_m

MiniZeus_ZFA_all <- MiniZeus_ZFA_all[,c(10,3,5,7)]
names(MiniZeus_ZFA_all) <- c("date_time","zoom_percent","focus_percent","aperture_percent")
write.csv(MiniZeus_ZFA_all, paste(save_dir,"MiniZeus_ZFA_MasterLog.csv", sep = "/"), quote = F, row.names = F)

}

##########################STEP 6 - READ IN THE RBR CTD LOG FROM ASDL########################################################

#List files and read into one larger file. Some files will contain status information from the RBR while it is connected to Ruskin
#these records include the text "Ready"; drop these records during the read in process.

RBR_files <- list.files(ASDL_dir, pattern = "RBR")

# if(length(RBR_files) != 0)
# {

temp <- data.frame()  
  
for(i in 1:length(RBR_files))
{
  name <- as.character(i)
  assign(name, read_csv(RBR_files[i], col_names = paste0("X", seq_len(10)), col_types =cols(X1 = "c", X2 = "c", X3 = "c", X4 = "c", X5 = "c", X6 = "c",
                                    X7 = "c", X8 = "c", X9 = "c", X10 = "c")))
  temp <- get(name)
  temp <- filter(temp, !str_detect(X1, "Re")) #Search for the first two characters in the word 'Ready". Drop these entries.
  if(name == "1")
  {RBR_all <- temp
  }else RBR_all <- bind_rows(RBR_all, temp)
  rm(list = c(i))
}

#Drop the ASDL timestamp value, if it is present.

RBR_all$X1 <- gsub("[[:print:]]{1,}>","",RBR_all$X1)

#Located date stamp values and time stamp values. Replace period in timestamp value with a colon. Parse date_time.

RBR_all$date <- str_extract(RBR_all$X1, "\\d{4}\\-\\d{2}\\-\\d{2}")
RBR_all$time <- str_extract(RBR_all$X1, "\\d{2}\\:\\d{2}\\:\\d{2}")
RBR_all$time <- gsub("\\.",":",RBR_all$time)
RBR_all$date_time <- ymd_hms(paste(RBR_all$date, RBR_all$time, sep = " "))

#Drop the records that failed to parse. Filter to 1 Hz.

RBR_all <- filter(RBR_all, !is.na(RBR_all$date_time))
RBR_all <- RBR_all[!duplicated(RBR_all$date_time),]

#Remove any ASDL timestamps in the last water quality data column

RBR_all$X10 <- gsub("<[[:print:]]{1,}>","",RBR_all$X10)

#If any other columns are NA, drop them. 
RBR_all <- filter(RBR_all, !is.na(X2))
RBR_all <- RBR_all[,c(13,2:10)]
names(RBR_all) <- c("date_time","Conductivity_mS/cm","Temp_C","Pressure_dbar","Dissolved_02_sat_%","Sea_Pressure_dbar",
                    "Depth_m","Salinity_PSU","Sound_Speed_m/s","Specific_Cond_uS/cm")

#Convert Depth column to numeric, drop any rows where the Depth is less than 1m (corresponding to on-deck time).
RBR_all$Depth_m <- as.numeric(RBR_all$Depth_m) 
RBR_all <- filter(RBR_all, Depth_m >= 1)

#Write to .CSV

write.csv(RBR_all, paste(save_dir,"RBR_CTD_MasterLog.csv", sep = "/"), quote = F, row.names = F)
# }

#########################################STEP 7 - READ IN THE HEMISPHERE GPS POSITION AND HEADING ################################

#List files and read into one larger file. This is a comma seperate record, but some files have more columns than others, so need to
#read in files by skipping the first line of each file to start. 

Hemisphere_GPS_files <- list.files(ASDL_dir, pattern = "position")

#if(length(Hemisphere_GPS_files != 0))
#{
for(i in 1:length(Hemisphere_GPS_files))
{
  name <- as.character(i)
  assign(name, read_delim(Hemisphere_GPS_files[i], col_names = paste0("X",seq_len(15)), delim = ",", skip = 1, col_types = cols(X1 = "c", X2 = "c", X3 = "c",
                          X4 = "c", X5 = "c", X6 = "c", X7 = "c", X8 = "c", X9 = "c", X10 = "c", X11 = "c", X12 = "c", X13 = "c",
                          X14 = "c", X15 = "c")))
  if(name == "1")
  {Hemisphere_GPS_all <- get(name)
  }else Hemisphere_GPS_all <- bind_rows(Hemisphere_GPS_all, get(name))
  rm(list = c(i))
}

#Filter out to only the $GPGGA, $GPZDA and $HEHDT strings.

Hemisphere_GPS_all <- filter(Hemisphere_GPS_all, X1 == "$GPGGA" | X1 == "$GPZDA")

#Located date stamp values and time stamp values in the $GPZDA strings. Parse the date_time. Ignore warnings.

Hemisphere_GPS_all$date <- dmy(paste(Hemisphere_GPS_all$X3,Hemisphere_GPS_all$X4, Hemisphere_GPS_all$X5, sep = "-"))
Hemisphere_GPS_all$X2 <- as.integer(Hemisphere_GPS_all$X2) 
Hemisphere_GPS_all$time <- str_extract(Hemisphere_GPS_all$X2, "\\d{6}")
Hemisphere_GPS_all$date_time <- ymd_hms(paste(Hemisphere_GPS_all$date, Hemisphere_GPS_all$time, sep = " "))

#Impute the time series, before filtering out any values. Set it as an integer before putting back in to original DF, to get rid of
#milliseconds.

full <- na_interpolation(as.numeric(Hemisphere_GPS_all$date_time))
full <- as.integer(full)
Hemisphere_GPS_all$date_time <- as.POSIXct(full, origin = "1970-01-01", tz = "UTC") #Standard R origin value

#Extract the degrees, minutes and seconds information. Combine to a single value with a space in-between.

GPS_position <- filter(Hemisphere_GPS_all, X1 == "$GPGGA")
GPS_position$Lat_deg <- str_extract(GPS_position$X3, "\\d{2}")
GPS_position$Lat_min <- str_extract(GPS_position$X3, "\\d{2}\\.\\d{5,}")
GPS_position$Long_deg <- str_extract(GPS_position$X5, "\\d{3}")
GPS_position$Long_min <- str_extract(GPS_position$X5, "\\d{2}\\.\\d{5,}")
GPS_position$Lat <- paste(GPS_position$Lat_deg, GPS_position$Lat_min, sep = " ")
GPS_position$Long <- paste(GPS_position$Long_deg, GPS_position$Long_min, sep = " ")

#Convert to decimal degrees, set to back to numeric

GPS_position$Lat <- conv_unit(GPS_position$Lat, "deg_dec_min", "dec_deg")
GPS_position$Long <- conv_unit(GPS_position$Long, "deg_dec_min", "dec_deg")

#Drop unused columns. Remove milliseconds

GPS_position <- GPS_position[,c(18,23:24)]

#Extract Hemisphere Heading data. Drop unused columns. Remove milliseconds!
#Heading string could be $GPHDT or $HEHDT (Hemisphere V100 ouputs $HEHDT, V500 outputs $GPHDT)

Heading <- filter(Hemisphere_GPS_all, X1 == "$HEHDT" | X1 == "$GPHDT")
Heading <- Heading[,c(18,2)]

#Heading$date_time <- as_datetime(floor(seconds(Heading$date_time)))


#Join the GPS Position and Heading data, remove duplicated timestamp first

GPS_position <- GPS_position[!duplicated(GPS_position$date_time),]
Heading <- Heading[!duplicated(Heading$date_time),]
GPS_and_Heading <- left_join(GPS_position, Heading, by = "date_time")
names(GPS_and_Heading) <- c("date_time","Lat","Long","Heading")

#Roung GPS decimal degrees to the 7th decimal place. This is likely the limit of the device's accuracy.

GPS_and_Heading$Lat <- as.numeric(GPS_and_Heading$Lat)
GPS_and_Heading$Long <- as.numeric(GPS_and_Heading$Long)
GPS_and_Heading$Long <- -GPS_and_Heading$Long  #Ensure that the longitude has a negative sign, to maintain compliance with calculate longitudes from 1_Hypack Data Parse script.
GPS_and_Heading$Lat <- round(GPS_and_Heading$Lat, digits =  7)
GPS_and_Heading$Long <- round(GPS_and_Heading$Long, digits = 7)

#Write to a .CSV.

write.csv(GPS_and_Heading, paste(save_dir,"Hemisphere_GPS_Heading_MasterLog.csv", sep = "/"), quote = F, row.names = F)

#}

#########################################STEP 8 - READ IN THE HEMISPHERE HEADING DATA ONLY###################################

#List files and read into one larger file. This is a comma seperate record, but some files have more columns than others, so need to
#read in files by skipping the first line of each file to start. 

Hemisphere_Heading_files <- list.files(ASDL_dir, pattern = "heading")

if(length(Hemisphere_Heading_files != 0))
{
for(i in 1:length(Hemisphere_Heading_files))
{
  name <- as.character(i)
  assign(name, read_delim(Hemisphere_Heading_files[i], col_names = F, delim = ",", skip = 1, col_types = cols(X1 = "c", X2 = "c", X3 = "c")))
  if(name == "1")
  {Hemisphere_Heading_all <- get(name)
  }else Hemisphere_Heading_all <- bind_rows(Hemisphere_Heading_all, get(name))
  rm(list = c(i))
}

#Locate date stamp values and time stamp values. Replace period in timestamp value with a colon. Parse date_time.

Hemisphere_Heading_all$date <- str_extract(Hemisphere_Heading_all$X1, "\\d{8}")
Hemisphere_Heading_all$time <- str_extract(Hemisphere_Heading_all$X1, "\\d{2}\\:\\d{2}\\.\\d{2}")
Hemisphere_Heading_all$time <- gsub("\\.",":",Hemisphere_Heading_all$time)
Hemisphere_Heading_all$date_time <- ymd_hms(paste(Hemisphere_Heading_all$date, Hemisphere_Heading_all$time, sep = " "))
Hemisphere_Heading_all <- filter(Hemisphere_Heading_all, X1 != "$GPROT")

#Impute the time series, before filtering out any values

full <- na_interpolation(as.numeric(Hemisphere_Heading_all$date_time))
full <- as.integer(full)
Hemisphere_Heading_all$date_time <- full #Put it back into the data frame.
Hemisphere_Heading_all <- filter(Hemisphere_Heading_all, X1 == "$GPHDT" | X1 == "$HEHDT")

#Remove duplicate time stamps row. Convert back to a POSIXct object.

Hemisphere_Heading_all <- Hemisphere_Heading_all[!duplicated(Hemisphere_Heading_all$date_time),]
Hemisphere_Heading_all$date_time <- as.POSIXct(Hemisphere_Heading_all$date_time, origin = "1970-01-01", tz = "UTC") #Standard R origin value

#Keep only the relevant columns, and write to a .CSV file.

Hemisphere_Heading_all <- Hemisphere_Heading_all[,c(6,2)]
names(Hemisphere_Heading_all) <- c("date_time","Ship_heading")
write.csv(Hemisphere_Heading_all, paste(save_dir,"Hemisphere_Heading_MasterLog.csv", sep ="/"), quote = F, row.names = F)
}

#######################################STEP 9 - READ IN TRACKMAN DATA RECORDS####################################################

#List files and read into one larger file. This is a comma seperate record, but some files have more columns than others, so need to
#read in files by skipping the first line of each file to start. 

TrackMan_files <- list.files(ASDL_dir, pattern = "TrackMan")

if(length(TrackMan_files) != 0)
{  

for(i in 1:length(TrackMan_files))
{
  name <- as.character(i)
  assign(name, read_delim(TrackMan_files[i], col_names = F, delim = ",", skip = 1, col_types = cols(X1 = "c", X2 = "c", X3 = "c",
                          X4 = "c", X5 = "c", X6 = "c", X7 = "c", X8 = "c", X9 = "c", X10 = "c", X11 = "c", X12 = "c", X13 = "c",
                          X14 = "c", X15 = "c", X16 = "c", X17 = "c", X18 = "c", X19 = "c")))
  if(name == "1")
  {TrackMan_all <- get(name)
  }else TrackMan_all <- bind_rows(TrackMan_all, get(name))
  rm(list = c(i))
}

#Parse date time from ASDL timestamp entries.

TrackMan_all$date <- str_extract(TrackMan_all$X1, "\\d{8}")
TrackMan_all$time <- str_extract(TrackMan_all$X1, "\\d{2}\\:\\d{2}\\.\\d{2}")
TrackMan_all$time <- gsub("\\.",":",TrackMan_all$time)
TrackMan_all$date_time <- ymd_hms(paste(TrackMan_all$date, TrackMan_all$time, sep = " "))

#Impute the time series, before filtering out any values

full <- na_interpolation(as.numeric(TrackMan_all$date_time))
TrackMan_all$date_time <- as.integer(full) #Put it back into the data frame as an integer, for filtering purposes.

#Remove duplicate values, convert the date_time back to a POSIXct object.

TrackMan_all <- TrackMan_all[!duplicated(TrackMan_all$date_time),]
TrackMan_all$date_time <- as.POSIXct(TrackMan_all$date_time, origin = "1970-01-01", tz = "UTC") #Standard R origin value

#Re-arrange columns 

TrackMan_all <- TrackMan_all[,c(22,2,4:18)]
names(TrackMan_all) <- c("date_time","Beacon_ID","Phase_Counts_A","Phase_Counts_B","Phase_Counts_C","Quality_Factor","Error_Code",
                         "Target_Slant_Range_m","Depression_Angle","Target_Bearing","DistanceX_m","DistanceY_m","DistanceZ_m",
                         "Ship_Heading","TSS_Pitch","TSS_Roll","Temp_C")

#Write to a .CSV 

write.csv(TrackMan_all, paste(save_dir,"TrackMan_Beacons_MasterLog.csv", sep = "/"), quote = F, row.names = F)
}


####################################STEP 10 - READ IN ROWETech DVL DATA#############################################################

#List files and read into one larger file. This is a comma seperate record, but some files have more columns than others, so need to
#read in files by skipping the first line of each file to start. 

DVL_files <- list.files(ASDL_dir, pattern = "DVL")

if(length(DVL_files) != 0)
{
  
for(i in 1:length(DVL_files))
  {
    name <- as.character(i)
    assign(name, read_delim(DVL_files[i], col_names = paste0("X", seq_len(17)), delim = ",", skip = 1, col_types = cols(X1 = "c", X2 = "c", X3 = "c",
                          X4 = "c", X5 = "c", X6 = "c", X7 = "c", X8 = "c", X9 = "c", X10 = "c", X11 = "c", X12 = "c", X13 = "c",
                         X14 = "c", X15 = "c", X16 = "c", X17 = "c")))
    if(name == "1")
    {DVL_all <- get(name)
    }else DVL_all <- bind_rows(DVL_all, get(name))
    rm(list = c(i))
}
  
#Parse date time from ASDL timestamp entries.
  
DVL_all$date <- str_extract(DVL_all$X1, "\\d{8}")
DVL_all$time <- str_extract(DVL_all$X1, "\\d{2}\\:\\d{2}\\.\\d{2}")
DVL_all$time <- gsub("\\.",":",DVL_all$time)
DVL_all$date_time <- ymd_hms(paste(DVL_all$date, DVL_all$time, sep = " "))
  
#Impute the time series, before filtering out any values
  
full <- na_interpolation(as.numeric(DVL_all$date_time))
DVL_all$date_time <- as.integer(full) #Put it back into the data frame as an integer, for filtering purposes.
  
#Remove duplicate values, convert the date_time back to a POSIXct object.
  
DVL_all <- DVL_all[!duplicated(DVL_all$date_time),]
DVL_all$date_time <- as.POSIXct(DVL_all$date_time, origin = "1970-01-01", tz = "UTC") #Standard R origin value
  
#Re-arrange columns, drop rows with missing Altitude
  
DVL_all <- DVL_all[,c(20,5:9)]
DVL_all <- filter(DVL_all, !is.na(X9))

#Convert units to meters/sec

DVL_all$X5 <- as.numeric(DVL_all$X5) / 1000
DVL_all$X6 <- as.numeric(DVL_all$X6) / 1000
DVL_all$X7 <- as.numeric(DVL_all$X7) / 1000
DVL_all$X8 <- as.numeric(DVL_all$X8) / 1000
DVL_all$X9 <- as.numeric(DVL_all$X9) / 1000

#Bottom Velocity values of -99.999 and 0 indicate out of range readings convert these to -9999, to make consistent with out-of-range values for altitude.

DVL_all$X6[DVL_all$X6 == 0] <- -9999
DVL_all$X6[DVL_all$X6 == -99.999] <- -9999
DVL_all$X7[DVL_all$X7 == 0] <- -9999
DVL_all$X7[DVL_all$X7 == -99.999] <- -9999
DVL_all$X8[DVL_all$X8 == 0] <- -9999
DVL_all$X8[DVL_all$X8 == -99.999] <- -9999

#Altitude values of 0 indicate out of range readings; set these to -9999

DVL_all$X9[DVL_all$X9 == 0] <- -9999

#Rename columns

names(DVL_all) <- c("date_time","Bottom_X_Velocity_ms","Bottom_Y_Velocity_ms","Bottom_Z_Velocity_ms","Bottom_3D_Velocity_ms",
                    "Altitude_m")

#Write to a .CSV 
  
write.csv(DVL_all, paste(save_dir,"ROWETECH_DVL_MasterLog.csv", sep = "/"), quote = F, row.names = F)
}

#############################################STEP 11 - READ IN VECTOR 12 KHZ SOUNDER DATA#############################################


Vector_12_files <- list.files(ASDL_dir, pattern = "Vector_12")

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


IMU_files <- list.files(ASDL_dir, pattern = "Zeus_Cans")

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

