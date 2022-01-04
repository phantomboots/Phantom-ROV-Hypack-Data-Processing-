#=====================================================================================================
# Script Name: 4b_Generate_KML_and_Shapefiles_Phantom.R
# Script Function: This script reads in the .CSV files created from "4_Append RBR CTD Data to NAV Exports.R".
#                 Hypack Planned lines, GPS tracks, and unsmoothed/smoothed transponder fixes are written to .KML and .SHP
#                 files. Shapefiles include a spatial lines file, as well as Spatial Points file for each dive. The Spatial Points file contains
#                 all collected data, which is written to the attribute table for the file. Spatial lines .SHP are generated from both the running median
#                 and LOESS smoothing of the Lat/Longs. Spatial Points .SHP are generated from the LOESS data only (i.e. generally the more accurate of the
#                 two smoothing functions).
#
# Script Author: Ben Snow
# Script Date: Jul 2, 2020
# R Version: 3.5.1
#
######################################################################################################
#                                            CHANGE LOG                                              #
######################################################################################################

#Check if necessary packages are present, install as required.
packages <- c("lubridate","readr","dplyr","stringr","rgdal","geosphere")
new_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
if(length(new_packages)) install.packages(new_packages)


#Required packages#

require(lubridate)
require(readr)
require(dplyr)
require(stringr)
require(rgdal) #This loads package sp 
require(geosphere)

#########################################STEP 1 - EDIT THESE DATA################################################################

#Name of Ship used in the survey

ship_name <- "RV Manyberries"

#Enter Project folder name

project_folder <- "~/Projects/Anchor Scour Cumulative Effects_2021_22"


#################################STEP 2 - CHECK AND MAKE DIRECTORIES AS NEEDED #################################

#Working directory for location of Hypack .RAW files

Hypack_input <- paste0(project_folder, "/Data/Hypack_Backup/Raw")

#Directory for processed .CSV files created from "3_QAQC_Interpolated and Smooth Data.R"

imports_dir <- paste0(project_folder,"/Data/Final_Processed_Data")

#Path for .KML files that are created in this script.

KML_path <- paste0(project_folder,"/Data/KML_Files")

#Path for .SHP files that are created in this script.

SHP_path <- paste0(project_folder,"/Data/Shape_Files")

#Vector of directories to check for

dirs <- c(Hypack_input, imports_dir, KML_path, SHP_path)

#Check and create directories as needed.

for(i in unique(dirs))
{
  if(dir.exists(i) == FALSE)
  {
    dir.create(i, recursive = TRUE)
  }
}

#############################STEP 3 - READ IN THE PLANNED LINE COORDINATES AND CONVERT TO LAT/LONG###########################

#Planned line file coordinates are logged in each Hypack .RAW file, and have the line designations 'PTS' and 
#'LNN'. The exact line number will vary depending on how many devices are present in the Hypack Hardware file, but 
#its a safe assumption that they will be in the first 100 lines, so only need to read in this much data.

planned_lines <- data.frame() #Empty DF to fill with line points

setwd(Hypack_input)
input_files <- list.files(pattern = ".RAW")

for(i in 1:length(input_files))
{
  name <- as.character(i)
  assign(name, read_delim(input_files[i], delim = " ", col_names = F, n_max = 100,
                          col_types = cols(X1 = "c", X2 = "c", X3 = "c"))) #Read in the data strings
  name <- filter(get(name), X1 =="LNN"| X1 == "PTS" | X1 == "PRO") #Filter to line records only.
  name$X4 <- NA
  name$X5 <- NA
  name$X6 <- NA
  locate_line <- which(name$X1 == "LNN") #Find the index where LNN is 
  locate_UTM <- which(name$X1 == "PRO") #Find the index where PRO is, the line containing information on the prime meridian for each UTM zone.
  name$X4[locate_line-1] <- paste0(name$X2[locate_line],"_PlannedEnd")
  name$X4[locate_line-2] <- paste0(name$X2[locate_line],"_PlannedStart")
  name$X5 <- as.integer(name$X3[locate_UTM])
  name$X6[name$X5 == -123] <- 10
  name$X6[name$X5 == -129] <- 9
  name$X6[name$X5 == -135] <- 8
  if(name == "1")
  {planned_lines <- name
  }else planned_lines <- bind_rows(planned_lines, name)
  
  rm(list = c(i))
}


#Remove any duplicates, remove LNN idenfiers. Convert UTM coords to numeric values

planned_lines <- planned_lines[!duplicated(planned_lines$X2),]
planned_lines <- planned_lines[!duplicated(planned_lines$X4),] #Also check for duplicate file name entries, remove them if there are any
planned_lines <- filter(planned_lines, !is.na(X4))  #Remove any lines that NA values for their names.
planned_lines <- filter(planned_lines, X1 != "LNN")
planned_lines$X2 <- as.numeric(planned_lines$X2)
planned_lines$X3 <- as.numeric(planned_lines$X3)

#Add a new column that contains only dive number
planned_lines$dive_num <- str_extract(planned_lines$X4, "\\w+(?=\\_)") #The regular expression means "find any number of letters & numbers followed by an underscore".


#Build Spatial DF to convert from UTM to lat/long. Then convert back to regular DF. Round decimal degree values to 5 decimal places.

planned_lines_geographic <- data.frame() #Empty data.frame to fill

for(i in unique(planned_lines$dive_num))
{
  planned_lines_convert <- filter(planned_lines, dive_num == i) #Filter to rows unique dive number
  coordinates <- planned_lines_convert[,c(2,3)] #Get the coordinates for this unique dive number
  data <- as.data.frame(planned_lines_convert[,c(4)]) #Need to coerce to a DF explicitly, since there is only one column.
  crs <- CRS(paste0("+proj=utm +zone=", planned_lines_convert$X6[1]," +datum=WGS84")) #The column X6 contains the UTM zone number, use the first index number to get a Zone number value.
  
  planned_lines_DF <- SpatialPointsDataFrame(coords = coordinates, data = data, proj4string = crs)
  planned_lines_DF <- spTransform(planned_lines_DF, CRS("+proj=longlat +datum=WGS84"))
  planned_lines_DF <- as.data.frame(planned_lines_DF)
  names(planned_lines_DF) <- c("Line_Name","Long","Lat")
  planned_lines_DF$Lat <- round(planned_lines_DF$Lat, digits = 5)
  planned_lines_DF$Long <- round(planned_lines_DF$Long, digits =5)
  
  planned_lines_geographic <- bind_rows(planned_lines_geographic, planned_lines_DF)
}

#Rename as planned_lines_DF 
planned_lines_DF <- planned_lines_geographic

#Remove the "_PlannedEnd" and "_PlannedStart" portions of the planned line names, to faciliate plotting as line features.
#Remove any NA values as well.

planned_lines_DF$Line_Name <- gsub("_PlannedStart","", planned_lines_DF$Line_Name)
planned_lines_DF$Line_Name <- gsub("_PlannedEnd","", planned_lines_DF$Line_Name)

#Remove any lines that may, for whatever reason, are unnammed (i.e NA values)

planned_lines_DF <- planned_lines_DF[which(!is.na(planned_lines_DF$Line_Name)),]

#Explicity Set Longitude values to negative, so that they match the sign of other Long/Lat values use later in this script

planned_lines_DF$Long <-  -1*abs(planned_lines_DF$Long)

#Create an empty list to fill with as a 'Lines' class object, in the short loop below.

Planned_Lines <- list()

#Filter the data frame containing the start/end point for each plan line, and extract the coordinates and name of each planned line
#to its own dataframe. Add each of these individual DFs as elements into a Lines class object, for inclusion into .KML and .SHP files later
#in this script.

for(i in unique(planned_lines_DF$Line_Name))
{
  name <- filter(planned_lines_DF, Line_Name == i)
  coords <- cbind(name$Long, name$Lat)
  colnames(coords) <- c("Long", "Lat")
  Planned_Lines <- append(Planned_Lines, Lines(list(Line(coords)), ID = i)) #Append each unique planned line into a seperate slot in the Lines object
}


###################################STEP 4 - CREATE SPATIAL LINES OBJECTS#######################################################

#Set the working dir to the directory where the processed files from "4_QAQC_Interpolated and Smooth Data.R" are located.
#List all dives.

setwd(imports_dir)
Dives <- list.files(imports_dir)


#Make sure that all the Lat/Long values are all of the same sign (negative or positive). By convention, all
#longitudes in BC should be negative, and latitudes should be positive

for(i in unique(Dives))
{
  name <- read_csv(i)
  name$Beacon_Lat_interp <- abs(name$Beacon_Lat_interp)
  name$Beacon_Long_interp <- -1*abs(name$Beacon_Long_interp) #Set longitudes to all negatives.
  name$Ship_Lat <- abs(name$Ship_Lat_interp)
  name$Ship_Long <- -1*abs(name$Ship_Long_interp) #As above
  assign(i, name)
}


#For each files, create a Line class object for the interpolated vehicle position and the ship's position for each dive.

Dives2 <- gsub(".csv","",Dives) #Make a new vector to use as names for the Dive Lines that will be produced.
Smooth_Lines <- list() #Empty list to fill
Interp_Lines <- list() 
Ship_Lines <- list()
Loess_Lines <- list()

for(k in 1:length(Dives2))
{
  dive <- get(Dives[k])
  coords1 <- cbind(dive$Beacon_Long_smoothed, dive$Beacon_Lat_smoothed)
  coords2 <- cbind(dive$Beacon_Long_interp, dive$Beacon_Lat_interp)
  coords3 <- cbind(dive$Ship_Long, dive$Ship_Lat)
  coords4 <- cbind(dive$Beacon_Long_loess, dive$Beacon_Lat_loess)
  colnames(coords1) <- c("Main_Beacon_Long_smooth","Main_Beacon_Lat_smooth") #MUST HAVE LONGS BEFORE LATS
  colnames(coords2) <- c("Main_Beacon_Long_interp","Main_Beacon_Lat_interp") #MUST HAVE LONGS BEFORE LATS
  colnames(coords3) <- c("Long_ship","Lat_ship") #MUST HAVE LONGS BEFORE LATS
  colnames(coords4) <- c("loess_long", "loess_lat")
  Smooth_Lines[[k]] <- Lines(list(Line(coords1)), ID = as.character(Dives[k]))
  Interp_Lines[[k]] <- Lines(list(Line(coords2)), ID = as.character(Dives[k]))
  Ship_Lines[[k]] <- Lines(list(Line(coords3)), ID = as.character(Dives[k]))
  Loess_Lines[[k]] <- Lines(list(Line(coords4)), ID = as.character(Dives[k]))
}


#Create a SpatialLines object, containing the interpolated lines positions for all dives.

Smooth_Lines <- SpatialLines(Smooth_Lines, proj4string = CRS("+proj=longlat +datum=WGS84"))
Interp_Lines <- SpatialLines(Interp_Lines, proj4string = CRS("+proj=longlat +datum=WGS84"))
Ship_Lines <- SpatialLines(Ship_Lines, proj4string = CRS("+proj=longlat +datum=WGS84"))
Loess_Lines <- SpatialLines(Loess_Lines, proj4string = CRS("+proj=longlat +datum=WGS84"))
Planned_Lines <- SpatialLines(Planned_Lines, proj4string = CRS("+proj=longlat +datum=WGS84"))

#Make a SpatialLinesDataFrames

smooth <- SpatialLinesDataFrame(Smooth_Lines, data.frame(Dive_Name = Dives2, row.names = Dives))
unsmooth <- SpatialLinesDataFrame(Interp_Lines, data.frame(Dive_Name = Dives2, row.names = Dives))
ship <- SpatialLinesDataFrame(Ship_Lines, data.frame(Dive_Name = Dives2, row.names = Dives))
loess <- SpatialLinesDataFrame(Loess_Lines, data.frame(Dive_Name = Dives2, row.names = Dives))
planned <- SpatialLinesDataFrame(Planned_Lines, data.frame(Dive_Name = unique(planned_lines_DF$Line_Name), 
                                                           row.names = unique(planned_lines_DF$Line_Name)))

################################STEP 5 - CREAT LINE FILES AS KML AND SHAPE FILES#######################################################

#Write a .KML file for the lines of interpolated beacon data, the smoothed beacon data, and the ship GPS track,and
#the planned survey lines.

setwd(KML_path)
writeOGR(smooth, "Phantom Smoothed Position.kml", layer="Dive_Name", driver="KML", overwrite_layer = T)
writeOGR(unsmooth, "Phantom Unsmoothed Position.kml", layer="Dive_Name", driver="KML", overwrite_layer = T)
writeOGR(ship, paste0(ship_name,"_track.kml"), layer="Dive_Name", driver="KML", overwrite_layer = T)
writeOGR(loess, "Phantom Loess Positions.kml", layer= "Dive_Name", driver="KML", overwrite_layer = T)
writeOGR(planned, "Planned Lines.kml", layer= "Dive_Name", driver="KML", overwrite_layer = T)

#Write a .SHP file for the lines of interpolated beacon data, the smoothed beacon data, and the ship GPS track, and 
#the planned survey lines.

setwd(SHP_path)
writeOGR(smooth, dsn= SHP_path, layer= "Phantom Smoothed Position", driver="ESRI Shapefile", overwrite_layer = T)
writeOGR(unsmooth, dsn= SHP_path, layer= "Phantom Unsmoothed Position", driver="ESRI Shapefile", overwrite_layer = T)
writeOGR(ship, dsn= SHP_path, layer= paste0(ship_name," Track"), driver="ESRI Shapefile", overwrite_layer = T)
writeOGR(loess, dsn= SHP_path, layer= "Phantom Loess Position", driver="ESRI Shapefile", overwrite_layer = T)
writeOGR(planned, dsn= SHP_path, layer= "Planned Lines", driver="ESRI Shapefile", overwrite_layer = T)

######################################STEP 6 - CREATE A SPATIAL POINTS DATA FRAME FOR EACH TRANSECT, WRITE POINTS TO SHAPEFILE######################

#Create a SpatialPointsDataFrame for each dive, and write to shape file. This will contain all collected data in the
#attribute tables of the generated point file.

for(k in unique(Dives))
{
  name <- get(k)
  title <- gsub(".csv","",k)
  coords <- name[,c(5,4)]  #The Long and lat generated from the LOESS smoothing.
  data <- name[,c(1:3,6:38)]
  crs <- CRS("+proj=longlat +datum=WGS84")
  spdf <- SpatialPointsDataFrame(coords = coords, data = data, proj4string = crs)
  writeOGR(spdf, dsn= SHP_path, layer= paste0(title,"_Points"), driver="ESRI Shapefile", overwrite_layer = T)
}
