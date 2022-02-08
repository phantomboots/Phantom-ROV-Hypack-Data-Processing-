#===============================================================================
# Script Name: 4b_Generate_KML_and_Shapefiles_Phantom.R
# Script Function: This script reads in the .CSV files created from 
#                 "3_QAQC_Interpolation_Offset_and_Data_Smoothing.R". Creates 
#                 Hypack Planned lines, GPS tracks, and unsmoothed/smoothed 
#                 transponder fixes are written to .KML and .SHP files. 
#                 Shapefiles include a spatial lines file, as well as Spatial 
#                 Points file for each dive. The Spatial Points file contains
#                 all collected data, which is written to the attribute table 
#                 for the file. Spatial lines .SHP are generated from both the 
#                 running median and LOESS smoothing of the Lat/Longs. Spatial 
#                 Points .SHP are generated from the LOESS data only (i.e. 
#                 generally the more accurate of the two smoothing functions).
#
# Script Author: Ben Snow
# Script Date: Jul 2, 2020
# R Version: 3.5.1


################################################################################
#                                 CHANGE LOG                                   #
################################################################################
#
# Jan 2022: - 
################################################################################




#===============================================================================
# Packages 

# Check if necessary packages are present, install as required
packages <- c("rgdal","dplyr")
new_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
if(length(new_packages)) install.packages(new_packages)

# Load required packages
lapply(packages, require, character.only = TRUE)



#===============================================================================
# STEP 1 - SET PATHS AND SELECT OPTIONS, MAKE EXPORT DIRECTORY

# Set working directory
# Use getwd() if you are already in the right directory
# The project folder and needs to be in the working directory
wdir <- getwd() 

# Project folder
project_folder <- "Pac2021-054_phantom"

# Name of Ship used in the survey
ship_name <- "RV Manyberries"

# Directories where final processed data are stored
imports_dir <- file.path(wdir, project_folder, "Data/3.Final_Processed_Data")
hypack_dir <- file.path(wdir, project_folder, "Data/1.Hypack_Processed_Data")

# Export directories 
KML_path <- file.path(wdir, project_folder, "Data/4.Spatial_Data/KML")
dir.create(KML_path, recursive = TRUE) # Will warn if already exists
SHP_path <- file.path(wdir, project_folder, "Data/4.Spatial_Data/Shapefiles")
dir.create(SHP_path, recursive = TRUE) # Will warn if already exists



#===============================================================================
# STEP 2 - LOAD PROCESSED DATA

# Load all sensor data (fdat)
load(file=file.path(imports_dir, paste0(project_folder, 
                                        "_alltransect_NAV.RData")))
# Load planned lines (plannedTransects)
load(file=file.path(hypack_dir, "Hypack_PlannedTransects.RData"))



#===============================================================================
# STEP 3 - CREATE SPATIAL LINES OBJECTS


# Create sp lines for planned transects
# Empty list to fill
slines <- list()
for (i in 1:nrow(plannedTransects)) {
  slines[[i]] <- Lines(Line(rbind(
    as.numeric(plannedTransects[i, c("Start_Longitude","Start_Latitude")]), 
    as.numeric(plannedTransects[i, c("End_Longitude", "End_Latitude")]))), 
    ID = as.character(i))
}
# Create spatial lines data frame from list of lines
Planned_Lines <- SpatialLinesDataFrame(
  SpatialLines(slines, proj4string = CRS("+proj=longlat +datum=WGS84")), 
  plannedTransects, match.ID = FALSE)

# Check
plot(Planned_Lines)

# Create sp lines from actual surveyed positions
# Smoothed and unsmoothed for the ROV and ship

# Create lines function 
createLines <- function(dat, x, y){
  # Empty list to fill
  slines <- list()
  # Create lines for each transect
  for (i in unique(dat$Transect_Name)) {
    tmp <- dat[dat$Transect_Name == i,]
    slines[[i]] <- Lines(list(Line(tmp[c(x,y)])), ID = as.character(i))
  }
  # Create spatial lines data frame from list of lines
  sldf <- SpatialLinesDataFrame(
    SpatialLines(slines, proj4string = CRS("+proj=longlat +datum=WGS84")), 
    data.frame(Name=unique(dat$Transect_Name)), match.ID = FALSE)  
  # Return
  return(sldf)
}

# Create ship lines
Ship_Lines <- createLines(dat=fdat,
                            x="Ship_Longitude",
                            y="Ship_Latitude")
# Create ROV lines
Interp_Lines <- createLines(dat=fdat,
                            x="Beacon_Longitude_offset",
                            y="Beacon_Latitude_offset")
# Create smoothed ROV lines
Smooth_Lines <- createLines(dat=fdat,
                            x="Beacon_Longitude_smoothed_window",
                            y="Beacon_Latitude_smoothed_window")
# Create loess ROV lines
Loess_Lines <- createLines(dat=fdat,
                            x="Beacon_Longitude_smoothed_loess",
                            y="Beacon_Latitude_smoothed_loess")
# Check
plot(Ship_Lines)
plot(Interp_Lines, add=T, col="red")
plot(Smooth_Lines, add=T, col="blue")
plot(Loess_Lines, add=T, col="green")



# -- start here


#===============================================================================
# STEP 4 - CREATE A SPATIAL POINTS DATA FRAME 

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


#===============================================================================
# STEP 5 - CREAT LINE FILES AS KML AND SHAPE FILES

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

