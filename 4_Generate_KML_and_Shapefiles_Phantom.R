#===============================================================================
# Script Name: 4_Generate_KML_and_Shapefiles_Phantom.R
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
# Jan 2022: - Moved processing of hypack raw files for planned transects to #1
#           - Wrote function for creating sp lines
#           - Reduced the number of ind files by exporting all points together
#           - Wrote function for spatial file export
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
  plannedTransects[,"Name", drop=F], match.ID = FALSE)

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



#===============================================================================
# STEP 4 - CREATE A SPATIAL POINTS DATA FRAME 

# Create a SpatialPointsDataFrame for all transect data. This will contain all 
# sensor data in the attribute table. Uses the LOESS smoothing coordinates
Loess_Points <- fdat
coordinates(Loess_Points) <- ~Beacon_Longitude_smoothed_loess + 
                              Beacon_Latitude_smoothed_loess
proj4string(Loess_Points) <- CRS("+proj=longlat +datum=WGS84")

# Check
plot(Loess_Points)



#===============================================================================
# STEP 5 - CREAT LINE FILES AS KML AND SHAPE FILES

# Export spatial layers function
exportSpatial <- function( layer, name, driver ){
  if( driver == "KML"){
    writeOGR(layer, file.path(KML_path, paste0(name,".kml")), layer="Name", 
             driver=driver, overwrite_layer = TRUE)
  } else if (driver == "ESRI Shapefile"){
    writeOGR(layer, dsn= SHP_path, layer=name, driver=driver, 
             overwrite_layer = TRUE)
  }
}

# Write KML files
kdriver <- "KML"
exportSpatial(Ship_Lines, paste0(ship_name,"_Track"), kdriver)
exportSpatial(Interp_Lines, "ROV_Unsmoothed_Track", kdriver)
exportSpatial(Smooth_Lines, "ROV_Smoothed_Track", kdriver)
exportSpatial(Loess_Lines, "ROV_Loess_Track", kdriver)
exportSpatial(Planned_Lines, "Planned_Transects", kdriver)

# Write shapefiles
edriver <- "ESRI Shapefile"
exportSpatial(Ship_Lines, paste0(ship_name,"_Track"), edriver)
exportSpatial(Interp_Lines, "ROV_Unsmoothed_Track", edriver)
exportSpatial(Smooth_Lines, "ROV_Smoothed_Track", edriver)
exportSpatial(Loess_Lines, "ROV_Loess_Track", edriver)
exportSpatial(Planned_Lines, "Planned_Transects", edriver)
exportSpatial(Loess_Points, "ROV_Loess_Points", edriver)
