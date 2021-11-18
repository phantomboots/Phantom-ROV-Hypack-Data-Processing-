#=====================================================================================================
# Script Name: Append RBR CTD data to NAV Export
# Script Function: Reads in RBR CTD data that has been exported from Ruskin to Excel files (.xlsx). Merges 
#                 all .xlsx files in working directory to a single, large file. Reads in Hypack NAV exports, 
#                 then searches for matches amongst the RBR data set and binds them to the NAV export data. 
#                 Writes the merged data to new .CSV files. 
# Script Author: Ben Snow
# Script Date: Aug 27, 2019
# R Version: 3.5.1
#
#
######################################################################################################
#                                            CHANGE LOG                                              #
######################################################################################################
#
# May 13, 2020: Added a loop (lines 108-129) to check to see if CTD data was missing from any files, if so new loop 
#               will attempt to retrieve missing data from ASDL and will fill in missing records if possible
#               Note that this process will not provide Dissolved Oxygen Concentration of Density Anomaly data, as
#               these are calculated in Ruskin. May be possible to manually calculate these and add at a later date.
#
#=====================================================================================================

#Check if necessary packages are present, install as required.
packages <- c("lubridate","readxl","readr","dplyr")
new_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
if(length(new_packages)) install.packages(new_packages)


#Required packages#

require(lubridate)
require(readxl)
require(readr)
require(dplyr)


#################################################STEP 1 - EDIT THESE VALUES ##################################

#Project folder name 

project_folder <- "~/Projects/Apr2021_Phantom_Cruise_PAC2021_035"

########################################STEP 2 - CHECK FOR AND GENERATE DIRECTORIES############################################## 


#Set working directory for location of RBR excel files (.xlsx) exported from Ruskin.

RBR_input <- paste0(project_folder, "/Data/RBR_CTD_Data")

#Set working directory for the ASDL RBR_Master, in case data needs to be recoverd

Master_ASDL <- paste0(project_folder, "/Data/Advanced_Serial_Data_Logger/Full_Cruise")

#Set working directory for location of smoothed .CSV files from the Hypack Export

NAV_input <- paste0(project_folder, "/Data/Secondary_Processed_Data")

#Set a path for saving data exported at the end of this script

save_path <- paste0(project_folder, "/Data/Final_Processed_Data")

#Vector of directories to check for

dirs <- c(NAV_input, save_path, Master_ASDL, RBR_input)

#Check and create directories as needed.

for(i in unique(dirs))
{
  if(dir.exists(i) == FALSE)
  {
    dir.create(i, recursive = TRUE)
  }
}


#######################################STEP 3 - READ IN RBR EXCEL FILES to DATA FRAMES############################################

#Read in the RBR CTD Master Log from ASDL

setwd(Master_ASDL)
RBR_Master <- read_csv("RBR_CTD_MasterLog.csv")

#Read in RBR data from .xlsx files.

setwd(RBR_input)

#Create blank data frame to fill in the loop below.

RBR_merged <- data.frame()

#Read in all RBR .xlsx files in the directory, and merged them into one larger file.

RBR_files <- list.files(pattern = ".xlsx")

for(i in 1:length(RBR_files))
{
  name <- as.character(i)
  assign(name, read_xlsx(RBR_files[i], sheet = "Data" , skip = 1))
  RBR_merged <- bind_rows(RBR_merged, get(name))
  rm(list = c(i)) #Discard the temp files.
}

######################################STEP 4 - READ IN PROCESSED NAV DATA .CSV FILES###########################################

#Move back to the directory with the Processed .CSV data.

setwd(NAV_input)
NAV_files <- list.files()

#Read in all NAV Data files into their own data frames.

for(i in 1:length(NAV_files))
{
  assign(NAV_files[i], read_csv(NAV_files[i]))
  names <- names(get(NAV_files[i])) #Store the columns names in a character vector, for use later.
}

#Match the RBR WQ data to each of the NAV data files, and append that WQ data to those files.

for(k in 1:length(NAV_files))
{
  temp <- get(NAV_files[k])
  temp <- left_join(temp, RBR_merged, by = c("date_time" = "Time"))
  assign(NAV_files[k], temp)
}

################################STEP 5 - CHECK FOR MISSING DATA FROM RBR EXPORTS, FILL IN FROM ASDL IF POSSIBLE ####################

#Check to see if there are NA values in any of the RBR WQ data that was appended to the NAV files in the left_join() from
#the previous loop. If NAs are present, it may mean that the data was not saved as a .RSK file, or that Ruskin crashed/failed 
#during the export. If this is the case, data may still be recoverable from the ADSL records. Check the RBR_Master file created by 
#'2_ASDL Data Parser_Phantom.R' to see if backup data can be found. If backup data is present, fill in the NA values with this data.

for(j in 1:length(NAV_files))
{
  fill <- get(NAV_files[j])
  temp1 <- which(is.na(fill$Pressure)) #Indices where there are NA values in the CTD data set from the Ruskin excel exports
  temp2 <- fill$date_time[temp1] #Timestamps of the indices where the NA values are
  RBR_to_fill <- get("RBR_Master")
  index <- match(temp2, RBR_to_fill$date_time)

  #Try to fill in the missing water quality data from the RBR Master
  
  fill$Conductivity[temp1] <- RBR_to_fill$`Conductivity_mS/cm`[index]
  fill$Temperature[temp1] <- RBR_to_fill$Temp_C[index]
  fill$Pressure[temp1] <- RBR_to_fill$Pressure_dbar[index]
  fill$`Dissolved O2 saturation`[temp1] <- RBR_to_fill$`Dissolved_02_sat_%`[index]
  fill$`Sea pressure`[temp1] <- RBR_to_fill$Sea_Pressure_dbar[index]
  fill$Depth[temp1] <- RBR_to_fill$Depth_m[index]
  fill$Salinity[temp1] <- RBR_to_fill$Salinity_PSU[index]
  fill$`Speed of sound`[temp1] <- RBR_to_fill$`Sound_Speed_m/s`[index]
  fill$`Specific conductivity`[temp1] <- RBR_to_fill$`Specific_Cond_uS/cm`[index]
  
  assign(NAV_files[j], fill)
}

#####################################STEP 6 - RENAME RBR CTD DATA COLUMNS, MERGE WITH NAV DATA ########################

#Rewrite the columns names

full_names <- c(names, "Conductivity_mScm","Temperature_C","Pressure_dbar","Ox_Sat_percent","Sea_pressure_dbar",
                "Depth_m","Salinity_PSU","SoundVelocity_ms","Specific_Cond_uScm","Density_kg_m3","Ox_conc_mgL")

#Merge the re-named CTD data columns with the rest of the NAV data.

for(h in 1:length(NAV_files))
{
  temp <- get(NAV_files[h])
  names(temp) <- full_names
  temp <- temp[, c(1:28,30:39)] #Remove the conductivity column, keep only specific conductivity.
  assign(NAV_files[h], temp)
}

###################################STEP 7 - WRITE NEW .CSV FILES FOR EACH MERGED NAV + RBR DATA FILE ##################


#Write new .CSV files for each of the data frames with merged data in it.

for(j in 1:length(NAV_files))
{
  temp <- get(NAV_files[j])
  write.csv(temp, paste0(save_path,"/",NAV_files[j]), quote = F, row.names = F)
}

