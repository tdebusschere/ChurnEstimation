
library('optparse')
setwd("..")
source('RCode/WriteRetentionList.R')
source('RCode/buildModel.R')
source('RCode/querydata.R')
source('RCode/RConfig.R')

#' For the RetentionList Process a wrapper around the process
#' this executes several steps:
#' --> query data
#' --> build the model
#' --> write the model in the database
#' This piece of code is called from python / but can be used independently
#' 
#' Author: Debusschere Tom; 1/30/2020
#' Version 0.2.0
#'
#' @param retentionlist_website: the systemcode of the website to analyze (DQ002 / FJ001)
#' @param retentionlist_activity: the date that needs to be executed
#' @param retentionlist_version: indicates if it is a test run or a production run
#' @return 
#' 


retentionlist_type <- ''
RetentionRateWrapper = function( retentionlist_website, 
                                 retentionlist_activity,
                                 retentionlist_version){
  ModelParameters <- GetModels()
  for (retentionlist_type in ('Whale')) {
    input_data <- QueryData( retentionlist_website, 
                             retentionlist_type, 
                             retentionlist_activity,
                             retentionlist_version) 
    model_selection <- ModelParameters[ (ModelParameters$SystemCode == retentionlist_website) &
                                        (ModelParameters$Type       == retentionlist_type), ]

    retentionlist_model <- BuildModel( input_data, 
                                       retentionlist_activity,
                                       ModelParameters )

    WriteRetentionList( retentionlist_model)

  }
}



#' For the RetentionList Process a function to connect to the DB
#' 
#' Author: Debusschere Tom; 2/15/2020
#' Version 0.2.0
#'
#' @param db: What DB to Connect to
#' @return 
#' 
ConnectToDB = function(db)
{
  db_user     <- DB_Settings[['User']]
  db_password <- keyring::key_get(DB_Settings[[db]], db_user)
  con <- odbcDriverConnect(paste0('driver={SQL Server};server=JG\\MSSQLSERVER2016;uid=', 
                                  db_user, ';pwd=', db_password))
  return(con)
}



#' For the RetentionList Process a function to connect to the DB
#' 
#' Author: Debusschere Tom; 2/15/2020
#' Version 0.2.0
#'
#' @param db: What DB to Connect to
#' @return : a connection string
#' 
GetModels = function ()
{
  db    <- DB[['ModelParameters']][['Database']]
  table <- DB[['ModelParameters']][['Table']]
  
  conn <- ConnectToDB(db)
  ModelParameters <- sqlQuery(conn, paste0("SELECT * FROM ", table," WHERE ACTIVE = 1"))
  return(ModelParameters)
}



option_list = list(
  make_option(c("-s", "--site"), type="character", default="", 
              help="site", metavar="character"),
  make_option(c("-a", "--activity"), type='character',default="",
              help="activity", metavar="character"),
  make_option(c("-v", "--version"), type='character', default='',
              help="version: _test or ''", metavar = "character")
)

opt_parser = OptionParser(option_list=option_list);
parsed_opts = parse_args(opt_parser);

print(parsed_opts)
# run the process with one site and activity; type is currently hardcoded, 
# but will be included as a parameter later

RetentionRateWrapper(parsed_opts$site, parsed_opts$activity, parsed_opts$version)
