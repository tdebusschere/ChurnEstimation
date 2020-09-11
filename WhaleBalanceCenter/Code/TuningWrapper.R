
setwd("..")

library('optparse')

#source('RCode/WriteRetentionList.R')
#source('RCode/BuildModel.R')
source('RCode/QueryData.R')
source('RCode/RConfig.R')
source('RCode/TuningFunctions.R')
source('RCode/TuningConfig.R')
source('RCode/TuneModel.R')

#' For the TuningProcess  Process a wrapper around the process
#' this executes several steps:
#' --> query data
#' --> tune the model
#' --> write the model parameters in the database
#' This piece of code is called from python / but can be used independently
#' 
#' Author: Debusschere Tom; 5/18/2020
#' Version 0.2.0
#'
#' @param retentionlist_website: the systemcode of the website to analyze
#' @param retentionlist_activity: the date that needs to be executed
#' @param retentionlist_version: indicates if it is a test, production or initiation run.
#' @return basic tuning parameters for the model
#' 



TuningWrapper = function( retentionlist_website, 
                          retentionlist_activity,
                          retentionlist_version){
  for (retentionlist_type in c('Whale','Dolphin')) {
    input_data <- QueryData( retentionlist_website, 
                             retentionlist_type, 
                             retentionlist_activity,
                             retentionlist_version,
                             diff = 90) 
    
    retentionlist_model <- TuneModel ( input_data, 
                                       retentionlist_activity )
    
    WriteTunedModel ( retentionlist_model)
    
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
  db_password <- kd$get(DB_Settings[[db]]['DB'], db_user)
  db_ip       <- DB_Settings[[db]][['IP']]
  
  con <- odbcDriverConnect(paste0('driver={ODBC DRIVER 17 FOR SQL Server};server=',db_ip,
                                  ';uid=',  db_user, ';pwd=', db_password))
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
  ModelParameters <- sqlQuery(conn, paste0("SELECT * FROM ", table," WHERE ACTIVE = 1"), stringsAsFactors=FALSE)
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

TuningWrapper(parsed_opts$site, parsed_opts$activity, parsed_opts$version)
