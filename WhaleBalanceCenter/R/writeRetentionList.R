
#' For the RetentionList; writes data to database (190);
#' This piece of code is called from RetentionRateWrapper
#' 
#' Author: Debusschere Tom; 1/30/2020
#' Version 0.2.0
#'
#' @param output_list: result of the retentionlist estimation (DQ002 / FJ001)
#' @return 
#' 


WriteRetentionList<- function(output_list)
{ 
  op <- output_list[,1:5]
  op$activity <- factor(op$activity )
  colnames(op) <- c('SystemCode', 'MemberId', 'Type', 'ExecuteDay', 'Leaveday')
  
  #Result
  ds_user <- 'DS.Tom'
  ds_password <- keyring::key_get("db_DataScience", ds_user)
  channel <- odbcDriverConnect(paste0('driver={SQL Server};server=10.80.16.190;database=tempdb;
                                       uid=', ds_user,';pwd=',ds_password))
  
  sqlSave(channel, op, tablename = "##tmp545", append = TRUE, rownames = F, safer = TRUE)
  odbcQuery(channel, paste0("DELETE FROM DataScientist.dbo.EstimationResults where executeday ='",unique(op$ExecuteDay),"'"))
  print(paste0("DELETE FROM DataScientist.dbo.EstimationResults where executeday ='",unique(op$ExecuteDay),"'"))
                     
  sqlQuery(channel, "Insert into DataScientist.dbo.EstimationResults 
                     SELECT * FROM ##tmp545")
}


