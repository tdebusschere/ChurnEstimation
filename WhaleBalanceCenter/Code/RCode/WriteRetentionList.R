
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
  output_list <- distinct(output_list)
  output_list <- output_list[output_list$pred != -1,]
  op <- output_list[,1:5]
  op$activity <- factor(op$activity )
  colnames(op) <- c('SystemCode', 'MemberId', 'Type', 'ExecuteDay', 'Leaveday')
  
  #Result
  channel <- ConnectToDB("DB190")
  
  sql_string <- "INSERT INTO DataScientist.dbo.EstimationResults VALUES"
  tmp_string <- ''
  counter <- 0
  #sqlSave(channel, op, tablename = "##tmp545", append = F, rownames = F, safer = TRUE)
  odbcQuery(channel, paste0("DELETE FROM DataScientist.dbo.EstimationResults 
                                    WHERE executeday ='",unique(op$ExecuteDay),"' AND 
                                          TYPE = '", unique(op$Type),"' AND
                                          Systemcode = '", unique(op$SystemCode),"'"))
  for ( k in c(1:dim(op)[1]))  {
    line <- paste0(" ('", op[k,'SystemCode'],"',",
                          op[k,'MemberId'],",'",
                          op[k,'Type'],"','",
                          op[k,'ExecuteDay'],"',",
                          op[k,'Leaveday'],")")
    if ((k %% 10) || (k == dim(op)[1]) ){
      tmp_string <- paste0(tmp_string, line)
      #print(sqlString)
      odbcQuery(channel,tmp_string)
      tmp_string <- sql_string
    } else {
      tmp_string <- paste0(tmp_string, line , ",")
    }
  }
  print(k)
  print(dim(op)[1])
  }


