
#' Tune The Model
#' this executes several steps:
#' --> preprocess the data
#' --> tune the model
#' --> 
#' This piece of code is called from python / but can be used independently
#' 
#' Author: Debusschere Tom; 5/21/2020
#' Version 0.2.0
#'
#' @param input_data: data used in tuning the model
#' @param retentionlist_activity: last date of the model
#' @return tuningsettings: the optimal results 
#' 

TuneModel <- function( input_data, retentionlist_activity)
{
  end_date <- as.Date(retentionlist_activity, origin = "1970-01-01")
  training_end    <-as.Date(min(input_data$activity), origin = "1970-01-01") 
  training_start   <- training_end + 30
  
  test_start <- end_date - 37
  test_end   <- training_start + 15
  
  ## Traindata 
  merge_w <- input_data[which(as.Date(data$activity) <= training_start & as.Date(data$activity) >= training_end), ]
  merge_w$isloss <- ifelse(merge_w$leave7d0 == 1, 1, 
                           ifelse(merge_w$leave7d1 == 1, 2,
                                  ifelse(merge_w$leave7d3 == 1, 3,
                                         ifelse(merge_w$leave7d5 == 1, 4, 0))))
  merge_w$leave7d5 <- merge_w$isloss
  merge_w$isloss <- NULL
  merge_w <- merge_w[, -del]
  colnames(merge_w)[2] <- "isloss"
  
  #balance
  rate_inAll <- c(0.5, 0.125, 0.125, 0.125, 0.125)  #0.2
  str <- merge_w[0, ]
  for (k in 0:4) 
  {  
    rate <- rate_inAll[k+1]
    tmp <- merge_w[which(merge_w$isloss==k), ]
    set.seed(101)
    if (nrow(tmp) >= nrow(merge_w)* rate) {
      tmp <- tmp[sample(1:nrow(tmp), round(nrow(merge_w)* rate), replace = F), ]  
    } else {
      tmp <- tmp[sample(1:nrow(tmp), round(nrow(merge_w)* rate), replace = T), ]
    }
    str <- rbind(str, tmp)
  }
  wdata_s <- str
  
  TestFinal <- input_data[which(as.Date(input_data$activity)    > test_end &
                                as.Date(input_data$activity)    <= test_start), ]
  TestFinal$isloss <- ifelse(TestFinal$leave7d0 == 1, 1, 
                             ifelse(TestFinal$leave7d1 == 1, 2,
                                    ifelse(TestFinal$leave7d3 == 1, 3,
                                           ifelse(TestFinal$leave7d5 == 1, 4,
                                                  ifelse(TestFinal$leave7d7 == 1, 5, 
                                                         ifelse(TestFinal$leave7d14 == 1, 6, 0))))))
  TestFinal$isloss2 <- ifelse(TestFinal$leave7d0 == 1, 1, 
                              ifelse(TestFinal$leave7d1 == 1, 2,
                                     ifelse(TestFinal$leave7d3 == 1, 3,
                                            ifelse(TestFinal$leave7d5 == 1, 4, 0))))
  TestFinal$leave7d0 <- TestFinal$isloss
  TestFinal$isloss <- NULL
  TestFinal <- TestFinal[, -c(2,4,6:10,18:25)]
  colnames(TestFinal)[3] <- "isloss"
  
  TestFinal <- TestFinal[, !(colnames(TestFinal) %in% del_var)]
  
  wdata_s2 <- wdata_s[, !(colnames(wdata_s) %in% del_var)]
  goal <- wdata_s2$isloss
  wdata_s3 <- wdata_s2[,colnames(wdata_s2)[c(3:length(colnames(wdata_s2)))]]
  obj <- colnames(TestFinal)[colnames(TestFinal) %in% colnames(wdata_s3)]
  
  factor <- need_FactorCol[!(need_FactorCol %in% del_var)]
  factor_n <- which(colnames(wdata_s3) %in% factor)    ##?]???O??python??encoding
  factor_n2 <- which(colnames(TestFinal[obj,]) %in% factor)

  results= c()
  for ( k in c(1:dim(searchGridSubCol)[1]))
  {
    results = c(results, TuneCatBoost(searchGridSubCol[k,], wdata_s3, goal, factor_n, TestFinal, obj, factor_n2))
    
  }

  
}
