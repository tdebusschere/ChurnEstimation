
library(RODBC)
library(dplyr)
library(pipeR)
library(odbc)
library(keyring)
library(catboost)

#' For the RetentionList Process query website (systemcode; eg. DQ002), 
#' type (whale, middle/dolphin, ...) This is the part of the code that 
#' 
#' This daterange is now 30 days (Should this become a parameter?)
#'
#' Author: 景雯;  2/14/2020
#' Version 0.2.0
#'
#' @param retentionlist_website: the systemcode of the website to analyze (DQ002 / FJ001)
#' @param retentionlist_data: data that is queried (Whale, Dolphin, ...)
#' @param retentionlist_activity: the date that needs to be executed
#' @return output_list: values that are to be written in database 
#' (already filtered out the ones that are estimated not to leave)
#' 



BuildModel = function(retentionlist_data, retentionlist_activity) {
  retentionlist_data$diff <- retentionlist_data$days_since_last_activity
  retentionlist_data$id <- retentionlist_data$memberlevelsettingid
  retentionlist_data$days_since_last_activity <- NULL
  retentionlist_data$memberlevelsettingid <- NULL
  retentionlist_data$leaveday <- NULL
  
  
  retentionlist_data$decreasing_times <- retentionlist_data$avg_depositamount_decreasing + 
  retentionlist_data$avg_deposittimes_decreasing +
  retentionlist_data$avg_withdrawamount_increasing
  
  t3 <- (retentionlist_data$walletamt7d + retentionlist_data$walletamt6d + 
           retentionlist_data$walletamt5d)/3
  t2 <- (retentionlist_data$walletamt4d + retentionlist_data$walletamt3d) / 2
  t1 <- (retentionlist_data$walletamt2d + retentionlist_data$walletamt1d) / 2
  retentionlist_data$t2 <- ifelse(t3 > t2, 1, 0)
  retentionlist_data$t1 <- ifelse(t2 > t1, 1, 0)
  
  retentionlist_data$dom_f <- as.factor(retentionlist_data$dom)
  retentionlist_data$dow_f <- as.factor(retentionlist_data$dow)
  retentionlist_data$amountsum <- 1.0
  
  
  set.seed(101)
  #This and other parameters can and probably should be moved to a configurationfile
  need_factor_col <- c('d14today','d14_0','d15_0','d14_1','d15_1','d14_3','d15_3','d14_5',
                       'd15_5','d14_7','d15_7','d14_14','d15_14','t2','t1','dom_f','dow_f',
                       #added on 4/22
                       'd15today','d24today','d25today','d24_0','d25_0','d24_1','d25_1',
                       'd24_3','d25_3','d24_5','d25_5','d24_7','d25_7','d24_14','d25_14',
                       'did_withdraw','d30orbiggerleave7d0','d30orbiggerleave7d1',
                       'd30orbiggerleave7d3','d30orbiggerleave7d5','d30orbiggerleave7d7',
                       'd30orbiggerleave7d14','m1leave7d0','m1leave7d1','m1leave7d3',
                       'm1leave7d5','m1leave7d7','m1leave7d14','id','avg_depositamount_decreasing',
                       'avg_deposittimes_decreasing','avg_withdrawamount_increasing')
  del_var <- c('creditdeposittimes',
               'discountsum', 'amountsum',
               'otherssum','did_withdraw',
               'd15today','d25today','d30orbiggerleave7d1','d30orbiggerleave7d3',
               'd30orbiggerleave7d5','m1leave7d0','m1leave7d1','m1leave7d3',
               'm1leave7d5','d24today','d25today','d24_0','d25_0','d24_1','d25_1',
               'd24_3','d25_3','d24_5','d25_5','d24_7','d25_7','d24_14','d25_14','t2','t1')
  train_indicators <- c('leave7d0','leave7d1','leave7d3','leave7d5','amountsum')
  test_indicators  <- c(train_indicators[1:4], 'leave7d7', 'leave7d14','amountsum')
  ##modelparameters
  minus <- c(15)
  del   <- c(1:10, 12, 18:25)
  diff <- 30
  
  fit_params <- list(iterations = 500,  
                     depth = 10,
                     learning_rate = 0.15,
                     l2_leaf_reg = 1,
                     rsm = 0.7,
                     verbose = 500,
                     loss_function  = "MultiClass")
  
  rate_in_all <- c(0.5, 0.125, 0.125, 0.125, 0.125)  #0.2
  result_weights = c(-1,0,1,3,5)
  results = c(-1,0,1,3,5)

  retentionlist_data[, need_factor_col] <- lapply(retentionlist_data[, need_factor_col], factor)
  
  end_date <- as.Date(retentionlist_activity)
  end <- end_date - minus
  start <- end - diff
  
  merge_w <- retentionlist_data[which(as.Date(retentionlist_data$activity) <= end &
                                        as.Date(retentionlist_data$activity) >= start), ]
  
  #give priority to the first leavecategory or put 
  merge_w$isloss <- apply(merge_w[,train_indicators] == 1,1,which.max)
  merge_w$isloss[ merge_w$isloss == length(train_indicators)] = 0
  merge_w <- merge_w[, -del]
  
  #balance
  str <- merge_w[0, ]
  for (k in 0:4) {  
    rate <- rate_in_all[k+1]
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
  
  #test data
  test_final <- retentionlist_data[which(as.Date(retentionlist_data$activity) == end_date), ]
  test_final$isloss <- apply(test_final[,test_indicators] == 1,1,which.max)
  test_final$isloss[ test_final$isloss == length(test_indicators)] = 0
  test_final <- test_final[, -c(2, 4:10, 12, 18:25)]
  
  ##################################################################
  ## train
  
  test_final <- test_final[, !(colnames(test_final) %in% del_var)]
  wdata_s <- wdata_s[, !(colnames(wdata_s) %in% del_var)]
  factor <- need_factor_col[!(need_factor_col %in% del_var)]
  factor_n <- which(colnames(wdata_s) %in% factor)  - 1  ##因為是用python的encoding
  objindex <- colnames(wdata_s) %in% c('isloss')
  
  wdata_s <- wdata_s[sample(nrow(wdata_s)),]
  
  # train test
  train <- catboost.load_pool(wdata_s[,!objindex], 
                              label = wdata_s[,objindex], 
                              cat_features = factor_n)
  model <- catboost.train(train, params = fit_params)
  
  final_ans <- catboost.predict(model,#model, 
                                catboost.load_pool(test_final,
                                                   cat_features = factor_n),
                                prediction_type = "Class")
  
  
  output_list <- data.frame(Web = as.character(unique(retentionlist_data$website)), 
                            memberid = test_final$memberid,
                            type=test_final$type,
                            activity=end_date,
                            pred = results[final_ans + 1])
  
  return(output_list[which(output_list$pred!=-1),] )
  
}

#change_factor <- function(a) {
#  #a must be a vector
#  a <- ifelse(a == 0, "active",
#              ifelse(a == 1, 'd0',
#                     ifelse(a == 2, "d1",
#                            ifelse(a == 3, "d3", 'd5'))))
#  result <- factor(a, levels = c("active", "d0", "d1", "d3", "d5"))
#  return(result)
#}

#change_factor_test <- function(a) {
#  # a must be a vector
#  a <- ifelse(a == 0, "active",
#              ifelse(a == 1, "d0",
#                     ifelse(a == 2, "d1",
#                            ifelse(a == 3, "d3",
#                                   ifelse(a == 4, "d5",
#                                          ifelse(a == 5, "d7", "d14"))))))
#  result <- factor(a, levels = c("active", "d0", "d1", "d3", "d5", "d7", "d14"))
#  return(result)
#}