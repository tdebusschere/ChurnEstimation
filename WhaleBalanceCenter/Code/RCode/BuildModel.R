
library(RODBC)
library(dplyr)
library(pipeR)
library(odbc)
library(keyring)
library(catboost)

set.seed(101)

#model_parameters<- model_selection

BuildModel = function(retentionlist_data, retentionlist_activity, 
                      model_parameters)
{
  retentionlist_data$diff <- retentionlist_data$days_since_last_activity
  retentionlist_data$id <- retentionlist_data$memberlevelsettingid
  retentionlist_data$days_since_last_activity <- NULL
  retentionlist_data$memberlevelsettingid <- NULL
  retentionlist_data$leaveday <- NULL
  
  retentionlist_data[is.na(retentionlist_data$avg_depositamount_decreasing),'avg_depositamount_decreasing'] = 0
  retentionlist_data[is.na(retentionlist_data$avg_deposittimes_decreasing),'avg_deposittimes_decreasing'] = 0
  retentionlist_data[is.na(retentionlist_data$avg_withdrawamount_increasing),'avg_withdrawamount_increasing'] = 0
  
  retentionlist_data$decreasing_times <- as.integer(retentionlist_data$avg_depositamount_decreasing) + 
                                         as.integer(retentionlist_data$avg_deposittimes_decreasing) +
                                         as.integer(retentionlist_data$avg_withdrawamount_increasing) 
  
  
  t3 <- ( retentionlist_data$walletamt7d + retentionlist_data$walletamt6d + 
            retentionlist_data$walletamt5d )/3
  t2 <- ( retentionlist_data$walletamt4d + retentionlist_data$walletamt3d ) / 2
  t1 <- ( retentionlist_data$walletamt2d + retentionlist_data$walletamt1d ) / 2
  retentionlist_data$t2 <- ifelse(t3 > t2, 1, 0)
  retentionlist_data$t1 <- ifelse(t2 > t1, 1, 0)
  
  retentionlist_data$dom_f <- as.factor( retentionlist_data$dom )
  retentionlist_data$dow_f <- as.factor( retentionlist_data$dow )
  retentionlist_data$amountsum <- 1.0
  
  print(model_parameters)
  fit_params <- list(iterations    = model_parameters['Iterations'][[1]],  #1000
                     #border_count = 210,
                     depth         = model_parameters['Depth'][[1]],
                     learning_rate = model_parameters['LearningRate'][[1]],
                     l2_leaf_reg   = model_parameters['L2LeafReg'][[1]],
                     #rsm           = model_parameters['Rsm'][[1]],
                     verbose = 500,
		                 task_type='GPU',
		                 devices='0:1:2',
                     loss_function = model_parameters['LossFunction'][[1]]#, task_type     = 'GPU'
                     )
  print(fit_params)
  rate_in_all <- eval( parse(
    text = model_parameters[1,'RateInAll']))  #0.2

  retentionlist_data[, need_factor_col] <- lapply(retentionlist_data[, need_factor_col], factor)
  
  end_date <- as.Date(retentionlist_activity)
  end <- end_date - minus
  start <- end - diff
  
  merge_w <- retentionlist_data[which(as.Date(retentionlist_data$activity) <= end &
                                      as.Date(retentionlist_data$activity) >= start), ]
  #give priority to the first leavecategory or put 
  tmp <- merge_w
  merge_w$isloss <- apply(merge_w[,train_indicators] == 1,1,which.max)
  merge_w$isloss[ merge_w$isloss == length(train_indicators)] = 0
  merge_w <- merge_w[, -del]

  
  #balance
  str <- merge_w[0, ]
  for (k in 0:4) 
  {  
    rate <- rate_in_all[k+1]
    tmp <- merge_w[which(merge_w$isloss==k), ]
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
  #test2 <- retentionlist_data[which(as.Date(retentionlist_data$activity) == '2020-03-30'),]
  
  #test_final <- rbind(test_final,test2)
  
  test_final$isloss <- apply(test_final[,test_indicators] == 1,1,which.max)
  test_final$isloss[ test_final$isloss == length(test_indicators)] = 0
  test_final <- test_final[, -Resultdel]

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
                            type= test_final$type,
                            activity=end_date,
                            pred = results[final_ans + 1])
  return(output_list[] )

}

