
library(e1071)
library(caret)
library(dplyr)
library(catboost)
library(ROSE)
library(data.table)
library(pipeR)
library(RODBC)
library(odbc)



## Fuction Zone
change_factor <- function(a) {
  # a must be a vector
  a <- ifelse(a == 0, "active",
              ifelse(a == 1, 'd0',
                     ifelse(a == 2, "d1",
                            ifelse(a == 3, "d3", 'd5'))))
  result <- factor(a, levels = c("active", "d0", "d1", "d3", "d5"))
  return(result)
}
change_factor_test <- function(a) {
  # a must be a vector
  a <- ifelse(a == 0, "active",
              ifelse(a == 1, "d0",
                     ifelse(a == 2, "d1",
                            ifelse(a == 3, "d3", 
                                   ifelse(a == 4, "d5", 
                                          ifelse(a == 5, "d7", "d14"))))))
  result <- factor(a, levels = c("active", "d0", "d1", "d3", "d5", "d7", "d14"))
  return(result)
}



TuneCatBoost <- function(parameterList, wdata_s3, goal, factor_n, TestFinal, obj, factor_n2){
  settings <- list()
  #Extract Parameters
  for (k in names(parameterList))
  {
    settings[[k]] <- parameterList[[k]]
  }
  settings[['loss_function']] <- 'MultiClass'
  settings[['custom_loss']] <- 'MultiClass'
  settings[['task_type']] <- 'GPU'
  settings[['devices']] <- '3'
  settings[['verbose']] <- 500
  score <- list()
  #CV
  
  
  traindat <- catboost.load_pool(data = wdata_s3[,c(1:5)], 
                                 label = goal,
                                 cat_features = factor_n)
  test <- catboost.load_pool(data = TestFinal[,obj],
                             label = TestFinal[, 'isloss2'],
                             cat_features = factor_n2)
  test2 <- catboost.load_pool(data = TestFinal[,obj],
                              cat_features = factor_n2)
  
  # Model
  set.seed(101)
  tmpmod <- catboost.train(traindat,
                           test,
                           settings)
  preds  <- catboost.predict(tmpmod,
                             test,
                             prediction_type = 'Class') 
  CMTable <- table(change_factor(preds), 
                   real = change_factor_test(TestFinal[, 'isloss']))
  CMTable_inside <- CMTable[-1, -1]
  #print(CMTable)
  #print(CMTable_inside)
  Sys.sleep(5)
  score <- (25*sum(diag(CMTable_inside)) + 
              12*(CMTable['d0','d1']+CMTable['d1','d3']+CMTable['d3','d5']) +  
              1*(CMTable['d0','d3']+CMTable['d1','d5']) +
              0.5*(CMTable['d0','d5'])+
              0.25*sum(CMTable_inside[lower.tri(CMTable_inside)]))/ sum(CMTable[-1, ])
  return(score)
}



VarImportant <- function(a) {
  result <- catboost.get_feature_importance(a)
  result <- data.frame(Var = row.names(result), Imp = result[, 1])
  result <- result[order(result$Imp, decreasing = T), ]
  row.names(result) <- 1:nrow(result)
  return(result)
}
Replace <- function(x) {
  x <- sub(pattern = c("1"), replacement = c("loss"), x)
  x <- sub(pattern = c("0"), replacement = c("active"), x)
  return(x)
}