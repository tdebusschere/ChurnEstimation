stringsAsFactors=FALSE

DB_Settings = list()
DB_Settings[['User']]                <- 'DS.Tom'
DB_Settings[['JG\\MSSQLSERVER2016']][['DB']]  <- 'db_DataScience'
DB_Settings[['JG\\MSSQLSERVER2016']][['IP']]  <- '10.80.29.155\\MSSQLSERVER2016'
DB_Settings[['DB190']][['DB']]                 <- 'db_190'
DB_Settings[['DB190']][['IP']]                 <- '10.80.16.190'

kd <- backend_env$new()
kd$set_with_value('db_DataScience','DS.Tom','5jkl$BGI')
kd$set_with_value('db_190','DS.Tom','5jkl$BGI')
#This and other parameters can and probably should be moved to a configurationfile
need_factor_col <- c('d14today','d14_0','d15_0','d14_1','d15_1','d14_3','d15_3','d14_5',
                     'd15_5','d14_7','d15_7','d14_14','d15_14','t2','t1','dom','dow',
                     #added on 4/22
                     'd15today','d24today','d25today','d24_0','d25_0','d24_1','d25_1',
                     'd24_3','d25_3','d24_5','d25_5','d24_7','d25_7','d24_14','d25_14',
                     'did_withdraw','d30orbiggerleave7d0','d30orbiggerleave7d1',
                     'd30orbiggerleave7d3','d30orbiggerleave7d5','d30orbiggerleave7d7',
                     'd30orbiggerleave7d14','m1leave7d0','m1leave7d1','m1leave7d3',
                     'm1leave7d5','m1leave7d7','m1leave7d14','id','avg_depositamount_decreasing',
                     'avg_deposittimes_decreasing','avg_withdrawamount_increasing')

del_var <- c('creditdeposittimes','discountsum','otherssum','did_withdraw',
             'd15today','d25today','d30orbiggerleave7d1','d30orbiggerleave7d3',
             'd30orbiggerleave7d5','m1leave7d0','m1leave7d1','m1leave7d3',
             'm1leave7d5','d24today','d25today','d24_0','d25_0','d24_1','d25_1',
             'd24_3','d25_3','d24_5','d25_5','d24_7','d25_7','d24_14','d25_14','t2','t1',
             "absent3dayslastmonth", "biggestdifflastmonth", "absent7dayslastmonth",
             "interval" , "leavedate","logindays7d")

train_indicators <- c('leave7d0','leave7d1','leave7d3','leave7d5','amountsum')
test_indicators  <- c(train_indicators[1:4], 'leave7d7', 'leave7d14','amountsum')
##modelparameters
minus     <- c(15)
del       <- c(1:10, 18:25)
Resultdel <- c(2,4:10,18:25)
result_weights <- c(-1,0,1,3,5)
results <- c(-1,0,1,3,5)


diff <- 30
difftuning <- 90

DB = list()
DB[['ModelParameters']][['Table']] <- 'DataScientist.dbo.RetentionListModelParameters'
DB[['ModelParameters']][['Database']] <- 'JG\\MSSQLSERVER2016'

DB[['Data']][['Database']]          <- 'JG\\MSSQLSERVER2016'
DB[['Data']][['Users']]             <- "[Datascientist].[dbo].[RetentionListUsers]"
DB[['Data']][['Activity']]          <- "[Datascientist].[dbo].[RetentionListActivity]"
DB[['Data']][['ActivityHistory']]   <- "[Datascientist].dbo.RetentionListActivityHistory"
DB[['Data']][['ActivityDates']]     <- "[Datascientist].dbo.RetentionListActivityDates"
DB[['Data']][['WalletHistory']]     <- "[Datascientist].dbo.RetentionListWalletHistory"
DB[['Data']][['IP']]                <- "[Datascientist].dbo.RetentionListIP"
DB[['Data']][['BetRecord']]         <- "[Datascientist].dbo.RetentionListBetrecord"
DB[['Data']][['FinancialPromotion']]<- "[Datascientist].[dbo].[RetentionListFinancialPromotion]"
DB[['Data']][['FinancialLastMonth']]<- "[Datascientist].dbo.RetentionListFinancialLastMonth"
DB[['Data']][['FinancialHistory']]  <- "[Datascientist].dbo.RetentionListFinancialHistory"

DB[['Data_test']][['Database']]           <- 'JG\\MSSQLSERVER2016'
DB[['Data_test']][['Users']]              <- "[DSSkunkworks].[dbo].[dRetentionListUsers]"
DB[['Data_test']][['Activity']]           <- "[DSSkunkworks].[dbo].[dRetentionListActivity]"
DB[['Data_test']][['ActivityHistory']]    <- "[Dsskunkworks].[dbo].[dRetentionListActivityHistory]"
DB[['Data_test']][['ActivityDates']]      <- "[Dsskunkworks].[dbo].[dRetentionListActivityDates]"
DB[['Data_test']][['WalletHistory']]      <- "[Dsskunkworks].[dbo].[dRetentionListWalletHistory]"
DB[['Data_test']][['IP']]                 <- "[Dsskunkworks].[dbo].[dRetentionListIP]"
DB[['Data_test']][['BetRecord']]          <- "[Dsskunkworks].[dbo].[dRetentionListBetrecord]"
DB[['Data_test']][['FinancialPromotion']] <- "[Dsskunkworks].[dbo].[dRetentionListFinancialPromotion]"
DB[['Data_test']][['FinancialLastMonth']] <- "[Dsskunkworks].[dbo].[dRetentionListFinancialLastMonth]"
DB[['Data_test']][['FinancialHistory']]   <- "[Dsskunkworks].[dbo].[dRetentionListFinancialHistory]"

