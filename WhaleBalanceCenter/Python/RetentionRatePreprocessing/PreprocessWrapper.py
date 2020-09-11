import sys
import logging
import datetime



import pandas as pd


sys.path.append('../')

from RetentionRateHelperFunctions import EnvironmentSetup as Setup
from RetentionRatePreprocessing.RetentionListPreprocessManager import RetentionListPreprocessManager

## they need to move to a different class,
## and writting to C:\Users\DS.Tom\RetentionRate\RetentionRateHelperFunctions
## so they can be reused accross the different processes

    

def getBalanceCenterStatus(date, connections):
    ##send email; something is horribly wrong with the setup of the process; continuing is pointless
    ##log error
    Problems = None
    #Only Thirthy days are needed atm for the process
    server = configs['Database']['BalanceCenterDailyQueryStatusbyType']['Server']
    try:
        Problems = connections[server].ExecQuery("SELECT  isnull(Min(updatetime),getdate()) \
                                                  FROM {database} \
                                                  WHERE STATUS NOT IN ('Success','Empty') AND \
                                                        Updatetime >= dateadd(d,-30,'{date}') AND \
                                                        Updatetime <= '{date}' \
                                                 ".format( date = date, \
                                                 database = configs['Database']['BalanceCenterDailyQueryStatusbyType']['Database']))
        print("Success")
    except:
        pass
    return(Problems)




def AggregateBalanceCenter(date, connections, aggregationproblems):
    process = RetentionListPreprocessManager(date, init=True)
    problemhistory = process.getAllWithStatus('incomplete')
    
    server = configs['SP']['AggregateBalanceCenterSummarize']['Server']
    proc   = configs['SP']['AggregateBalanceCenterSummarize']['Procedure']
    refdate = pd.Timestamp(aggregationproblems.values[0,0])
    for data in problemhistory.ProcesStartDate:
        print("Date:{}".format(pd.Timestamp(data).date()))

        if refdate > pd.Timestamp(data):
          try:
            dsserver = configs['Database']['DS_RetentionlistBetrecordDailySummary']['Server']
            table    = configs['Database']['DS_RetentionlistBetrecordDailySummary']['Database']
            connections[dsserver].ExecNoQuery(" Delete FROM {table} where updatedate = '{data}' ".
                         format(data  = pd.Timestamp(data).date(),
                                table = table ))
          except:
            pass
                        
          try:  
            process.setStep('ProcessStatus',pd.Timestamp(data).date(),'initialized')             
            connections[server].ExecNoQuery(" EXEC {proc} @startdate='{data}' ".
                         format(proc = proc, data = pd.Timestamp(data).date()))
            process.setStep('BalanceCenterUpdatedTime',pd.Timestamp(data).date(), pd.Timestamp(data).date())
            
          except:
            pass
        else:
            #step, timeslot, value
            process.setStep('BalanceCenterUpdatedTime',pd.Timestamp(data).date(), refdate)
    
    #run main model:
    try:
        
        connections[server].ExecNoQuery(" EXEC {proc} @startdate='{data}' ".
                         format(proc = proc, data = pd.Timestamp(date).date()))
        
        process.setStep('BalanceCenterUpdatedTime',pd.Timestamp(date).date(), pd.Timestamp(date).date())
    except:
        pass


def Cleanup(date,connection, aggregationproblems):
    process = RetentionListPreprocessManager(date, init = False)
    initializedhistory = process.getAllWithStatus('initialized')
    sectored = initializedhistory.loc[ initializedhistory.ProcesStartDate == initializedhistory.BalanceCenterUpdatedTime , :]
    for k in sectored.iterrows():
        process.setStep('ProcessStatus',pd.Timestamp(k.ProcessStartDate).date(),'completed')             

        
  



if __name__ =='__main__':
    
    #time
    date = (datetime.datetime.utcnow() - datetime.timedelta(days = 1)).strftime("%Y-%m-%d 00:00:00.000") #UTC+0
    configs = Setup.readConfig()
    dbs     = Setup.initDBs()
    Problems = getBalanceCenterStatus(date, dbs)
    AggregateBalanceCenter(date,dbs,Problems )
    Cleanup(date, dbs, Problems)
    