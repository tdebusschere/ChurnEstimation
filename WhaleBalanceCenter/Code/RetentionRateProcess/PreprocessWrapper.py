
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


def HandleProblems(connections, proc, server,  step, data):
    process = RetentionListPreprocessManager(date, init=False)
    try:
            dsserver = configs['Database'][step]['Server']
            table    = configs['Database'][step]['Database']
            connections[dsserver].ExecNoQuery(" Delete FROM {table} where updatedate = '{data}' ".
                         format(data  = pd.Timestamp(data).date(),
                                table = table ))
    except:
            pass
                        
    try:  
            process.setStep('ProcessStatus',pd.Timestamp(data).date(),'initialized')             
            connections[server].ExecNoQuery(" EXEC {proc} @startdate='{data}' ".
                         format(proc = proc, data = pd.Timestamp(data).date()))
            process.setStep(step,pd.Timestamp(data).date(), pd.Timestamp.now())
            
    except:
            pass
        


def AggregateBalanceCenter(date, connections, aggregationproblems):
    process = RetentionListPreprocessManager(date, init=True)
    problemhistory = process.getAllWithStatus('incomplete')
    
    server = configs['SP']['AggregateBalanceCenterSummarize']['Server']
    proc   = configs['SP']['AggregateBalanceCenterSummarize']['Procedure']
    refdate = pd.Timestamp(aggregationproblems.values[0,0])
    for data in problemhistory.ProcesStartDate:
        print("Date:{}".format(pd.Timestamp(data).date()))

        if refdate > pd.Timestamp(data):
            HandleProblems(connections, proc, server, 'BalanceCenterUpdatedTime', data)
        else:
            #step, timeslot, value
            process.setStep('BalanceCenterUpdatedTime',pd.Timestamp(data), refdate)
    
    #run main model:
    try:
        connections[server].ExecQuery(" EXEC {proc} @startdate='{data}' ".
                         format(proc = proc, data = pd.Timestamp(date)))
        
        process.setStep('BalanceCenterUpdatedTime',pd.Timestamp(date).date(), pd.Timestamp.now())
    except:
        pass

def CollectTop10Commissionable(date,connections,aggregationproblems):
    process = RetentionListPreprocessManager(date, init=False)
    problemhistory = process.getAllWithStatus('incomplete')
    
    server = configs['SP']['MonthlyTop10GamesCommissionable']['Server']
    proc   = configs['SP']['MonthlyTop10GamesCommissionable']['Procedure']
    refdate = pd.Timestamp(aggregationproblems.values[0,0])

    for data in problemhistory.ProcesStartDate:
        print("Date:{Date}; RefDate:{Refdate}".format(Date = pd.Timestamp(data).date(),
                                                      RefDate = pd.Timestamp(refdate).date()))
        if refdate > pd.Timestamp(data):
            HandleProblems(connections, proc, server, 'MonthlyTop10Commissionable', data)
        else:
            #step, timeslot, value
            process.setStep('MonthlyTop10Commissionable',pd.Timestamp(data), refdate)
    #run main model:
    try:
        connections[server].ExecNoQuery(" EXEC {proc} @startdate='{data}' ".
                         format(proc = proc, data = pd.Timestamp(date).date()))
        
        process.setStep('MonthlyTop10Commissionable',pd.Timestamp(date), pd.Timestamp(date).now())
    except:
        pass


def Cleanup(date,connection, Problems):
    process = RetentionListPreprocessManager(date, init = False)
    initializedhistory = process.getAllWithStatus('initialized')

    '''
    for k in sectored.iterrows():
        process.setStep('ProcessStatus',pd.Timestamp(k[1].ProcesStartDate).date(),'completed')             
    '''
        
  



if __name__ =='__main__':
    version = ''
    #time
    date = (datetime.datetime.utcnow() - datetime.timedelta(days = 1)).strftime("%Y-%m-%d 00:00:00.000") #UTC+0
    configs = Setup.readConfig(version)
    dbs     = Setup.initDBs()
    Problems = getBalanceCenterStatus(date, dbs)
    AggregateBalanceCenter( date,dbs,Problems )
    CollectTop10Commissionable( date,dbs,Problems )
    Cleanup(date, dbs, Problems)
    
