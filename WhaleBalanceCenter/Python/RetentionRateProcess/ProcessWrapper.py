import sys
import os
import logging
import datetime
from datetime import datetime as dt
import time
import subprocess
import argparse


import pandas as pd


sys.path.append('../')

from RetentionRateHelperFunctions import EnvironmentSetup as Setup
from RetentionRateHelperFunctions import WriteToDB as WritetoDB

from RetentionRatePreprocessing.RetentionListPreprocessManager import RetentionListPreprocessManager
from RetentionRateProcess.RetentionListProcessManager import RetentionListProcessManager

from RetentionRateProcess.initObject import *



def getPreprocessingState(date,querytime, connections):
    ##send email; something is horribly wrong with the setup of the process; continuing is pointless
    ##log error
    Problems = None
    process = RetentionListPreprocessManager(date)

    #Only Thirthy days are needed atm for the process
    try:
        PreviousRuns = process.getAllSince(querytime)
    except:
        PreviousRuns = None     
    return(PreviousRuns)
  
def Cleanup(date,connection, aggregationproblems):
    process = RetentionListPreprocessManager(date, init = False)
    initializedhistory = process.getAllWithStatus('initialized')
    configdata  = configs['Database']['TransferRetentionListActivity']
    writer       = WritetoDB.Writer(configdata)
    writer.write(pd.Timestamp(querytime).date())

    print(initializedhistory)
  


def EstimateModel(Sites,querytime):
    process = RetentionListProcessManager(querytime, websites = Sites, init = False)
    for k in Sites.itertuples():
        if k.Website != 'DQ002':
            continue
        else:
         #Rlocation = configs['RPrograms']['Rdirectory']
         Execution = configs['RPrograms']['RetentionRateWrapper']
         Arguments = " --site {site} --activity {dt} ".format( site = k.Website, dt=pd.Timestamp(querytime).date())
        print( 'Rscript ' + Execution['Name'] + Arguments)
        cwd = os.getcwd()
        print(cwd)
        try:
            zmm = subprocess.Popen('Rscript ' + Execution['Name'] + Arguments )
            zmm.wait()
            process.setStepSite('Model',k.siteid,pd.Timestamp(querytime).date(), pd.Timestamp.now().round('1s'))
        except:
            pass


def TransferActivity(Sites,dbs, querytime):
    configdata  = configs['Database']['TransferRetentionListActivity']
    writer       = WritetoDB.Writer(configdata)
    writer.write(pd.Timestamp(querytime).date())

    
if __name__ =='__main__':
    #time
    ##first date, last date, version and which websites
    parser = argparse.ArgumentParser(description="python ProcessWrapper.py ") 
    parser.add_argument('--startdays',nargs='?', type=int)
    parser.add_argument('--enddays'  ,nargs='?', type=int)
    parser.add_argument('--version'  ,nargs='?', type=str)
    parser.add_argument('--sitesetting' ,nargs='?', type=str)
    
    args = Setup.preprocess(parser.parse_args())
    print(args)
    
    date = (datetime.datetime.utcnow() - datetime.timedelta(days = 0)).strftime("%Y-%m-%d 00:00:00.000") #UTC+0
    querytime = (datetime.datetime.utcnow() - datetime.timedelta(days = args['startdays']))
    
    configs =  Setup.readConfig(version= args['version'])
    dbs     =  Setup.initDBs()
    
    Sites    = Setup.getSites()
    Sites2 = Sites.copy()

    
    if args['sitesetting'] != 'all':
        for k in Sites2.itertuples():
            if k.Website == args['sitesetting']:
               Sites = Sites.loc[ Sites2.Website == args['sitesetting'], :]
    
    Problems = getPreprocessingState(date, date, dbs)
    date = (datetime.datetime.utcnow() - datetime.timedelta(days = args['enddays']))
    testdate = querytime
    print(datetime.datetime.utcnow())

    while testdate <= date:
     print(testdate)

     tdate = testdate.strftime("%Y-%m-%d 00:00:00.000")
     initialization = initObject(Sites,Problems, dbs,testdate,args['version'])
     '''    
     AggregateFinancial(initialization, configs).executeProcess()
     AggregateBetrecord(initialization, configs).executeProcess()
     PrepareDailyTop10(initialization, configs).executeProcess()
     
     RetentionListUpdateUserList(initialization, configs).executeProcess()
     CollectActivity(initialization, configs).executeProcess()

     CollectFinancialHistory(initialization, configs).executeProcess()
     CollectFinancialLastMonth(initialization, configs).executeProcess()

     CollectBetrecord(initialization, configs).executeProcess()
     CollectIp(initialization, configs).executeProcess()
     CollectFinancialPromotion(initialization, configs).executeProcess()
     TransferActivity(Sites,dbs,tdate)  
     
     UpdateFinancialPromotion(initialization, configs).executeProcess()
     UpdateActivity(initialization,configs).executeProcess()
     UpdateActivityDates(initialization, configs).executeProcess()
     UpdateActivityHistory(initialization, configs).executeProcess()
     CollectWalletHistory(initialization, configs).executeProcess()
     '''
     EstimateModel(Sites,tdate)
     ProcessResult(initialization,configs).executeProcess()
     
     print(datetime.datetime.utcnow())
     time.sleep(5)
     
     #Cleanup(tdate, dbs, Problems)
     testdate = testdate + datetime.timedelta(days = 1)
     #Cleanup(date, dbs, Problems)



