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
    print(process)
    #Only Thirthy days are needed atm for the process
    
    PreviousRuns = process.getAllSince(querytime)
    try:
        PreviousRuns = process.getAllSince(querytime)
    except:
        pass      
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
        if k.Website != 'DQ002' and  k.Website != 'FJ001' and k.Website != 'BM001' and k.Website != 'BM002':
            continue
        else:
         Rlocation = configs['RPrograms']['Rdirectory']
         Execution = configs['RPrograms']['RetentionRateWrapper']
         #print(Execution)
         Arguments = " --site {site} --activity {dt} ".format( site = k.Website, dt=pd.Timestamp(querytime).date())
        print( Rlocation + ' ' + Execution['Name'] + Arguments)
        cwd = os.getcwd()
        print(cwd)
        try:
            zmm = subprocess.Popen(Rlocation + ' ' + Execution['Name'] + Arguments, cwd=cwd, shell = True )
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
    print(Problems)
    print(sys.exit)

    date = (datetime.datetime.utcnow() - datetime.timedelta(days = args['enddays']))
    testdate = querytime
    print(datetime.datetime.utcnow())

    while testdate <= date:
     print(testdate)
    
     tdate = testdate.strftime("%Y-%m-%d 00:00:00.000")
     initialization = initObject(Sites,Problems, dbs,testdate,args['version'])
     
     #AggregateFinancial(initialization, configs).executeProcess()
     RetentionListUpdateUserList(initialization, configs).executeProcess()
     
     print(datetime.datetime.utcnow())
     time.sleep(5)
     
     #Cleanup(tdate, dbs, Problems)
     testdate = testdate + datetime.timedelta(days = 1)
     #Cleanup(date, dbs, Problems)



