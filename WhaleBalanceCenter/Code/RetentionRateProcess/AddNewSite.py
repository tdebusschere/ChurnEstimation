import sys
import os
import logging
import datetime
import time
import subprocess
import argparse
import pandas as pd
from   datetime import datetime as dt


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

def TransferActivity(Sites,dbs, querytime):
    configdata  = configs['Database']['TransferRetentionListActivity']
    writer       = WritetoDB.Writer(configdata)
    writer.write(pd.Timestamp(querytime).date())

    
def TuneModel(Sites,querytime):
    process = RetentionListProcessManager(querytime, websites = Sites, init = False)
    for k in Sites.itertuples():
        Rlocation = configs['RPrograms']['Rdirectory']
        Execution = configs['RPrograms']['TuningWrapper']
        #print(Execution)
        Arguments = " --site {site} --activity {dt} ".format( site = k.Website, dt=pd.Timestamp(querytime).date())
        print( Rlocation + ' ' + Execution['Name'] + Arguments)
        cwd = os.getcwd()
        print(cwd)
        try:
            zmm = subprocess.Popen(Rlocation + ' ' + Execution['Name'] + Arguments, cwd=cwd, shell = True )
            zmm.wait()
            #process.setStepSite('Model',k.siteid,pd.Timestamp(querytime).date(), pd.Timestamp.now().round('1s'))
        except:
            pass


if __name__ =='__main__':
    #time
    ##first date, last date, version and which websites

    ##to do: systemint FROM table 
    ##to do: change the Datascientist.dbo.DS_RetentionlistActiveWebsites from [jG} to [190]
    parser = argparse.ArgumentParser(description="python ProcessWrapper.py ") 
    parser.add_argument('--startdays',  nargs='?', type=int)
    parser.add_argument('--enddays'  ,  nargs='?', type=int)
    parser.add_argument('--sitesetting',nargs='?', type=str)
    parser.add_argument('--systemint',  nargs='?', type=int)
    
    ##parse arguments
    args = Setup.preprocessnew(parser.parse_args())
    
    ##load configs and databases
    configs =  Setup.readConfig(version= args['version'])
    dbs     =  Setup.initDBs()
    sites   =  Setup.setSites( args['systemint'],
                               args['sitesetting'],
                               args['startdays'],
                               args['enddays'])
   
    date = (datetime.datetime.utcnow() - datetime.timedelta(days = args['enddays']))
    startdate = (datetime.datetime.utcnow() - datetime.timedelta( days = args['startdays']))
    testdate  = startdate 

    #needs implementation and then use the individual wrapper to handle it
    Problems = getPreprocessingState(date, date, dbs)
    
    



    '''
    initialization: 
     sites    : data.frame containingsitesetting / systemint (FROM Datascientist.dbo.DS_RetentionlistActiveWebsites)
     Problems : which should be rerun (but that probably should move to another proces)
     dbs      : databases
     startdate: startdays
     date     : enddays
     args['version'] : what version of config and process to use: test / new / standard / fill
    '''
    initialization = initObject(sites, Problems, dbs,startdate, args['version'], date )
   

    ## Handle the promotion; so only some  promotion are used in the model
    ProcessPromotions(initialization,configs).executeProcess()
    ## Every website has it's own activitydate
    #ProcessActionDates()

    ## longterm aggregation from ETL data
    AggregateFinancialLongterm(initialization,configs).executeProcess()
    AggregateLoginLongterm(initialization,configs).executeProcess()
    AggregateFinancialPromotionLongterm(initialization, configs).executeProcess()
    
    ##Remove Previous Data FROM 190 and JG
    Cleanup(initialization,configs).executeProcess()

    #run one day at a time
    while testdate.strftime("%Y-%m-%d 00:00:00.000") <= date.strftime("%Y-%m-%d 00:00:00.0000"):
    
      tdate = testdate.strftime("%Y-%m-%d 00:00:00.000")
     
      ##190
      RetentionListUpdateUserList(initialization, configs).executeProcess()
      CollectActivity(initialization, configs).executeProcess()
      CollectFinancialHistory(initialization, configs).executeProcess()
      CollectFinancialLastMonth(initialization, configs).executeProcess()
      CollectBetrecord(initialization, configs).executeProcess()
      CollectIp(initialization, configs).executeProcess()
      CollectFinancialPromotion(initialization, configs).executeProcess()
      TransferActivity(Sites,dbs,tdate)

      # JG:    
      UpdateFinancialPromotion(initialization, configs).executeProcess()
      UpdateActivity(initialization,configs).executeProcess()
      UpdateActivityDatesSite(initialization, configs).executeProcess()
      UpdateActivityHistorySite(initialization, configs).executeProcess()
      CollectWalletHistorySite(initialization, configs).executeProcess()
      testdate = testdate + datetime.timedelta(days = 1)
      print(datetime.datetime.utcnow())

    # ValidateData
    # validate

    TuneModel(initialization, configs)
