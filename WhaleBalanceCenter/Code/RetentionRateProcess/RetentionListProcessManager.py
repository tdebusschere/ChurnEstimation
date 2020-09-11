import sys
sys.path.append('../')

import pandas as pd

from RetentionRateHelperFunctions import EnvironmentSetup as Setup
from RetentionRateHelperFunctions.RetentionListManager import RetentionListManager


class RetentionListProcessManager(RetentionListManager):
    
    def __init__(self, date, websites= pd.DataFrame(), init=False, newSites = False ):      
        configs = Setup.readConfig()
        connections = Setup.initDBs()
        if newSites != True:
            self.database = configs['Database']['DS_RetentionListProcesStatus']['Database']
            self.server   = configs['Database']['DS_RetentionListProcesStatus']['Server']
        else :
            self.database  = configs['Database']['DS_RetentionListNewSitesProcess']['Database']
            self.server    = configs['Database']['DS_RetentionListNewSitesProcess']['Server']

        self.connections = connections
        if not websites.empty and init==True:
          for k in websites.itertuples():
              self.connections[self.server].ExecNoQuery(" INSERT INTO {database} (ProcesStartDate, Website, \
                                                                                  Initialization,ProcessStatus) \
                                                           SELECT '{date}',{websites},'{now}','initialized' ".
                                                           format(date = pd.Timestamp(date).date(), 
                                                                  now  = pd.Timestamp.now().round('1s'),
                                                                  database = self.database,
                                                                  websites = k.siteid))
      
    def setStepSite(self,step, site, timeslot, value):     
        try:
            self.connections[self.server].ExecNoQuery(" UPDATE {database} SET {step} = '{value}' \
                                                        WHERE ProcesStartDate = '{timeslot}' AND \
                                                              website = {Website}".
                                           format(database = self.database, 
                                                  step     = step,
                                                  timeslot = timeslot,
                                                  Website  = site,
                                                  value    = value))
        except:
            pass
