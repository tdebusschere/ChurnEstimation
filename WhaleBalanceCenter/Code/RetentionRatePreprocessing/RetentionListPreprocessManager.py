import sys
sys.path.append('../')

import pandas as pd

from RetentionRateHelperFunctions import EnvironmentSetup as Setup
from RetentionRateHelperFunctions.RetentionListManager import RetentionListManager


class RetentionListPreprocessManager(RetentionListManager):
    
    def __init__(self, date, init = False):      
        configs = Setup.readConfig()
        connections = Setup.initDBs()

        self.database = configs['Database']['DS_RetentionlistPreprocesStatus']['Database']
        self.server   = configs['Database']['DS_RetentionlistPreprocesStatus']['Server']
     
        self.connections = connections
        if init ==True:
          self.connections[self.server].ExecNoQuery(" INSERT INTO {database} (ProcesStartDate,Initialization, ProcessStatus) \
                                                      SELECT '{date}', '{datetime}','initialized' ".
                                                      format(date = pd.Timestamp(date).date(), 
                                                             datetime= pd.Timestamp(date),
                                                             database = self.database))
        
          self.connections[self.server].ExecNoQuery(" INSERT INTO {database} (ProcesStartDate,ProcessStatus) \
                                                      SELECT '{date}', 'initialized' ".
                                                      format(date = pd.Timestamp(date).date(), database = self.database))

    
   
