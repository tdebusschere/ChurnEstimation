import sys
sys.path.append('../')

import pandas as pd

from RetentionRateHelperFunctions import EnvironmentSetup as Setup
from RetentionRateHelperFunctions import RetentionListProcessManager


class RetentionListProcessManager(RetentionListProcessManager):
    
    def __init__(self, date, init = False):      
        configs = Setup.readConfig()
        connections = Setup.initDBs()

        self.database = configs['Database']['DS_RetentionlistProcesStatus']['Database']
        self.server   = configs['Database']['DS_RetentionlistProcesStatus']['Server']
     
        self.connections = connections
        if init ==True:
          print(self.server)
          self.connections[self.server].ExecNoQuery(" INSERT INTO {database} (ProcesStartDate,ProcessStatus) \
                                                      SELECT '{date}', 'initialized' ".
                                                      format(date = pd.Timestamp(date).date(), database = self.database))
        
    
    
   