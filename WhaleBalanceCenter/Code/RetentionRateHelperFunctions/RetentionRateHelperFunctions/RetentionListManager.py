# -*- coding: utf-8 -*-
"""
Created on Tue Dec 31 17:37:45 2019

@author: DS.Tom
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Dec 30 15:39:33 2019

@author: DS.Tom
"""
import sys
sys.path.append('../')

import pandas as pd

from RetentionRateHelperFunctions import EnvironmentSetup as Setup



class RetentionListManager:
    
    def __init__(self, date):      
        pass
        
    def getDayStatus(self, date):
        try:
            result = self.connections[self.server].ExecQuery(" SELECT {*} FROM {database} WHERE ProcesStartDate = '{date}'".
                                           format(database = self.database, date = date))
            return(result)      
        except:
            pass
        
    def getWithStatus(self,status, field):
        try:
            result = self.connections[self.server].ExecQuery(" SELECT {field} FROM {database} WHERE ProcessStatus = '{status}'".
                                           format(database = self.database, status = status, field = field))
            return(result)
        except:
            pass
        
    def getAllWithStatus(self,status):
        try:
            result = self.connections[self.server].ExecQuery(" SELECT * FROM {database} WHERE ProcessStatus = '{status}' \
                                                              order by Processtartdate".
                                                   format(database = self.database, status = status))
            return(result)
        except:
            pass
    
    def getAllSince(self,date):
        try:
            result = self.connections[self.server].ExecQuery(" SELECT * FROM {database} WHERE ProcesStartDate >= '{date}' \
                                                              order by Processtartdate".
                                                   format(date = pd.Timestamp(date).date(), database = self.database))

            return(result)
        except:
            pass        
    
    
    def setStep(self,step, timeslot, value):
        try:
            result = self.connections[self.server].ExecNoQuery(" UPDATE {database} SET {step} = '{value}' WHERE ProcesStartDate = '{timeslot}'".
                                           format(database = self.database, 
                                                  step     = step,
                                                  timeslot = timeslot,
                                                  value    = value))
            return(result)
        except:
            pass
        
    def getStep(self, step, timeslot):
        try:
            result = self.connections[self.server].ExecQuery(" SELECT {step} FROM {database} WHERE ProcesStartDate = '{timeslot}'".
                                           format(database = self.database, 
                                                  timeslot = timeslot,
                                                  step     = step))
            return(result)
        except:
            pass
    
    
   