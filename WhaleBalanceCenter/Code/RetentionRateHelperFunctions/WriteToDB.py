# -*- coding: utf-8 -*-
"""
Created on Tue Jan 14 14:05:46 2020

@author: DS.Tom
"""
from RetentionRateHelperFunctions import EnvironmentSetup as Setup


class Writer(object):
    
        def __init__(self, config):
            self.config = config
            self.destinationserver = config['DestinationServer']
            self.destinationdb     = config['DestinationDB']
            self.originserver  = config['OriginServer']
            self.origindb      = config['OriginDB']
            self.linkedname    = config['LinkedName']
            self.schema = config['Schema']
            self.dbs    =  Setup.initDBs()
            self.timevar = 'Activity'
            self.timevar2 = ''
            self.switch  = '='
            try:
                self.timevar = config['TimeField']
            except:
                pass
            try:
                self.switch = config['Switch']
            except:
                pass
            try:
                self.timevar2 = config['TimeField2']
            except:
                pass
                
        def write(self, date):
            deletion = " DELETE FROM {db} where {timefield} {switch} {time} "
            print(deletion.format(db=self.origindb, 
                                  timefield = self.timevar,
                                  switch = self.switch,
                                  time = "'" + str(date) + "'"))
            String = "Insert into {} SELECT ".format(self.destinationdb)
            for k in self.schema:
                String = String + '[' +  k + "],"
            query =  String[:-1] + " FROM [{link}].{origindb} where {timefield}  {switch} '{time}'".format(
                   link = self.linkedname , 
                   origindb = self.origindb, 
                   timefield = self.timevar,
                   time = date,
                   switch = self.switch)
            print(query)
            self.dbs[self.destinationserver].ExecNoQuery(query)
            self.dbs[self.originserver].ExecNoQuery(deletion.format(
                 db = self.origindb,
                 timefield = self.timevar,
                 switch = self.switch,
                 time = "'" + str(date) + "'"))

        def writeUserlist(self,date):
            deletion = " DELETE FROM {db} where {timefield} < {time} "
            print(deletion.format(
                db=self.origindb,
                timefield = self.timevar2,
                time = "'" + str(date) + "'"))
            self.dbs[self.originserver].ExecNoQuery(deletion.format(
                 db = self.origindb,
                 timefield = self.timevar2,
                 time = "'" + str(date) + "'"))
            String = "Insert into {} SELECT ".format(self.destinationdb)
            for k in self.schema:
                String = String + '[' +  k + "],"
            query =  String[:-1] + " FROM [{link}].{origindb} where {timefield}  = '{time}'".format(
                   link = self.linkedname , 
                   origindb = self.origindb, 
                   timefield = self.timevar,
                   time = date )
            print(query)
            self.dbs[self.destinationserver].ExecNoQuery(query)
            self.dbs[self.destinationserver].ExecNoQuery(deletion)

        
    
