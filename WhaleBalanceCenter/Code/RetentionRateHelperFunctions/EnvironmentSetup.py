# -*- coding: utf-8 -*-
"""
Created on Mon Dec 30 15:54:57 2019

@author: DS.Tom
"""
import sys
import yaml
import numpy as np 

sys.path.append('../')


from RetentionRateHelperFunctions import DB as DB


def readConfig(version = '_test'):
    with open('../RetentionRateHelperFunctions/config' + version + '.yaml') as cfile:
        data = yaml.load(cfile, Loader=yaml.FullLoader)
    return(data)

def getSites():
    conf = readConfig()
    dbs  = initDBs()
    database = conf['Database']['DS_RetentionlistActiveWebsites']['Database']
    server   = conf['Database']['DS_RetentionlistActiveWebsites']['Server']
    websites = dbs[server].ExecQuery("select * from {db}  WHERE active = 1 ".format(db = database))
    return(websites)

def setSites(siteid, sitename, startdate, enddate):
    conf = readConfig()
    dbs  = initDBs()
    database = conf['Database']['DS_RetentionlistActiveWebsites']['Database']
    server   = conf['Database']['DS_RetentionlistActiveWebsites']['Server']
    websites = dbs[server].ExecQuery("SELECT * FROM {db} WHERE active = 1".format(db = database))
    if np.shape(websites.loc[websites.loc[:,'Website'] == sitename,:])[1]  == 0:
        dbs[server].ExecNoQuery("INSERT INTO {db} \
                                 SELECT '{Website}', {Siteid}, 1, '{startdate}','{enddate}'".
                                 format( db = database,
                                         Website = sitename,
                                         Siteid  = siteid,
                                         startdate = startdate,
                                         enddate   = enddate))
    Sites = getSites()
    return(Sites.loc[ Sites.Website == sitename,:])



def setLastUpdate(dt):
    conf = readConfig()
    dbs  = initDBs()
    database = conf['Database']['DS_RetentionlistActiveWebsites']['Database']
    server   = conf['Database']['DS_RetentionlistActiveWebsites']['Server']
    try:
        dbs[server].ExecNoQuery(" update {db} SET lastupdatedate = {date} WHERE active = 1".\
                format( db = database, date = dt )) 
    except:
        pass

def initDBs():
    connections = dict()
    try:
        connections['JG'] = DB.JG()
        connections['BalanceCenter_190'] = DB.BalanceCenter_190()
    except:
        pass
    return(connections)
    
def preprocess(args):
    
    if args.enddays == None or not isinstance(args.enddays,int):
        enddays = 1
    else:
        enddays = args.enddays
        
    if args.startdays == None or not isinstance(args.startdays,int):
        startdays = 1
    else:
        startdays = args.startdays
        
    if enddays > startdays:
        raise Exception("startday shouldn't be greater than endday; original startday = {sd},\
                         original endday={ed}".format(st = args.startdays, ed = args.enddays))
        
    if args.version != '' and args.version != '_test':
        version ='' ##better to assumet it's only a test
    else:
        version = args.version
        
    if (args.sitesetting != 'all' or ~isinstance(args.sitesetting,int)) and args.sitesetting != None:
        sitesetting = args.sitesetting
    else:
        sitesetting = 'all'
        
    return({'enddays'   : enddays,
            'startdays' : startdays,
            'version'   : version,
            'sitesetting' : sitesetting})

def preprocessnew(args):
    
    if args.enddays == None or not isinstance(args.enddays,int):
        enddays = 1
    else:
        enddays = args.enddays

    if args.startdays == None or not isinstance(args.startdays,int):
        startdays = 91
    else:
        startdays = args.startdays

    if args.sitesetting is  None or not isinstance(args.sitesetting,str):
        raise Exception("should provide a string that is appropriate; and that is a SystemCode")
    else:
        sitesetting = args.sitesetting
    
    if args.systemint is None or not isinstance(args.systemint, int):
        raise Exception("should provide an int that is a siteid")
    else:
        systemint = args.systemint

    return({'enddays' : enddays,
            'startdays' : startdays,
            'systemint' : systemint,
            'version'   : 'new',
            'sitesetting' : sitesetting})


