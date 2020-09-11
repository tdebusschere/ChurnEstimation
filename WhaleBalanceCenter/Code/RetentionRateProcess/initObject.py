import datetime
import pandas as pd

from datetime import datetime as dt

from RetentionRateHelperFunctions import EnvironmentSetup as Setup
from RetentionRateHelperFunctions import WriteToDB as WritetoDB

from RetentionRatePreprocessing.RetentionListPreprocessManager import RetentionListPreprocessManager
from RetentionRateProcess.RetentionListProcessManager import RetentionListProcessManager



class initObject(object):
    def __init__(self, sites, Problems, dbs, time, version, endtime = None):
        self.sites    = sites
        self.Problems = Problems
        self.dbs      = dbs
        self.time     = time.strftime("%Y-%m-%d 00:00:00.000")
        try:
          self.endtime  = endtime.strftime("%Y-%m-%d 00:00:00.000")
        except:
          self.endtime  = None
        if version == '':
            self.write_to_db()
        elif version == 'new':
            self.write_new_to_db()

    def write_to_db(self):
        if isinstance(self.sites,str):
            return(0)

        for k in self.sites.itertuples():
            try:
                self.dbs['BalanceCenter_190'].ExecQuery("INSERT INTO ResultPool.dbo.RetentionListResultStatus \
                                                 ( Systemcode, Date, ExecutionTime, ProcessStatus ) \
                                          VALUES ( '{site}', '{date}',  getdate() , 0 ) ".
                                                 format(site = k.Website,
                                                        date = self.time))
            except:
                ##send a warning mail to TomTong@xinwang.com.tw
                pass

    def write_new_to_db(self):
        if isinstance(self.sites,str):
            try:
                pass
            except:
                pass

class RetentiontionRateSPWrapper(object):
    
    def __init__(self,initObject, configs, execution, logvar, init=False ):
        self.initObject = initObject
        self.configs    = configs
        self.init = init
        self.execution = execution
        self.logvar    = logvar
        
        
    def getProcess(self):
        process = RetentionListProcessManager(self.initObject.time, 
                                              websites = self.initObject.sites, 
                                              init = self.init)
        return(process)
        
    
    def executeProcess(self):
        print(self.logvar)
        server = self.configs['SP'][self.execution]['Server']
        proc   = self.configs['SP'][self.execution]['Procedure']
        process = self.getProcess()
        #run main model:
        try:
            res = self.initObject.dbs[server].ExecQuery(" EXEC {proc} '{data}'".
                          format(proc = proc, 
                                 data = pd.Timestamp(self.initObject.time).date()))
            if not res.empty:
                process.setStep(self.logvar,pd.Timestamp(self.initObject.time).date(), pd.Timestamp.now().round('1s'))
        except:
           pass  


class UpdateActivityHistory(RetentiontionRateSPWrapper):
    def __init__(self, initObject,configs):
        super(UpdateActivityHistory,self).__init__(initObject,configs,'UpdateActivityHistory','ActivityHistory')
                
class UpdateActivityDates(RetentiontionRateSPWrapper):
    def __init__(self, initObject,configs):
        super(UpdateActivityDates,self).__init__(initObject,configs,'UpdateActivityDates','ActivityDates')
            
class CollectWalletHistory(RetentiontionRateSPWrapper):
    def __init__(self, initObject,configs):
        super(CollectWalletHistory,self).__init__(initObject,configs,'CollectWalletHistory','WalletHistory')

class AggregateBetrecord(RetentiontionRateSPWrapper):
    def __init__(self, initObject,configs):
        super(AggregateBetrecord,self).__init__(initObject,configs,'AggregateBalanceCenterSummarize','BalanceCenter', init=True)

class PrepareDailyTop10(RetentiontionRateSPWrapper):
    def __init__(self, initObject,configs):
        super(PrepareDailyTop10,self).__init__(initObject,configs,'MonthlyTop10Commissionable','Top10Games')
   
class AggregateLogin(RetentiontionRateSPWrapper):
    def __init__(self,initObject,configs):
        super(AggregateLogin,self).__init__(initObject,configs,'AggregateLogin','Appel')

class UpdateActivityDatesSite(RetentiontionRateSPWrapper):
    def __init__(self,initObject,configs):
        super(UpdateActivityDatesSite,self).__init__(initObject,configs,'UpdateActivityDatesSite','UpdateActivityDatesSite')

class CollectWalletHistorySite(RetentiontionRateSPWrapper):
    def __init__(self,initObject,configs):
        super(CollectWalletHistorySite,self).__init__(initObject,configs,'CollectWalletHistorySite','CollectWalletHistorySite')

class UpdateActivityHistorySite(RetentiontionRateSPWrapper):
    def __init__(self,initObject,configs):
        super(UpdateActivityHistorySite,self).__init__(initObject,configs,'UpdateActivityHistorySite','UpdateActivityHistorySite')

class RetentionRateSiteWrapper(object):
    
    def __init__(self, initObject, configs, execution, logvar, writeout = False, transfer = '', init = False ):
        self.initObject = initObject
        self.configs    = configs
        self.init = init
        self.execution = execution
        self.logvar    = logvar
        self.writeout  = writeout
        self.transfer  = transfer
        
    def getProcess(self):
        process = RetentionListProcessManager(self.initObject.time, 
                                              websites = self.initObject.sites, 
                                              init = self.init)
        return(process)
    
    def executeProcess(self):
        server = self.configs['SP'][self.execution]['Server']
        proc   = self.configs['SP'][self.execution]['Procedure']
        process = self.getProcess()
        for k in self.initObject.sites.itertuples():
         try:
           res = self.initObject.dbs[server].ExecQuery(" EXEC {proc} '{data}','{Systemcode}',{Siteid} ".
                             format(proc = proc, 
                                    data = pd.Timestamp(self.initObject.time).date(),
                                    Systemcode = k.Website,
                                    Siteid     = k.siteid))
           if not res.empty:
             process.setStepSite(self.logvar,
                                 k.siteid,
                                 pd.Timestamp(self.initObject.time).date(), 
                                 pd.Timestamp.now().round('1s'))
         except:
          pass 
        if self.writeout == True:
           self.writeOut()
      
    def writeOut(self):
        configdata  = self.configs['Database'][self.transfer]
        writer       = WritetoDB.Writer(configdata)
        writer.write(pd.Timestamp(self.initObject.time).date())


class AggregateFinancial(RetentionRateSiteWrapper):
    def __init__(self, initObject,configs):
        super(AggregateFinancial,self).__init__(initObject,configs,'AggregateFinancial','FinancialAggregation', init=True)

class UpdateActivity(RetentionRateSiteWrapper):
    def __init__(self, initObject,configs):
        super(UpdateActivity,self).__init__(initObject,configs,'UpdateActivity','UpdateActivity')

class UpdateFinancialPromotion(RetentionRateSiteWrapper):
    def __init__(self, initObject,configs):
        super(UpdateFinancialPromotion,self).__init__(initObject,configs,'UpdateFinancialPromotion','FinancialPromotionU')

class ProcessResult(RetentionRateSiteWrapper):
    def __init__(self, initObject,configs):
        super(ProcessResult,self).__init__(initObject,configs,'RetentionListProcessResult','Integration')

class CollectActivity(RetentionRateSiteWrapper):
    def __init__(self, initObject,configs):
        super(CollectActivity,self).__init__(initObject,configs,'CollectActivity','Activity')

class CollectIp(RetentionRateSiteWrapper):
    def __init__(self, initObject,configs):
        super(CollectIp,self).__init__(initObject,configs,'CollectIp','Ipdata', True, 'TransferRetentionListIP')

class CollectBetrecord(RetentionRateSiteWrapper):
    def __init__(self, initObject,configs):
        super(CollectBetrecord,self).__init__(initObject,configs,'CollectBetrecord',
                                              'Betrecord', True, 'TransferRetentionListBetrecord')

class CollectFinancialHistory(RetentionRateSiteWrapper):
    def __init__(self, initObject,configs):
        super(CollectFinancialHistory,self).__init__(initObject,configs,'CollectFinancialHistory',
                                              'FinancialHistory', True, 'TransferRetentionListFinancialHistory')

class CollectFinancialLastMonth(RetentionRateSiteWrapper):
    def __init__(self, initObject,configs):
        super(CollectFinancialLastMonth,self).__init__(initObject,configs,'CollectFinancialLastMonth',
                                              'FinancialLastMonth', True, 'TransferRetentionListFinancialLastMonth')

class CollectFinancialPromotion(RetentionRateSiteWrapper):
    def __init__(self, initObject,configs):
        super(CollectFinancialPromotion,self).__init__(initObject,configs,'CollectFinancialPromotion',
                                              'FinancialPromotion', True, 'TransferRetentionListFinancialPromotion')


class RetentionListUpdateUserList(RetentionRateSiteWrapper):
        def __init__(self, initObject,configs):
            super(RetentionListUpdateUserList,self).__init__(initObject,configs,'UpdateUserList','Users', 
                                                          True, 'TransferRetentionListUsers')
        def executeProcess(self):
            leftover = ((dt.strptime(self.initObject.time,'%Y-%m-%d %H:%M:%S.%f') - 
                         dt.strptime('2019-12-20','%Y-%m-%d')).days % 14)
            if leftover == 0:
                super().executeProcess()
            else:
                process = self.getProcess()
                for k in self.initObject.sites.itertuples():
                    process.setStepSite(self.logvar,
                                 k.siteid,
                                 pd.Timestamp(self.initObject.time).date(), 
                                 pd.Timestamp.now().round('1s'))


            
        def writeOut(self):
            configdata  = self.configs['Database'][self.transfer]
            writer       = WritetoDB.Writer(configdata)
            writer.writeUserlist(pd.Timestamp(self.initObject.time).date()) 


class RetentionRateSPWrapperLongTerm(object):
    
    def __init__(self,initObject, configs, execution, logvar, init=False ):
        self.initObject = initObject
        self.configs    = configs
        self.init       =  init
        self.execution  = execution
        self.logvar     = logvar
        
    def getProcess(self):
        process = RetentionListProcessManager(self.initObject.time, 
                                              websites = self.initObject.sites, 
                                              init = self.init)
        return(process)
        
    
    def executeProcess(self):
        print(self.logvar)
        server = self.configs['SP'][self.execution]['Server']
        proc   = self.configs['SP'][self.execution]['Procedure']
        process = self.getProcess()
        print(server)
        print(proc)
        #run main model:
        for k in self.initObject.sites.itertuples():
         print(" EXEC {proc} '{systemcode}','{startdata}','{enddate}'".\
                          format(proc       = proc,
                                 startdata  = pd.Timestamp(self.initObject.time).date(),
                                 enddate    = pd.Timestamp(self.initObject.endtime).date(),
                                 systemcode = k.Website))
         try:
            res = self.initObject.dbs[server].ExecQuery(" EXEC {proc} '{systemcode}','{startdata}','{enddate}'".
                          format(proc       = proc, 
                                 startdata  = pd.Timestamp(self.initObject.time).date(),
                                 enddate    = pd.Timestamp(self.initObject.endtime).date(),
                                 systemcode = k.Website))
            if not res.empty:
                process.setStep(self.logvar,pd.Timestamp(self.initObject.time).date(), pd.Timestamp.now().round('1s'))
         except:
           pass 

class AggregateFinancialLongterm(RetentionRateSPWrapperLongTerm):
    def __init__(self, initObject,configs):
        super(AggregateFinancialLongterm,self).\
                __init__(initObject,configs,'AggregateFinancialLongterm','FinancialAggregationLongTerm')

class AggregateLoginLongterm(RetentionRateSPWrapperLongTerm):
    def __init__(self, initObject,configs):
        super(AggregateLoginLongterm,self).\
                __init__(initObject,configs,'AggregateLoginLongterm','AggregateLoginLongterm')

class AggregateFinancialPromotionLongterm(RetentionRateSPWrapperLongTerm):
    def __init__(self, initObject,configs):
        super(AggregateFinancialPromotionLongterm,self).\
                __init__(initObject,configs,'AggregateFinancialPromotionLongterm','AggregateFinancialPromotionLongterm')

class Cleanup(RetentionRateSPWrapperLongTerm):
    def __init__(self,initObject,configs):
        super(Cleanup,self).\
                __init__(initObject,configs,'CleanupJG','CleanupJG')
