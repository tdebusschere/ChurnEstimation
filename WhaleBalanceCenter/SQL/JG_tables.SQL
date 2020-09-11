DROP TABLE if exists DataScientist.dbo.DS_RetentionlistPreprocesStatus

  CREATE TABLE DataScientist.dbo.DS_RetentionlistPreprocesStatus
  ( 
	ProcesStartDate DATE,
	Initialization DATETIME,
	BalanceCenterUpdatedTime DATETIME,
	MonthlyTop10Commissionable DATETIME,
	ProcessStatus nvarchar(20)
  )


 CREATE TABLE DataScientist.dbo.DS_RetentionlistActiveWebsites
 (
	 Website nvarchar(10),
	 siteid int,
	 Active bit,
	 FirstUpdatedate date,
	 Lastupdatedate date
 )



  Drop Table if exists DataScientist.dbo.DS_RetentionlistProcesStatus

  CREATE TABLE DataScientist.dbo.DS_RetentionlistProcesStatus
  ( 
	ProcesStartDate DATE,
	Website int,
	Initialization DateTime,
	FinancialAggregation DateTime,
	Top10Games DateTime,
	BalanceCenter DateTime,
	Users DateTime,
	Activity DateTime,
	Betrecord DateTime,
	FinancialHistory DateTime,
	FinancialLastMonth DateTime,
	IPdata DateTime,
	FinancialPromotion DateTime,
	UpdateActivity DateTime,
	FinancialPromotionU DateTime,
	WalletHistory DateTime,
	ActivityDates DateTime,
	ActivityHistory DateTime,
	Model DateTime,
	Integration DateTime,
	ProcessStatus nvarchar(20)
  )



