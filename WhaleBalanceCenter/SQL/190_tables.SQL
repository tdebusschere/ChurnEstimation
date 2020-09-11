CREATE TABLE DATASCIENTIST.dbo.DS_RetentionListBetrecordDailySummary
(
	indx int identity(1,1),
	Gameaccount nvarchar(30),
	siteid int,
	updatedate date,
	commissionable float,
	wagerscount int,
	payoff float,
	betswon int,
	hoursplayed int,
	gamesplayed int,
	lasttimeplayed datetime,
	lastgame int,
	timesincelastwin datetime
)

create index DS_RetentionListBetrecordDailySummaryidx 
on DATASCIENTIST.dbo.DS_RetentionListBetrecordDailySummary 
(siteid, updatedate) 

CREATE TABLE DataScientist.dbo.FinancialDailyAggregate 
(
	indx int identity(1,1),
	Website NVARCHAR(10),
	[Day] DATE,
	MemberId INT,
	DepositAmt FLOAT,
	DepositTimes INT,
	WithdrawAmt FLOAT,
	WithdrawTimes INT,
	DepositMax INT,
	WithdrawMax INT
)

create index FinancialDailyAggregate 
on DATASCIENTIST.dbo.FinancialDailyAggregate 
(Website, [Day], MemberId) 

DROP TABLE IF EXISTS [RetentionListUsers]
CREATE TABLE [RetentionListUsers]
( 
    ID INT NOT NULL IDENTITY(1,1) PRIMARY KEY, 
    MemberId INT, 
	GameAccount NVARCHAR(20),
    FirstUpdateDay DATE,
    LastUpdateDay DATE, 
    MaxActivityVector DATE, 
    [Type] NVARCHAR(20), 
    SystemCode NVARCHAR(20),
    JoinTime DATETIME,
    MemberLevelSettingId INT,
	DiscountSettingId INT,
    FirstDayPlayed DATETIME
)
CREATE INDEX RetentionlistUserIdx ON 
RetentionListUsers (MemberId, Type,SystemCode) 
GO

DROP TABLE IF EXISTS [RetentionListActivity]
CREATE TABLE [RetentionListActivity] 
(
    id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,   
    TYPE NVARCHAR(10), 
    website NVARCHAR(6), 
    MemberId INT, 
    activity DATE,  
    leave7d0 INT, 
    leave7d1 INT, 
    leave7d3 INT, 
    leave7d5 INT, 
    leave7d7 INT, 
    leave7d14 INT, 
    included INT, 
    dayssincelasttime INT,
    walletamount FLOAT)
CREATE INDEX RetentionListActivityIdx ON [RetentionListActivity] 
(memberid, activity, website) INCLUDE ( [id], [type])
CREATE INDEX RetentionListActivityws ON [RetentionListActivity] 
(website, [type]) INCLUDE ([id],[memberid],[activity] )
GO

CREATE table resultpool.dbo.retentionlistResult
(
	[SystemCode] [nvarchar](50) NOT NULL,
	[MemberId] [int] NOT NULL,
	[RetentionDays] [int] NOT NULL,
	[CreateDate] [datetime] NOT NULL,
	[BetAmountSum] [decimal](18, 2) NOT NULL,
	[DepositSum] [decimal](18, 2) NOT NULL,
	[DepositTimes] [int] NOT NULL,
	[WithdrawSum] [decimal](18, 2) NOT NULL,
	[RealCommissionableSum] [decimal](18, 2) NOT NULL,
	[SportCommissionableSum] [decimal](18, 2) NOT NULL,
	[LotteryCommissionableSum] [decimal](18, 2) NOT NULL,
	[SlotCommissionableSum] [decimal](18, 2) NOT NULL,
	[BoardCommissionableSum] [decimal](18, 2) NOT NULL,
	[FishCommissionableSum] [decimal](18, 2) NOT NULL,
	[GameId1] [int] NULL,
	[GameId2] [int] NULL,
	[GameId3] [int] NULL,
	[GameId4] [int] NULL,
	[GameId5] [int] NULL,
	[GameId6] [int] NULL,
	[GameId7] [int] NULL,
	[GameId8] [int] NULL,
	[GameId9] [int] NULL,
	[GameId10] [int] NULL,
	[GameId1CommissionableSum] [decimal](18, 2) NULL,
	[GameId2CommissionableSum] [decimal](18, 2) NULL,
	[GameId3CommissionableSum] [decimal](18, 2) NULL,
	[GameId4CommissionableSum] [decimal](18, 2) NULL,
	[GameId5CommissionableSum] [decimal](18, 2) NULL,
	[GameId6CommissionableSum] [decimal](18, 2) NULL,
	[GameId7CommissionableSum] [decimal](18, 2) NULL,
	[GameId8CommissionableSum] [decimal](18, 2) NULL,
	[GameId9CommissionableSum] [decimal](18, 2) NULL,
	[GameId10CommissionableSum] [decimal](18, 2) NULL
)

drop table if exists [RetentionListBetrecord]
create table  [RetentionListBetrecord] 
(
    id int not null identity(1,1) primary key,   
    [type] nvarchar(10), 
    website nvarchar(40), 
    activity date , 
    memberid int, 
    wagerscount float, 
    amountsum float, 
    amountsum7d float, 
    payoffsum float, 
    commissionablesum float, 
    hoursplayed int, 
    diffgamesplayed int,
    lastperiodplayed int,
    lastrawdatatype int,
    lastcode nvarchar(30),
    payoff7d float,
    wagerscount7d int,
    payoff1d float, 
    wagerscount1d int, 
    mostrawdatatype int, 
    mostcode nvarchar(30), 
    betamountrawdatatype int, 
    betamountcode nvarchar(30),
    wagerscountrawdatatype int, 
    wagerscountcode nvarchar(30),
	betswon int,
	betswonlastday int,
	timesincelastwin int
)
create index RetentionListBetrecordIdx on [RetentionListBetrecord] (memberid, activity, [type], website)
go

drop table if exists [RetentionListWalletHistory]
create table [RetentionListWalletHistory] 
( 
    id int not null identity(1,1) primary key,      
    website nvarchar(10),
    type nvarchar(10), 
    activity date, 
    memberid int,
    memberlevelsettingid int, 
    walletamount float, 
    jointime int, 
    first_day_played int, 
    walletamt1d float, 
    walletamt2d float, 
    walletamt3d float, 
    walletamt4d float, 
    walletamt5d float, 
    walletamt6d float, 
    walletamt7d float
)
create index RetentionListWalletHistoryIdx on [RetentionListWalletHistory] (memberid, activity,[type], website)
go

CREATE TABLE
resultpool.dbo.retentionlistResultStatus
(systemcode nvarchar(10) , 
 date date, 
 executiontime datetime, 
 processstatus int)

DROP TABLE IF EXISTS [dRetentionListUsers]
CREATE TABLE [dRetentionListUsers]
( 
    ID INT NOT NULL IDENTITY(1,1) PRIMARY KEY, 
    MemberId INT, 
	GameAccount NVARCHAR(20),
    FirstUpdateDay DATE,
    LastUpdateDay DATE, 
    MaxActivityVector DATE, 
    [Type] NVARCHAR(20), 
    SystemCode NVARCHAR(20),
    JoinTime DATETIME,
    MemberLevelSettingId INT,
	DiscountSettingId INT,
    FirstDayPlayed DATETIME
)
CREATE INDEX dRetentionlistUserIdx ON 
dRetentionListUsers (MemberId, Type,SystemCode) 
GO
