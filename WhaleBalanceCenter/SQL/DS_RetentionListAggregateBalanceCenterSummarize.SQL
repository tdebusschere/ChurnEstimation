USE [DataScientist]
GO
/****** Object:  StoredProcedure [dbo].[DS_RetentionListAggregateBalanceCenterSummarize]    Script Date: 2020/1/13 上午 11:03:36 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[DS_RetentionListAggregateBalanceCenterSummarize] @STARTDATE DATE
AS

BEGIN


DECLARE @Startdate2 DATETIME = CONVERT(DATETIME, @StartDate)
DECLARE @BeginTime DATETIME   = (SELECT DateAdd(HH,-11,@StartDate2))
DECLARE @EndTime DATETIME = (SELECT DateAdd(HH,13,@StartDate2))

insert into DATASCIENTIST.dbo.DS_RetentionListBetrecordDailySummary
SELECT Gameaccount,
	   Siteid,
	   @Startdate UpdateDate,
	   sum( Commissionable ) Commissionable,
	   sum( Wagerscount) wagerscount,
	   sum( payoff) payoff,
	   sum( winscount) betswon,
	   count( distinct dateplayed) hoursplayed,
	   count( distinct game) gamesplayed,
	   max( case when prs = 1 then dateplayed else NULL end) lasttimeplayed,
	   max( case when prs = 1 then game else 0 end) lastgame,
	   max( case when winct =1 and winscount >= 1 then dateplayed else NULL end) timesincelastwin
FROM
	(
	 SELECT Gameaccount,
	        siteid, 
			commissionable, 
			wagerscount, 
			payoff, 
			winscount, 
			dateplayed, 
			ROW_NUMBER() over( partition by siteid, gameaccount order by dateplayed desc) prs,
			row_number() over( partition by siteid, gameaccount, case when winscount > 0  then 1 else 0 end order by dateplayed desc) winct,
			gametypesourceid game 
	 FROM balancecentersummarize.dbo.DS_balanceCenterdailyQuery
	 WHERE [DatePlayed] >= @BeginTime AND
		   [Dateplayed] <  @EndTime	
	)	x  
	 GROUP BY Gameaccount, Siteid
 	
END