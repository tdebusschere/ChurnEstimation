USE [DataScientist]
GO
/****** Object:  StoredProcedure [dbo].[RetentionListCollectBetrecord]    Script Date: 2020/1/14 上午 09:21:39 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[RetentionListCollectBetrecord] @date date, @systemCode nvarchar(10), @systemint int 
AS
BEGIN

	DECLARE @Days7  DATE = (SELECT DATEADD(d,-7,@Date))
	DECLARE @days30 DATE = (SELECT DATEADD(d,-30,@Date))


	Insert into DataScientist.dbo.retentionlistbetrecord
SELECT 
	   rlu.[type],
	   @systemCode,
	   @Date as [Date],
	   memberid,
	   SUM(wagerscount) as Wagerscount,
	   NULL as amountsum,
   	   SUM( CASE WHEN updatedate > @Days7 then commissionable else 0.0 end ) Wagerscount7d,
	   SUM(Payoff) as PayoffSum,
	   SUM(Commissionable) as CommissionableSum,
	   SUM(Hoursplayed) as hoursplayed,
	   SUM(gamesplayed) as Diffgamesplayed,
	   MIN( DATEDIFF(hh, CASE WHEN Playorder = 1 THEN lasttimeplayed ELSE NULL END, @Date)) lastperiodplayed,
   	   MAX( CASE WHEN playorder = 1 THEN lookupt.[Type] ELSE NULL END) lrawdatatype,
	   MAX( CASE WHEN playorder = 1 THEN lastgame ELSE NULL END) lgamecode,
	   SUM( CASE WHEN updatedate > @Days7 then payoff else 0.0 end ) Payoff7d,
	   SUM( CASE WHEN updatedate > @Days7 then wagerscount else 0 end ) Wagerscount7d,
	   SUM( CASE WHEN updatedate = @Date then payoff else 0.0 end ) Payoff1d,
	   SUM( CASE WHEN updatedate = @Date then wagerscount else 0 end ) Wagerscount1d,
	   0 AS MostRawDataType, 
	   '0' AS MostCode , 
	   0 AS WagersRawDataType, 
	   '0' AS WagersCode, 
	   0 AS BetamountRawDataType,
	   '0' AS BetamountCode,
	   SUM( betswon) bets_won,
	   SUM( case when updatedate = @Date then betswon else 0 end ) betswonlastday,
	   MIN( DAtediff(hh, case when winorder =1 and betswon > 0 THEN timesincelastwin ELSE NULL END ,@Date)) timesincelastwin
FROM
	(
	SELECT gameaccount, 
		   siteid, 
		   updatedate, 
		   commissionable,
		   wagerscount,
		   payoff,
		   betswon,
		   hoursplayed,
		   lasttimeplayed,
		   lastgame,
		   gamesplayed,
		   timesincelastwin,
		   ROW_NUMBER() OVER( PARTITION BY Gameaccount, siteid ORDER BY lasttimeplayed DESC) playorder,
		   ROW_NUMBER() OVER( PARTITION BY Gameaccount, siteid, CASE WHEN betswon IS NULL THEN 0 ELSE 1 END 
						ORDER BY updatedate DESC) winorder
	FROM DataScientist.dbo.DS_RetentionlistBetrecordDailySummary (NOLOCK)
	WHERE updatedate >= @Days30  AND
		  updatedate <= @date AND
		  siteid = @systemint
	) orderedsummary JOIN 
	BalanceOutcome.dbo.LookUpTable lookupt on
	orderedsummary.lastgame = lookupt.GameTypeSourceID	
	JOIN DataScientist.dbo.RetentionListUsers rlu on
		 @date >= rlu.FIrstupdateday AND 
		 @date <= rlu.lastupdateday AND 
		 RLU.gameaccount = orderedsummary.gameaccount
	GROUP BY siteid,
			 memberid,
			 rlu.[type]

	SELECT 'Success' SuccessCriteria

END