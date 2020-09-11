USE [DataScientist]
GO
/****** Object:  StoredProcedure [dbo].[CollectMonthlyTop10GamesCommissionable]    Script Date: 2020/1/13 下午 03:40:31 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER PROCEDURE [dbo].[CollectMonthlyTop10GamesCommissionable] @Enddate date
AS
BEGIN
	DECLARE @days30 DATE = (SELECT DATEADD(d,-30,@EndDate))
	DECLARE @days10 DATE = (SELECT DATEADD(d,-10,@EndDate))

	DROP TABLE IF EXISTS ##balancecenter_temporary_aggregation
	DROP TABLE IF EXISTS ##Ranked_temporary_aggregation

	DELETE FROM DS_MonthlyTop10Commissionable
	WHERE updatedate < @days10

	SELECT [siteid], 
           [gameaccount], 
           balance.[gametypesourceid], 
           Sum([commissionable]) [Commissionable] ,
		   CASE WHEN [category] = N'真人' THEN N'視訊' 
                WHEN [category] = N'機率' THEN N'電子' 
                ELSE [category] 
			END  category
	INTO ##balancecenter_temporary_aggregation
    FROM   [BalanceCenterSummarize ].dbo.[ds_balancecenterdailyquery] balance
	LEFT JOIN [Datapool].dbo.vw_gamelookup lup
	ON   lup.GameTypesourceid = balance.GameTypeSourceId
    WHERE  dateplayed >= @Days30 AND
           dateplayed <= @EndDate AND
		   siteid in (210,224,59,1)
    GROUP  BY [siteid], 
              [gameaccount], 
              balance.[gametypesourceid],
			  [category]

	SELECT 
		GameAccount, 
		Siteid, 
		ROW_NUMBER()  OVER( PARTITION BY gameaccount, siteid ORDER BY Commissionable DESC ) GameRank, 
		Commissionable, 
		GameTypeSourceId,
		Category
	INTO ##Ranked_temporary_aggregation
	FROM ##balancecenter_temporary_aggregation

	INSERT INTO DS_MonthlyTop10Commissionable
	SELECT GameAccount, 
		   Siteid, 
		   @Enddate,
		   SUM(CASE WHEN CATEGORY = '捕魚' THEN Commissionable ELSE 0.0 END) CommissionableFish,
		   SUM(CASE WHEN CATEGORY = '彩票' THEN Commissionable ELSE 0.0 END) CommissionableLottery,
		   SUM(CASE WHEN CATEGORY = '棋牌' THEN Commissionable ELSE 0.0 END) CommissionableBoard,
		   SUM(CASE WHEN CATEGORY = '視訊' THEN Commissionable ELSE 0.0 END) CommissionableReal,
	       SUM(CASE WHEN CATEGORY = '電子' THEN Commissionable ELSE 0.0 END) CommissionableSlots,
		   SUM(CASE WHEN CATEGORY = '體育' THEN Commissionable ELSE 0.0 END) CommissionableSports,
	       SUM(commissionable) CommissionableTotal,
	       MAX(CASE WHEN GameRank = 1 THEN GameTypeSourceId ELSE NULL END) Top1Game,
	       MAX(CASE WHEN GameRank = 2 THEN GameTypeSourceId ELSE NULL END) Top2Game,
	       MAX(CASE WHEN GameRank = 3 THEN GameTypeSourceId ELSE NULL END) Top3Game,
	       MAX(CASE WHEN GameRank = 4 THEN GameTypeSourceId ELSE NULL END) Top4Game,
	       MAX(CASE WHEN GameRank = 5 THEN GameTypeSourceId ELSE NULL END) Top5Game,
	       MAX(CASE WHEN GameRank = 6 THEN GameTypeSourceId ELSE NULL END) Top6Game,
	       MAX(CASE WHEN GameRank = 7 THEN GameTypeSourceId ELSE NULL END) Top7Game,
	       MAX(CASE WHEN GameRank = 8 THEN GameTypeSourceId ELSE NULL END) Top8Game,
	       MAX(CASE WHEN GameRank = 9 THEN GameTypeSourceId ELSE NULL END) Top9Game,
	       MAX(CASE WHEN GameRank = 10 THEN GameTypeSourceId ELSE NULL END) Top10Game,
	       MAX(CASE WHEN GameRank = 1 THEN Commissionable ELSE NULL END) Top1Commissionable,
	       MAX(CASE WHEN GameRank = 2 THEN Commissionable ELSE NULL END) Top2Commissionable,
	       MAX(CASE WHEN GameRank = 3 THEN Commissionable ELSE NULL END) Top3Commissionable,
	       MAX(CASE WHEN GameRank = 4 THEN Commissionable ELSE NULL END) Top4Commissionable,
	       MAX(CASE WHEN GameRank = 5 THEN Commissionable ELSE NULL END) Top5Commissionable,
	       MAX(CASE WHEN GameRank = 6 THEN Commissionable ELSE NULL END) Top6Commissionable,
	       MAX(CASE WHEN GameRank = 7 THEN Commissionable ELSE NULL END) Top7Commissionable,
	       MAX(CASE WHEN GameRank = 8 THEN Commissionable ELSE NULL END) Top8Commissionable,
	       MAX(CASE WHEN GameRank = 9 THEN Commissionable ELSE NULL END) Top9Commissionable,
	       MAX(CASE WHEN GameRank = 10 THEN Commissionable ELSE NULL END) Top10Commissionable
   FROM ##Ranked_temporary_aggregation
   GROUP BY GameAccount,
			Siteid
END
