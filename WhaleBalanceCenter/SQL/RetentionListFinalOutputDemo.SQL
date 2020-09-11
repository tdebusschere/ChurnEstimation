
--DQ002 = 210

SELECT DSS.memberid,
	   DSS.systemcode, 
	   CASE WHEN churnday = 'd0' THEN 0 
			WHEN churnday = 'd1' THEN 1
			WHEN churnday = 'd3' THEN 3
			WHEN churnday = 'd5' THEN 5       
	   END LeaveDay,
	   CommissionableSum,
	   depositsum + creditdepositsum + thirdpartypaymentsum DepositSum, 
	   deposittimes + creditdeposittimes + thirdpartypaymenttimes DepositTimes,
	   WithdrawSum,
	   CommissionableFish,
	   CommissionableLottery,
	   CommissionableBoard,
	   CommissionableReal,
	   CommissionableSlots,
	   CommissionableSports,
	   Game1,
	   Game2,
	   Game3,
	   Game4,
	   Game5,
	   Game6,
	   Game7,
	   Game8,
	   Game9,
	   Game10,
	   Game1CommissionableSum,
	   Game2CommissionableSum,
	   Game3CommissionableSum,
	   Game4CommissionableSum,
	   Game5CommissionableSum,
	   Game6CommissionableSum,
	   Game7CommissionableSum,
	   Game8CommissionableSum,
	   Game9CommissionableSum,
	   Game10CommissionableSum
FROM ##predictResult DSS 
JOIN [ETLR].[_PersonTrackCenter].dbo.[VW_MemberGameAccount_DQ002] dq
ON DSS.memberid = dq.memberid AND Diffgameaccount = 0
JOIN DataScientist.dbo.RetentionListFinancialLastMonth	RFLM
ON dss.memberid = RFLM.memberid and RFLM.activity = '2019-10-26'
JOIN 
(
	SELECT GameAccount, 
	   Siteid, 
	   SUM(CASE WHEN CATEGORY = '捕魚' THEN Commissionable ELSE 0.0 END) CommissionableFish,
	   SUM(CASE WHEN CATEGORY = '彩票' THEN Commissionable ELSE 0.0 END) CommissionableLottery,
	   SUM(CASE WHEN CATEGORY = '棋牌' THEN Commissionable ELSE 0.0 END) CommissionableBoard,
	   SUM(CASE WHEN CATEGORY = '視訊' THEN Commissionable ELSE 0.0 END) CommissionableReal,
	   SUM(CASE WHEN CATEGORY = '電子' THEN Commissionable ELSE 0.0 END) CommissionableSlots,
	   SUM(CASE WHEN CATEGORY = '體育' THEN Commissionable ELSE 0.0 END) CommissionableSports,
	   SUM(commissionable) CommissionableSum,
	   MAX(CASE WHEN GameRank = 1 THEN GameTypeSourceId ELSE NULL END) Game1,
	   MAX(CASE WHEN GameRank = 2 THEN GameTypeSourceId ELSE NULL END) Game2,
	   MAX(CASE WHEN GameRank = 3 THEN GameTypeSourceId ELSE NULL END) Game3,
	   MAX(CASE WHEN GameRank = 4 THEN GameTypeSourceId ELSE NULL END) Game4,
	   MAX(CASE WHEN GameRank = 5 THEN GameTypeSourceId ELSE NULL END) Game5,
	   MAX(CASE WHEN GameRank = 6 THEN GameTypeSourceId ELSE NULL END) Game6,
	   MAX(CASE WHEN GameRank = 7 THEN GameTypeSourceId ELSE NULL END) Game7,
	   MAX(CASE WHEN GameRank = 8 THEN GameTypeSourceId ELSE NULL END) Game8,
	   MAX(CASE WHEN GameRank = 9 THEN GameTypeSourceId ELSE NULL END) Game9,
	   MAX(CASE WHEN GameRank = 10 THEN GameTypeSourceId ELSE NULL END) Game10,
	   MAX(CASE WHEN GameRank = 1 THEN Commissionable ELSE NULL END) Game1CommissionableSum,
	   MAX(CASE WHEN GameRank = 2 THEN Commissionable ELSE NULL END) Game2CommissionableSum,
	   MAX(CASE WHEN GameRank = 3 THEN Commissionable ELSE NULL END) Game3CommissionableSum,
	   MAX(CASE WHEN GameRank = 4 THEN Commissionable ELSE NULL END) Game4CommissionableSum,
	   MAX(CASE WHEN GameRank = 5 THEN Commissionable ELSE NULL END) Game5CommissionableSum,
	   MAX(CASE WHEN GameRank = 6 THEN Commissionable ELSE NULL END) Game6CommissionableSum,
	   MAX(CASE WHEN GameRank = 7 THEN Commissionable ELSE NULL END) Game7CommissionableSum,
	   MAX(CASE WHEN GameRank = 8 THEN Commissionable ELSE NULL END) Game8CommissionableSum,
	   MAX(CASE WHEN GameRank = 9 THEN Commissionable ELSE NULL END) Game9CommissionableSum,
	   MAX(CASE WHEN GameRank = 10 THEN Commissionable ELSE NULL END) Game10CommissionableSum
	FROM ##Ranked_temporary_aggregation
	GROUP BY GameAccount,
	      	 Siteid
) X on X.gameaccount = dq.gameaccount AND
	   X.siteid = 210	
WHERE ChurnDay != 'active'
