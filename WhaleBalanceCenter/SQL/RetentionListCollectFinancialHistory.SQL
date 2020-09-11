/*
Declare @Date DATE = '2019-12-31'
DECLARE @SystemCode NVARCHAR(10) = 'DQ002'
*/
ALTER PROCEDURE [dbo].[RetentionListCollectFinancialHistory]
	@Date Date,
	@SystemCode nvarchar(10),
	@Systemint int
AS
BEGIN

	DECLARE @Days30Ago DATE = DATEADD(d,-30,@Date)
	DECLARE @Days120Ago Date = DATEADD(d,-120,@Date)

	INSERT into [DataScientist].dbo.[RetentionListFinancialHistory]
	SELECT MemberID, 
		   @Date,
		   [type],
		   @SystemCode,
	       ISNULL(MAX( CASE WHEN [Day] > @Days30Ago THEN DepositMax ELSE 0 END),0) AS DepositsMax,
		   SUM( CASE WHEN SortDeposit = 1 AND LastDepositAmt > 0 THEN LastDepositAmt ELSE 0 END) AS LastDepositAmt,
		   SUM( CASE WHEN SortDeposit = 1 AND LastDepositAmt > 0 THEN LastDeposit ELSE 0 END) AS LastDeposit,
		   min( Datediff(d,CASE WHEN LastDepositAmt > 0  THEN [Day] ELSE '2000-01-01' END,@Date)) AS TimeSinceLastDeposit,
		   ISNULL(SUM( CASE WHEN SortDeposit = 2 AND LastDepositAmt > 0 THEN LastDepositAmt ELSE 0 END),0) AS PreviousDepositAmt,
		   ISNULL(SUM( CASE WHEN SortDeposit = 2 AND LastDepositAmt > 0 THEN LastDeposit ELSE 0 END),0) AS PreviousDeposit,
		   MIN( DATEDIFF(d,CASE WHEN SortDeposit = 2 AND LastDepositAmt > 0 THEN [Day] ELSE '2000-01-01' END, @Date)) AS PreviousDepositLag,
		   ISNULL(SUM( CASE WHEN LastDepositAmt > 0 THEN 1 ELSE 0 END),0) AS DatesDeposit, 
		   AVG( CASE WHEN LastDepositAmt > 0 THEN CONVERT(FLOAT,TimeGap) ELSE NULL END) AS AvgTimeBetweenDeposit,
		   ISNULL(AVG( CASE WHEN LastDepositAmt > 0 THEN CONVERT(FLOAT,LastDeposit) ELSE NULL END),0) AS AvgDepositAmt, 
		   ISNULL(AVG( CASE WHEN LastDepositAmt > 0 THEN CONVERT(FLOAT,LastDepositAmt) ELSE NULL END),0) AS AvgDepositTimes,   
		   ISNULL(MAX( CASE WHEN [Day] > @Days30Ago THEN WithdrawMax ELSE 0 END),0) AS WithdrawMax,
		   ISNULL(SUM( CASE WHEN SortWithdraw = 1 AND LastWithdrawAmt > 0 THEN LastWithdrawAmt ELSE 0 END),0) AS LastWithdrawAmt, 
		   SUM( CASE WHEN SortWithdraw = 1 AND LastWithdrawAmt > 0 THEN LastWithdraw ELSE 0 END) AS LastWithdraw, 
		   min( DATEDIFF(d,CASE WHEN LastWithdrawAmt > 0 THEN [Day] ELSE '2000-01-01' END,@Date)) AS TimeSinceLastWithdraw,
		   ISNULL(SUM( CASE WHEN SortWithdraw = 2 AND LastWithdrawAmt > 0 THEN LastWithdrawAmt ELSE 0 END),0) AS PreviousWithdrawAmt,
		   ISNULL(SUM( CASE WHEN SortWithdraw = 2 AND LastWithdrawAmt > 0 THEN LastWithdraw ELSE 0 END),0) AS PreviousWithdraw,
		   MIN( DATEDIFF(d,CASE WHEN SortWithdraw = 2 AND LastWithdrawAmt > 0 THEN [Day] ELSE '2000-01-01' END, @Date)) AS PreviousWithdrawLag,
		   ISNULL(SUM( CASE WHEN LastWithdrawAmt > 0 THEN 1 ELSE 0 END),0) AS DatesWithdraw, 
		   AVG( CASE WHEN LastWithdraw > 0 THEN CONVERT(FLOAT,TimeGap) ELSE NULL END) AS AvgTimeBetweenWithdraw,
		   ISNULL(AVG( CASE WHEN LastWithdraw > 0 THEN CONVERT(FLOAT,LastWithdraw) ELSE NULL END),0) AS AvgWithdrawAmt, 
		   ISNULL(AVG( CASE WHEN LastWithdrawAmt > 0 THEN CONVERT(FLOAT,LastWithdrawAmt) ELSE NULL END),0) AS  AvgWithdrawTimes
	 FROM
	 (
		SELECT act.MemberId, 
				act.Activity,  
				[day], 
				act.[type], 
				DepositTimes LastDeposit,
				DepositAmt LastDepositAmt,
				ROW_NUMBER() OVER( PARTITION BY mtqs.MemberID , CASE WHEN depositamt > 0 THEN 1 ELSE 0 END  ORDER BY DAY DESC) AS SortDeposit,
				ROW_NUMBER() OVER( PARTITION BY mtqs.MemberID , CASE WHEN withdrawamt > 0 THEN 1 ELSE 0 END ORDER BY  DAY DESC) AS SortWithdraw,
				Withdrawtimes LastWithdraw, 
				Withdrawamt LastWithdrawAmt,
				DATEDIFF(d,LEAD([Day],1,[Day]) OVER( PARTITION BY mtqs.memberid, act.Activity ORDER BY [Day] DESC), [Day]) TimeGap,
				[DepositMax],
				[WithdrawMax]
		    FROM [DataScientist].[dbo].[FinancialDailyAggregate] mtqs
		RIGHT JOIN  
		(
		SELECT MemberID,
	         Activity, 
			 [Type]
		FROM [DataScientist].dbo.[RetentionListActivity] 
		WHERE @Date = Activity AND
	        Website = @SystemCode and included =1
		) act 
		ON mtqs.MemberID = act.MemberID AND
			  Day <= @Date 
		WHERE mtqs.[Day] >= @Days120Ago		
	) x GROUP BY x.MemberID, x.[type]

END
