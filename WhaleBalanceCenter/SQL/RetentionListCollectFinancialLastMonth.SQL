USE [DataScientist]
GO
/****** Object:  StoredProcedure [dbo].[RetentionListCollectFinancialLastMonth]    Script Date: 2020/1/14 上午 11:12:30 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[RetentionListCollectFinancialLastMonth]
	@InputDate DATE,
	@SystemCode NVARCHAR(10),
	@SystemInt INT
AS
BEGIN

	

DECLARE @Query  NVARCHAR(Max) = '',
        @LastDate DATE = DATEADD(d,-30,@InputDate),
		@Days7ago DATE = DATEADD(d,-7 ,@InputDate)

	SELECT Name FROM [ETLR].[_PersonTrackCenter].sys.Views where name like '%Membertransaction[_]%' and name like '%'+ LTRIM(RTRIM(@systemcode)) + '%'
	IF @@ROWCOUNT >0
	BEGIN

SELECT @Query = 
'INSERT INTO DataScientist.dbo.RetentionListFinancialLastMonth
 SELECT act.memberid,
	   act.website,	
	   act.[type],
	   act.[activity],
	   ISNULL(SUM(CASE WHEN trans.[type] = 1 AND Income = 1 THEN Times ELSE 0 END ),0) AS deposittimes,
	   ISNULL(SUM(CASE WHEN trans.[type] = 4 AND Income = 1 THEN Times ELSE 0 END),0) AS creditdeposittimes,
	   ISNULL(SUM(CASE WHEN trans.[type] = 2 AND Income = 1 THEN Times ELSE 0 END),0) AS thirdpartypaymenttimes,
	   ISNULL(SUM(CASE WHEN trans.[type] = 1 AND Income = 1 THEN Amount ELSE 0 END ),0) AS depositSUM,
	   ISNULL(SUM(CASE WHEN trans.[type] = 4 AND Income = 1 THEN Amount ELSE 0 END),0) AS creditdepositSUM,
	   ISNULL(SUM(CASE WHEN trans.[type] = 2 AND Income = 1 THEN Amount ELSE 0 END),0) AS thirdpartypaymentSUM,
	   ISNULL(SUM(CASE WHEN trans.[type] IN (3,4) AND Income = 0 THEN Times ELSE 0 END),0) AS withdrawtimes,
	   ISNULL(SUM(CASE WHEN trans.[type] IN (3,4) AND Income = 0 THEN Amount ELSE 0 END),0) AS withdrawSUM,
	   ISNULL(SUM(CASE WHEN trans.[type] = 5 AND Income = 1 THEN amount ELSE 0 END),0) as FavorableSUM,
	   ISNULL(SUM(CASE WHEN trans.[type] = 6 AND Income = 1 THEN amount ELSE 0 END),0) as DiscountSUM,
	   ISNULL(SUM(CASE WHEN trans.[type] = 99 AND Income = 1 THEN amount ELSE 0 END),0) as OthersSUM,
	   ISNULL(SUM(CASE WHEN trans.[type] = 8 AND Income = 1 THEN amount ELSE 0 END),0) as MemberDiscountSUM,
	   ISNULL(SUM(CASE WHEN CONVERT(date,[datetime]) = @date AND trans.[type] IN (1,2,4) AND Income = 1 THEN Amount ELSE 0 END),0) AS yesterdaydepositamt,
	   ISNULL(SUM(CASE WHEN CONVERT(date,[datetime]) = @date AND trans.[type] IN (1,2,4) AND Income = 1 THEN Times ELSE 0 END),0) AS yesterdaydeposit,
	   ISNULL(SUM(CASE WHEN CONVERT(date,[datetime]) = @date AND trans.[type] IN (1,2,3,4) AND Income = 0 THEN Amount ELSE 0 END),0) AS yesterdaywithdrawamt,
	   ISNULL(SUM(CASE WHEN CONVERT(date,[datetime]) = @date AND trans.[type] IN (1,2,3,4) AND Income = 0 THEN Times ELSE 0 END),0) AS yesterdaywithdraw,
   	   ISNULL(SUM(CASE WHEN CONVERT(date,[datetime]) > @days7ago AND trans.[type] IN (1,2,4) AND Income = 1 THEN Amount ELSE 0 END),0) AS last7dDepositamt,
	   ISNULL(SUM(CASE WHEN CONVERT(date,[datetime]) > @days7ago AND trans.[type] IN (1,2,4) AND Income = 1 THEN Times ELSE 0 END),0) AS last7dDeposit,
   	   ISNULL(SUM(CASE WHEN CONVERT(date,[datetime]) > @days7ago AND trans.[type] IN (1,2,3,4) AND Income = 0 THEN Amount ELSE 0 END),0) AS last7dwithdrawamt,
	   ISNULL(SUM(CASE WHEN CONVERT(date,[datetime]) > @days7ago AND trans.[type] IN (1,2,3,4) AND Income = 0 THEN Times ELSE 0 END),0) AS last7dwithdraw
 FROM DataScientist.dbo.RetentionListActivity act
 JOIN [ETLR].[_PersonTrackCenter].dbo.VW_membertransaction_' + @SystemCode + ' trans
 ON act.memberid = trans.memberid
 WHERE activity = @Date AND 
	  [Datetime] <= @Date AND
	  [DateTime] >= @LastDate AND
	  trans.[Type] != 7 AND 
	  WebSite = @SystemCode AND
	  Included = 1
 GROUP BY act.memberid,
		 act.activity,
		 act.[type],
		 act.website'

EXEC sp_executesql @Query, N'@Date DATE, @LastDate DATE,
							 @Days7ago DATE, @SystemCode NVARCHAR(10)', 
							 @InputDate, @LastDate, 
							 @Days7ago, @SystemCode

	SELECT 'Success' Success 

	END
	ELSE 
	BEGIN
		THROW 51000, 'The Table does not exist.', 1;   
	END

END
