/****** Object:  StoredProcedure [dbo].[AggregateFinancialWebsite]    Script Date: 2020/1/13 下午 05:24:04 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[AggregateFinancialWebsite] @date date, @systemcode nvarchar(10) = 'DQ002', @systemint int
AS
BEGIN
	DECLARE @query   NVARCHAR(max) = ''

	SELECT Name FROM [ETLR].[_PersonTrackCenter].sys.Views where name like '%Membertransaction[_]%' and name like '%'+ LTRIM(RTRIM(@systemcode)) + '%'
	IF @@ROWCOUNT >0
	BEGIN

	SELECT @query = '
				INSERT INTO DataScientist.dbo.FinancialDailyAggregate
				SELECT @Systemcode website,
					   CONVERT(DATE,[datetime]) [day], 
					   mtqs.MemberId, 
					   ISNULL(SUM(CASE WHEN mtqs.type IN (1,2,4) AND income = 1 THEN amount END),0.0) DepositAmt,
					   ISNULL(SUM(CASE WHEN mtqs.type IN (1,2,4) AND income = 1 THEN times END),0)  DepositTimes,
					   ISNULL(SUM(CASE WHEN mtqs.type IN (3,4) AND income = 0 THEN amount END),0.0)  WithdrawAmt,
					   ISNULL(SUM(CASE WHEN mtqs.type IN (3,4) AND income = 0 THEN times END),0)   WithdrawTimes,
					   ISNULL(MAX(CASE WHEN mtqs.type IN (1,2,4) AND income = 1 THEN amount END),0.0) DepositMax,
					   ISNULL(MAX(CASE WHEN mtqs.type IN (3,4)   AND income = 0 THEN amount END),0.0) WithdrawMax
    	 		 FROM [ETLR].[_PersonTrackCenter].dbo.VW_membertransaction_' + @Systemcode + ' mtqs
		 		 WHERE (( mtqs.[type] in (1,2,4) and income = 1 )  OR 
					   ( mtqs.[type] in (3,4) and income = 0   )) AND 
						 convert(date,[datetime])  = @date
				 GROUP BY mtqs.memberid, 
						  CONVERT(DATE,[datetime])	'

	EXEC sp_executesql @query, N'@date date, @Systemcode nvarchar(10)', @Date, @Systemcode
	SELECT 'Success' Success 

	END
	ELSE 
	BEGIN
		THROW 51000, 'The Table does not exist.', 1;   
	END
END

