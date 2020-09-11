USE [DataScientist]
GO
/****** Object:  StoredProcedure [dbo].[RetentionListCollectFinancialPromotion]    Script Date: 2020/1/14 上午 11:15:31 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[RetentionListCollectFinancialPromotion]  @Date DATE, @SystemCode NVARCHAR(10) = 'DQ002', @SystemInt int
AS
BEGIN

DECLARE @shiftedframe DATE = DATEADD(d,-30,@Date),
		@Query nvarchar(max)=''

SELECT Name FROM [ETLR].[_PersonTrackCenter].sys.Views where name like '%membertransactionExtraInfo[_]%' and name like '%'+ LTRIM(RTRIM(@systemcode)) + '%'
if  @@ROWCOUNT = 1 
BEGIN


SELECT @Query = 
'
INSERT INTO datascientist.dbo.RetentionListFinancialPromotion
SELECT 
	rla.[Type], 
	rla.Website,  
	rla.MemberID, 
	rla.Activity,  
	0.0 AS Avg_DepositAmount_Decreasing, 
	0.0 AS Avg_DepositTimes_Decreasing,
	0.0 AS Avg_WithdrawAmount_Increasing,
	0.0 AS Avg_DepositAmt_Over_1000,
	isnull(am,0) AS Discount
 FROM 
 (
 	SELECT mt.MemberID, Sum(mt.Amount) AS am
	FROM [ETLR].[_PersonTrackCenter].dbo.VW_membertransactionExtraInfo_' + @SystemCode + ' mt  
    WHERE  
	  	mt.[Type] = 5 AND
		mt.[DateTime]  >= @ShiftedFrame AND
		mt.[DateTime] <= @Date AND
		TypeForMember IN( N''优惠活动'',N''红包派送'') AND
		NOT exists (select portalmemo from DataScientist.dbo.RetentionListPromotionExclusion x where x.portalmemo = mt.portalmemo and Website = @SystemCode) 
		AND mt.PortalMemo is not NULL
	GROUP BY mt.MemberID
) x RIGHT JOIN datascientist.dbo.RetentionListActivity RLA ON
			   RLA.memberid = x.memberid
	WHERE  RLA.Included = 1 AND RLA.Activity = @date and RLA.website = @SystemCode
	'

	EXEC sp_executesql @query, N'@date date, @ShiftedFrame date, @SystemCode nvarchar(10)', @Date, @ShiftedFrame, @SystemCode
		SELECT 'SUCCESS' successcriteria
END
ElSE
BEGIN
		THROW 51000, 'Not all tables do exists.', 1;   
END

END
