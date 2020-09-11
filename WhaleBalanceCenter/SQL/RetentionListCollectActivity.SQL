USE [DataScientist]
GO
/****** Object:  StoredProcedure [dbo].[RetentionListCollectActivity]    Script Date: 2020/1/15 下午 01:23:21 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[RetentionListCollectActivity]

@Date DATE,
@SystemCode NVARCHAR(20),
@SystemInt int

AS
BEGIN

SET NOCOUNT ON;
-- =============================================
-- Author:		Tom
-- Create date: 2020-01-08
-- Description:	Second step in sequence of SPs: intended to execute every day and update the activity and money of a user;
--												the list of Users is as defined in retentionlistUsers 																		
--              Input: date (starttime)
--                     systemid
--                     systemcode (website)
--              will create a list of RetentionListUsers with  RetentionListActivity 
-- =============================================

DECLARE @Query  nvarchar(MAX) = ''


	SELECT Name FROM [ETLR].[_PersonTrackCenter].sys.Views where name like '%memberlogin[_]%' and name like '%'+ LTRIM(RTRIM(@systemcode)) + '%'
	UNION ALL
	SELECT Name FROM [ETLR].[_PersonTrackCenter].sys.Views where name like '%memberSubTotal[_]%' and name like '%'+ LTRIM(RTRIM(@systemcode)) + '%'


	if  @@ROWCOUNT = 2 
	BEGIN

SELECT  @Query = 'INSERT INTO DataScientist.dbo.RetentionListActivity
				  SELECT agg.[type] , 
						 @SystemCode SystemCode , 
						 agg.memberid, 
						 @date activity, 
						 NULL d0,
						 NULL d1,
						 NULL d3,
						 NULL d5,
						 NULL d7,
						 NULL d14,
						 MAX(CASE WHEN @date >= Firstupdateday and @date <= lastupdateday THEN 1 ELSE 0 END) included,
						 0 dayssincelasttime,
						 MAX(TOT) walletamount
				  FROM
				  (
					SELECT memberid, 
						   max(subtotal) tot 
					FROM 
					[ETLR].[_PersonTrackCenter].dbo.VW_memberSubtotal_' + @SystemCode + '
					WHERE Date = @Date
					GROUP BY memberid
				  UNION ALL
				    SELECT memberid,
						   NULL tot 
					FROM
					[ETLR].[_PersonTrackCenter].dbo.VW_memberlogin_' + @SystemCode + '
					WHERE CONVERT(DATE,[logintime]) = @Date
				   ) Active JOIN
				   [DataScientist].dbo.RetentionlistUsers agg on 
					 active.memberid = agg.memberid AND
					 agg.Firstupdateday <= @Date AND
					 agg.maxactivityVector >= @Date AND
					 agg.systemCode = @SystemCode
					GROUP BY agg.memberid,
							 agg.[type]'

EXEC sp_executesql @Query, N'@Date DATE, @SystemCode NVARCHAR(10)', @Date, @SystemCode
	SELECT 'SUCCESS' successcriteria
	END
	ElSE
	BEGIN
		THROW 51000, 'Not all tables do exists.', 1;   
	END
END
