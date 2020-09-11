USE [DataScientist]
GO
/****** Object:  StoredProcedure [dbo].[RetentionListCollectIP]    Script Date: 2020/1/15 下午 01:27:29 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[RetentionListCollectIP]

	@InputDate DATE,
	@SystemCode NVARCHAR(10),
	@Systemint INT

AS
BEGIN

/* 90 days vs 30 days compare mean*/
DECLARE @Range    DATE = DATEADD(d,-90, @InputDate),
		@Range7d  DATE = DATEADD(d,-6 , @InputDate),
		@Range30d DATE = DATEADD(d,-30, @InputDate),
		@Query    NVARCHAR(MAX)


SELECT Name FROM [ETLR].[_PersonTrackCenter].sys.Views where name like '%memberlogin[_]%' and name like '%'+ LTRIM(RTRIM(@systemcode)) + '%'
UNION ALL
SELECT Name FROM [ETLR].[_PersonTrackCenter].sys.Views where name like '%memberSubTotal[_]%' and name like '%'+ LTRIM(RTRIM(@systemcode)) + '%'

if  @@ROWCOUNT = 2 
BEGIN

SELECT @Query = 
	' INSERT INTO DataScientist.dbo.RetentionListIP
	  SELECT [Type],
	         Website, 
			 MemberId, 
			 @Date Activity,
			 SUM(CASE WHEN [LoginTime] < @Date AND [LoginTime] >= @Range30d THEN 1 ELSE 0 END ) AS [LoginTimes], 
			 0 AS LoginDays, 
			 0 AS LoginDays7d,
			 DATEDIFF(MINUTE,MAX([LoginTime]), @Date) TimeSinceLastTime, 
			 MAX( CASE WHEN rnk = 2 THEN diff ELSE 0 END) TimeBefore,
			 AVG(diff) TimeBetween
	  FROM 
	  (
		SELECT a.MemberId, 
			   [Type],
			   Website, 
			   [LoginTime], 
			   LEAD([LoginTime], 1, [LoginTime]) OVER (partition by ml.MemberID ORDER BY [LoginTime]) AS TIMES ,
			   DATEDIFF(MINUTE, [LoginTime], LEAD([LoginTime], 1, [LoginTime]) OVER ( PARTITION BY ml.MemberID ORDER BY [LoginTime])) AS DIFF,
			   ROW_NUMBER() OVER( PARTITION BY ml.memberid ORDER BY [LoginTime] DESC) RNK
		FROM [ETLR].[_PersonTrackCenter].dbo.[VW_MemberLogin_' + @SystemCode + '] ml 
		RIGHT JOIN 
		(
		SELECT MemberID, 
			   Website, 
			   [Type]
		FROM DataScientist.dbo.RetentionListActivity 
		WHERE @Date = Activity AND 
		      Website = @SystemCode AND 
			  Included = 1
		) a 
		ON a.MemberID = ml.MemberId AND
		   [LoginTime] <= @Date AND
		   [LoginTime] > @Range
	   )T
	   GROUP BY MemberID,
				WebSite,
				[Type]'

	   	EXEC sp_executesql @Query, N'@Range DATE, @Range30d DATE,
									 @Date DATE, @SystemCode NVARCHAR(10)', 
									 @Range7d, @Range30d, 
									 @inputDate, @SystemCode


--CTE
/**can be combined with SP2; or moved to JG**/

SELECT @Query = 'UPDATE DIP SET
				 LoginDays = Days30dLogin,
				 LoginDays7d = Days7dLogin
				 FROM
				 (
				 SELECT SUM( CASE WHEN [DATE] >=  @Range7d THEN 1 ELSE 0 END) Days7dLogin,
						SUM( CASE WHEN [DATE] >= @Range30d THEN 1 ELSE 0 END) Days30dLogin, 
						MemberID,
						@SystemCode website
				 FROM
				 (
				  SELECT DISTINCT agg.memberid,  
								  DATE
				  FROM
				  (
					SELECT memberid, 
						   DATE 
					FROM 
					[ETLR].[_PersonTrackCenter].dbo.VW_memberSubtotal_' + @SystemCode + '
					WHERE Date > @Range30d
					GROUP BY MemberId, DATE
				  UNION ALL
				    SELECT memberid, 
							CONVERT(DATE,[logintime])
					FROM
					[ETLR].[_PersonTrackCenter].dbo.VW_memberlogin_' + @SystemCode + '
					WHERE CONVERT(DATE,[logintime]) > @Range30d AND 
						  CONVERT(DATE,[logintime]) <= @inputdate
					GROUP BY Memberid, CONVERT(DATE,[logintime])
				   ) Active JOIN
				   DataScientist.dbo.RetentionListIP agg on 
					 active.memberid = agg.memberid AND
					 agg.website = @SystemCode AND
					 agg.activity = @Date
				  ) x 
				  GROUP BY MemberId
				  ) fin JOIN
				  DataScientist.dbo.RetentionListIP DIP on 
				  DIP.memberid = fin.memberid AND
				  DIP.website = fin.website AND
				  DIP.activity = @Date
				  '

	   	EXEC sp_executesql @Query, N'@Range7d DATE, @Range30d DATE,
									 @Date DATE, @SystemCode NVARCHAR(10)', 
									 @Range7d, @Range30d, 
									 @inputDate, @SystemCode

		UPDATE DataScientist.dbo.RetentionListIP
		SET Time_Since_Last_Time = 0 
		WHERE Time_Since_Last_Time IS NULL

		UPDATE DataScientist.dbo.RetentionListIP
		SET Time_Between = 0 
		WHERE Time_Between IS NULL

		SELECT 'SUCCESS' successcriteria
END
ElSE
BEGIN
		THROW 51000, 'Not all tables do exists.', 1;   
END

END
