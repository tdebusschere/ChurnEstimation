USE [DataScientist]
GO
/****** Object:  StoredProcedure [dbo].[RetentionListUpdateUserList]    Script Date: 2020/1/14 下午 02:40:54 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[RetentionListUpdateUserList]

@Date DATE,
@SystemCode NVARCHAR(20),
@SystemInt int

AS
BEGIN

SET NOCOUNT ON;
-- =============================================
-- Author:		Tom
-- Create date: 2020-01-07
-- Description:	first step in sequence of SPs: intended to execute every 14 days and create a list of users that we need to query on a daily basis;
--                                             will also insert the personal information of the user (memberid, usertype)              
--              Input: date (starttime)
--                     type (whale / middle)
--                     systemcode (website)
--              will create a list of RetentionListUsers with  RetentionListActivity 
-- =============================================


DROP TABLE IF EXISTS ##financialtotal
DROP TABLE IF EXISTS ##lastwalletstatus


DECLARE @Depositsum Decimal(24,8),
		@LowerLimitShrimp DECIMAL(24,8) = 1.0,
		@UpperLimitShrimp DECIMAL(24,8) = 0.975,
		@LowerLimitMiddle DECIMAL(24,8) = 0.975,
		@UpperLimitMiddle DECIMAL(24,8) = 0.8,
		@LowerLimitWhale  DECIMAL(24,8) = 0.8,
		@UpperLimitWhale  DECIMAL(24,8) = 0.0,
		@Query			  NVARCHAR(Max) = '',
		@LastUpdateDay     DATE = (SELECT DATEADD(d,14,@Date)),
		@MaxActivityVector DATE = (SELECT DATEADD(d,54,@Date)),
		@months3ago        DATE = (SELECT DATEADD(d,-90,@Date)),
		@days30ago		   DATE = (SELECT DATEADD(d,-30,@Date))

DECLARE @Counter table (Name nvarchar(30))


CREATE TABLE ##financialtotal 
(
	totamount FLOAT,
	memberid  INT,
	gameaccount NVARCHAR(30),
	MemberLevelSettingId INT,
	DiscountSettingId INT,
	FirstBetTime DATE,
	JoinTime DateTime
);
	
	INSERT INTO @Counter
	SELECT Name FROM [ETLR].[_PersonTrackCenter].sys.Views where name like '%Membertransaction[_]%' and name like '%'+ LTRIM(RTRIM(@systemcode)) + '%'
	UNION ALL
	SELECT Name FROM [ETLR].[_PersonTrackCenter].sys.Views where name like '%membergameaccount[_]%' and name like '%'+ LTRIM(RTRIM(@systemcode)) + '%'
	UNION ALL
	SELECT Name FROM [ETLR].[_PersonTrackCenter].sys.Views where name like '%member[_]%' and name like '%'+ LTRIM(RTRIM(@systemcode)) + '%'
	UNION ALL
	SELECT Name FROM [ETLR].[_PersonTrackCenter].sys.Views where name like '%MemberSubTotal[_]%' and name like '%'+ LTRIM(RTRIM(@systemcode)) + '%'

	if  @@ROWCOUNT = 4 
	BEGIN

SELECT @Query ='	INSERT INTO ##financialtotal
					SELECT SUM(amount) totamount, 
						   mtq.memberid,
						   min(Gameaccount) GameAccount,
						   max(MemberLevelSettingId) MemberLevelSettingId,
						   max(DiscountSettingId) DiscountSettingid,
						   min(FirstBetTime) FIrstBettime,
						   min(JoinTime) JoinTime
					FROM [ETLR].[_PersonTrackCenter].dbo.VW_membertransaction_' + @SystemCode + ' Mtq 
					LEFT JOIN [ETLR].[_PersonTrackCenter].dbo.VW_membergameaccount_' + @SystemCode +  ' mq
					ON mtq.memberid = mq.memberid
					LEFT JOIN [ETLR].[_PersonTrackCenter].dbo.VW_Member_' + @SystemCode + ' me
					ON mtq.memberid = me.memberid
					WHERE [DateTime] > @months3ago AND 
						  [Datetime] < @Date AND
						  ( [TYPE] in (1,2,3,4) OR ([Type] =99 AND typeformember = ''其他'')) AND 
						  income = 1 AND
						  Diffgameaccount = 0
					GROUP BY mtq.memberid
							 '

	EXEC sp_executesql @query, N'@months3ago DATE, @Date DATE', @months3ago, @Date
	

SELECT @Depositsum = SUM(totamount) FROM ##financialtotal
SELECT @LowerLimitShrimp  = 1.0 * @DepositSum, 
	   @UpperLimitShrimp  = 0.975 * @DepositSum,
	   @LowerLimitMiddle  = 0.975 * @DepositSum,
	   @UpperLimitMiddle  = 0.8   * @DepositSum,
	   @LowerLimitWhale   = 0.8   * @DepositSum,
	   @UpperLimitWhale   = 0.0

----using upsert is preferrable to using a temp table
----write the userdata into userlist if it's a new user; otherwise update the old userdata


MERGE INTO [Datascientist].dbo.[RetentionListUsers] RLU
USING 
( 
	SELECT MEMBERID,
		   GameAccount,
		   @Date AS FirstUpdateDay,
		   @LastUpdateDay AS LastUpdateDay,
		   @MaxActivityVector AS MaxActivityVector,
		   UserCategorization AS [Type],
		   @SystemCode AS SystemCode,
		   JoinTime,
		   MemberLevelSettingId,
		   DiscountSettingId,
		   FirstBetTime as Firstdayplayed
	FROM
	(
		SELECT  MemberId, 
				GameAccount,
				CASE WHEN SUM(totamount) OVER (ORDER BY totamount DESC ROWS UNBOUNDED PRECEDING )  < @lowerlimitwhale THEN 'Whale'
					 WHEN SUM(totamount) OVER (ORDER BY totamount DESC ROWS UNBOUNDED PRECEDING )  >= @UpperLimitMiddle and 
						  SUM(totamount) OVER (ORDER BY totamount DESC ROWS UNBOUNDED PRECEDING )  < @LowerLimitMiddle	THEN 'Dolphin'
					 WHEN SUM(totamount) OVER (ORDER BY totamount DESC ROWS UNBOUNDED PRECEDING )  >= @UpperLimitShrimp THEN 'Shrimp' 
				END UserCategorization,
				Memberlevelsettingid,
				DiscountSettingId,
				Firstbettime,
				JoinTime	
		FROM ##financialtotal  ft  
  	) AS TT
)MTQRS
ON MTQRS.MEMBERID = RLU.MEMBERID AND 
   MTQRS.[Type] = RLU.[Type] AND 
   MTQRS.[SystemCode] = RLU.[SystemCode] 
WHEN  MATCHED THEN 
	UPDATE SET RLU.FirstUpdateDay      = MTQRS.FirstUpdateDay,
			   RLU.LastUpdateDay        = MTQRS.LastUpdateDay,
		       RLU.MaxActivityVector    = MTQRS.MaxActivityVector,
			   RLU.memberlevelsettingid = MTQRS.memberlevelsettingid,
			   RLU.DiscountSettingId    = MTQRS.discountsettingid

WHEN  NOT MATCHED THEN 
	INSERT VALUES (
	    MemberId, 
		GameAccount,
		FirstUpdateDay,
		LastUpdateDay, 
		MaxActivityVector, 
		[Type], 
		SystemCode,
		JoinTime,
		MemberLevelSettingId,
		DiscountSettingId,
		FirstDayPlayed
	);

	SELECT @Query =   '	SELECT DISTINCT
								RLU.[Type],		
								ER.Memberid,
								ISNULL(FIRST_VALUE(Subtotal) OVER( Partition by er.memberid ORDER BY Date DESC),0) walletamount,
								ISNULL(FIRST_VALUE(Date) OVER( Partition by er.memberid ORDER BY Date DESC),0) activity
						into ##lastwalletstatus
						FROM [Datascientist].dbo.[RetentionListUsers] RLU 
						join 
						[ETLR].[_PersonTrackCenter].dbo.VW_MemberSubTotal_' + @SystemCode + ' ER
						on RLU.MemberId = ER.memberid AND 
						   RLU.FirstUpdateDay = @Date AND
						   ER.[Date] >= @days30ago AND
						   ER.[Date] <  @Date AND
						   RLU.[SystemCode] = @SystemCode'


	EXEC sp_executesql @Query, N'@days30ago DATE, @Date DATE, @SystemCode NVARCHAR(10)', @days30ago, @Date, @SystemCode


	--INSERT INTO DataScientist.dbo.RetentionListActivity
	SELECT    
		   [Type],
		   @SystemCode,
		   Memberid, 
		   activity,    
		   0 AS D0, 
		   0 AS D1,
		   0 AS D3,
		   0 AS D5,
		   0 AS D7,
		   0 AS D14,
		   0 AS Included,
		   0 As DaysSinceLasttime,
		   WalletAmount
	FROM 
	##lastwalletstatus


END
ELSE
BEGIN
	THROW 51000, 'Not all tables do exists.', 1;   
END
END