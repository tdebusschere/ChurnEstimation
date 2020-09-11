SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[RetentionListProcessResult]
	@Date DATE,
	@Website NVARCHAR(10),
	@Systemint INT

AS
BEGIN

	SELECT Name FROM [ETLR].[_PersonTrackCenter].sys.Views where name like '%MemberGameAccount[_]%' and name like '%'+ LTRIM(RTRIM(@Website)) + '%'
	DECLARE @Lastmonth Date = dateadd(d,-30, @Date)

	if  @@ROWCOUNT = 1
	BEGIN
	
	DELETE FROM ResultPool.dbo.retentionlistResult 
	WHERE createdate = @date AND
	      Systemcode = @Website 

	DROP TABLE if EXISTS ##tmp_deposit;

	SELECT MemberId,  
		   website, 
		   sum(DepositAmt) DepositAmt, 
		   sum(DepositTimes) DepositTimes, 
		   sum(WithdrawAmt) WithdrawAmt
	into ##tmp_deposit
	FROM DataScientist.dbo.FinancialDailyAggregate RFLM 
	Where [day] <= @date AND 
	      [day] > @lastmonth AND
		  website = @website
	GROUP BY MemberId, website

DECLARE @Query NVARCHAR(MAX)

SELECT @Query ='
	INSERT INTO ResultPool.dbo.retentionlistResult
	SELECT DSS.systemcode, 
		   DSS.memberid,
		   leaveday LeaveDay,
		   executeday,
	       CommissionableTotal,
	       isnull(DepositAmt,0) AS DepositSum, 
	       isnull(DepositTimes,0) AS DepositTimes,
	       isnull(WithdrawAmt,0) AS WithdrawSum,
	       CommisisonableFish,
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
      FROM DataScientist.dbo.EstimationResults DSS 
	  LEFT JOIN [ETLR].[_PersonTrackCenter].dbo.[VW_MemberGameAccount_' + @Website + '] dq
	  ON DSS.memberid = dq.memberid AND Diffgameaccount = 0
	  LEFT JOIN
	  ##tmp_deposit RFLM
	  ON dss.memberid = RFLM.memberid and RFLM.website = @website	
	  JOIN DataScientist.dbo.DS_MonthlyTop10Commissionable X 
	  on X.gameaccount = dq.gameaccount AND
	  X.siteid = @Siteid AND 
	  X.UpdateDate = @activity AND 
	  DSS.ExecuteDay = @activity '


EXEC sp_executesql @Query, N'@Activity DATE, @Website NVARCHAR(10) , @Siteid INT',
							 @Date, @website, @Systemint


	  DECLARE @rowct int 
	  select @rowct = count(*) FROM ResultPool.dbo.retentionlistResult
						where systemcode = @website AND CreateDate = @Date
	  SELECT @rowct

      SELECT @rowct
	  if @rowct > 0
	  BEGIN
		UPDATE ResultPool.dbo.retentionlistResultStatus 
		SET executiontime = GETDATE(), 
		    processstatus = 1
	    WHERE systemcode = @Website and Date = @date
	   END
	   IF @rowct = 0
	   BEGIN
		UPDATE ResultPool.dbo.retentionlistResultStatus 
		SET executiontime = GETDATE(), 
		    processstatus = -1
	    WHERE systemcode = @Website and Date = @date		
	   END

    DROP TABLE if exists ##tmp_deposit

	SELECT 'SUCCESS' successcriteria
	END
	ElSE
	BEGIN
		THROW 51000, 'Not all tables do exists.', 1;   
	END

END
