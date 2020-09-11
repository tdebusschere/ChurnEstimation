


 
      truncate table DataScientist.dbo.RetentionListActivity
	  insert into DataScientist.dbo.RetentionListActivity
	  SELECT type,website,Memberid,activity,
		      leave7d0,leave7d1,leave7d3,leave7d5,leave7d7,leave7d14,included,dayssincelasttime,walletamount      
	  FROM dbo.dRetentionListActivity

	  truncate table DataScientist.dbo.RetentionListUsers
	  insert into DataScientist.dbo.RetentionListUsers
	  SELECT Memberid,GameAccount,FirstUpdateDay,LastUpdateDay,MaxActivityVector,Type,SystemCode,JoinTime,
		     MemberLevelSettingId,DiscountSettingId,FirstDayPlayed   
	  FROM dbo.dRetentionListUsers
	  
	truncate table DataScientist.dbo.RetentionListBetrecord
	insert into DataScientist.dbo.RetentionListBetrecord
	SELECT type, website, activity, memberid, wagerscount, amountsum, amountsum7d, 
		   payoffsum, commissionablesum, hoursplayed, diffgamesplayed, lastperiodplayed,
		   lastrawdatatype,lastcode, payoff7d, wagerscount7d, payoff1d, wagerscount1d,
		   mostrawdatatype, mostcode, betamountrawdatatype, betamountcode, 
		   wagerscountrawdatatype, wagerscountcode, betswon, betswonlastday, timesincelastwin
	FROM dbo.dRetentionListBetrecord


	truncate table DataScientist.dbo.RetentionListFinancialLastMonth
	insert into DataScientist.dbo.RetentionListFinancialLastMonth
	SELECT memberid, website,type,activity,deposittimes,creditdeposittimes,
		  thirdpartypaymenttimes,depositsum,creditdepositsum,thirdpartypaymentsum,
		  withdrawtimes,withdrawsum,favorablesum,discountsum,otherssum,memberdiscountsum,
		  yesterday_depositamt,yesterday_deposit,yesterday_withdrawamt,yesterday_withdraw,
		  last_7d_depositamt,last_7d_deposit,last_7d_withdraw_amt,last_7d_withdraw
	FROM dbo.dRetentionListFinancialLastMonth
    


	truncate table DataScientist.dbo.RetentionListIP
	insert into DataScientist.dbo.RetentionListIP
	SELECT type,website,memberid,activity,logintimes,
		   logindays,logindays7d,time_since_last_time,
		   time_before,time_between
	FROM dbo.dRetentionListIP

	truncate table DataScientist.dbo.RetentionListFinancialHistory
	insert into DataScientist.dbo.RetentionListFinancialHistory
    select  memberid,activity,type,website,depositmax,last_depositamt,
			last_deposit,timesincelastdeposit,previous_depositamt,
			previous_deposit,previous_depositlag,datesdeposited,
			avg_time_between_deposit,avg_depositamt,avg_deposittimes,
			withdrawmax,last_withdrawamt,last_withdraw,timesincelastwithdraw,
			previous_withdrawamt,previous_withdraw,previous_withdrawlag,
			dateswithdraw,avg_time_between_withdraw,avg_withdrawamt,avg_withdrawtimes
	FROM dbo.dRetentionListFinancialHistory


	truncate table DataScientist.dbo.RetentionListFinancialPromotion
	insert into DataScientist.dbo.RetentionListFinancialPromotion
    select  type,website,memberid,activity,avg_depositamount_decreasing,
	avg_deposittimes_decreasing,avg_withdrawamount_increasing,avg_depositamt_over_1000,discount
	FROM dbo.dRetentionListFinancialPromotion
  

  truncate table DataScientist.dbo.RetentionListActivityDates
  INSERT INTO DataScientist.dbo.RetentionListActivityDates
  SELECT type,website, memberid, activity, included, d14today, d15today,d24today, d25today,
         d14_0, d15_0, d24_0, d25_0, d14_1, d15_1, d24_1, d25_1, d14_3, d15_3, d24_3, d25_3,
		 d14_5, d15_5, d24_5, d25_5, d14_7, d15_7, d24_7, d25_7, d14_14, d15_14, d24_14, d25_14
  FROM dbo.dRetentionListActivityDates

  TRUNCATE TABLE datascientist.dbo.retentionlistactivityhistory
   INSERT INTO DataScientist.dbo.retentionlistactivityhistory
    SELECT type, website, memberid, activity, include, absent3days, absent7days, biggestdiff,
         absent3dayslastmonth, absent7dayslastmonth, biggestdifflastmonth, d30orbiggerleave7d0,
		 d30orbiggerleave7d1, d30orbiggerleave7d3, d30orbiggerleave7d5, d30orbiggerleave7d7, d30orbiggerleave7d14,
		 m1leave7d0,m1leave7d1,m1leave7d3, m1leave7d5, m1leave7d7, m1leave7d14, 
		 interval, leavedate, lastmonthactive, leaveday, lastweekactive
  FROM dbo.dretentionlistactivityhistory

  TRUNCATE TABLE datascientist.dbo.retentionlistwallethistory
  INSERT INTO datascientist.dbo.RetentionListWalletHistory
  select website,type,activity, memberid, walletamount, walletamt1d,
		 walletamt2d, walletamt3d, walletamt4d, walletamt5d,walletamt6d, walletamt7d
FROM dbo.dretentionlistwallethistory

