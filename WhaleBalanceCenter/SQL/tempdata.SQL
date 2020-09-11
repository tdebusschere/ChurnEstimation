SELECT  a.[type],
                   a.website, 
                   a.memberid,
                   a.activity, 
                   leave7d0, 
                   leave7d1, 
                   leave7d3, 
                   leave7d5, 
                   leave7d7, 
                   leave7d14 , 
                   dayssincelasttime days_since_last_activity,
                   memberlevelsettingid,
                   wagerscount,
                   ISNULL(amountsum,0.0) amountsum ,
                   payoffsum,
                   commissionablesum ,
                   hoursplayed,
                   diffgamesplayed,
                   lastperiodplayed,
                   CASE WHEN lastrawdatatype IS NULL THEN 0 ELSE lastrawdatatype END lastrawdatatype,
                   lastcode,
                   mostrawdatatype,
                   mostcode,
                   betamountrawdatatype, 
                   betamountcode, 
                   wagerscountrawdatatype, 
                   wagerscountcode,
                   wh.walletamount,
                   DATEDIFF(d,us.jointime,a.activity)+1 jointime,
				           DATEDIFF(d,us.Firstdayplayed,a.activity) first_day_played,
                   logintimes,
                   logindays,
                   time_since_last_time, 
                   time_before,
                   time_between,
                   CASE WHEN deposittimes IS NULL THEN 0 ELSE deposittimes END deposittimes,
                   CASE WHEN creditdeposittimes IS NULL THEN 0 ELSE creditdeposittimes END creditdeposittimes,
                   CASE WHEN thirdpartypaymenttimes IS NULL THEN 0 ELSE thirdpartypaymenttimes END thirdpartypaymenttimes,
                   CASE WHEN depositsum IS NULL THEN 0 ELSE depositsum END depositsum,
                   CASE WHEN creditdepositsum IS NULL THEN 0 ELSE creditdepositsum END creditdepositsum,
                   CASE WHEN thirdpartypaymentsum IS NULL THEN 0 ELSE thirdpartypaymentsum END thirdpartypaymentsum,
                   depositmax,
                   CASE WHEN withdrawtimes IS NULL THEN 0 ELSE withdrawtimes END withdrawtimes,
                   CASE WHEN withdrawsum IS NULL THEN 0 ELSE withdrawsum END withdrawsum,
                   withdrawmax,
                   CASE WHEN favorablesum IS NULL THEN 0 ELSE favorablesum END favorablesum,
                   CASE WHEN discountsum IS NULL THEN 0 ELSE discountsum END discountsum,
                   CASE WHEN otherssum IS NULL THEN 0 ELSE otherssum END otherssum ,
                   CASE WHEN MemberDiscountsum IS NULL THEN 0 ELSE MemberDiscountsum END MemberDiscountsum,
                   last_deposit,
                   previous_deposit,
                   previous_depositamt,
                   previous_depositlag,
                   datesdeposited,
                   avg_time_between_deposit,
                   avg_depositamt,
                   avg_deposittimes,
                   Last_depositamt,
                   last_withdraw ,
                   last_withdrawamt,
                   previous_withdraw,
                   previous_withdrawamt,
                   previous_withdrawlag, 
                   dateswithdraw,
                   avg_time_between_withdraw,
                   avg_withdrawamt,
                   avg_withdrawtimes,
                   CASE WHEN yesterday_deposit IS NULL THEN 0 ELSE yesterday_deposit END yesterday_deposit,
                   CASE WHEN yesterday_depositamt IS NULL THEN 0 ELSE yesterday_depositamt END yesterday_depositamt,
                   CASE WHEN yesterday_withdraw IS NULL THEN 0 ELSE yesterday_withdraw END yesterday_withdraw,
                   CASE WHEN yesterday_withdrawamt IS NULL THEN 0 ELSE yesterday_withdrawamt END yesterday_withdrawamt,
                   CASE WHEN last_7d_deposit IS NULL THEN 0 ELSE last_7d_deposit END last_7d_deposit,
                   CASE WHEN last_7d_depositamt IS NULL THEN 0 ELSE last_7d_depositamt END last_7d_depositamt,
                   CASE WHEN last_7d_withdraw IS NULL THEN 0 ELSE last_7d_withdraw END last_7d_withdraw,
                   CASE WHEN last_7d_withdraw_amt IS NULL THEN 0 ELSE last_7d_withdraw_amt END last_7d_withdraw_amt,
                   timesincelastdeposit,
                   timesincelastwithdraw,
                   CASE WHEN dateswithdraw > 0 THEN 1 ELSE 0 END did_withdraw,
                   absent3days,
                   absent7days,
                   biggestdiff, 
                   absent3dayslastmonth,
                   absent7dayslastmonth,
                   biggestdifflastmonth,
                   interval,
                   leavedate,
                   payoff7d,
                   wagerscount1d
                   wagerscount7d,
                   payoff1d, 
                   wagerscount1d,
                   DATEPART(dw,a.activity) dow, 
                   DATEPART(dd, a.activity) dom,
                   d14today, 
                   d15today, 
                   d24today, 
                   d25today, 
                   d14_0, 
                   d14_1, 
                   d14_3, 
                   d14_5, 
                   d14_7, 
                   d14_14, 
                   d15_0, 
                   d15_1, 
                   d15_3, 
                   d15_5, 
                   d15_7, 
                   d15_14,
                   d24_0, 
                   d24_1,
                   d24_3, 
                   d24_5, 
                   d24_7, 
                   d24_14, 
                   d25_0, 
                   d25_1, 
                   d25_3, 
                   d25_5, 
                   d25_7, 
                   d25_14, 
                   leaveday, 
                   walletamt1d,
                   walletamt2d, 
                   walletamt3d, 
                   walletamt4d, 
                   walletamt5d, 
                   walletamt6d, 
                   walletamt7d, 
                   d30orbiggerleave7d0, 
                   d30orbiggerleave7d1,
                   d30orbiggerleave7d3, 
                   d30orbiggerleave7d5, 
                   d30orbiggerleave7d7, 
                   d30orbiggerleave7d14, 
                   m1leave7d0, 
                   m1leave7d1, 
                   m1leave7d3,
                   m1leave7d5, 
                   m1leave7d7, 
                   m1leave7d14, 
                   amountsum7d, 
                   logindays7d, 
                   avg_depositamount_decreasing,
                   avg_deposittimes_decreasing, 
                   avg_withdrawamount_increasing, 
                   avg_depositamt_over_1000,
                   discount
                   FROM [Datascientist].[dbo].[RetentionListActivity] a 
                   LEFT JOIN [Datascientist].dbo.RetentionListBetrecord  b
                   ON a.MemberId = b.memberid AND 
                      a.activity = b.activity AND
                      a.[type] = b.type AND
                      a.website = b.website
                   LEFT JOIN [Datascientist].dbo.RetentionListIP i 
                   ON i.memberid = a.MemberId AND
                      a.activity = i.activity AND
                      a.website = i.website AND
                      a.type = i.type
                   LEFT JOIN [Datascientist].dbo.RetentionListFinancialHistory f
                   ON f.memberid = a.MemberId AND
                      a.activity = f.activity AND
                      a.website = f.website AND
                      a.type = f.type
				           LEFT JOIN [Datascientist].dbo.RetentionListFinancialLastMonth lm
                   ON lm.memberid = a.MemberId AND
                      a.activity = lm.activity AND
                      a.website = lm.website AND
                      a.type = lm.type
                   LEFT JOIN [Datascientist].dbo.RetentionListWalletHistory  wh 
                   ON a.MemberId = wh.memberid AND
                      a.activity = wh.activity AND 
                      a.website = wh.website AND
                      a.type = wh.type
				           LEFT JOIN [Datascientist].dbo.RetentionListActivityDates d 
                   ON d.memberid = a.MemberId AND
                      d.activity = a.activity AND 
                      a.website = d.website AND 
                      a.type = d.type
                   LEFT JOIN [Datascientist].dbo.RetentionListFinancialPromotion fp 
                   ON a.MemberId = fp.memberid AND 
                      a.activity = fp.activity AND
                      a.website = fp.website AND
                      a.type = fp.type
				           LEFT JOIN [Datascientist].dbo.RetentionListActivityHistory h 
                   ON h.memberid = a.MemberId AND
                      h.activity = a.activity AND
                      a.type = h.type AND
                      a.website = h.website 
				           LEFT JOIN 
				           ( SELECT MIN(jointime) jointime,
						                MIN(firstdayplayed) firstdayplayed,
						                MAX(memberlevelsettingid) Memberlevelsettingid,
						                memberid,
						                [type] 
						         FROM [Datascientist].[dbo].[RetentionListUsers] 
				             GROUP BY memberid,[type] ) us
        				   ON a.MemberId = us.memberid AND 
        				      a.[type] = us.[type]
                   WHERE a.website = 'DQ002' AND
                         a.included = 1 AND
                         hoursplayed > 0 AND
                         DATEDIFF(D,us.jointime,a.activity) >=30 AND
                         DATEDIFF(D,us.Firstdayplayed,a.activity) >=30  AND 
						             a.activity >= '2019-11-01' AND
						             a.activity <= '2020-01-29' AND
						             a.[Type] ='whale' 
                  ORDER BY a.memberid, a.activity