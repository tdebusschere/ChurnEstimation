library(keyring)
library(RODBC)


#' For the RetentionList Process query website (systemcode; eg. DQ002), 
#' type (whale, middle/dolphin, ...) A certain daterange
#' This daterange is now 30 days (Should this become a parameter?)
#'
#' Author: Debusschere Tom; 1/30/2020
#' Version 0.2.0
#'
#' @param retentionlist_website: the systemcode of the website to analyze (DQ002 / FJ001)
#' @param retentionlist_type: what type of user (Whale, Dolphin, ...)
#' @param retentionlist_activity: the date that needs to be executed
#' @param retentionlist_version: sets if it is the test version or the production version
#' @return 
#' 
QueryData = function(retentionlist_website, retentionlist_type,
                     retentionlist_activity, retentionlist_version) {

  version_string <- paste0('Data', retentionlist_version)
  db             <- DB[[version_string]][['Database']]
  db_connection  <- ConnectToDB(db)

  UsersDB              <- DB[[version_string]][['Users']]
  ActivityDB           <- DB[[version_string]][['Activity']] 
  ActivityHistoryDB    <- DB[[version_string]][['ActivityHistory']]
  ActivityDatesDB      <- DB[[version_string]][['ActivityDates']]  
  WalletHistoryDB      <- DB[[version_string]][['WalletHistory']]  
  IPDB                 <- DB[[version_string]][['IP']]                
  BetRecordDB          <- DB[[version_string]][['BetRecord']]         
  FinancialPromotionDB <- DB[[version_string]][['FinancialPromotion']]
  FinancialLastMonthDB <- DB[[version_string]][['FinancialLastMonth']]
  FinancialHistoryDB   <- DB[[version_string]][['FinancialHistory']]

  retentionlist_activity = as.Date(retentionlist_activity)
  
  query <- paste0("SELECT  a.[type],
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
                   lastrawdatatype,
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
                   deposittimes,
                   creditdeposittimes,
                   thirdpartypaymenttimes,
                   depositsum,
                   creditdepositsum,
                   thirdpartypaymentsum,
                   depositmax,
                   withdrawtimes,
                   withdrawsum,
                   withdrawmax,
                   favorablesum,
                   discountsum,
                   otherssum ,
                   MemberDiscountsum,
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
                   yesterday_deposit,
                   yesterday_depositamt,
                   yesterday_withdraw,
                   yesterday_withdrawamt,
                   last_7d_deposit,
                   last_7d_depositamt,
                   last_7d_withdraw,
                   last_7d_withdraw_amt,
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
                   FROM ", ActivityDB, " a 
                   LEFT JOIN ", BetRecordDB, "  b
                   ON a.MemberId = b.memberid AND 
                      a.activity = b.activity AND
                      a.[type] = b.type AND
                      a.website = b.website
                   LEFT JOIN ", IPDB, " i 
                   ON i.memberid = a.MemberId AND
                      a.activity = i.activity AND
                      a.website = i.website AND
                      a.type = i.type
                   LEFT JOIN ", FinancialHistoryDB, " f
                   ON f.memberid = a.MemberId AND
                      a.activity = f.activity AND
                      a.website = f.website AND
                      a.type = f.type
				           LEFT JOIN ", FinancialLastMonthDB," lm
                   ON lm.memberid = a.MemberId AND
                      a.activity = lm.activity AND
                      a.website = lm.website AND
                      a.type = lm.type
                   LEFT JOIN ",WalletHistoryDB," wh 
                   ON a.MemberId = wh.memberid AND
                      a.activity = wh.activity AND 
                      a.website = wh.website AND
                      a.type = wh.type
				           LEFT JOIN ",ActivityDatesDB," d 
                   ON d.memberid = a.MemberId AND
                      d.activity = a.activity AND 
                      a.website = d.website AND 
                      a.type = d.type
                   LEFT JOIN ", FinancialPromotionDB," fp 
                   ON a.MemberId = fp.memberid AND 
                      a.activity = fp.activity AND
                      a.website = fp.website AND
                      a.type = fp.type
				           LEFT JOIN ", ActivityHistoryDB," h 
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
						         FROM ", UsersDB, "
				             GROUP BY memberid,[type] ) us
        				   ON a.MemberId = us.memberid AND 
        				      a.[type] = us.[type]
                   WHERE a.website = '", retentionlist_website, "' AND
                         a.included = 1 AND
                         hoursplayed > 0 AND
                         DATEDIFF(D,us.jointime,a.activity) >=30 AND
                         DATEDIFF(D,us.Firstdayplayed,a.activity) >= 30  AND 
						             a.activity >= '",retentionlist_activity - diff, "' AND
						             a.activity <= '",retentionlist_activity, "' AND
						             a.[Type] ='", retentionlist_type, "' 
                  ORDER BY a.memberid, a.activity ")  
  
  #sqlPrepare(con, query)
  activity_data <- sqlQuery(db_connection, query)
  return(activity_data)
}


