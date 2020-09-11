
labels<- c('leave7d0','leave7d1','leave7d3','leave7d5','leave7d7','leave7d14')

searchGridSubCol <- expand.grid(depth = c(10,12),
                                learning_rate = c(0.1,0.15,0.2), 
                                iterations = c(500,800),
                                l2_leaf_reg = c(1,3), #5
                                border_count = c(170,250))

need_FactorCol <- c('d14today','d14_0','d15_0','d14_1','d15_1','d14_3','d15_3','d14_5',
                    'd15_5','d14_7','d15_7','d14_14','d15_14','t2','t1','dom','dow',
                    #added on 4/22
                    'd15today','d24today','d25today','d24_0','d25_0','d24_1','d25_1',
                    'd24_3','d25_3','d24_5','d25_5','d24_7','d25_7','d24_14','d25_14',
                    'did_withdraw','d30orbiggerleave7d0','d30orbiggerleave7d1',
                    'd30orbiggerleave7d3','d30orbiggerleave7d5','d30orbiggerleave7d7',
                    'd30orbiggerleave7d14','m1leave7d0','m1leave7d1','m1leave7d3',
                    'm1leave7d5','m1leave7d7','m1leave7d14','id','avg_depositamount_decreasing',
                    'avg_deposittimes_decreasing','avg_withdrawamount_increasing')

##################################################################
## train
del_var <- c('creditdeposittimes','discountsum','otherssum','did_withdraw',
             'd15today','d25today','d30orbiggerleave7d1','d30orbiggerleave7d3',
             'd30orbiggerleave7d5','m1leave7d0','m1leave7d1','m1leave7d3',
             'm1leave7d5','d24today','d25today','d24_0','d25_0','d24_1','d25_1',
             'd24_3','d25_3','d24_5','d25_5','d24_7','d25_7','d24_14','d25_14','t2','t1',
             "absent3dayslastmonth", "biggestdifflastmonth", "absent7dayslastmonth",
             "interval" , "leavedate","logindays7d")

minus <- c(15)
DIFF <- 30
del <- c(2:7, 9:10, 18:25)

