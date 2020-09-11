
USE [DSSkunkworks]
GO
/****** Object:  StoredProcedure [dbo].[RetentionListUpdateActivityHistory]    Script Date: 2020/1/9 上午 09:02:13 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].RetentionListUpdateActivityHistory  @date DATE
as



DECLARE @Date90ago DATE = DATEADD(d,-90,@Date)
DECLARE @Date30ago DATE = DATEADD(d,-30,@Date)

INSERT INTO RetentionListActivityHistory
SELECT Y.[type],
	   X.[Website],
	   X.MemberId,
	   @Date,
	   SUM( CASE WHEN dayssincelasttime >=3 THEN 1 ELSE 0 END ) absent3days,
	   SUM( CASE WHEN dayssincelasttime >=7 THEN 1 ELSE 0 END ) absent7days,
	   MAX(  dayssincelasttime  ) biggestdiff,
	   SUM( CASE WHEN activity > @Date30ago AND dayssincelasttime >=3 THEN 1 ELSE 0 END ) absent3dayslastmonth,
	   SUM( CASE WHEN activity > @Date30ago AND dayssincelasttime >=7 THEN 1 ELSE 0 END ) absent7dayslastmonth,
	   MAX( CASE WHEN activity > @Date30ago THEN dayssincelasttime ELSE 0 END  ) biggestdifflastmonth,
	   MAX( CASE WHEN Lms = 1 AND DATEDIFF(d,@Date,activity) >= 30 THEN leave7d0 ELSE 0 END) d30orbiggerleave7d0,
	   MAX( CASE WHEN Lms = 1 AND DATEDIFF(d,@Date,activity) >= 30 THEN leave7d1 ELSE 0 END) d30orbiggerleave7d1,
	   MAX( CASE WHEN Lms = 1 AND DATEDIFF(d,@Date,activity) >= 30 THEN leave7d3 ELSE 0 END) d30orbiggerleave7d3,
	   MAX( CASE WHEN Lms = 1 AND DATEDIFF(d,@Date,activity) >= 30 THEN leave7d5 ELSE 0 END) d30orbiggerleave7d5,
	   MAX( CASE WHEN Lms = 1 AND DATEDIFF(d,@Date,activity) >= 30 THEN leave7d7 ELSE 0 END) d30orbiggerleave7d7,
	   MAX( CASE WHEN Lms = 1 AND DATEDIFF(d,@Date,activity) >= 30 THEN leave7d14 ELSE 0 END) d30orbiggerleave7d14,
	   MAX( CASE WHEN DATEPART(MONTH,@Date) = DATEPART(MONTH,activity) + 1 AND DATEPART(DAY,@Date) = DATEPART(DAY,activity) THEN leave7d0 ELSE 0 END) m1leave7d0,
	   MAX( CASE WHEN DATEPART(MONTH,@Date) = DATEPART(MONTH,activity) + 1 AND DATEPART(DAY,@Date) = DATEPART(DAY,activity) THEN leave7d1 ELSE 0 END) m1leave7d1,
	   MAX( CASE WHEN DATEPART(MONTH,@Date) = DATEPART(MONTH,activity) + 1 AND DATEPART(DAY,@Date) = DATEPART(DAY,activity) THEN leave7d3 ELSE 0 END) m1leave7d3,
	   MAX( CASE WHEN DATEPART(MONTH,@Date) = DATEPART(MONTH,activity) + 1 AND DATEPART(DAY,@Date) = DATEPART(DAY,activity) THEN leave7d5 ELSE 0 END) m1leave7d5,
	   MAX( CASE WHEN DATEPART(MONTH,@Date) = DATEPART(MONTH,activity) + 1 AND DATEPART(DAY,@Date) = DATEPART(DAY,activity) THEN leave7d7 ELSE 0 END) m1leave7d7,
	   MAX( CASE WHEN DATEPART(MONTH,@Date) = DATEPART(MONTH,activity) + 1 AND DATEPART(DAY,@Date) = DATEPART(DAY,activity) THEN leave7d14 ELSE 0 END) m1leave7d14,
	   MAX( CASE WHEN RNV = 1 AND dayssincelasttime >= 7 THEN dayssincelasttime ELSE 0 END) Interval, 
	   MIN( CASE WHEN RNV = 1 AND dayssincelasttime >= 7 THEN DATEDIFF(d, @Date,DATEADD(d,dayssincelasttime,activity)) ELSE 90 END)  LeaveDate,
	   MAX( CASE WHEN DATEPART(MONTH,@Date) = DATEPART(MONTH,activity) + 1 AND DATEPART(DAY,@Date) = DATEPART(DAY,activity) THEN 1 ELSE 0 END) ActiveLastMonth,
	   0 LeaveDay,
	   MAX( CASE WHEN DATEPART(MONTH,@Date) = DATEPART(MONTH,activity) AND DATEPART(DAY,@Date) = DATEPART(DAY,activity) THEN 1 ELSE 0 END) ActiveLastWeek
	   FROM
	   (
		SELECT * , 
			   ROW_NUMBER() OVER( PARTITION BY MemberId, Website, [Type], 
				    		CASE WHEN dayssincelasttime >= 7 THEN 1 ELSE 0 END 
							ORDER BY activity DESC) RNV,
				ROW_NUMBER() OVER( PARTITION BY MemberId, Website, [Type],
							CASE WHEN DATEDIFF(d,@Date,activity) >= 30 THEN 1 ELSE 0 END
							ORDER BY Activity DESC) Lms
		FROM RetentionListActivity
		WHERE Activity > @Date90ago and Activity 《= @Date
		) X JOIN 
		(
		 SELECT MemberId,
				WebSite,
				[Type]
		 FROM RetentionListActivity 
		 WHERE activity = @Date AND Included =1
		 ) Y ON x.MemberId = y.MemberId AND
				x.website  = y.website
		GROUP BY X.Website,
				 X.memberid,
				 Y.[type]
