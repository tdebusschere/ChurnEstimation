DELETE FROM DATASCIENTIST.dbo.DS_RetentionListBetrecordDailySummary


DECLARE @StartDate Date = '2019-12-01'
WHILE   @Startdate < '2020-01-10'
BEGIN 
	EXEC dbo.DS_RetentionListAggregateBalanceCenterSummarize @STARTDATE=@StartDate
	SELECT @StartDate = DATEADD(d,1,@StartDate)
END



EXEC dbo.CollectMonthlyTop10GamesCommissionable @Enddate=@StartDate
EXEC dbo.AggregateFinancialWebsite @Date=@StartDate, @systemcode='DQ002'
EXEC dbo.AggregateFinancialWebsite @Date=@StartDate, @systemcode='FJ001'

DECLARE @Date	 DATE  = '2019-12-08'
DECLARE @EndDate DATE  = '2019-12-15'

WHILE   @Date <  @EndDate
BEGIN
	IF DATEDIFF(d,'2019-12-08',@Date) % 14 = 0
	BEGIN
		EXEC RetentionListUpdateUserList @InputDate=@Date,@SystemCode='DQ002',@SystemInt=210
		EXEC RetentionListUpdateUserList @InputDate=@Date,@SystemCode='FJ001',@SystemInt=224
	END

	/*Activity*/
	EXEC dbo.RetentionListCollectActivity @Date=@Date,@SystemCode='DQ002',@SystemInt=210
	EXEC dbo.RetentionListCollectActivity @Date=@Date,@SystemCode='FJ001',@SystemInt=224

	/*Financial*/
	EXEC dbo.RetentionListCollectFinancialHistory @Date=@Date,@Website='DQ002',@SystemInt=210
	EXEC dbo.RetentionListCollectFinancialHistory @Date=@Date,@website='FJ001',@SystemInt=224

	EXEC dbo.RetentionListCollectFinancialLastMonth @InputDate=@Date,@SystemCode='DQ002',@SystemInt=210
	EXEC dbo.RetentionListCollectFinancialLastMonth @InputDate=@Date,@SystemCode='FJ001',@SystemInt=224

	EXEC dbo.RetentionListCollectFinancialPromotion @Date=@Date,@Website='DQ002'
	EXEC dbo.RetentionListCollectFinancialPromotion @Date=@Date,@Website='FJ001'

	EXEC dbo.RetentionListCollectIP @Date=@Date,@SystemCode='DQ002',@SystemInt=210
	EXEC dbo.RetentionListCollectIP @Date=@Date,@SystemCode='FJ001',@SystemInt=224

	EXEC dbo.RetentionListCollectBetrecord @Date=@Date,@SystemCode='DQ002',@SystemInt=210
	EXEC dbo.RetentionListCollectBetrecord @Date=@Date,@SystemCode='FJ001',@SystemInt=224

	EXEC dbo.RetentionListUpdateFinancialPromotion @Date=@Date,@Website='DQ002'
	EXEC dbo.RetentionListUpdateFinancialPromotion @Date=@Date,@Website='FJ001'

	EXEC dbo.RetentionListUpdateActivity @Date=@Date,@Website='DQ002'
	EXEC dbo.RetentionListUpdateActivity @Date=@Date,@Website='FJ001'


	EXEC dbo.RetentionListUpdateActivityHistory @Date=@Date
	EXEC dbo.RetentionListCollectWalletHistory  @Date=@Date
	EXEC dbo.RetentionListUpdateActivityDates   @Date=@Date

	SELECT Dateadd(d,1,@Date)
END


DELETE FROM RetentionListUsers WHERE FirstUpdateDay >= '2019-12-08'

DELETE FROM RetentionListActivity  WHERE Activity >= '2019-12-08'
DELETE FROM RetentionListBetrecord WHERE Activity >= '2019-12-08'
DELETE FROM RetentionListIp        WHERE Activity >= '2019-12-08'

DELETE FROM RetentionListFinancialHistory   WHERE Activity >= '2019-12-08'
DELETE FROM RetentionListFinancialLastMonth WHERE Activity >= '2019-12-08'
DELETE FROM RetentionListFinancialPromotion WHERE Activity >= '2019-12-08'

DELETE FROM RetentionListActivityDates      WHERE Activity >= '2019-12-08'
DELETE FROM RetentionListActivityHistory    WHERE Activity >= '2019-12-08'
DELETE FROM RetentionListWalletHistory      WHERE Activity >= '2019-12-08'