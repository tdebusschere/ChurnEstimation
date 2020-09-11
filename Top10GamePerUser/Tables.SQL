DROP TABLE IF EXISTS DS_MonthlyTop10Commissionable

CREATE TABLE DS_MonthlyTop10Commissionable
(
	GameAccount NVARCHAR(20),
	SiteId INT,
    UpdateDate DATE,
	CommisisonableFish FLOAT,
	CommissionableLottery FLOAT,
	CommissionableBoard FLOAT,
	CommissionableReal FLOAT,
	CommissionableSlots FLOAT,
	CommissionableSports FLOAT,
	CommissionableTotal FLOAT,
	Game1 INT,
	Game2 INT,
	Game3 INT,
	Game4 INT,
	Game5 INT,
	Game6 INT,
	Game7 INT,
	Game8 INT,
	Game9 INT,
	Game10 INT,
	Game1CommissionableSum FLOAT,
	Game2CommissionableSum FLOAT,
	Game3CommissionableSum FLOAT,
	Game4CommissionableSum FLOAT,
	Game5CommissionableSum FLOAT,
	Game6CommissionableSum FLOAT,
	Game7CommissionableSum FLOAT,
	Game8CommissionableSum FLOAT,
	Game9CommissionableSum FLOAT,
	Game10CommissionableSum FLOAT
)

CREATE INDEX DS_MonthlyTop10CommissionableIndex 
       ON DS_MonthlyTop10Commissionable 
       (
        Updatedate,
        SiteId,
        GameAccount
       ) 
