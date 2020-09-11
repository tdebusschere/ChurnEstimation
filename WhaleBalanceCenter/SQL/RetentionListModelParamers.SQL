/* Model Settings For DQ002*/


DROP TABLE IF EXISTS DataScientist.dbo.RetentionListModelParameters

CREATE TABLE DataScientist.dbo.RetentionListModelParameters
(
	SystemCode    NVARCHAR(10),
	Type          NVARCHAR(15),
	Iterations    INT,
	Depth         INT,
	LearningRate  FLOAT,
	L2LeafReg     INT,
	Rsm           FLOAT,
	LossFunction  NVARCHAR(40),
	RateInAll     NVARCHAR(100),
	Active        INT,
	LastUpdated   DATETIME
)

INSERT INTO RetentionListModelParameters
VALUES
('DQ002', 'Dolphin' , 500, 10,0.1,1,0.7,'MultiClass', 'c(0.5,0.125,0.125,0.125,0.125)', 0, GETDATE()),
('DQ002', 'Whale'   , 500, 10,0.1,1,0.7,'MultiClass', 'c(0.5,0.125,0.125,0.125,0.125)', 1, GETDATE())
