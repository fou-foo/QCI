set statistics  time on 
set statistics io on 


DECLARE @SystemNameInt int = 3, @MaxDate DATE, @SystemName nvarchar(100) = 'HRC';
SELECT @MaxDate = MAX(GamingDate) FROM [qcids].[Fact_DeviceDay] WITH (NOLOCK) WHERE SystemNameInt = @SystemNameInt
SELECT @MaxDate = ISNULL(@MaxDate,'2000-01-01')
	
	SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'						)								AS SystemName
		,TRY_CONVERT(INT			,3							)								AS SystemNameInt
		--Primary Key (CasinoID, DeviceID, GamingDate)
		,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId					)								AS CasinoID
		,TRY_CONVERT(INT			,b.CasinoIDInt				)								AS CasinoIDInt
		,TRY_CONVERT(NVARCHAR(100)	,a.Asset					)								AS DeviceID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](a.Asset,2))			AS DeviceIDBigInt
		,TRY_CONVERT(DATE			,a.GamingDt					)								AS GamingDate
		--Gaming TOTAL metrics
		,TRY_CONVERT(BIGINT			,ROUND(a.Bet*100,0)			)								AS CoinIn     
		,TRY_CONVERT(BIGINT			,ROUND(a.Paidout*100,0)		)								AS CoinOut
		,TRY_CONVERT(INT			,ROUND(a.Actual*100,0)		)								AS ActualWinWithFreeplay
		,TRY_CONVERT(INT			,ROUND(a.Actual*100 - a.CouponIn*100 - a.TransferIn*100,0))	AS ActualWinNetFreeplay
		,TRY_CONVERT(INT			,a.GamesPlayed				)								AS NumberGamesPlayed
		,TRY_CONVERT(INT			,ROUND(a.JackpotOut*100,0)	)								AS Jackpot
		,TRY_CONVERT(INT			,ROUND(a.CouponIn*100 + a.TransferIn*100,0))				AS Freeplay
		,TRY_CONVERT(INT			,ROUND(a.Theo*100,0)		)								AS TheoWinWithFreeplay  
		,TRY_CONVERT(INT			,ROUND(a.Theo*100 - a.CouponIn*100 - a.TransferIn*100,0))	AS TheoWinNetFreeplay  
		,TRY_CONVERT(INT			,0							)								AS TimePlayed
		,TRY_CONVERT(BIGINT			,ROUND(a.CashIn*100,0)		)								AS CashIn   
		,TRY_CONVERT(BIGINT			,0							)								AS ChipsIn
		,TRY_CONVERT(BIGINT			,0							)								AS CreditIn   
		,TRY_CONVERT(BIGINT			,0							)								AS WalkedWith
		--Game dimension attributes (most of the time there is one game per device, this will only show the MOST COMMON game)
		,TRY_CONVERT(NVARCHAR(100)	,a.gametheme				)								AS GameTheme	
		,TRY_CONVERT(NVARCHAR(100)	,''							)								AS GameThemeCount	
		,TRY_CONVERT(FLOAT			,a.TheoHoldPct				)								AS GameTheoHold
		--Device dimension attributes
		,TRY_CONVERT(FLOAT			,a.TheoHoldPct				)								AS DeviceTheoHoldPercent
		,TRY_CONVERT(NVARCHAR(200)	,a.StdMfg					)								AS DeviceManufacturer
		,TRY_CONVERT(FLOAT			,a.Denom					)								AS Denomination
		,TRY_CONVERT(NVARCHAR(200)	,a.GameType					)								AS DeviceGameType	
		,TRY_CONVERT(NVARCHAR(100)	,''							)								AS DeviceCabinetType 
		,TRY_CONVERT(NVARCHAR(200)	,a.Cabinet					)								AS DeviceCabinetModel	
		,TRY_CONVERT(NVARCHAR(20)	,a.Location					)								AS DeviceLocation		
		,TRY_CONVERT(NVARCHAR(20)	,LEFT(CONCAT(a.Location,'00'),2))							AS DeviceLocationArea
		,TRY_CONVERT(NVARCHAR(20)	,RIGHT(LEFT(CONCAT(a.Location,'0000'),4),2))				AS DeviceLocationBank
		,TRY_CONVERT(NVARCHAR(20)	,RIGHT(CONCAT('00',a.Location),2))							AS DeviceLocationStand
		,TRY_CONVERT(DATE			,DATEADD(DAY,-1 * a.DaysOnFloor,a.GamingDt))				AS DeviceStartDate
		,TRY_CONVERT(NVARCHAR(50)	,a.Serial					)								AS DeviceSerial	
		,TRY_CONVERT(INT			,3							)								AS DeviceGameClass	
		,TRY_CONVERT(MONEY			,0							)								AS DeviceLeaseFeeInt100
		,TRY_CONVERT(NVARCHAR(4000)	,''							)								AS DeviceLeaseAttributes	
		,TRY_CONVERT(NVARCHAR(4000)	,''							)								AS DeviceExtraAttributes	
		,TRY_CONVERT(INT			,1							)								AS IsSlotDevice
		,TRY_CONVERT(INT			,0							)								AS IsTableDevice
		,TRY_CONVERT(INT			,0							)								AS IsPokerDevice
		,TRY_CONVERT(INT			,0							)								AS IsOtherDevice
		,TRY_CONVERT(INT			,1							)								AS IsActiveDevice
		,DeviceLocationBigInt = TRY_CONVERT(BIGINT			,CHECKSUM(a.Location))
		,TRY_CONVERT(DATETIME		,GETUTCDATE()				)								AS InsertDateTime
		FROM	 [qcic].[HRC_Fact_DayMeterHistory] AS a WITH (NOLOCK)
		JOIN	 [qcids].[App_Casino] AS b WITH (NOLOCK)
		ON		(a.CasinoId = b.CasinoID AND b.SystemNameInt = 3)
		WHERE a.GamingDt  >= DATEADD(DAY,-10,@MaxDate) ;





DECLARE @SystemNameInt int = 3, @MaxDate DATE, @SystemName nvarchar(100) = 'HRC';
SELECT @MaxDate = MAX(GamingDate) FROM [qcids].[Fact_DeviceDay] WITH (NOLOCK) WHERE SystemNameInt = @SystemNameInt
SELECT @MaxDate = ISNULL(@MaxDate,'2000-01-01')
	
 SELECT * 
		FROM [test].[udf_HRC_ETL15](@SystemName , @SystemNameInt)
where 
GamingDate >= DATEADD(DAY,-10,@MaxDate);


