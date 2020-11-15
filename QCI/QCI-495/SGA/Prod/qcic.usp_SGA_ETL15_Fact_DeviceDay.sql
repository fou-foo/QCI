/****** Object:  StoredProcedure [qcic].[usp_SGA_ETL15_Fact_DeviceDay]   Script Date: 10/2/2020 5:29:43 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [qcic].usp_SGA_ETL15_Fact_DeviceDay 
@MaxDate DATE,  @days int
/*
  Inputs: @MaxDate  It is the maximum GamingDate from the table  [tqcids].[Fact_DeviceDay]  where SystemNameInt is equal to 3 (HRC) 
          @days Only for no hardcoding another value
*/
AS
	SELECT 
			--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
			 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'					)								AS SystemName
			,TRY_CONVERT(INT			,2							)								AS SystemNameInt
			--Primary Key (CasinoID, DeviceID, GamingDate)
			,TRY_CONVERT(NVARCHAR(100)	,a.Casino					)								AS CasinoID
			,TRY_CONVERT(INT			,b.CasinoIDInt				)								AS CasinoIDInt
			,TRY_CONVERT(NVARCHAR(100)	,a.MetaAsset				)								AS DeviceID
			,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](a.MetaAsset,2))		AS DeviceIDBigInt
			,TRY_CONVERT(DATE			,a.GamingDt					)								AS GamingDate
			--Gaming TOTAL metrics
			,TRY_CONVERT(BIGINT			,ROUND(a.MoneyPlayed*100,0))								AS CoinIn     
			,TRY_CONVERT(BIGINT			,ROUND(a.MoneyWon*100,0)	)								AS CoinOut
			,TRY_CONVERT(INT			,ROUND(a.CasinoWin*100,0)	)								AS ActualWinWithFreeplay
			,TRY_CONVERT(INT			,ROUND(a.CasinoWin*100 - a.CouponIn*100 - a.TransferIn*100,0))	AS ActualWinNetFreeplay
			,TRY_CONVERT(INT			,a.GamesPlayed				)								AS NumberGamesPlayed
			,TRY_CONVERT(INT			,ROUND(a.Jackpot*100,0)	)									AS Jackpot
			,TRY_CONVERT(INT			,ROUND(a.CouponIn*100 + a.TransferIn*100,0))				AS Freeplay
			,TRY_CONVERT(INT			,ROUND(a.TheoWin*100,0)	)									AS TheoWinWithFreeplay  
			,TRY_CONVERT(INT			,ROUND(a.TheoWin*100 - a.CouponIn*100 - a.TransferIn*100,0)) AS TheoWinNetFreeplay  
			,TRY_CONVERT(INT			,0							)								AS TimePlayed
			,TRY_CONVERT(BIGINT			,ROUND(a.CashIn*100,0)		)								AS CashIn   
			,TRY_CONVERT(BIGINT			,0							)								AS ChipsIn
			,TRY_CONVERT(BIGINT			,0							)								AS CreditIn   
			,TRY_CONVERT(BIGINT			,0							)								AS WalkedWith
			--Game dimension attributes (most of the time there is one game per device, this will only show the MOST COMMON game)
			,TRY_CONVERT(NVARCHAR(100)	,a.gametheme				)								AS GameTheme	
			,TRY_CONVERT(NVARCHAR(100)	,''							)								AS GameThemeCount	
			,TRY_CONVERT(FLOAT			,1 - a.GlobalPayout			)								AS GameTheoHold
			--Device dimension attributes
			,TRY_CONVERT(FLOAT			,1 - a.GlobalPayout			)								AS DeviceTheoHoldPercent
			,TRY_CONVERT(NVARCHAR(200)	,a.MetaMfg					)								AS DeviceManufacturer
			,TRY_CONVERT(FLOAT			,a.Denom					)								AS Denomination
			,TRY_CONVERT(NVARCHAR(200)	,a.GameType					)								AS DeviceGameType	
			,TRY_CONVERT(NVARCHAR(100)	,''							)								AS DeviceCabinetType 
			,TRY_CONVERT(NVARCHAR(200)	,a.Cabinet					)								AS DeviceCabinetModel	
			,TRY_CONVERT(NVARCHAR(20)	,a.MetaLocation				)								AS DeviceLocation		
			,TRY_CONVERT(NVARCHAR(20)	,LEFT(CONCAT(a.MetaLocation,'00'),2))						AS DeviceLocationArea
			,TRY_CONVERT(NVARCHAR(20)	,RIGHT(LEFT(CONCAT(a.MetaLocation,'0000'),4),2))			AS DeviceLocationBank
			,TRY_CONVERT(NVARCHAR(20)	,RIGHT(CONCAT('00',a.MetaLocation),2))						AS DeviceLocationStand
			,TRY_CONVERT(DATE			,a.startdate				)								AS DeviceStartDate
			,TRY_CONVERT(NVARCHAR(50)	,a.MetaSerial				)								AS DeviceSerial	
			,TRY_CONVERT(INT			,a.GameClass				)								AS DeviceGameClass	
			,TRY_CONVERT(MONEY			,ROUND(a.LeaseFee*100,0)	)								AS DeviceLeaseFeeInt100
			,TRY_CONVERT(VARCHAR(100)	,''							)								AS DeviceLeaseAttributes	
			,TRY_CONVERT(VARCHAR(100)	,concat('Reels:',a.Reels, 'MaxBet:',a.MaxBet)	) 			AS DeviceExtraAttributes	
			,TRY_CONVERT(INT			,1							)								AS IsSlotDevice
			,TRY_CONVERT(INT			,0							)								AS IsTableDevice
			,TRY_CONVERT(INT			,0							)								AS IsPokerDevice
			,TRY_CONVERT(INT			,0							)								AS IsOtherDevice
			,TRY_CONVERT(INT			,a.Active					)								AS IsActiveDevice
			,DeviceLocationBigInt = TRY_CONVERT(BIGINT			,CHECKSUM(a.MetaLocation))
			,TRY_CONVERT(DATETIME		,GETUTCDATE()				)								AS InsertDateTime
			FROM	 [qcic].[SGA_Fact_SlotAnalysis_Staging] a WITH (NOLOCK)
			JOIN	 [qcids].[App_Casino] b WITH (NOLOCK)
			ON		(a.Casino = b.CasinoID AND b.SystemNameInt = 2)
			WHERE a.GamingDt	 >= DATEADD(DAY, @days ,@MaxDate) 
	RETURN;


