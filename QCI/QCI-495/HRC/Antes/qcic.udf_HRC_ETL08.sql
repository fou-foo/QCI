/****** Object:  UserDefinedFunction [qcic].[udf_HRC_ETL08]    Script Date: 10/2/2020 5:29:43 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE FUNCTION [qcic].[udf_HRC_ETL08](  @MaxDate DATE,  @days int   )
/*
  Inputs: @MaxDate  It is the maximum GamingDate from the table  [qcids].[Fact_PlayerDeviceRatingDay] where SystemNameInt is equal to 3 (HRC) 
          @days Only for no hardcoding another value
*/
RETURNS TABLE
AS
RETURN
	SELECT * 
		FROM (
			SELECT 
				--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
				 TRY_CONVERT(NVARCHAR(100)	,'HRC'						)								AS SystemName
				,TRY_CONVERT(INT			,3							)								AS SystemNameInt
				--Primary Key (CasinoID, DeviceID, PlayerID, GamingDate)
				,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId					)								AS CasinoID
				,TRY_CONVERT(INT			,b2.CasinoIDInt				)								AS CasinoIDInt
				,TRY_CONVERT(NVARCHAR(100)	,b.Asset					)								AS DeviceID
				,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](b.Asset,2))			AS DeviceIDBigInt
				,TRY_CONVERT(NVARCHAR(100)	,a.PlayerID					)								AS PlayerID
				,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](a.PlayerID,1))			AS PlayerIDBigInt
				,TRY_CONVERT(DATE			,a.GamingDt					)								AS GamingDate
				--Player Related Metrics
				,TRY_CONVERT(DATETIME		,a.RatingStartDtm			)								AS StartDateTime
				,TRY_CONVERT(DATETIME		,a.RatingEndDtm				)								AS EndDateTime
				--Gaming TOTAL metrics
				,TRY_CONVERT(BIGINT			,ROUND(a.Bet*100,0)			)								AS CoinInInt100
				,TRY_CONVERT(BIGINT			,ROUND(a.PaidOut*100,0)		)								AS CoinOutInt100
				,TRY_CONVERT(INT			,ROUND(a.Actual*100,0)		)								AS ActualWinWithFreeplayInt100
				,TRY_CONVERT(INT			,ROUND(a.Actual*100,0)		)								AS ActualWinNetFreeplayInt100
				,TRY_CONVERT(INT			,a.GamesPlayed				)								AS NumberGamesPlayed
				,TRY_CONVERT(INT			,ROUND(Jackpot*100,0)		)								AS JackpotInt100
				,TRY_CONVERT(INT			,0							)								AS FreeplayInt100
				,TRY_CONVERT(INT			,ROUND(a.Theo*100,0)		)								AS TheoWinWithFreeplayInt100
				,TRY_CONVERT(INT			,ROUND(a.Theo*100,0)		)								AS TheoWinNetFreeplayInt100
				,TRY_CONVERT(INT			,ROUND(MinsPlayed*100,0)	)								AS TimePlayedInt100
				,TRY_CONVERT(BIGINT			,ROUND(CashBuyIn*100,0)		)								AS CashInInt100
				,TRY_CONVERT(BIGINT			,0							)								AS ChipsInInt100
				,TRY_CONVERT(BIGINT			,0							)								AS CreditInInt100
				,TRY_CONVERT(BIGINT			,0							)								AS WalkedWithInt100
				--Player dimension attributes
				,TRY_CONVERT(VARCHAR(100)	 ,p.HomePostalCode			)								AS PlayerZipCode
				,TRY_CONVERT(NVARCHAR(50)	 ,p.Gender					)								AS PlayerGender
				,TRY_CONVERT(DATE			 ,p.SetupDate				)								AS PlayerEnrollmentDate
				,TRY_CONVERT(DATE			 ,p.BirthDate				)								AS PlayerBirthDate
				,TRY_CONVERT(FLOAT			 ,d.PlayerDistanceToCasino	)								AS PlayerAddressDistance																								
				--Game dimension attributes (most of the time there is one game per device, this will only show the MOST COMMON game)
				,TRY_CONVERT(NVARCHAR(100)	,b.SDSGameTheme				)								AS GameTheme	
				,TRY_CONVERT(NVARCHAR(100)	,''							)								AS GameThemeCount	
				,TRY_CONVERT(FLOAT			,a.TheoHold					)								AS GameTheoHold
				--Device dimension attributes
				,TRY_CONVERT(FLOAT			,b.TheoHold					)								AS DeviceTheoHoldPercent
				,TRY_CONVERT(NVARCHAR(200)	,b.Mfg						)								AS DeviceManufacturer
				,TRY_CONVERT(FLOAT			,b.Denom					)								AS Denomination
				,TRY_CONVERT(NVARCHAR(200)	,b.SDSGameType				)								AS DeviceGameType
				,TRY_CONVERT(NVARCHAR(100)	,''							)								AS DeviceCabinetType 
				,TRY_CONVERT(NVARCHAR(200)	,''							)								AS DeviceCabinetModel	
				,TRY_CONVERT(NVARCHAR(20)	,b.Location					)								AS DeviceLocation
				,TRY_CONVERT(NVARCHAR(20)	,LEFT(CONCAT(b.Location,'00'),2))							AS DeviceLocationArea
				,TRY_CONVERT(NVARCHAR(20)	,RIGHT(LEFT(CONCAT(b.Location,'0000'),4),2))				AS DeviceLocationBank
				,TRY_CONVERT(NVARCHAR(20)	,RIGHT(CONCAT('00',b.Location),2))							AS DeviceLocationStand
				,TRY_CONVERT(DATE			,b.StartGamingDt			)								AS DeviceStartDate
				,TRY_CONVERT(NVARCHAR(50)	,b.Serial					)								AS DeviceSerial	
				,TRY_CONVERT(INT			,3							)								AS DeviceGameClass	
				,TRY_CONVERT(INT			,0							)								AS DeviceLeaseFeeInt100	
				,TRY_CONVERT(NVARCHAR(4000)	,''							)								AS DeviceLeaseAttributes	
				,TRY_CONVERT(NVARCHAR(4000)	,''							)								AS DeviceExtraAttributes	
				,TRY_CONVERT(INT			,1							)								AS IsSlotDevice
				,TRY_CONVERT(INT			,0							)								AS IsTableDevice
				,TRY_CONVERT(INT			,0							)								AS IsPokerDevice
				,TRY_CONVERT(INT			,0							)								AS IsOtherDevice
				,TRY_CONVERT(INT			,1							)								AS IsActiveDevice
				,DeviceLocationBigInt = TRY_CONVERT(BIGINT			,CHECKSUM(b.Location))
				,TRY_CONVERT(DATETIME		,GETUTCDATE()				)								AS InsertDateTime
				FROM	 [qcic].[HRC_Fact_PlayerSlotRating_Tran] AS a WITH (NOLOCK)
				JOIN	 [qcids].[App_Casino] AS  b2 WITH (NOLOCK)
				ON		(a.CasinoID = b2.CasinoID AND b2.SystemNameInt = 3)
				LEFT JOIN [qcic].[HRC_Dim_SlotAttribute_History] AS b WITH (NOLOCK)
				ON		(a.SlotHistoryId = b.SlotHistoryId AND a.CasinoId = b.CasinoId)
				LEFT JOIN [qcids].[Dim_Player_SCD] AS p WITH (NOLOCK)
				ON		(p.SystemName = 'HRC' AND a.PlayerID = p.PlayerID AND p.StartDateTime <= a.GamingDt AND a.GamingDt < p.EndDateTime)
				LEFT JOIN [qcids].[Dim_PlayerDistanceToCasino_SCD] d WITH (NOLOCK)
				ON (d.SystemName = 'HRC' AND a.CasinoId = d.CasinoID AND a.PlayerID = d.PlayerID 
				     AND d.StartDateTime <= a.GamingDt AND a.GamingDt < d.EndDateTime)
				WHERE a.GamingDt	 >= DATEADD(DAY, @days , @MaxDate)
		UNION ALL
		--Table Ratings
		SELECT 
				--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 		TRY_CONVERT(NVARCHAR(100)	,'HRC'						)								AS SystemName
				,TRY_CONVERT(INT			,3							)								AS SystemNameInt
				--Primary Key (CasinoID, DeviceID, PlayerID, GamingDate)
				,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId					)								AS CasinoID
				,TRY_CONVERT(INT			,b2.CasinoIDInt				)								AS CasinoIDInt
				,TRY_CONVERT(NVARCHAR(100)	,b.GameId					)								AS DeviceID
				,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](b.GameId,2))			AS DeviceIDBigInt
				,TRY_CONVERT(NVARCHAR(100)	,a.PlayerID					)								AS PlayerID
				,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](a.PlayerID,1))			AS PlayerIDBigInt
				,TRY_CONVERT(DATE			,a.GamingDt					)								AS GamingDate
				--Player Related Metrics
				,TRY_CONVERT(DATETIME		,a.RatingStartDtm			)								AS StartDateTime
				,TRY_CONVERT(DATETIME		,a.RatingEndDtm				)								AS EndDateTime
				--Gaming TOTAL metrics
				,TRY_CONVERT(INT			,ROUND(a.Bet*100,0)			)								AS CoinInInt100
				,TRY_CONVERT(INT			,ROUND(a.PaidOut*100,0)		)								AS CoinOutInt100
				,TRY_CONVERT(INT			,ROUND(a.Actual*100,0)		)								AS ActualWinWithFreeplayInt100
				,TRY_CONVERT(INT			,ROUND(a.Actual*100,0)		)								AS ActualWinNetFreeplayInt100
				,TRY_CONVERT(INT			,a.GamesPlayed				)								AS NumberGamesPlayed
				,TRY_CONVERT(INT			,0							)								AS JackpotInt100
				,TRY_CONVERT(INT			,0							)								AS FreeplayInt100
				,TRY_CONVERT(INT			,ROUND(a.Theo*100,0)		)								AS TheoWinWithFreeplayInt100
				,TRY_CONVERT(INT			,ROUND(a.Theo*100,0)		)								AS TheoWinNetFreeplayInt100
				,TRY_CONVERT(INT			,ROUND(MinsPlayed*100,0)	)								AS TimePlayedInt100
				,TRY_CONVERT(INT			,ROUND(CashBuyIn*100,0)		)								AS CashInInt100
				,TRY_CONVERT(INT			,ROUND(ChipBuyIn*100,0)		)								AS ChipsInInt100
				,TRY_CONVERT(INT			,ROUND(CreditBuyIn*100,0)	)								AS CreditInInt100
				,TRY_CONVERT(INT			,ROUND(WalkedWith*100,0)	)								AS WalkedWithInt100
				--Player dimension attributes
				,TRY_CONVERT(VARCHAR(100)	 ,p.HomePostalCode			)								AS PlayerZipCode
				,TRY_CONVERT(NVARCHAR(50)	 ,p.Gender					)								AS PlayerGender
				,TRY_CONVERT(DATE			 ,p.SetupDate				)								AS PlayerEnrollmentDate
				,TRY_CONVERT(DATE			 ,p.BirthDate				)								AS PlayerBirthDate
				,TRY_CONVERT(FLOAT			 ,d.PlayerDistanceToCasino	)								AS PlayerAddressDistance																								
				--Game dimension attributes (most of the time there is one game per device, this will only show the MOST COMMON game)
				,TRY_CONVERT(NVARCHAR(100)	,b.GameName					)								AS GameTheme	
				,TRY_CONVERT(NVARCHAR(100)	,NULL						)								AS GameThemeCount	
				,TRY_CONVERT(FLOAT			,a.TheoHold					)								AS GameTheoHold
				--Device dimension attributes
				,TRY_CONVERT(FLOAT			,b.TheoHold					)								AS DeviceTheoHoldPercent
				,TRY_CONVERT(NVARCHAR(200)	,''							)								AS DeviceManufacturer
				,TRY_CONVERT(FLOAT			,''							)								AS Denomination
				,TRY_CONVERT(NVARCHAR(200)	,b.GameCode					)								AS DeviceGameType	
				,TRY_CONVERT(NVARCHAR(100)	,''							)								AS DeviceCabinetType 
				,TRY_CONVERT(NVARCHAR(200)	,''							)								AS DeviceCabinetModel	
				,TRY_CONVERT(NVARCHAR(20)	,b.LocnCode					)								AS DeviceLocation		
				,TRY_CONVERT(NVARCHAR(20)	,LEFT(CONCAT(b.LocnCode,'00'),2))							AS DeviceLocationArea
				,TRY_CONVERT(NVARCHAR(20)	,RIGHT(LEFT(CONCAT(b.LocnCode,'0000'),4),2))				AS DeviceLocationBank
				,TRY_CONVERT(NVARCHAR(20)	,RIGHT(CONCAT('00',b.LocnCode),2))							AS DeviceLocationStand
				,TRY_CONVERT(DATE			,b.InsertDtm				)								AS DeviceStartDate
				,TRY_CONVERT(NVARCHAR(50)	,''							)								AS DeviceSerial	
				,TRY_CONVERT(INT			,0							)								AS DeviceGameClass	
				,TRY_CONVERT(INT			,0							)								AS DeviceLeaseFeeInt100
				,TRY_CONVERT(NVARCHAR(4000)	,''							)								AS DeviceLeaseAttributes	
				,TRY_CONVERT(NVARCHAR(4000)	,concat('MinBet:',b.MinBet,
													'MaxBet:',b.MaxBet,
													'AreaName:',b.AreaName)
																		)								AS DeviceExtraAttributes	
				,TRY_CONVERT(INT			,0							)								AS IsSlotDevice
				,TRY_CONVERT(INT			,1							)								AS IsTableDevice
				,TRY_CONVERT(INT			,0							)								AS IsPokerDevice
				,TRY_CONVERT(INT			,0							)								AS IsOtherDevice
				,TRY_CONVERT(INT			,1							)								AS IsActiveDevice
				,DeviceLocationBigInt = TRY_CONVERT(BIGINT			,CHECKSUM(b.LocnCode))
				,TRY_CONVERT(DATETIME		,GETUTCDATE()				)								AS InsertDateTime
				FROM	 [qcic].[HRC_Fact_PlayerTableRating_Tran] AS a WITH (NOLOCK)
				JOIN	 [qcids].[App_Casino] b2 WITH (NOLOCK)
				ON		(a.CasinoID = b2.CasinoID AND b2.SystemNameInt = 3)
				LEFT JOIN [qcic].[HRC_Dim_TableAttribute_History] b WITH (NOLOCK)
				ON		(a.TableHistoryId = b.TableHistoryId AND a.CasinoId = b.CasinoId)
				LEFT JOIN [qcids].[Dim_Player_SCD] AS p WITH (NOLOCK)
				ON	(p.SystemName = 'HRC' AND a.PlayerID = p.PlayerID AND p.StartDateTime <= a.GamingDt AND a.GamingDt < p.EndDateTime)
				LEFT JOIN [qcids].[Dim_PlayerDistanceToCasino_SCD] AS d WITH (NOLOCK)
				ON	(d.SystemName = 'HRC' AND a.CasinoId = d.CasinoID AND a.PlayerID = d.PlayerID AND 
				     d.StartDateTime <= a.GamingDt AND a.GamingDt < d.EndDateTime)
				WHERE a.GamingDt	 >= DATEADD(DAY, @days ,@MaxDate)
			UNION ALL
			--Free Play and Table Games Coupon
			SELECT 
				--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
				 TRY_CONVERT(NVARCHAR(100)	,'HRC'						)								AS SystemName
				,TRY_CONVERT(INT			,3							)								AS SystemNameInt
				--Primary Key (CasinoID, DeviceID, PlayerID, GamingDate)
				,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId					)								AS CasinoID
				,TRY_CONVERT(INT			,b2.CasinoIDInt				)								AS CasinoIDInt
				,TRY_CONVERT(NVARCHAR(100)	,''							)								AS DeviceID
				,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt]('',2))					AS DeviceIDBigInt
				,TRY_CONVERT(NVARCHAR(100)	,a.PlayerID					)								AS PlayerID
				,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](a.PlayerID,1))			AS PlayerIDBigInt
				,TRY_CONVERT(DATE			,a.GamingDt					)								AS GamingDate
				--Player Related Metrics
				,TRY_CONVERT(DATETIME		,a.RedeemedDtm				)								AS StartDateTime
				,TRY_CONVERT(DATETIME		,a.RedeemedDtm				)								AS EndDateTime
				--Gaming TOTAL metrics
				,TRY_CONVERT(INT			,0							)								AS CoinInInt100
				,TRY_CONVERT(INT			,0							)								AS CoinOutInt100
				,TRY_CONVERT(INT			,0							)								AS ActualWinWithFreeplayInt100
				,TRY_CONVERT(INT			,ROUND(-1 * a.AmountUsed*100,0))								AS ActualWinNetFreeplayInt100
				,TRY_CONVERT(INT			,0							)								AS NumberGamesPlayed
				,TRY_CONVERT(INT			,0							)								AS JackpotInt100
				,TRY_CONVERT(INT			,ROUND(a.AmountUsed*100,0)	)								AS FreeplayInt100
				,TRY_CONVERT(INT			,0							)								AS TheoWinWithFreeplayInt100
				,TRY_CONVERT(INT			,ROUND(-1 * a.AmountUsed*100,0))								AS TheoWinNetFreeplayInt100
				,TRY_CONVERT(INT			,0							)								AS TimePlayedInt100
				,TRY_CONVERT(INT			,0							)								AS CashInInt100
				,TRY_CONVERT(INT			,0							)								AS ChipsInInt100
				,TRY_CONVERT(INT			,0							)								AS CreditInInt100
				,TRY_CONVERT(INT			,0							)								AS WalkedWithInt100
				--Player dimension attributes
				,TRY_CONVERT(VARCHAR(100)	 ,p.HomePostalCode			)								AS PlayerZipCode
				,TRY_CONVERT(NVARCHAR(50)	 ,p.Gender					)								AS PlayerGender
				,TRY_CONVERT(DATE			 ,p.SetupDate				)								AS PlayerEnrollmentDate
				,TRY_CONVERT(DATE			 ,p.BirthDate				)								AS PlayerBirthDate
				,TRY_CONVERT(FLOAT			 ,d.PlayerDistanceToCasino	)								AS PlayerAddressDistance																								
				--Game dimension attributes (most of the time there is one game per device, this will only show the MOST COMMON game)
				,TRY_CONVERT(NVARCHAR(100)	,''							)								AS GameTheme	
				,TRY_CONVERT(NVARCHAR(100)	,NULL						)								AS GameThemeCount	
				,TRY_CONVERT(FLOAT			,''							)								AS GameTheoHold
				--Device dimension attributes
				,TRY_CONVERT(FLOAT			,''							)								AS DeviceTheoHoldPercent
				,TRY_CONVERT(NVARCHAR(200)	,''							)								AS DeviceManufacturer
				,TRY_CONVERT(FLOAT			,''							)								AS Denomination
				,TRY_CONVERT(NVARCHAR(200)	,''							)								AS DeviceGameType	
				,TRY_CONVERT(NVARCHAR(100)	,''							)								AS DeviceCabinetType 
				,TRY_CONVERT(NVARCHAR(200)	,''							)								AS DeviceCabinetModel	
				,TRY_CONVERT(NVARCHAR(20)	,''							)								AS DeviceLocation		
				,TRY_CONVERT(NVARCHAR(20)	,''							)								AS DeviceLocationArea	
				,TRY_CONVERT(NVARCHAR(20)	,''							)								AS DeviceLocationBank	
				,TRY_CONVERT(NVARCHAR(20)	,''							)								AS DeviceLocationStand
				,TRY_CONVERT(DATE			,''							)								AS DeviceStartDate
				,TRY_CONVERT(NVARCHAR(50)	,''							)								AS DeviceSerial	
				,TRY_CONVERT(INT			,0							)								AS DeviceGameClass	
				,TRY_CONVERT(INT			,0							)								AS DeviceLeaseFeeInt100
				,TRY_CONVERT(NVARCHAR(4000)	,''							)								AS DeviceLeaseAttributes	
				,TRY_CONVERT(NVARCHAR(4000)	,''							)								AS DeviceExtraAttributes
				,TRY_CONVERT(INT			,case when a.TranType = 'Coupon' then 0 else 1 end)			AS IsSlotDevice
				,TRY_CONVERT(INT			,case when a.TranType = 'Coupon' then 1 else 0 end)			AS IsTableDevice
				,TRY_CONVERT(INT			,0							)								AS IsPokerDevice
				,TRY_CONVERT(INT			,0							)								AS IsOtherDevice
				,TRY_CONVERT(INT			,1							)								AS IsActiveDevice
				,DeviceLocationBigInt = TRY_CONVERT(BIGINT			,CHECKSUM(''))
				,TRY_CONVERT(DATETIME		,GETUTCDATE()				)								AS InsertDateTime
				FROM	 [qcic].[HRC_Fact_PlayerRedemption_Tran] AS a WITH (NOLOCK)
				JOIN	 [qcids].[App_Casino] AS b2 WITH (NOLOCK)
				ON		(a.CasinoID = b2.CasinoID AND b2.SystemNameInt = 3)
				LEFT JOIN [qcids].[Dim_Player_SCD] AS p WITH (NOLOCK)
				ON	(p.SystemName = 'HRC' AND a.PlayerID = p.PlayerID AND p.StartDateTime <= a.GamingDt AND a.GamingDt < p.EndDateTime)
				LEFT JOIN [qcids].[Dim_PlayerDistanceToCasino_SCD] AS d WITH (NOLOCK)
				ON (d.SystemName = 'HRC' AND a.CasinoId = d.CasinoID AND a.PlayerID = d.PlayerID AND
				    d.StartDateTime <= a.GamingDt AND a.GamingDt < d.EndDateTime)
				WHERE	 a.TranType IN ('Autoload','Coupon') AND a.AmountUsed > 0 
						AND  a.GamingDt	 >= DATEADD(DAY, @days ,@MaxDate) ) as foo
			WHERE foo.GamingDate >= DATEADD(DAY, @days ,@MaxDate);
