CREATE   FUNCTION [tqcic].[udf_SGA_ETL01] ( )
RETURNS TABLE
AS
RETURN
	SELECT 
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'							)				AS SystemName
		,TRY_CONVERT(NVARCHAR(100)	,CasinoID							)				AS CasinoGroupID
		,TRY_CONVERT(NVARCHAR(100)	,EmpID								)				AS HostID
		,TRY_CONVERT(NVARCHAR(50)	,ISNULL(FirstName,'NoFirstName')	)				AS HostFirstName
		,TRY_CONVERT(NVARCHAR(50)	,ISNULL(LastName,'NoLastName')		)				AS HostLastName
		,TRY_CONVERT(NVARCHAR(100)	,ISNULL(Position,'NoPosition')		)				AS HostPosition
		,TRY_CONVERT(NVARCHAR(50)	,EmpNum								)				AS HostCode
		,TRY_CONVERT(DATETIME		,InsertDT							)				AS StartDateTime
		,TRY_CONVERT(DATETIME		,ISNULL(RetireDT,'2999-12-31')		)				AS EndDateTime
		,TRY_CONVERT(INT			,isCurrent							)				AS IsCurrent
		,GETUTCDATE()																	AS InsertDateTime
	FROM	 [qcic].[SGA_Dim_CMPEmployees]
	WHERE	 isHost = 1 and isInactive = 0;
-- testing select * from [tqcic].[udf_SGA_ETL01] ( )

CREATE   FUNCTION [tqcic].[udf_SGA_ETL02] ( )
RETURNS TABLE
AS
RETURN
	SELECT 
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)							AS SystemName
		,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)							AS PlayerID
		,TRY_CONVERT(NVARCHAR(20)	,title					)							AS Title
		,TRY_CONVERT(NVARCHAR(50)	,lastname				)							AS LastName
		,TRY_CONVERT(NVARCHAR(50)	,firstname				)							AS FirstName
		,TRY_CONVERT(NVARCHAR(50)	,ISNULL(displayname,concat(firstname,' ',lastname)))AS DisplayName
		,TRY_CONVERT(NVARCHAR(50)	,NickNameAlias			)							AS NickNameAlias
		,TRY_CONVERT(NVARCHAR(100)	,HomeAddr1				)							AS HomeAddress1
		,TRY_CONVERT(NVARCHAR(100)	,HomeAddr2				)							AS HomeAddress2
		,TRY_CONVERT(NVARCHAR(100)	,HomeCity				)							AS HomeCity
		,TRY_CONVERT(NVARCHAR(50)	,HomeStateCode			)							AS HomeStateCode
		,TRY_CONVERT(NVARCHAR(50)	,HomeCountryCode		)							AS HomeCountryCode
		,TRY_CONVERT(NVARCHAR(20)	,HomePostalCode			)							AS HomePostalCode
		,TRY_CONVERT(NVARCHAR(20)	,HomeTel1Type			)							AS HomeTelephone1Type
		,TRY_CONVERT(NVARCHAR(20)	,HomeTel1				)							AS HomeTelephone1
		,TRY_CONVERT(NVARCHAR(20)	,HomeTel2Type			)							AS HomeTelephone2Type
		,TRY_CONVERT(NVARCHAR(20)	,HomeTel2				)							AS HomeTelephone2
		,TRY_CONVERT(NVARCHAR(255)	,HomeEmail				)							AS HomeEmail
		,TRY_CONVERT(NVARCHAR(50)	,AltName				)							AS AltName
		,TRY_CONVERT(NVARCHAR(100)	,AltAddr1				)							AS AltAddress1
		,TRY_CONVERT(NVARCHAR(100)	,AltAddr2				)							AS AltAddress2
		,TRY_CONVERT(NVARCHAR(100)	,AltCity				)							AS AltCity
		,TRY_CONVERT(NVARCHAR(50)	,AltStateCode			)							AS AltStateCode
		,TRY_CONVERT(NVARCHAR(50)	,AltCountryCode			)							AS AltCountryCode
		,TRY_CONVERT(NVARCHAR(20)	,AltPostalCode			)							AS AltPostalCode
		,TRY_CONVERT(NVARCHAR(20)	,AltTel1Type			)							AS AltTelephone1Type
		,TRY_CONVERT(NVARCHAR(20)	,AltTel1				)							AS AltTelephone1
		,TRY_CONVERT(NVARCHAR(20)	,AltTel2Type			)							AS AltTelephone2Type
		,TRY_CONVERT(NVARCHAR(20)	,AltTel2				)							AS AltTelephone2
		,TRY_CONVERT(NVARCHAR(255)	,AltEmail				)							AS AltEmail
		,TRY_CONVERT(DATE			,BirthDT				)							AS BirthDate
		,TRY_CONVERT(INT			,isPrivacy				)							AS IsPrivacy
		,TRY_CONVERT(INT			,isMailToAlt			)							AS IsMailToAlt
		,TRY_CONVERT(INT			,isNoMail				)							AS IsNoMail
		,TRY_CONVERT(INT			,isReturnMail			)							AS IsReturnMail
		,TRY_CONVERT(INT			,isVIP					)							AS IsVIP
		,TRY_CONVERT(INT			,isLostAndFound			)							AS IsLostAndFound
		,TRY_CONVERT(INT			,isCreditAcct			)							AS IsCreditAccount
		,TRY_CONVERT(INT			,isBanned				)							AS IsBanned
		,TRY_CONVERT(INT			,isInactive				)							AS IsInactive
		,TRY_CONVERT(INT			,isCall					)								AS IsCall
		,TRY_CONVERT(INT			,isEmailSend			)							AS IsEmailSend
		,TRY_CONVERT(NVARCHAR(100)	,SetupCasinoID			)							AS SetupCasinoID
		,TRY_CONVERT(NVARCHAR(100)	,SetupEmpID				)							AS SetupEmployeeID
		,TRY_CONVERT(DATE			,SetupDTM				)							AS SetupDate
		,TRY_CONVERT(NVARCHAR(50)	,case when Sex = 0 then 'F' when Sex = 1 then 'M' else 'O' end)	AS Gender
		,TRY_CONVERT(NVARCHAR(100)	,MailingAddr1			)							AS MailingAddress1
		,TRY_CONVERT(NVARCHAR(100)	,MailingAddr2			)							AS MailingAddress2
		,TRY_CONVERT(NVARCHAR(100)	,MailingCity			)							AS MailingCity
		,TRY_CONVERT(NVARCHAR(50)	,MailingStateCode		)							AS MailingStateCode
		,TRY_CONVERT(NVARCHAR(50)	,MailingCountryCode		)							AS MailingCountryCode
		,TRY_CONVERT(NVARCHAR(20)	,MailingPostalCode		)							AS MailingPostalCode
		,TRY_CONVERT(NVARCHAR(20)	,Addr_MatchType			)							AS Address_MatchType
		,TRY_CONVERT(REAL			,Addr_LAT				)							AS Address_Latitude
		,TRY_CONVERT(REAL			,Addr_LONG				)							AS Address_Longitude
		,TRY_CONVERT(DATETIME		,InsertDT				)							AS StartDateTime
		,TRY_CONVERT(DATETIME		,RetireDT				)							AS EndDateTime
		,TRY_CONVERT(INT			,isCurrent				)							AS IsCurrent
		,GETUTCDATE()																	AS InsertDateTime
	FROM	 [qcic].[SGA_uvw_MarketablePlayersAttributes]
	WHERE	 isBanned = 0;
-- testing select * from [tqcic].[udf_SGA_ETL02] ( )

CREATE   FUNCTION [tqcic].[udf_SGA_ETL03] ( )
RETURNS TABLE
AS
RETURN
	SELECT	 	
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)							AS SystemName
		,TRY_CONVERT(NVARCHAR(100)	,b.CasinoID				)							AS CasinoID
		,TRY_CONVERT(NVARCHAR(100)	,a.PlayerID				)							AS PlayerID
		,TRY_CONVERT(FLOAT			,case when b.CasinoID = 'BSG' then a.P_DistanceTo_BSG
										  when b.CasinoID = 'CSG' then a.P_DistanceTo_CSG
										  when b.CasinoID = 'HSG' then a.P_DistanceTo_HSG
										  when b.CasinoID = 'ISG' then a.P_DistanceTo_ISG
										  when b.CasinoID = 'SHC' then a.P_DistanceTo_SHC
										  when b.CasinoID = 'TSG' then a.P_DistanceTo_TSG
										  else 0 end		)							AS PlayerDistanceToCasino
		,TRY_CONVERT(DATETIME		,a.InsertDT				)							AS StartDateTime
		,TRY_CONVERT(DATETIME		,a.RetireDt				)							AS EndDateTime
		,TRY_CONVERT(INT			,a.IsCurrent			)							AS IsCurrent
		,GETUTCDATE()																	AS InsertDateTime
		FROM	 [qcic].[SGA_uvw_MarketablePlayersAttributes] a
		CROSS JOIN (
			SELECT	 a.SystemName
					,CasinoID = a.CasinoGroupID
					FROM	 [qcids].[App_CasinoGroup] AS a
					JOIN	 [qcids].[App_CasinoGroupCasino] AS b
					ON		(a.SystemName = b.SystemName AND a.CasinoGroupID = b.CasinoGroupID AND a.CasinoGroupID = b.CasinoID)
					WHERE	 a.SystemName = 'SGAHRI') b
		WHERE	 a.isBanned = 0 AND b.SystemName = 'SGAHRI';
--- TESTIG SELECT * FROM   [tqcic].[udf_SGA_ETL03] ( )

CREATE   FUNCTION [tqcic].[udf_SGA_ETL04] ( )
RETURNS TABLE
AS
RETURN
	SELECT
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)							AS SystemName
		,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)							AS PlayerID
		,TRY_CONVERT(NVARCHAR(100)	,P_ClubStatus			)							AS LoyaltyCardLevel
		,TRY_CONVERT(DATETIME		,InsertDT				)							AS StartDateTime
		,TRY_CONVERT(DATETIME		,RetireDT				)							AS EndDateTime
		,TRY_CONVERT(INT			,isCurrent				)							AS IsCurrent
		,GETUTCDATE()																	AS InsertDateTime
	FROM	 [qcic].[SGA_uvw_MarketablePlayersAttributes];
-- TESTING SELECT * FROM [tqcic].[udf_SGA_ETL04] ( )


CREATE   FUNCTION [tqcic].[udf_SGA_ETL05] ( )
RETURNS TABLE
AS
RETURN
	SELECT	 	
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)		AS SystemName
		,TRY_CONVERT(NVARCHAR(100)	,CasinoID				)		AS CasinoID
		,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)		AS PlayerID
		,TRY_CONVERT(NVARCHAR(100)	,HostEmpID				)		AS HostID
		,TRY_CONVERT(NVARCHAR(100)	,'Primary'				)		AS PlayerHostType
		,TRY_CONVERT(DATETIME		,HostStartDTM			)		AS StartDateTime
		,TRY_CONVERT(DATETIME		,HostEndDTM				)		AS EndDateTime
		,TRY_CONVERT(INT			,isCurrent				)		AS IsCurrent
		,GETUTCDATE()												AS InsertDateTime
		FROM	 [qcic].[SGA_Fact_PlayerHostHistory_Primary]
	UNION ALL
	SELECT	 	
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)		AS SystemName
		,TRY_CONVERT(NVARCHAR(100)	,CasinoID				)		AS CasinoID
		,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)		AS PlayerID
		,TRY_CONVERT(NVARCHAR(100)	,HostEmpID				)		AS HostID
		,TRY_CONVERT(NVARCHAR(100)	,'Secondary'			)		AS PlayerHostType
		,TRY_CONVERT(DATETIME		,HostStartDTM			)		AS StartDateTime
		,TRY_CONVERT(DATETIME		,HostEndDTM				)		AS EndDateTime
		,TRY_CONVERT(INT			,isCurrent				)		AS IsCurrent
		,GETUTCDATE()												AS InsertDateTime
		FROM	 [qcic].[SGA_Fact_PlayerHostHistory_Secondary];

CREATE   FUNCTION [tqcic].[udf_SGA_ETL06] ( )
RETURNS TABLE
AS
RETURN
	SELECT	 	
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)		AS SystemName
		,TRY_CONVERT(NVARCHAR(100)	,TranCasinoID			)		AS CasinoID
		,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)		AS PlayerID
		,TRY_CONVERT(MONEY			,CreditLimit			)		AS CreditLimit
		,TRY_CONVERT(INT			,IsCreditPlayer			)		AS IsActiveCredit
		,TRY_CONVERT(NVARCHAR(100)	,SetupCasinoID			)		AS SetupCasinoID
		,TRY_CONVERT(NVARCHAR(4000)	,GroupCode				)		AS Note
		,TRY_CONVERT(DATETIME		,StartGamingDt			)		AS StartDateTime
		,TRY_CONVERT(DATETIME		,EndGamingDt			)		AS EndDateTime
		,TRY_CONVERT(INT			,isCurrent				)		AS IsCurrent
		,GETUTCDATE()												AS InsertDateTime
	FROM	 [qcic].[SGA_Dim_PlayerCreditLimit_History];
-- testing  select * from  [tqcic].[udf_SGA_ETL06] ( )

CREATE   FUNCTION [tqcic].[udf_SGA_ETL07] ( )
RETURNS TABLE
AS
RETURN



CREATE FUNCTION [tqcic].[udf_SGA_ETL08](  @MaxDate DATE,  @days int   )
/*
  Inputs: @MaxDate  It is the maximum GamingDate from the table  [tqcids].[Fact_PlayerDeviceRatingDay] where SystemNameInt is equal to 2 (SGA) 
          @days Only for no hardcoding another value
*/
RETURNS TABLE
AS
RETURN
	SELECT * 
		FROM (	--Slot Ratings
		    SELECT 
				--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
				 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'					)								AS SystemName
				,TRY_CONVERT(INT			,2							)								AS SystemNameInt
				--Primary Key (CasinoID, DeviceID, PlayerID, GamingDate)
				,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId					)								AS CasinoID
				,TRY_CONVERT(INT			,b2.CasinoIDInt				)								AS CasinoIDInt
				,TRY_CONVERT(NVARCHAR(100)	,b.AssetMeta				)								AS DeviceID
				,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](b.AssetMeta,2))		AS DeviceIDBigInt
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
				,TRY_CONVERT(NVARCHAR(100)	,b.GameTheme				)								AS GameTheme	
				,TRY_CONVERT(NVARCHAR(100)	,''							)								AS GameThemeCount	
				,TRY_CONVERT(FLOAT			,a.TheoHold					)								AS GameTheoHold
				--Device dimension attributes
				,TRY_CONVERT(FLOAT			,1 - b.GlobalPayout			)								AS DeviceTheoHoldPercent
				,TRY_CONVERT(NVARCHAR(200)	,b.MfgMeta					)								AS DeviceManufacturer
				,TRY_CONVERT(FLOAT			,b.Denom01					)								AS Denomination
				,TRY_CONVERT(NVARCHAR(200)	,b.GameType					)								AS DeviceGameType
				,TRY_CONVERT(NVARCHAR(100)	,''							)								AS DeviceCabinetType 
				,TRY_CONVERT(NVARCHAR(200)	,b.Cabinet					)								AS DeviceCabinetModel	
				,TRY_CONVERT(NVARCHAR(20)	,b.LocationMeta				)								AS DeviceLocation
				,TRY_CONVERT(NVARCHAR(20)	,LEFT(CONCAT(b.LocationMeta,'00'),2))						AS DeviceLocationArea
				,TRY_CONVERT(NVARCHAR(20)	,RIGHT(LEFT(CONCAT(b.LocationMeta,'0000'),4),2))			AS DeviceLocationBank
				,TRY_CONVERT(NVARCHAR(20)	,RIGHT(CONCAT('00',b.LocationMeta),2))						AS DeviceLocationStand
				,TRY_CONVERT(DATE			,b.ConfigStartDTM			)								AS DeviceStartDate
				,TRY_CONVERT(NVARCHAR(50)	,b.SerialMeta				)								AS DeviceSerial	
				,TRY_CONVERT(INT			,b.GameClass				)								AS DeviceGameClass	
				,TRY_CONVERT(INT			,ROUND(b.LeaseFee*100,0)	)								AS DeviceLeaseFeeInt100	
				,TRY_CONVERT(VARCHAR(100)	,concat('LeaseTerm:', b.LeaseTerm, 'LeaseBy:', b.LeaseBy, 'GameTranMachFee:', 
		                                         b.GameTranMachFee, 'PurchaseType:', b.PurchaseType)) 	AS DeviceLeaseAttributes	
				,TRY_CONVERT(VARCHAR(100)	,concat('ReelsLinesConfig:',b.ReelsLinesConfig))			AS DeviceExtraAttributes	
				,TRY_CONVERT(INT			,1							)								AS IsSlotDevice
				,TRY_CONVERT(INT			,0							)								AS IsTableDevice
				,TRY_CONVERT(INT			,0							)								AS IsPokerDevice
				,TRY_CONVERT(INT			,0							)								AS IsOtherDevice
				,TRY_CONVERT(INT			,1							)								AS IsActiveDevice
				,DeviceLocationBigInt = TRY_CONVERT(BIGINT			, CHECKSUM( b.LocationMeta ) )
				,TRY_CONVERT(DATETIME		,GETUTCDATE()				)								AS InsertDateTime
		        FROM	 [qcic].[SGA_Fact_PlayerSlotRating_Day]  AS a WITH (NOLOCK)
		        JOIN	 [qcids].[App_Casino]  AS b2 WITH (NOLOCK)
		        ON		(a.CasinoID = b2.CasinoID AND b2.SystemNameInt = 2)
		        LEFT JOIN [qcic].[SGA_Dim_SlotAttribute_History] AS b WITH (NOLOCK)
		        ON		(a.SlotHistoryId = b.SlotHistoryId AND a.CasinoId = b.CasinoId)
		        LEFT JOIN [qcids].[Dim_Player_SCD] AS p WITH (NOLOCK)
		        ON		(p.SystemNameInt = 2 AND a.PlayerID = p.PlayerID AND p.StartDateTime <= a.GamingDt AND a.GamingDt < p.EndDateTime)
		        LEFT JOIN [qcids].[Dim_PlayerDistanceToCasino_SCD] AS d WITH (NOLOCK)
		        ON		(d.SystemNameInt = 2 AND a.CasinoId = d.CasinoID AND a.PlayerID = d.PlayerID AND 
		                d.StartDateTime <= a.GamingDt AND a.GamingDt < d.EndDateTime)
				WHERE a.GamingDt >= DATEADD(DAY, @days, @MaxDate)
 		   UNION ALL
    		--Table Ratings
    	SELECT 
			--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
				 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'					)								AS SystemName
				,TRY_CONVERT(INT			,2							)								AS SystemNameInt
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
				,TRY_CONVERT(NVARCHAR(100)	,''							)								AS GameThemeCount	
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
				,TRY_CONVERT(VARCHAR(100)	,''							)								AS DeviceLeaseAttributes	
				,TRY_CONVERT(VARCHAR(100), concat('MinBet:',b.MinBet, 'MaxBet:',
		                                          b.MaxBet, 'AreaName:',b.AreaName) )					AS DeviceExtraAttributes	
				,TRY_CONVERT(INT			,0							)								AS IsSlotDevice
				,TRY_CONVERT(INT			,1							)								AS IsTableDevice
				,TRY_CONVERT(INT			,0							)								AS IsPokerDevice
				,TRY_CONVERT(INT			,0							)								AS IsOtherDevice
				,TRY_CONVERT(INT			,1							)								AS IsActiveDevice
				,DeviceLocationBigInt = TRY_CONVERT(BIGINT			, CHECKSUM(b.LocnCode))
				,TRY_CONVERT(DATETIME		,GETUTCDATE()				)								AS InsertDateTime
		        FROM	 [qcic].[SGA_Fact_PlayerTableRating_Day]  AS a WITH (NOLOCK)
		        JOIN	 [qcids].[App_Casino] AS b2 WITH (NOLOCK)
		        ON		(a.CasinoID = b2.CasinoID AND b2.SystemNameInt = 2)
		        LEFT JOIN [qcic].[SGA_Dim_TableAttribute_History] AS b WITH (NOLOCK)
		        ON		(a.TableHistoryId = b.TableHistoryId AND a.CasinoId = b.CasinoId)
		        LEFT JOIN [qcids].[Dim_Player_SCD]  AS p WITH (NOLOCK)
		        ON		(p.SystemNameInt = 2 AND a.PlayerID = p.PlayerID AND p.StartDateTime <= a.GamingDt AND a.GamingDt < p.EndDateTime)
		        LEFT JOIN [qcids].[Dim_PlayerDistanceToCasino_SCD] AS d WITH (NOLOCK)
		        ON		(d.SystemNameInt = 2 AND a.CasinoId = d.CasinoID AND a.PlayerID = d.PlayerID 
		                    AND d.StartDateTime <= a.GamingDt AND a.GamingDt < d.EndDateTime)
				WHERE a.GamingDt >= DATEADD(DAY, @days, @MaxDate)
		    UNION ALL
    		--Free Play and Table Games Coupon
    	SELECT 
			--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
				 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'					)								AS SystemName
				,TRY_CONVERT(INT			,2							)								AS SystemNameInt
				--Primary Key (CasinoID, DeviceID, PlayerID, GamingDate)
				,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId					)								AS CasinoID
				,TRY_CONVERT(INT			,b2.CasinoIDInt				)								AS CasinoIDInt
				,TRY_CONVERT(NVARCHAR(100)	,''							)								AS DeviceID
				,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt]('',2))					AS DeviceIDBigInt
				,TRY_CONVERT(NVARCHAR(100)	,a.PlayerID					)								AS PlayerID
				,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](a.PlayerID,1))			AS PlayerIDBigInt
				,TRY_CONVERT(DATE			,a.GamingDt					)								AS GamingDate
				--Player Related Metrics
				,TRY_CONVERT(DATETIME		,a.FirstRedemptionDtm		)								AS StartDateTime
				,TRY_CONVERT(DATETIME		,a.LastRedemptionDtm		)								AS EndDateTime
				--Gaming TOTAL metrics
				,TRY_CONVERT(INT			,0							)								AS CoinInInt100
				,TRY_CONVERT(INT			,0							)								AS CoinOutInt100
				,TRY_CONVERT(INT			,0							)								AS ActualWinWithFreeplayInt100
				,TRY_CONVERT(INT			,ROUND(-1 * a.ExpUsed*100,0))								AS ActualWinNetFreeplayInt100
				,TRY_CONVERT(INT			,0							)								AS NumberGamesPlayed
				,TRY_CONVERT(INT			,0							)								AS JackpotInt100
				,TRY_CONVERT(INT			,ROUND(a.ExpUsed*100,0)		)								AS FreeplayInt100
				,TRY_CONVERT(INT			,0							)								AS TheoWinWithFreeplayInt100
				,TRY_CONVERT(INT			,ROUND(-1 * a.ExpUsed*100,0))								AS TheoWinNetFreeplayInt100
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
				,TRY_CONVERT(NVARCHAR(100)	,''							)								AS GameThemeCount	
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
				,TRY_CONVERT(DATE			,NULL						)								AS DeviceStartDate
				,TRY_CONVERT(NVARCHAR(50)	,''							)								AS DeviceSerial	
				,TRY_CONVERT(INT			,0							)								AS DeviceGameClass	
				,TRY_CONVERT(INT			,0							)								AS DeviceLeaseFeeInt100
				,TRY_CONVERT(VARCHAR(100)	,''							)								AS DeviceLeaseAttributes	
				,TRY_CONVERT(VARCHAR(100)	,''							)								AS DeviceExtraAttributes
				,TRY_CONVERT(INT			,case when a.TranType = 'Coupon' then 0 else 1 end)			AS IsSlotDevice
				,TRY_CONVERT(INT			,case when a.TranType = 'Coupon' then 1 else 0 end)			AS IsTableDevice
				,TRY_CONVERT(INT			,0							)								AS IsPokerDevice
				,TRY_CONVERT(INT			,0							)								AS IsOtherDevice
				,TRY_CONVERT(INT			,1							)								AS IsActiveDevice
				,DeviceLocationBigInt = TRY_CONVERT(BIGINT			,CHECKSUM(''))
				,TRY_CONVERT(DATETIME		,GETUTCDATE()				)								AS InsertDateTime
   		     FROM	 [qcic].[SGA_Fact_PlayerPrizeExpense_Day] AS a WITH (NOLOCK)
   		     JOIN	 [qcids].[App_Casino] AS b2 WITH (NOLOCK)
	        ON		(a.CasinoID = b2.CasinoID AND b2.SystemNameInt = 2)
	        LEFT JOIN [qcids].[Dim_Player_SCD] AS p WITH (NOLOCK)
	        ON		(p.SystemNameInt = 2 AND a.PlayerID = p.PlayerID AND p.StartDateTime <= a.GamingDt AND a.GamingDt < p.EndDateTime)
	        LEFT JOIN [qcids].[Dim_PlayerDistanceToCasino_SCD] AS d WITH (NOLOCK)
	        ON		(d.SystemNameInt = 2 AND a.CasinoId = d.CasinoID AND a.PlayerID = d.PlayerID 
	                    AND d.StartDateTime <= a.GamingDt AND a.GamingDt < d.EndDateTime)
	        WHERE	 a.TranType IN ('Autoload','Coupon') AND a.ExpUsed > 0
			 AND  a.GamingDt >= DATEADD(DAY, @days, @MaxDate)  )  AS foo 
		WHERE foo.GamingDate >= DATEADD(DAY, @days ,@MaxDate);


CREATE FUNCTION [tqcic].[udf_SGA_ETL12](  @MaxDate DATE,  @days int   )
/*
  Inputs: @MaxDate  It is the maximum PrizeStartDateTime from the table   [tqcids].[Fact_PlayerPrize] where SystemNameInt is equal to 2 (SGA) 
          @days Only for no hardcoding another value

  the last four columns  (NULLs) is to take care of the integrity with the table [qcids].[Fact_PlayerPrize]
*/
RETURNS TABLE
AS
RETURN
--Slot Ratings
	SELECT 
			--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
			 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)								AS SystemName
			,TRY_CONVERT(INT			,2						)								AS SystemNameInt
			,TRY_CONVERT(NVARCHAR(100)	,a.IssueCasinoId		)								AS IssueCasinoID
			,TRY_CONVERT(INT			,c.CasinoIDInt			)								AS IssueCasinoIDInt
			,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId				)								AS RedeemCasinoID
			,TRY_CONVERT(INT			,b.CasinoIDInt			)								AS RedeemCasinoIDInt
			,TRY_CONVERT(NVARCHAR(100)	,PlayerId				)								AS PlayerID
			,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](PlayerID,1))		AS PlayerIDBigInt
			,TRY_CONVERT(DATETIME		,GamingDt				)								AS RedeemDateTime
			,TRY_CONVERT(NVARCHAR(100)	,PrizeId				)								AS PrizeID
			,TRY_CONVERT(NVARCHAR(100)	,TranType				)								AS PrizeType
			,TRY_CONVERT(INT			,ROUND(ExpIssued*100,0)	)								AS PrizeIssueAmountInt100
			,TRY_CONVERT(INT			,ROUND(ExpUsed*100,0)	)								AS PrizeRedeemAmountInt100
			,TRY_CONVERT(INT			,ROUND(PtsUsed*100,0)	)								AS PointForPrizeRedeemAmountInt100
			,TRY_CONVERT(DATETIME		,FirstRedemptionDtm		)								AS PrizeStartDateTime
			,TRY_CONVERT(DATETIME		,LastRedemptionDtm		)								AS PrizeEndDateTime
			,TRY_CONVERT(NVARCHAR(100)	,''						)								AS PrizeCampaignID
			,TRY_CONVERT(NVARCHAR(100)	,''						)								AS PrizeCampaignSegmentID
			,TRY_CONVERT(DATETIME		,GETUTCDATE()			)								AS InsertDateTime
			, NULL AS [IsComp]
	        , NULL AS [PrizeDescription]
	        , NULL  AS [EmployeeID]
	        , NULL AS [EmployeeIDBigInt]
	FROM	 [qcic].[SGA_Fact_PlayerPrizeExpense_Day] a WITH (NOLOCK)
	JOIN	 [qcids].[App_Casino] b WITH (NOLOCK)
	ON		(a.CasinoID = b.CasinoID AND b.SystemNameInt = 2)
	JOIN	 [qcids].[App_Casino] c WITH (NOLOCK)
	ON		(a.IssueCasinoId = c.CasinoID AND c.SystemNameInt = 2)
	WHERE FirstRedemptionDtm  >= DATEADD(DAY, @days ,@MaxDate) ;


CREATE FUNCTION [tqcic].[udf_SGA_ETL15](  @MaxDate DATE,  @days int   )
/*
  Inputs: @MaxDate  It is the maximum GamingDate from the table  [tqcids].[Fact_DeviceDay]  where SystemNameInt is equal to 3 (HRC) 
          @days Only for no hardcoding another value
*/
RETURNS TABLE
AS
RETURN
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
			WHERE a.GamingDt	 >= DATEADD(DAY, @days ,@MaxDate) ;

