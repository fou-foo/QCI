---PASO 0 
CREATE SCHEMA [tqcic];
CREATE SCHEMA [tqcids];
--drop table [tqcic].[SGA_ETL_Runner_Tracker];
SELECT * 
INTO  [tqcic].[HRI_ETL_Runner_Tracker]
FROM [qcic].[HRI_ETL_Runner_Tracker];

----------------------------------------------
-- Paso 1 
--drop table [tqcids].[Dim_Host_SCD];
SELECT * 
INTO [tqcids].[Dim_Host_SCD]
from [qcids].[Dim_Host_SCD] ;

CREATE VIEW [tqcic].[uvw_HRI_ETL01_Dim_Player_SCD]  AS
SELECT DISTINCT
		 TRY_CONVERT(NVARCHAR(100)	,'HRI'					)									AS SystemName
		,TRY_CONVERT(INT			,7						)									AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,a.Loyaltyid			)									AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](a.Loyaltyid,1))		AS PlayerIDBigInt
		,TRY_CONVERT(NVARCHAR(100)	,a.Loyaltyid			)									AS Account
		,TRY_CONVERT(NVARCHAR(20)	,''						)									AS Title
		,TRY_CONVERT(NVARCHAR(50)	,LTRIM(RTRIM(a.LoyaltyidDesc)))								AS LastName
		,TRY_CONVERT(NVARCHAR(50)	,''						)									AS FirstName
		,TRY_CONVERT(NVARCHAR(50)	,LTRIM(RTRIM(a.LoyaltyidDesc)))								AS DisplayName
		,TRY_CONVERT(NVARCHAR(50)	,LTRIM(RTRIM(a.LoyaltyidDesc)))								AS NickNameAlias
		,TRY_CONVERT(NVARCHAR(100)	,a.LoyaltyidAddressLine1)									AS HomeAddress1
		,TRY_CONVERT(NVARCHAR(100)	,a.LoyaltyidAddressLine2)									AS HomeAddress2
		,TRY_CONVERT(NVARCHAR(100)	,a.LoyaltyidCity		)									AS HomeCity
		,TRY_CONVERT(NVARCHAR(50)	,a.StateCode			)									AS HomeStateCode
		,TRY_CONVERT(NVARCHAR(50)	,a.CountryID			)									AS HomeCountryCode
		,TRY_CONVERT(NVARCHAR(20)	,a.LoyaltyidPostalCode	)									AS HomePostalCode
		,TRY_CONVERT(NVARCHAR(20)	,''						)									AS HomeTelephone1Type
		,TRY_CONVERT(NVARCHAR(20)	,a.LoyaltyidTelephone1	)									AS HomeTelephone1
		,TRY_CONVERT(NVARCHAR(20)	,''						)									AS HomeTelephone2Type
		,TRY_CONVERT(NVARCHAR(20)	,''						)									AS HomeTelephone2
		,TRY_CONVERT(NVARCHAR(255)	,a.LoyaltyidEmailaddress1)									AS HomeEmail
		,TRY_CONVERT(NVARCHAR(50)	,''						)									AS AltName
		,TRY_CONVERT(NVARCHAR(100)	,''						)									AS AltAddress1
		,TRY_CONVERT(NVARCHAR(100)	,''						)									AS AltAddress2
		,TRY_CONVERT(NVARCHAR(100)	,''						)									AS AltCity
		,TRY_CONVERT(NVARCHAR(50)	,''						)									AS AltStateCode
		,TRY_CONVERT(NVARCHAR(50)	,''						)									AS AltCountryCode
		,TRY_CONVERT(NVARCHAR(20)	,''						)									AS AltPostalCode
		,TRY_CONVERT(NVARCHAR(20)	,''						)									AS AltTelephone1Type
		,TRY_CONVERT(NVARCHAR(20)	,''						)									AS AltTelephone1
		,TRY_CONVERT(NVARCHAR(20)	,''						)									AS AltTelephone2Type
		,TRY_CONVERT(NVARCHAR(20)	,''						)									AS AltTelephone2
		,TRY_CONVERT(NVARCHAR(255)	,''						)									AS AltEmail
		,TRY_CONVERT(DATE			,case when ISNULL(MemberBirthYear,0) = 0 OR ISNULL(MemberBirthMonth,0) = 0 then '1900-01-01'
										  else concat(MemberBirthYear,'-',MemberBirthMonth,'-1')
										  end				)									AS BirthDate
		,TRY_CONVERT(INT			,0						)									AS IsPrivacy
		,TRY_CONVERT(INT			,0						)									AS IsMailToAlt
		,TRY_CONVERT(INT			,1						)									AS IsNoMail
		,TRY_CONVERT(INT			,0						)									AS IsReturnMail
		,TRY_CONVERT(INT			,0						)									AS IsVIP
		,TRY_CONVERT(INT			,0						)									AS IsLostAndFound
		,TRY_CONVERT(INT			,0						)									AS IsCreditAccount
		,TRY_CONVERT(INT			,0						)									AS IsBanned
		,TRY_CONVERT(INT			,0						)									AS IsInactive
		,TRY_CONVERT(INT			,1						)									AS IsCall
		,TRY_CONVERT(INT			,1						)									AS IsEmailSend
		,TRY_CONVERT(NVARCHAR(100)	,case when a.CountryID = 37 then 'USA' else 'OTH' end)		AS SetupCasinoID
		,TRY_CONVERT(NVARCHAR(100)	,''						)									AS SetupEmployeeID
		,TRY_CONVERT(DATE			,ISNULL(a.MemberEnrolledDate,'2000-01-01'))					AS SetupDate
		,TRY_CONVERT(NVARCHAR(50)	,case when a.GenderID = 0 then 'F'
										  when a.GenderID = 1 then 'M'
										  else 'O' end		)									AS Gender
		,TRY_CONVERT(NVARCHAR(100)	,''						)									AS MailingAddress1
		,TRY_CONVERT(NVARCHAR(100)	,''						)									AS MailingAddress2
		,TRY_CONVERT(NVARCHAR(100)	,''						)									AS MailingCity
		,TRY_CONVERT(NVARCHAR(50)	,''						)									AS MailingStateCode
		,TRY_CONVERT(NVARCHAR(50)	,''						)									AS MailingCountryCode
		,TRY_CONVERT(NVARCHAR(20)	,''						)									AS MailingPostalCode
		,TRY_CONVERT(NVARCHAR(20)	,''						)									AS Address_MatchType
		,TRY_CONVERT(REAL			,b.Latitude				)									AS Address_Latitude
		,TRY_CONVERT(REAL			,b.Longitude			)									AS Address_Longitude
		,TRY_CONVERT(DATETIME		,ISNULL(a.MemberEnrolledDate,'2000-01-01'))					AS StartDateTime
		,TRY_CONVERT(DATETIME		,'2999-12-31'			)									AS EndDateTime
		,TRY_CONVERT(INT			,1						)									AS IsCurrent
		,GETUTCDATE()																			AS InsertDateTime
FROM	 [qcic].[HRI_Dim_Loyalty] a WITH (NOLOCK)
LEFT JOIN [qcids].[Dim_ZipCode] b WITH (NOLOCK)
ON		(a.LoyaltyidPostalCode = b.ZipCode)
WHERE	 a.LoyaltyID <> 0;
--------------------------------------------------
-- PASO 2 
CREATE VIEW [tqcic].[uvw_HRI_ETL02_Dim_Host_SCD]  AS

SELECT
		 TRY_CONVERT(NVARCHAR(100)	,'HRI'										)				AS SystemName
		,TRY_CONVERT(INT			,7											)				AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,case when CountryID = 37 then 'USA' else 'OTH' end)		AS CasinoGroupID
		,TRY_CONVERT(INT			,case when CountryID = 37 then 1 else 2 end	)				AS CasinoGroupIDInt
		,TRY_CONVERT(NVARCHAR(100)	,LOWER(REPLACE(REPLACE(GenMgr,' ',''),',','')))				AS HostID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](LOWER(REPLACE(REPLACE(GenMgr,' ',''),',','')),2)) AS HostIDBigInt
		,TRY_CONVERT(NVARCHAR(50)	,LTRIM(RTRIM(ISNULL(GenMgr,'NoFirstName')))	)				AS HostFirstName
		,TRY_CONVERT(NVARCHAR(50)	,CONCAT('(',StoreDesc,')')					)				AS HostLastName
		,TRY_CONVERT(NVARCHAR(100)	,'GM'										)				AS HostPosition
		,TRY_CONVERT(NVARCHAR(50)	,LOWER(REPLACE(REPLACE(GenMgr,' ',''),',','')))				AS HostCode
		,TRY_CONVERT(DATETIME		,OpenDate									)				AS StartDateTime
		,TRY_CONVERT(DATETIME		,CloseDate									)				AS EndDateTime
		,TRY_CONVERT(INT			,case when CloseDate >= GETDATE()
										  then 1 else 0 end						)				AS IsCurrent
		,GETUTCDATE()																			AS InsertDateTime
FROM	 [qcic].[HRI_Dim_Store] WITH (NOLOCK)
WHERE	 GenMgr NOT IN ('N/A','TBD','General Manager');
------------------
--- PASO 3 
--
------------------------
--- PASO 4 
CREATE VIEW [tqcic].[uvw_HRI_ETL04_Dim_PlayerDistanceToCasino_SCD]  AS

SELECT DISTINCT	 	
		 TRY_CONVERT(NVARCHAR(100)	,'HRI'					)							AS SystemName
		,TRY_CONVERT(INT			,7						)							AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,b.CasinoID				)							AS CasinoID
		,TRY_CONVERT(INT			,b.CasinoIDInt			)							AS CasinoIDInt
		,TRY_CONVERT(NVARCHAR(100)	,a.PlayerID				)							AS PlayerID
		,TRY_CONVERT(BIGINT			,a.PlayerIDBigInt		)							AS PlayerIDBigInt
		,TRY_CONVERT(FLOAT			,0						)							AS PlayerDistanceToCasino --Casino is USA / OTH so this isn't relevant here
		,TRY_CONVERT(DATETIME		,a.StartDateTime		)							AS StartDateTime
		,TRY_CONVERT(DATETIME		,MAX(a.EndDateTime)		)							AS EndDateTime
		,TRY_CONVERT(INT			,a.IsCurrent			)							AS IsCurrent
		,GETUTCDATE()																	AS InsertDateTime
FROM	 [qcids].[Dim_Player_SCD] a WITH (NOLOCK)
CROSS JOIN (
		SELECT	 SystemName
				,SystemNameInt
				,CasinoID
				,CasinoIDInt
		FROM	 [qcids].[App_Casino] WITH (NOLOCK)
		WHERE	 SystemNameInt = 7
		GROUP BY SystemName
				,SystemNameInt
				,CasinoID
				,CasinoIDInt
) b
WHERE	a.SystemNameInt = 7
GROUP BY CasinoID
		,CasinoIDInt
		,PlayerID
		,PlayerIDBigInt
		,StartDateTime
		,IsCurrent;

--------------------------------------------
--- PASO 5 
CREATE VIEW [tqcic].[uvw_HRI_ETL05_Dim_PlayerLoyalty_SCD]  AS

SELECT
		 TRY_CONVERT(NVARCHAR(100)	,'HRI'					)							AS SystemName
		,TRY_CONVERT(INT			,7						)							AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)							AS PlayerID
		,TRY_CONVERT(BIGINT			,PlayerIDBigInt			)							AS PlayerIDBigInt
		,TRY_CONVERT(NVARCHAR(100)	,'Hard Rock Rewards'	)							AS LoyaltyCardLevel --No Card Tiers at HRI yet
		,TRY_CONVERT(DATETIME		,StartDateTime			)							AS StartDateTime
		,TRY_CONVERT(DATETIME		,MAX(EndDateTime)		)							AS EndDateTime
		,TRY_CONVERT(INT			,IsCurrent				)							AS IsCurrent
		,GETUTCDATE()																	AS InsertDateTime
FROM	 [qcids].[Dim_Player_SCD] WITH (NOLOCK)
WHERE	 SystemNameInt = 7
GROUP BY PlayerID
		,PlayerIDBigInt
		,StartDateTime
		,EndDateTime
		,IsCurrent;
---------------------------
-- PASO 6 
CREATE VIEW [tqcic].[uvw_HRI_ETL06_Dim_PlayerCredit_SCD]  AS

SELECT	 	
		 TRY_CONVERT(NVARCHAR(100)	,'HRI'					)									AS SystemName
		,TRY_CONVERT(INT			,7						)									AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,'HRI'					)									AS CasinoID
		,TRY_CONVERT(INT			,0						)									AS CasinoIDInt
		,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)									AS PlayerID
		,TRY_CONVERT(BIGINT			,PlayerIDBigInt			)									AS PlayerIDBigInt
		,TRY_CONVERT(INT			,0						)									AS CreditLimitInt100 --No Credit at HRI
		,TRY_CONVERT(INT			,1						)									AS IsActiveCredit
		,TRY_CONVERT(NVARCHAR(100)	,'HRI'					)									AS SetupCasinoID
		,TRY_CONVERT(NVARCHAR(4000)	,''						)									AS Note
		,TRY_CONVERT(DATETIME		,'2018-01-01'			)									AS StartDateTime
		,TRY_CONVERT(DATETIME		,'2999-12-31'			)									AS EndDateTime
		,TRY_CONVERT(INT			,1						)									AS IsCurrent
		,GETUTCDATE()																			AS InsertDateTime
FROM	 [qcids].[Dim_Player_SCD] WITH (NOLOCK)
WHERE	 SystemNameInt = 7
		 AND 1<>1;
---- paso 7 
CREATE VIEW [tqcic].[uvw_HRI_ETL07_Dim_PlayerCreditElectronic_SCD]  AS
SELECT	 SystemName
		,SystemNameInt
		,CasinoID
		,CasinoIDInt
		,PlayerID
		,PlayerIDBigInt
		,ElectronicCreditType = ''
		,IsEnrolled = 0
		,ElectronicCreditAccountID = ''
		,SignatureID = ''
		,CreatedByID = ''
		,Note = ''
		,StartDateTime
		,EndDateTime
		,IsCurrent
		,InsertDateTime
FROM	 [qcids].[Dim_PlayerHost_SCD] WITH (NOLOCK)
WHERE	 SystemNameInt = 7
		 AND 1<>1; --Electronic Credit not available at this time
-----------------------------
--Paso 8 
CREATE VIEW [tqcic].[uvw_HRI_ETL08_Fact_PlayerDeviceRatingDay]  AS

SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'HRI'						)										AS SystemName
		,TRY_CONVERT(INT			,7							)										AS SystemNameInt

		--Primary Key (CasinoID, DeviceID, PlayerID, GamingDate)
		,TRY_CONVERT(NVARCHAR(100)	,case when IsUSA = 1 then 'USA' else 'OTH' end)						AS CasinoID
		,TRY_CONVERT(INT			,case when IsUSA = 1 then 1 else 2 end)								AS CasinoIDInt
		,TRY_CONVERT(NVARCHAR(100)	,concat(StoreID,'-',SkuID)	)										AS DeviceID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](concat(StoreID,'-',SkuID),2))	AS DeviceIDBigInt
		,TRY_CONVERT(NVARCHAR(100)	,LoyaltyID					)										AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](LoyaltyID,1))					AS PlayerIDBigInt
		,TRY_CONVERT(DATE			,BusinessDate				)										AS GamingDate

		--Player Related Metrics
		,TRY_CONVERT(DATETIME		,BusinessDate				)										AS StartDateTime
		,TRY_CONVERT(DATETIME		,BusinessDate				)										AS EndDateTime

		--Gaming TOTAL metrics
		,TRY_CONVERT(BIGINT			,ROUND(UsdNetsalesAmt*100,0))										AS CoinInInt100
		,TRY_CONVERT(BIGINT			,0							)										AS CoinOutInt100
		,TRY_CONVERT(INT			,ROUND(UsdNetsalesAmt*100,0))										AS ActualWinWithFreeplayInt100
		,TRY_CONVERT(INT			,ROUND((UsdNetsalesAmt - UsdDiscountAmt)*100,0))					AS ActualWinNetFreeplayInt100
		,TRY_CONVERT(INT			,ItemCnt					)										AS NumberGamesPlayed
		,TRY_CONVERT(INT			,0							)										AS JackpotInt100
		,TRY_CONVERT(INT			,ROUND(UsdDiscountAmt*100,0))										AS FreeplayInt100
		,TRY_CONVERT(INT			,ROUND(UsdNetsalesAmt*100,0))										AS TheoWinWithFreeplayInt100
		,TRY_CONVERT(INT			,ROUND((UsdNetsalesAmt - UsdDiscountAmt)*100,0))					AS TheoWinNetFreeplayInt100
		,TRY_CONVERT(INT			,0							)										AS TimePlayedInt100
		,TRY_CONVERT(BIGINT			,0							)										AS CashInInt100
		,TRY_CONVERT(BIGINT			,0							)										AS ChipsInInt100
		,TRY_CONVERT(BIGINT			,0							)										AS CreditInInt100
		,TRY_CONVERT(BIGINT			,0							)										AS WalkedWithInt100

		--Player dimension attributes
		,TRY_CONVERT(VARCHAR(100)	 ,PostalCode				)										AS PlayerZipCode
		,TRY_CONVERT(NVARCHAR(50)	 ,''						)										AS PlayerGender
		,TRY_CONVERT(DATE			 ,'1900-01-01'				)										AS PlayerEnrollmentDate
		,TRY_CONVERT(DATE			 ,'1900-01-01'				)										AS PlayerBirthDate
		,TRY_CONVERT(FLOAT			 ,0							)										AS PlayerAddressDistance																								

		--Game dimension attributes (most of the time there is one game per device, this will only show the MOST COMMON game)
		,TRY_CONVERT(NVARCHAR(100)	,FamilyGroupDesc			)										AS GameTheme	
		,TRY_CONVERT(NVARCHAR(100)	,''							)										AS GameThemeCount	
		,TRY_CONVERT(FLOAT			,0							)										AS GameTheoHold

		--Device dimension attributes
		,TRY_CONVERT(FLOAT			,0							)										AS DeviceTheoHoldPercent
		,TRY_CONVERT(NVARCHAR(200)	,StoreDesc					)										AS DeviceManufacturer
		,TRY_CONVERT(FLOAT			,0							)										AS Denomination
		,TRY_CONVERT(NVARCHAR(200)	,SkuCategoryDesc			)										AS DeviceGameType
		,TRY_CONVERT(NVARCHAR(100)	,FamilyGroupingDesc			)										AS DeviceCabinetType 
		,TRY_CONVERT(NVARCHAR(200)	,''							)										AS DeviceCabinetModel	
		,TRY_CONVERT(NVARCHAR(20)	,CONCAT(MajorGroupTypeDesc,MajorGroupDesc,''))						AS DeviceLocation
		,TRY_CONVERT(NVARCHAR(20)	,MajorGroupTypeDesc			)										AS DeviceLocationArea
		,TRY_CONVERT(NVARCHAR(20)	,MajorGroupDesc				)										AS DeviceLocationBank
		,TRY_CONVERT(NVARCHAR(20)	,''							)										AS DeviceLocationStand
		,TRY_CONVERT(DATE			,'1900-01-01'				)										AS DeviceStartDate
		,TRY_CONVERT(NVARCHAR(50)	,concat(StoreID,'-',SkuID)	)										AS DeviceSerial	
		,TRY_CONVERT(INT			,0							)										AS DeviceGameClass	
		,TRY_CONVERT(INT			,ROUND(UsdPrepCostAmt*100,0))										AS DeviceLeaseFeeInt100	
		,TRY_CONVERT(NVARCHAR(4000)	,''							)										AS DeviceLeaseAttributes	
		,TRY_CONVERT(NVARCHAR(4000)	,''							)										AS DeviceExtraAttributes	
		,TRY_CONVERT(INT			,1							)										AS IsSlotDevice
		,TRY_CONVERT(INT			,0							)										AS IsTableDevice
		,TRY_CONVERT(INT			,0							)										AS IsPokerDevice
		,TRY_CONVERT(INT			,0							)										AS IsOtherDevice
		,TRY_CONVERT(INT			,1							)										AS IsActiveDevice
		,TRY_CONVERT(BIGINT			,CHECKSUM(CONCAT(MajorGroupTypeDesc,MajorGroupDesc,'')))			AS DeviceLocationBigInt
		,TRY_CONVERT(DATETIME		,GETUTCDATE()				)										AS InsertDateTime
FROM	 [qcic].[uvw_HRI_SaleSKU1] WITH (NOLOCK)
WHERE	 LoyaltyID <> -1 AND LoyaltyID <> 0;
----------------------- Paso 9 
-------------
CREATE VIEW [tqcic].[uvw_HRI_ETL12_Fact_PlayerPrize]  AS
--Slot Ratings
SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'HRI'					)								AS SystemName
		,TRY_CONVERT(INT			,7						)								AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,'USA'					)								AS IssueCasinoID
		,TRY_CONVERT(INT			,1						)								AS IssueCasinoIDInt
		,TRY_CONVERT(NVARCHAR(100)	,'USA'					)								AS RedeemCasinoID
		,TRY_CONVERT(INT			,1						)								AS RedeemCasinoIDInt
		,TRY_CONVERT(NVARCHAR(100)	,0						)								AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](0,1))				AS PlayerIDBigInt
		,TRY_CONVERT(DATETIME		,'1900-01-01'			)								AS RedeemDateTime
		,TRY_CONVERT(NVARCHAR(100)	,''						)								AS PrizeID
		,TRY_CONVERT(NVARCHAR(100)	,''						)								AS PrizeType
		,TRY_CONVERT(INT			,0						)								AS PrizeIssueAmountInt100
		,TRY_CONVERT(INT			,0						)								AS PrizeRedeemAmountInt100
		,TRY_CONVERT(INT			,0						)								AS PointForPrizeRedeemAmountInt100
		,TRY_CONVERT(DATETIME		,'1900-01-01'			)								AS PrizeStartDateTime
		,TRY_CONVERT(DATETIME		,'1900-01-01'			)								AS PrizeEndDateTime
		,TRY_CONVERT(NVARCHAR(100)	,''						)								AS PrizeCampaignID
		,TRY_CONVERT(NVARCHAR(100)	,''						)								AS PrizeCampaignSegmentID
		,TRY_CONVERT(DATETIME		,GETUTCDATE()			)								AS InsertDateTime
FROM	 [qcic].[uvw_HRI_SaleSKU1] WITH (NOLOCK)
WHERE	 LoyaltyID <> -1 AND LoyaltyID <> 0
		 AND 1<>1; --No Prizes at this time at HRI
--------------------------
-- paso 10 
CREATE VIEW [tqcic].[uvw_HRI_ETL14_Fact_PlayerEvaluationCurrent]  AS
--Slot Ratings
SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'HRI'					)								AS SystemName
		,TRY_CONVERT(INT			,7						)								AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,LoyaltyID				)								AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](LoyaltyID,1))		AS PlayerIDBigInt
		,TRY_CONVERT(NVARCHAR(100)	,'HRI'					)								AS CasinoGroupID
		,TRY_CONVERT(INT			,0						)								AS CasinoGroupIDInt
		,TRY_CONVERT(INT			,ROUND(SUM(UsdNetsalesAmt - UsdDiscountAmt)*100,0))		AS EvaluationScoreInt100
		,TRY_CONVERT(DATETIME		,GETUTCDATE()			)								AS InsertDateTime
FROM	 [qcic].[uvw_HRI_SaleSKU1] WITH (NOLOCK)
WHERE	 LoyaltyID <> -1 AND LoyaltyID <> 0
GROUP BY LoyaltyID;

--------------------
-- paso 11 
CREATE VIEW [tqcic].[uvw_HRI_ETL15_Fact_DeviceDay]  AS

SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'HRI'						)										AS SystemName
		,TRY_CONVERT(INT			,7							)										AS SystemNameInt

		--Primary Key (CasinoID, DeviceID, PlayerID, GamingDate)
		,TRY_CONVERT(NVARCHAR(100)	,case when IsUSA = 1 then 'USA' else 'OTH' end)						AS CasinoID
		,TRY_CONVERT(INT			,case when IsUSA = 1 then 1 else 2 end)								AS CasinoIDInt
		,TRY_CONVERT(NVARCHAR(100)	,concat(StoreID,'-',SkuID)	)										AS DeviceID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](concat(StoreID,'-',SkuID),2))	AS DeviceIDBigInt
		,TRY_CONVERT(DATE			,BusinessDate				)										AS GamingDate

		--Gaming TOTAL metrics
		,TRY_CONVERT(BIGINT			,ROUND(SUM(UsdNetsalesAmt)*100,0))									AS CoinInInt100
		,TRY_CONVERT(BIGINT			,0							)										AS CoinOutInt100
		,TRY_CONVERT(INT			,ROUND(SUM(UsdNetsalesAmt)*100,0))									AS ActualWinWithFreeplayInt100
		,TRY_CONVERT(INT			,ROUND(SUM(UsdNetsalesAmt - UsdDiscountAmt)*100,0))					AS ActualWinNetFreeplayInt100
		,TRY_CONVERT(INT			,SUM(ItemCnt)				)										AS NumberGamesPlayed
		,TRY_CONVERT(INT			,0							)										AS JackpotInt100
		,TRY_CONVERT(INT			,ROUND(SUM(UsdDiscountAmt)*100,0))									AS FreeplayInt100
		,TRY_CONVERT(INT			,ROUND(SUM(UsdNetsalesAmt)*100,0))									AS TheoWinWithFreeplayInt100
		,TRY_CONVERT(INT			,ROUND(SUM(UsdNetsalesAmt - UsdDiscountAmt)*100,0))					AS TheoWinNetFreeplayInt100
		,TRY_CONVERT(INT			,0							)										AS TimePlayedInt100
		,TRY_CONVERT(BIGINT			,0							)										AS CashInInt100
		,TRY_CONVERT(BIGINT			,0							)										AS ChipsInInt100
		,TRY_CONVERT(BIGINT			,0							)										AS CreditInInt100
		,TRY_CONVERT(BIGINT			,0							)										AS WalkedWithInt100

		--Game dimension attributes (most of the time there is one game per device, this will only show the MOST COMMON game)
		,TRY_CONVERT(NVARCHAR(100)	,FamilyGroupDesc			)										AS GameTheme	
		,TRY_CONVERT(NVARCHAR(100)	,''							)										AS GameThemeCount	
		,TRY_CONVERT(FLOAT			,0							)										AS GameTheoHold

		--Device dimension attributes
		,TRY_CONVERT(FLOAT			,0							)										AS DeviceTheoHoldPercent
		,TRY_CONVERT(NVARCHAR(200)	,StoreDesc					)										AS DeviceManufacturer
		,TRY_CONVERT(FLOAT			,0							)										AS Denomination
		,TRY_CONVERT(NVARCHAR(200)	,SkuCategoryDesc			)										AS DeviceGameType
		,TRY_CONVERT(NVARCHAR(100)	,FamilyGroupingDesc			)										AS DeviceCabinetType 
		,TRY_CONVERT(NVARCHAR(200)	,''							)										AS DeviceCabinetModel	
		,TRY_CONVERT(NVARCHAR(20)	,CONCAT(MajorGroupTypeDesc,MajorGroupDesc,''))						AS DeviceLocation
		,TRY_CONVERT(NVARCHAR(20)	,MajorGroupTypeDesc			)										AS DeviceLocationArea
		,TRY_CONVERT(NVARCHAR(20)	,MajorGroupDesc				)										AS DeviceLocationBank
		,TRY_CONVERT(NVARCHAR(20)	,''							)										AS DeviceLocationStand
		,TRY_CONVERT(DATE			,'1900-01-01'				)										AS DeviceStartDate
		,TRY_CONVERT(NVARCHAR(50)	,concat(StoreID,'-',SkuID)	)										AS DeviceSerial	
		,TRY_CONVERT(INT			,0							)										AS DeviceGameClass	
		,TRY_CONVERT(INT			,ROUND(SUM(UsdPrepCostAmt)*100,0))									AS DeviceLeaseFeeInt100	
		,TRY_CONVERT(NVARCHAR(4000)	,''							)										AS DeviceLeaseAttributes	
		,TRY_CONVERT(NVARCHAR(4000)	,''							)										AS DeviceExtraAttributes	
		,TRY_CONVERT(INT			,1							)										AS IsSlotDevice
		,TRY_CONVERT(INT			,0							)										AS IsTableDevice
		,TRY_CONVERT(INT			,0							)										AS IsPokerDevice
		,TRY_CONVERT(INT			,0							)										AS IsOtherDevice
		,TRY_CONVERT(INT			,1							)										AS IsActiveDevice
		,TRY_CONVERT(BIGINT			,CHECKSUM(CONCAT(MajorGroupTypeDesc,MajorGroupDesc,'')))			AS DeviceLocationBigInt
		,TRY_CONVERT(DATETIME		,GETUTCDATE()				)										AS InsertDateTime
FROM	 [qcic].[uvw_HRI_SaleSKU1] WITH (NOLOCK)
GROUP BY SkuID
		,StoreID
		,SkuCategoryDesc
		,StoreDesc
		,FamilyGroupDesc
		,FamilyGroupingDesc
		,MajorGroupDesc
		,MajorGroupTypeDesc
		,IsUSA
		,BusinessDate;

