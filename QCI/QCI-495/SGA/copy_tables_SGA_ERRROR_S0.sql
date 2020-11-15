SELECT *
INTO  [tqcic].[SGA_ETL_Runner_Tracker] 
FROM [qcic].[SGA_ETL_Runner_Tracker]; 
----------------------------------------------
-- requeri crearla a pie por el indice 
CREATE TABLE [tqcids].[App_CasinoGroup](
	[SystemName] [nvarchar](100) NULL,
	[CasinoGroupID] [nvarchar](100) NULL,
	[CasinoName] [nvarchar](100) NULL,
	[CasinoAddress] [nvarchar](100) NULL,
	[CasinoCity] [nvarchar](100) NULL,
	[CasinoStateCode] [nvarchar](100) NULL,
	[CasinoPostalCode] [nvarchar](100) NULL,
	[CasinoCountryCode] [nvarchar](100) NULL,
	[CasinoWeatherHref] [nvarchar](255) NULL,
	[CasinoLatitude] [float] NULL,
	[CasinoLongitude] [float] NULL,
	[InsertDateTime] [datetime] NULL
) ON [PRIMARY];
--CREATE CLUSTERED COLUMNSTORE INDEX [cci_tqcids_App_CasinoGroup] ON [tqcids].[App_CasinoGroup] WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [PRIMARY];
DROP INDEX [cci_qcids_App_CasinoGroup] ON [qcids].[App_CasinoGroup];---- si esto sigue igual todo va a ser tardadisimo 

INSERT INTO   [tqcids].[App_CasinoGroup]
SELECT * 
FROM  [qcids].[App_CasinoGroup];

------------------------------
DROP INDEX [cci_qcids_App_CasinoGroupCasino] ON [qcids].[App_CasinoGroupCasino];

SELECT * 
INTO [tqcids].[App_CasinoGroupCasino]
from [qcids].[App_CasinoGroupCasino] ;
--------------------------------------------------
DROP INDEX [cci_qcids_App_ContactType] ON [qcids].[App_ContactType];

select * 
into [tqcids].[App_ContactType]
from [qcids].[App_ContactType];
------------------
DROP INDEX [cci_qcids_App_PreferenceType] ON [qcids].[App_PreferenceType];


select * 
into [tqcids].[App_PreferenceType] 
from [qcids].[App_PreferenceType] ;
------------------------
DROP INDEX [cci_qcids_App_PreferenceValue] ON [qcids].[App_PreferenceValue];
select * 
into [tqcids].[App_PreferenceValue] 
from [qcids].[App_PreferenceValue];
--------------------------------------------
DROP INDEX [cci_qcids_App_EvaluationGroup] ON [qcids].[App_EvaluationGroup]

select *
into [tqcids].[App_EvaluationGroup] 
from [qcids].[App_EvaluationGroup] ;
-------------------------------------------------
DROP INDEX [cci_qcids_App_CasinoTarget] ON [qcids].[App_CasinoTarget];
select * 
into [tqcids].[App_CasinoTarget]
from [qcids].[App_CasinoTarget] ;
-----------------------------------------
DROP INDEX [cci_qcids_App_CasinoTask_SCD] ON [qcids].[App_CasinoTask_SCD];
select * 
into [tqcids].[App_CasinoTask_SCD] 
from [qcids].[App_CasinoTask_SCD];
--------------------------------------------------
DROP INDEX [cci_qcic_SGA_Dim_CMPEmployees] ON [qcic].[SGA_Dim_CMPEmployees];

CREATE VIEW [tqcic].[uvw_SGA_ETL01_Dim_Host_SCD]  AS

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

DROP INDEX [cci_qcids_Dim_Host_SCD] ON [qcids].[Dim_Host_SCD];

select  * 
into [tqcids].[Dim_Host_SCD]
from [qcids].[Dim_Host_SCD] ; 
----------------------------
DROP INDEX [cci_qcids_Dim_Player_SCD] ON [qcids].[Dim_Player_SCD];

select * 
into [tqcids].[Dim_Player_SCD]
from [qcids].[Dim_Player_SCD]; 

DROP INDEX [cci_qcic_SGA_uvw_MarketablePlayersAttributes] ON [qcic].[SGA_uvw_MarketablePlayersAttributes]; 


CREATE VIEW [tqcic].[uvw_SGA_ETL02_Dim_Player_SCD]  AS
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
-----------------------------------
DROP INDEX [cci_qcids_Dim_PlayerDistanceToCasino_SCD] ON [qcids].[Dim_PlayerDistanceToCasino_SCD];
select * 
into [tqcids].[Dim_PlayerDistanceToCasino_SCD] 
from [qcids].[Dim_PlayerDistanceToCasino_SCD]; 

CREATE VIEW [tqcic].[uvw_SGA_ETL03_Dim_PlayerDistanceToCasino_SCD]  AS

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
		FROM	 [qcids].[App_CasinoGroup] a
		JOIN	 [qcids].[App_CasinoGroupCasino] b
		ON		(a.SystemName = b.SystemName AND a.CasinoGroupID = b.CasinoGroupID AND a.CasinoGroupID = b.CasinoID)
		WHERE	 a.SystemName = 'SGAHRI'
) b
WHERE	 a.isBanned = 0 AND b.SystemName = 'SGAHRI';
------------------
--PASO 4 
DROP INDEX [cci_qcids_Dim_PlayerLoyalty_SCD] ON [qcids].[Dim_PlayerLoyalty_SCD];
SELECT * 
INTO [tqcids].[Dim_PlayerLoyalty_SCD]
FROM [qcids].[Dim_PlayerLoyalty_SCD];


CREATE VIEW [tqcic].[uvw_SGA_ETL04_Dim_PlayerLoyalty_SCD]  AS

SELECT
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)							AS SystemName
		,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)							AS PlayerID
		,TRY_CONVERT(NVARCHAR(100)	,P_ClubStatus			)							AS LoyaltyCardLevel
		,TRY_CONVERT(DATETIME		,InsertDT				)							AS StartDateTime
		,TRY_CONVERT(DATETIME		,RetireDT				)							AS EndDateTime
		,TRY_CONVERT(INT			,isCurrent				)							AS IsCurrent
		,GETUTCDATE()																	AS InsertDateTime
FROM	 [qcic].[SGA_uvw_MarketablePlayersAttributes];
-- TESTING SELECT * FROM [tqcic].[uvw_SGA_ETL04_Dim_PlayerLoyalty_SCD]

-----------------------
-- paso 5 
DROP INDEX [cci_qcids_Dim_PlayerHost_SCD] ON [qcids].[Dim_PlayerHost_SCD]

select * 
into [tqcids].[Dim_PlayerHost_SCD]
from [qcids].[Dim_PlayerHost_SCD];


DROP INDEX [cci_qcic_SGA_Fact_PlayerHostHistory_Primary] ON [qcic].[SGA_Fact_PlayerHostHistory_Primary];
DROP INDEX [cci_qcic_SGA_Fact_PlayerHostHistory_Secondary] ON [qcic].[SGA_Fact_PlayerHostHistory_Secondary];



CREATE VIEW [tqcic].[uvw_SGA_ETL05_Dim_PlayerHost_SCD]  AS

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
FROM	 [qcic].[SGA_Fact_PlayerHostHistory_Secondary]
-- testing select * from [tqcic].[uvw_SGA_ETL05_Dim_PlayerHost_SCD]
----
-- PASO 6 
DROP INDEX [cci_qcids_Dim_PlayerCredit_SCD] ON [qcids].[Dim_PlayerCredit_SCD];

SELECT * 
INTO [tqcids].[Dim_PlayerCredit_SCD]
FROM [qcids].[Dim_PlayerCredit_SCD];

DROP INDEX [cci_qcic_SGA_Dim_PlayerCreditLimit_History] ON [qcic].[SGA_Dim_PlayerCreditLimit_History];

CREATE VIEW [tqcic].[uvw_SGA_ETL06_Dim_PlayerCredit_SCD]  AS

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
-- TESTING SELECT * FROM [tqcic].[uvw_SGA_ETL06_Dim_PlayerCredit_SCD] 
------------ paso 7 
-----------------
DROP INDEX [cci_qcic_SGA_Dim_PlayerECreditEnrollment_History] ON [qcic].[SGA_Dim_PlayerECreditEnrollment_History];

DROP INDEX [cci_qcids_App_Casino] ON [qcids].[App_Casino]


CREATE VIEW [tqcic].[uvw_SGA_ETL07_Dim_PlayerCreditElectronic_SCD] 
AS
	SELECT	 SystemName	,SystemNameInt, CasinoID
		,CasinoIDInt, PlayerID, PlayerIDBigInt, ElectronicCreditType
		,IsEnrolled, ElectronicCreditAccountID, SignatureID
		,CreatedByID, Note, StartDateTime
		,EndDateTime = LEAD(StartDateTime, 1, {d '2999-12-31'}) OVER (  PARTITION BY SystemName, CasinoID, PlayerID ,ElectronicCreditType
				                                                        ORDER BY StartDateTime )
		,IsCurrent , InsertDateTime
		FROM	(
			SELECT	 	
				 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)		AS SystemName
				,TRY_CONVERT(INT			,2						)		AS SystemNameInt
				,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId				)		AS CasinoID
				,TRY_CONVERT(INT			,b.CasinoIDInt			)		AS CasinoIDInt
				,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)		AS PlayerID
				,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt]( PlayerID, 1) ) AS PlayerIDBigInt
				,TRY_CONVERT(NVARCHAR(100)	,
							case when EnrollOption = 2 then 'Power Bank'
							else 'Self-Pay Jackpots' end  			)		AS ElectronicCreditType
				,TRY_CONVERT(INT			,IsEnroll				)		AS IsEnrolled
				,TRY_CONVERT(NVARCHAR(100)	,Acct					)		AS ElectronicCreditAccountID
				,TRY_CONVERT(NVARCHAR(100)	,SignatureId			)		AS SignatureID
				,TRY_CONVERT(NVARCHAR(100)	,CreatedBy				)		AS CreatedByID
				,TRY_CONVERT(NVARCHAR(4000)	,concat(ReasonDesc,' ',Remarks) )	AS Note
				,TRY_CONVERT(DATETIME		,CreatedDtm				)		AS StartDateTime
				,TRY_CONVERT(INT			,isCurrent				)		AS IsCurrent
				,GETUTCDATE()												AS InsertDateTime
				FROM	 (
					SELECT	 *
					FROM	 [qcic].[SGA_Dim_PlayerECreditEnrollment_History] WITH (NOLOCK)
					WHERE	 EnrollOption IN (2,3)
			UNION ALL
			SELECT	 EnrollmentId, Acct , PlayerId
						,IsEnroll = case when EnrollOption = 0 then 0 else IsEnroll end
						,EnrollOption = 2, CasinoId
						,GamingDt, ReasonDesc
						,Remarks, SignatureId
						,CreatedBy, ModifiedBy
						,CreatedDtm, ModifiedDtm
						,IsCurrent, InsertDtm
				FROM	 [qcic].[SGA_Dim_PlayerECreditEnrollment_History] WITH (NOLOCK)
				WHERE	 EnrollOption IN (0,1)
			UNION ALL
			SELECT	 EnrollmentId, Acct
						,PlayerId, IsEnroll = case when EnrollOption = 0 then 0 else IsEnroll end
						,EnrollOption = 3 , CasinoId
						,GamingDt, ReasonDesc
						,Remarks, SignatureId
						,CreatedBy, ModifiedBy
						,CreatedDtm, ModifiedDtm
						,IsCurrent, InsertDtm
				FROM	 [qcic].[SGA_Dim_PlayerECreditEnrollment_History] WITH (NOLOCK)
				WHERE	 EnrollOption IN (0,1) 	)  AS a
		JOIN	 [qcids].[App_Casino] AS b 
		ON		(a.CasinoID = b.CasinoID AND b.SystemNameInt = 2)		) x;


		SELECT * FROM  [qcic].[SGA_Dim_PlayerECreditEnrollment_History]
		SELECT * from [qcids].[App_Casino] --- aqui hay pedo con esta vista NO ESTA EL CAMPO SystemNameInt
-------------------
--- paso 8 
DROP INDEX [cci_qcids_Fact_PlayerDeviceRatingDay] ON [qcids].[Fact_PlayerDeviceRatingDay]; 


select * 
into [tqcids].[Fact_PlayerDeviceRatingDay]
from  [qcids].[Fact_PlayerDeviceRatingDay];

CREATE VIEW [tqcic].[uvw_SGA_ETL08_Fact_PlayerDeviceRatingDay]  AS
--Slot Ratings
	SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'					)								AS SystemName
		--Primary Key (CasinoID, DeviceID, PlayerID, GamingDate)
		,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId					)								AS CasinoID
		,TRY_CONVERT(NVARCHAR(100)	,b.AssetMeta				)								AS DeviceID
		,TRY_CONVERT(NVARCHAR(100)	,a.PlayerID					)								AS PlayerID
		,TRY_CONVERT(DATE			,a.GamingDt					)								AS GamingDate
		--DeNormalized SCD Dates
		,TRY_CONVERT(DATE			,b.ConfigStartDTM			)								AS DeviceStartDateSCD 
		--Player Related Metrics
		,TRY_CONVERT(DATETIME		,a.RatingStartDtm			)								AS StartDateTime
		,TRY_CONVERT(DATETIME		,a.RatingEndDtm				)								AS EndDateTime
		--Gaming TOTAL metrics
		,TRY_CONVERT(NUMERIC(27,10)	 ,a.Bet						)								AS CoinIn     
		,TRY_CONVERT(NUMERIC(27,10)	 ,a.PaidOut					)								AS CoinOut
		,TRY_CONVERT(NUMERIC(27,10)	 ,a.Actual					)								AS ActualWinWithFreeplay
		,TRY_CONVERT(NUMERIC(27,10)	 ,a.Actual					)								AS ActualWinNetFreeplay
		,TRY_CONVERT(INT			 ,a.GamesPlayed				)								AS NumberGamesPlayed
		,TRY_CONVERT(NUMERIC(27,10)	 ,Jackpot					)								AS Jackpot
		,TRY_CONVERT(NUMERIC(27,10)	 ,0							)								AS Freeplay
		,TRY_CONVERT(NUMERIC(27,10)	 ,a.Theo					)								AS TheoWinWithFreeplay  
		,TRY_CONVERT(NUMERIC(27,10)	 ,a.Theo					)								AS TheoWinNetFreeplay  
		,TRY_CONVERT(NUMERIC(27,10)  ,MinsPlayed				)								AS TimePlayed
		,TRY_CONVERT(NUMERIC(27,10)  ,CashBuyIn					)								AS FrontIn   
		,TRY_CONVERT(NUMERIC(27,10)  ,0							)								AS ChipsIn   
		,TRY_CONVERT(NUMERIC(27,10)  ,0							)								AS CreditIn   
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
		,TRY_CONVERT(FLOAT			,1 - b.GlobalPayout			)								AS DeviceCalcTheoHoldPercent	
		,TRY_CONVERT(FLOAT			,b.DenomTheoHold01			)								AS DeviceBaseTheoHoldPercent
		,TRY_CONVERT(NVARCHAR(200)	,b.MfgMeta					)								AS DeviceManufacturer
		,TRY_CONVERT(FLOAT			,b.Denom01					)								AS Denomination
		,TRY_CONVERT(NVARCHAR(200)	,b.GameType					)								AS DeviceGameType	
		,TRY_CONVERT(NVARCHAR(200)	,b.Cabinet					)								AS DeviceCabinetModel	
		,TRY_CONVERT(NVARCHAR(20)	,b.LocationMeta				)								AS DeviceLocation		
		,TRY_CONVERT(DATE			,b.ConfigStartDTM			)								AS DeviceSCDStartDate	
		,TRY_CONVERT(INT			,b.IsCurrent				)								AS DeviceActiveFlag
		,TRY_CONVERT(NVARCHAR(50)	,b.SerialMeta				)								AS DeviceSerial	
		,TRY_CONVERT(INT			,b.GameClass				)								AS DeviceGameClass	
		,TRY_CONVERT(FLOAT			,1 - b.GlobalPayout			)								AS DeviceTheoHoldPercent	
		,TRY_CONVERT(MONEY			,b.LeaseFee					)								AS DeviceLeaseFee	
		,TRY_CONVERT(VARCHAR(100)	,concat('LeaseTerm:',b.LeaseTerm, 'LeaseBy:', b.LeaseBy, 'GameTranMachFee:',b.GameTranMachFee,
											'PurchaseType:',b.PurchaseType) )    				AS DeviceLeaseAttributes	
		,TRY_CONVERT(VARCHAR(100)	,concat('ReelsLinesConfig:', b.ReelsLinesConfig ) ) 			AS DeviceExtraAttributes	
		,TRY_CONVERT(NVARCHAR(100)	,''							)								AS DeviceCabinetType 
		,TRY_CONVERT(NVARCHAR(100)	,RIGHT( LEFT(concat( b.LocationMeta, '0000'), 4), 2))			AS DeviceBankName
		,TRY_CONVERT(NVARCHAR(100)	,RIGHT( concat('00', b.LocationMeta), 2))						AS DeviceStand
		,TRY_CONVERT(INT			,0							)								AS IsTableDevice
		,TRY_CONVERT(INT			,''							)								AS PaddedBankName  --Note sure about this field
		,TRY_CONVERT(DATETIME		,GETUTCDATE()				)								AS InsertDateTime
		FROM	 [qcic].[SGA_Fact_PlayerSlotRating_Day]  AS a
		LEFT JOIN [qcic].[SGA_Dim_SlotAttribute_History] AS b
		ON		(a.SlotHistoryId = b.SlotHistoryId AND a.CasinoId = b.CasinoId)
		LEFT JOIN [qcids].[Dim_Player_SCD]  AS p
		ON		(p.SystemName = 'SGAHRI' AND a.PlayerID = p.PlayerID AND p.StartDateTime <= a.GamingDt AND a.GamingDt < p.EndDateTime)
		LEFT JOIN [qcids].[Dim_PlayerDistanceToCasino_SCD] d
		ON		(d.SystemName = 'SGAHRI' AND a.CasinoId = d.CasinoID AND a.PlayerID = d.PlayerID AND d.StartDateTime <= a.GamingDt AND a.GamingDt < d.EndDateTime)
	UNION ALL
	--Table Ratings
	SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'					)								AS SystemName
		--Primary Key (CasinoID, DeviceID, PlayerID, GamingDate)
		,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId					)								AS CasinoID
		,TRY_CONVERT(NVARCHAR(100)	,b.GameId					)								AS DeviceID
		,TRY_CONVERT(NVARCHAR(100)	,a.PlayerID					)								AS PlayerID
		,TRY_CONVERT(DATE			,a.GamingDt					)								AS GamingDate
		--DeNormalized SCD Dates
		,TRY_CONVERT(DATE			,b.InsertDtm				)								AS DeviceStartDateSCD 
		--Player Related Metrics
		,TRY_CONVERT(DATETIME		,a.RatingStartDtm			)								AS StartDateTime
		,TRY_CONVERT(DATETIME		,a.RatingEndDtm				)								AS EndDateTime
		--Gaming TOTAL metrics
		,TRY_CONVERT(NUMERIC(27,10)	 ,a.Bet						)								AS CoinIn     
		,TRY_CONVERT(NUMERIC(27,10)	 ,a.PaidOut					)								AS CoinOut
		,TRY_CONVERT(NUMERIC(27,10)	 ,a.Actual					)								AS ActualWinWithFreeplay
		,TRY_CONVERT(NUMERIC(27,10)	 ,a.Actual					)								AS ActualWinNetFreeplay
		,TRY_CONVERT(INT			 ,a.GamesPlayed				)								AS NumberGamesPlayed
		,TRY_CONVERT(NUMERIC(27,10)	 ,0							)								AS Jackpot
		,TRY_CONVERT(NUMERIC(27,10)	 ,0							)								AS Freeplay
		,TRY_CONVERT(NUMERIC(27,10)	 ,a.Theo					)								AS TheoWinWithFreeplay  
		,TRY_CONVERT(NUMERIC(27,10)	 ,a.Theo					)								AS TheoWinNetFreeplay  
		,TRY_CONVERT(NUMERIC(27,10)  ,MinsPlayed				)								AS TimePlayed
		,TRY_CONVERT(NUMERIC(27,10)  ,CashBuyIn					)								AS FrontIn   
		,TRY_CONVERT(NUMERIC(27,10)  ,ChipBuyIn					)								AS ChipsIn   
		,TRY_CONVERT(NUMERIC(27,10)  ,CreditBuyIn				)								AS CreditIn   
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
		,TRY_CONVERT(FLOAT			,b.TheoHold					)								AS DeviceCalcTheoHoldPercent	
		,TRY_CONVERT(FLOAT			,b.TheoHold					)								AS DeviceBaseTheoHoldPercent
		,TRY_CONVERT(NVARCHAR(200)	,''							)								AS DeviceManufacturer
		,TRY_CONVERT(FLOAT			,''							)								AS Denomination
		,TRY_CONVERT(NVARCHAR(200)	,b.GameCode					)								AS DeviceGameType	
		,TRY_CONVERT(NVARCHAR(200)	,''							)								AS DeviceCabinetModel	
		,TRY_CONVERT(NVARCHAR(20)	,b.LocnCode					)								AS DeviceLocation		
		,TRY_CONVERT(DATE			,b.InsertDtm				)								AS DeviceSCDStartDate	
		,TRY_CONVERT(INT			,1							)								AS DeviceActiveFlag
		,TRY_CONVERT(NVARCHAR(50)	,''							)								AS DeviceSerial	
		,TRY_CONVERT(INT			,''							)								AS DeviceGameClass	
		,TRY_CONVERT(FLOAT			,b.TheoHold					)								AS DeviceTheoHoldPercent	
		,TRY_CONVERT(MONEY			,0							)								AS DeviceLeaseFee	
		,TRY_CONVERT(VARCHAR(100)	,''							)								AS DeviceLeaseAttributes	
		,TRY_CONVERT(VARCHAR(100)	,concat('MinBet:',b.MinBet,
											'MaxBet:',b.MaxBet,
											'AreaName:',b.AreaName)
																)								AS DeviceExtraAttributes	
		,TRY_CONVERT(NVARCHAR(100)	,''							)								AS DeviceCabinetType 
		,TRY_CONVERT(NVARCHAR(100)	,b.AreaCode					)								AS DeviceBankName
		,TRY_CONVERT(NVARCHAR(100)	,b.Seat						)								AS DeviceStand
		,TRY_CONVERT(INT			,1							)								AS IsTableDevice
		,TRY_CONVERT(INT			,''							)								AS PaddedBankName  --Note sure about this field
		,TRY_CONVERT(DATETIME		,GETUTCDATE()				)								AS InsertDateTime
FROM	 [qcic].[SGA_Fact_PlayerTableRating_Day] a
LEFT JOIN [qcic].[SGA_Dim_TableAttribute_History] b
ON		(a.TableHistoryId = b.TableHistoryId AND a.CasinoId = b.CasinoId)
LEFT JOIN [qcids].[Dim_Player_SCD] p
ON		(p.SystemName = 'SGAHRI' AND a.PlayerID = p.PlayerID AND p.StartDateTime <= a.GamingDt AND a.GamingDt < p.EndDateTime)
LEFT JOIN [qcids].[Dim_PlayerDistanceToCasino_SCD] d
ON		(d.SystemName = 'SGAHRI' AND a.CasinoId = d.CasinoID AND a.PlayerID = d.PlayerID AND d.StartDateTime <= a.GamingDt AND a.GamingDt < d.EndDateTime)
UNION ALL
--Free Play and Table Games Coupon
SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'					)								AS SystemName

		--Primary Key (CasinoID, DeviceID, PlayerID, GamingDate)
		,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId					)								AS CasinoID
		,TRY_CONVERT(NVARCHAR(100)	,''							)								AS DeviceID
		,TRY_CONVERT(NVARCHAR(100)	,a.PlayerID					)								AS PlayerID
		,TRY_CONVERT(DATE			,a.GamingDt					)								AS GamingDate

		--DeNormalized SCD Dates
		,TRY_CONVERT(DATE			,''							)								AS DeviceStartDateSCD 

		--Player Related Metrics
		,TRY_CONVERT(DATETIME		,a.FirstRedemptionDtm		)								AS StartDateTime
		,TRY_CONVERT(DATETIME		,a.LastRedemptionDtm		)								AS EndDateTime

		--Gaming TOTAL metrics
		,TRY_CONVERT(NUMERIC(27,10)	 ,0							)								AS CoinIn     
		,TRY_CONVERT(NUMERIC(27,10)	 ,0							)								AS CoinOut
		,TRY_CONVERT(NUMERIC(27,10)	 ,0							)								AS ActualWinWithFreeplay
		,TRY_CONVERT(NUMERIC(27,10)	 ,-1 * a.ExpUsed			)								AS ActualWinNetFreeplay
		,TRY_CONVERT(INT			 ,0							)								AS NumberGamesPlayed
		,TRY_CONVERT(NUMERIC(27,10)	 ,0							)								AS Jackpot
		,TRY_CONVERT(NUMERIC(27,10)	 ,a.ExpUsed					)								AS Freeplay
		,TRY_CONVERT(NUMERIC(27,10)	 ,0							)								AS TheoWinWithFreeplay  
		,TRY_CONVERT(NUMERIC(27,10)	 ,-1 * a.ExpUsed			)								AS TheoWinNetFreeplay  
		,TRY_CONVERT(NUMERIC(27,10)  ,0							)								AS TimePlayed
		,TRY_CONVERT(NUMERIC(27,10)  ,0							)								AS FrontIn   
		,TRY_CONVERT(NUMERIC(27,10)  ,0							)								AS ChipsIn   
		,TRY_CONVERT(NUMERIC(27,10)  ,0							)								AS CreditIn   

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
		,TRY_CONVERT(FLOAT			,''							)								AS DeviceCalcTheoHoldPercent	
		,TRY_CONVERT(FLOAT			,''							)								AS DeviceBaseTheoHoldPercent
		,TRY_CONVERT(NVARCHAR(200)	,''							)								AS DeviceManufacturer
		,TRY_CONVERT(FLOAT			,''							)								AS Denomination
		,TRY_CONVERT(NVARCHAR(200)	,''							)								AS DeviceGameType	
		,TRY_CONVERT(NVARCHAR(200)	,''							)								AS DeviceCabinetModel	
		,TRY_CONVERT(NVARCHAR(20)	,''							)								AS DeviceLocation		
		,TRY_CONVERT(DATE			,''							)								AS DeviceSCDStartDate	
		,TRY_CONVERT(INT			,1							)								AS DeviceActiveFlag
		,TRY_CONVERT(NVARCHAR(50)	,''							)								AS DeviceSerial	
		,TRY_CONVERT(INT			,''							)								AS DeviceGameClass	
		,TRY_CONVERT(FLOAT			,''							)								AS DeviceTheoHoldPercent	
		,TRY_CONVERT(MONEY			,0							)								AS DeviceLeaseFee	
		,TRY_CONVERT(VARCHAR(100)	,''							)								AS DeviceLeaseAttributes	
		,TRY_CONVERT(VARCHAR(100)	,''							)								AS DeviceExtraAttributes	
		,TRY_CONVERT(NVARCHAR(100)	,''							)								AS DeviceCabinetType 
		,TRY_CONVERT(NVARCHAR(100)	,''							)								AS DeviceBankName
		,TRY_CONVERT(NVARCHAR(100)	,''							)								AS DeviceStand
		,TRY_CONVERT(INT			,case when a.TranType = 'Coupon' then 1 else 0 end)			AS IsTableDevice
		,TRY_CONVERT(INT			,''							)								AS PaddedBankName  --Note sure about this field
		,TRY_CONVERT(DATETIME		,GETUTCDATE()				)								AS InsertDateTime
FROM	 [qcic].[SGA_Fact_PlayerPrizeExpense_Day] a
LEFT JOIN [qcids].[Dim_Player_SCD] p
ON		(p.SystemName = 'SGAHRI' AND a.PlayerID = p.PlayerID AND p.StartDateTime <= a.GamingDt AND a.GamingDt < p.EndDateTime)
LEFT JOIN [qcids].[Dim_PlayerDistanceToCasino_SCD] d
ON		(d.SystemName = 'SGAHRI' AND a.CasinoId = d.CasinoID AND a.PlayerID = d.PlayerID AND d.StartDateTime <= a.GamingDt AND a.GamingDt < d.EndDateTime)
WHERE	 a.TranType IN ('Autoload','Coupon') AND a.ExpUsed > 0





-----------------------------------------------------------------------------------
DROP INDEX [cci_qcids_App_DeviceManufacturerGroup] ON [qcids].[App_DeviceManufacturerGroup]

select * 
into [tqcids].[App_DeviceManufacturerGroup]
from  [qcids].[App_DeviceManufacturerGroup] ;

CREATE VIEW [tqcic].[uvw_SGA_ETL09_App_DeviceManufacturerGroup]  AS
--Slot Ratings
	SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'					)								AS SystemName
		,TRY_CONVERT(NVARCHAR(100)	,OrigMfg					)								AS OriginalManufacturer
		,TRY_CONVERT(NVARCHAR(100)	,Mfg						)								AS ManufacturerCleaned
		,TRY_CONVERT(NVARCHAR(100)	,Mfg						)								AS ManufacturerGroup
		,TRY_CONVERT(DATETIME		,GETUTCDATE()				)								AS InsertDateTime
	FROM	 [qcic].[SGA_Dim_SlotMFGGroup];




