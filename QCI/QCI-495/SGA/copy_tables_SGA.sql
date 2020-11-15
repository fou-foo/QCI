---PASO 0 
CREATE SCHEMA [tqcic];
CREATE SCHEMA [tqcids];
--drop table [tqcic].[SGA_ETL_Runner_Tracker];
SELECT * 
INTO  [tqcic].[SGA_ETL_Runner_Tracker]
FROM [qcic].[SGA_ETL_Runner_Tracker];

----------------------------------------------
-- Paso 1 
--drop table [tqcids].[Dim_Host_SCD];
SELECT * 
INTO [tqcids].[Dim_Host_SCD]
from [qcids].[Dim_Host_SCD] ;

CREATE VIEW [tqcic].[uvw_SGA_ETL01_Dim_Host_SCD]  AS

SELECT 
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'																)		AS SystemName
		,TRY_CONVERT(INT			,2																		)		AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,ISNULL(c.CasinoGroupID,a.CasinoID)										)		AS CasinoGroupID
		,TRY_CONVERT(INT			,ISNULL(c.CasinoGroupIDInt,b.CasinoIDInt)								)		AS CasinoGroupIDInt
		,TRY_CONVERT(NVARCHAR(100)	,a.EmpID																)		AS HostID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](a.EmpID,2)							)		AS HostIDBigInt
		,TRY_CONVERT(NVARCHAR(50)	,ISNULL(c.HostFirstName,LTRIM(RTRIM(ISNULL(a.FirstName,'NoFirstName')))))		AS HostFirstName
		,TRY_CONVERT(NVARCHAR(50)	,ISNULL(c.HostLastName,LTRIM(RTRIM(ISNULL(a.LastName,'NoLastName'))))	)		AS HostLastName
		,TRY_CONVERT(NVARCHAR(100)	,ISNULL(c.HostPosition,ISNULL(a.Position,'NoPosition'))					)		AS HostPosition
		,TRY_CONVERT(NVARCHAR(50)	,ISNULL(c.HostCode,a.EmpNum)											)		AS HostCode
		,TRY_CONVERT(DATETIME		,ISNULL(c.StartDateTime,a.InsertDT)										)		AS StartDateTime
		,TRY_CONVERT(DATETIME		,ISNULL(c.EndDateTime,ISNULL(a.RetireDT,'2999-12-31'))					)		AS EndDateTime
		,TRY_CONVERT(INT			,ISNULL(c.IsCurrent,a.isCurrent)										)		AS IsCurrent
		,GETUTCDATE()																								AS InsertDateTime
FROM	 [qcic].[SGA_Dim_CMPEmployees] a WITH (NOLOCK)
JOIN	 [qcids].[App_Casino] b WITH (NOLOCK)
ON		(a.CasinoID = b.CasinoID AND b.SystemNameInt = 2)
LEFT JOIN [qcids].[App_Dim_Host_SCD_Override] c WITH (NOLOCK)
ON		(a.EmpID = c.HostID AND c.SystemNameInt = 2 AND c.OverrideIsActive = 1)
WHERE	 a.isHost = 1 AND a.isInactive = 0; 
--------------------------------------------------
-- PASO 2 
--drop table [tqcids].[Dim_Player_SCD];
select * 
into [tqcids].[Dim_Player_SCD]
from [qcids].[Dim_Player_SCD]; --- 5:29    minutos


CREATE NONCLUSTERED INDEX [test_sga_etl_Dim_Player_SCD]
ON [tqcids].[Dim_Player_SCD] ([SystemNameInt]); -- 00:32

CREATE VIEW [tqcic].[uvw_SGA_ETL02_Dim_Player_SCD]  AS
SELECT 
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)							AS SystemName
		,TRY_CONVERT(INT			,2						)							AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)							AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](PlayerID,1))	AS PlayerIDBigInt
		,TRY_CONVERT(NVARCHAR(100)	,Acct					)							AS Account
		,TRY_CONVERT(NVARCHAR(20)	,title					)							AS Title
		,TRY_CONVERT(NVARCHAR(50)	,LTRIM(RTRIM(lastname))	)							AS LastName
		,TRY_CONVERT(NVARCHAR(50)	,LTRIM(RTRIM(firstname)))							AS FirstName
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
		,TRY_CONVERT(INT			,isCall					)							AS IsCall
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
FROM	 [qcic].[SGA_uvw_MarketablePlayersAttributes] WITH (NOLOCK)
WHERE	 isBanned = 0;
------------------
--- PASO 3 
--drop table [tqcids].[Dim_PlayerDistanceToCasino_SCD];
select * 
into [tqcids].[Dim_PlayerDistanceToCasino_SCD] 
from [qcids].[Dim_PlayerDistanceToCasino_SCD]  ; -- 4:39

CREATE NONCLUSTERED INDEX [test_sga_etl_Dim_PlayerDistanceToCasino_SCD]
ON [tqcids].[Dim_PlayerDistanceToCasino_SCD] ([SystemNameInt]); -- 01:31


CREATE VIEW [tqcic].[uvw_SGA_ETL03_Dim_PlayerDistanceToCasino_SCD]  AS
SELECT	 	
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)							AS SystemName
		,TRY_CONVERT(INT			,2						)							AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,b.CasinoID				)							AS CasinoID
		,TRY_CONVERT(INT			,b.CasinoIDInt			)							AS CasinoIDInt
		,TRY_CONVERT(NVARCHAR(100)	,a.PlayerID				)							AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](a.PlayerID,1))	AS PlayerIDBigInt
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
FROM	 [qcic].[SGA_uvw_MarketablePlayersAttributes] a WITH (NOLOCK)
CROSS JOIN (
		SELECT	 SystemName
				,SystemNameInt
				,CasinoID
				,CasinoIDInt
		FROM	 [qcids].[App_Casino] WITH (NOLOCK)
		WHERE	 SystemNameInt = 2
		GROUP BY SystemName
				,SystemNameInt
				,CasinoID
				,CasinoIDInt
) b
WHERE	 a.isBanned = 0 AND b.SystemNameInt = 2;

------------------------
--- PASO 4 
--drop table  [tqcids].[Dim_PlayerLoyalty_SCD];
select *
into [tqcids].[Dim_PlayerLoyalty_SCD]
from [qcids].[Dim_PlayerLoyalty_SCD]; -- 2:27

CREATE NONCLUSTERED INDEX [test_sga_etl_Dim_PlayerLoyalty_SCD]
ON [tqcids].[Dim_PlayerLoyalty_SCD] ([SystemNameInt]) -- 00:29

CREATE VIEW [tqcic].[uvw_SGA_ETL04_Dim_PlayerLoyalty_SCD]  AS

SELECT
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)							AS SystemName
		,TRY_CONVERT(INT			,2						)							AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)							AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](PlayerID,1))	AS PlayerIDBigInt
		,TRY_CONVERT(NVARCHAR(100)	,P_ClubStatus			)							AS LoyaltyCardLevel
		,TRY_CONVERT(DATETIME		,InsertDT				)							AS StartDateTime
		,TRY_CONVERT(DATETIME		,RetireDT				)							AS EndDateTime
		,TRY_CONVERT(INT			,isCurrent				)							AS IsCurrent
		,GETUTCDATE()																	AS InsertDateTime
FROM	 [qcic].[SGA_uvw_MarketablePlayersAttributes] WITH (NOLOCK);
--------------------------------------------
--- PASO 5 
--drop table [tqcids].[Dim_PlayerHost_SCD] ;
select *
into [tqcids].[Dim_PlayerHost_SCD]  
from [qcids].[Dim_PlayerHost_SCD]  ;

CREATE VIEW [tqcic].[uvw_SGA_ETL05_Dim_PlayerHost_SCD]  AS

SELECT	 	
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)		AS SystemName
		,TRY_CONVERT(INT			,2						)		AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,a.CasinoID				)		AS CasinoID
		,TRY_CONVERT(INT			,b.CasinoIDInt			)		AS CasinoIDInt
		,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)		AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](PlayerID,1)) AS PlayerIDBigInt
		,TRY_CONVERT(NVARCHAR(100)	,HostEmpID				)		AS HostID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](HostEmpID,2)) AS HostIDBigInt
		,TRY_CONVERT(NVARCHAR(100)	,'Primary'				)		AS PlayerHostType
		,TRY_CONVERT(DATETIME		,HostStartDTM			)		AS StartDateTime
		,TRY_CONVERT(DATETIME		,HostEndDTM				)		AS EndDateTime
		,TRY_CONVERT(INT			,isCurrent				)		AS IsCurrent
		,GETUTCDATE()												AS InsertDateTime
FROM	 [qcic].[SGA_Fact_PlayerHostHistory_Primary] a WITH (NOLOCK)
JOIN	 [qcids].[App_Casino] b WITH (NOLOCK)
ON		(a.CasinoID = b.CasinoID AND b.SystemNameInt = 2)
GROUP BY a.CasinoID
		,b.CasinoIDInt
		,PlayerID
		,HostEmpID
		,HostStartDTM
		,HostEndDTM
		,isCurrent
UNION ALL
SELECT	 	
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)		AS SystemName
		,TRY_CONVERT(INT			,2						)		AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,a.CasinoID				)		AS CasinoID
		,TRY_CONVERT(INT			,b.CasinoIDInt			)		AS CasinoIDInt
		,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)		AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](PlayerID,1)) AS PlayerIDBigInt
		,TRY_CONVERT(NVARCHAR(100)	,HostEmpID				)		AS HostID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](HostEmpID,2)) AS HostIDBigInt
		,TRY_CONVERT(NVARCHAR(100)	,'Secondary'			)		AS PlayerHostType
		,TRY_CONVERT(DATETIME		,HostStartDTM			)		AS StartDateTime
		,TRY_CONVERT(DATETIME		,HostEndDTM				)		AS EndDateTime
		,TRY_CONVERT(INT			,isCurrent				)		AS IsCurrent
		,GETUTCDATE()												AS InsertDateTime
FROM	 [qcic].[SGA_Fact_PlayerHostHistory_Secondary] a WITH (NOLOCK)
JOIN	 [qcids].[App_Casino] b WITH (NOLOCK)
ON		(a.CasinoID = b.CasinoID AND b.SystemNameInt = 2)
GROUP BY a.CasinoID
		,b.CasinoIDInt
		,PlayerID
		,HostEmpID
		,HostStartDTM
		,HostEndDTM
		,isCurrent;
-------------------------------------------------
-- paso 6 
drop table [tqcids].[Dim_PlayerCredit_SCD];
select * 
into [tqcids].[Dim_PlayerCredit_SCD]
from [qcids].[Dim_PlayerCredit_SCD] ; -- :02 s

CREATE VIEW [tqcic].[uvw_SGA_ETL06_Dim_PlayerCredit_SCD]  AS

SELECT	 	
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)		AS SystemName
		,TRY_CONVERT(INT			,2						)		AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,TranCasinoID			)		AS CasinoID
		,TRY_CONVERT(INT			,b.CasinoIDInt			)		AS CasinoIDInt
		,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)		AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](PlayerID,1)) AS PlayerIDBigInt
		,TRY_CONVERT(INT			,ROUND(CreditLimit * 100,0))	AS CreditLimitInt100
		,TRY_CONVERT(INT			,IsCreditPlayer			)		AS IsActiveCredit
		,TRY_CONVERT(NVARCHAR(100)	,SetupCasinoID			)		AS SetupCasinoID
		,TRY_CONVERT(NVARCHAR(4000)	,GroupCode				)		AS Note
		,TRY_CONVERT(DATETIME		,StartGamingDt			)		AS StartDateTime
		,TRY_CONVERT(DATETIME		,EndGamingDt			)		AS EndDateTime
		,TRY_CONVERT(INT			,isCurrent				)		AS IsCurrent
		,GETUTCDATE()												AS InsertDateTime
FROM	 [qcic].[SGA_Dim_PlayerCreditLimit_History] a WITH (NOLOCK)
JOIN	 [qcids].[App_Casino] b WITH (NOLOCK)
ON		(a.TranCasinoID = b.CasinoID AND b.SystemNameInt = 2);
-----------------------------------------
---- paso 7 
--drop table [tqcids].[Dim_PlayerCreditElectronic_SCD];
select * 
into [tqcids].[Dim_PlayerCreditElectronic_SCD]
from [qcids].[Dim_PlayerCreditElectronic_SCD]; -- 02 s

CREATE VIEW [tqcic].[uvw_SGA_ETL07_Dim_PlayerCreditElectronic_SCD]  AS
SELECT	 SystemName
		,SystemNameInt
		,CasinoID
		,CasinoIDInt
		,PlayerID
		,PlayerIDBigInt
		,ElectronicCreditType
		,IsEnrolled
		,ElectronicCreditAccountID
		,SignatureID
		,CreatedByID
		,Note
		,StartDateTime
		,EndDateTime = LEAD(StartDateTime, 1, {d '2999-12-31'}) OVER 
				(
				   PARTITION BY	 SystemName
								,CasinoID
								,PlayerID
								,ElectronicCreditType
				   ORDER BY StartDateTime
			    )
		,IsCurrent
		,InsertDateTime
FROM	(
		SELECT	 	
				 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)		AS SystemName
				,TRY_CONVERT(INT			,2						)		AS SystemNameInt
				,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId				)		AS CasinoID
				,TRY_CONVERT(INT			,b.CasinoIDInt			)		AS CasinoIDInt
				,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)		AS PlayerID
				,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](PlayerID,1)) AS PlayerIDBigInt
				,TRY_CONVERT(NVARCHAR(100)	,
							case when EnrollOption = 2 then 'Power Bank'
							else 'Self-Pay Jackpots' end
																	)		AS ElectronicCreditType
				,TRY_CONVERT(INT			,IsEnroll				)		AS IsEnrolled
				,TRY_CONVERT(NVARCHAR(100)	,Acct					)		AS ElectronicCreditAccountID
				,TRY_CONVERT(NVARCHAR(100)	,SignatureId			)		AS SignatureID
				,TRY_CONVERT(NVARCHAR(100)	,CreatedBy				)		AS CreatedByID
				,TRY_CONVERT(NVARCHAR(4000)	,concat(ReasonDesc,' ',Remarks)
																	)		AS Note
				,TRY_CONVERT(DATETIME		,CreatedDtm				)		AS StartDateTime
				,TRY_CONVERT(INT			,isCurrent				)		AS IsCurrent
				,GETUTCDATE()												AS InsertDateTime
		FROM	 (
				SELECT	 *
				FROM	 [qcic].[SGA_Dim_PlayerECreditEnrollment_History] WITH (NOLOCK)
				WHERE	 EnrollOption IN (2,3)
				UNION ALL
				SELECT	 EnrollmentId
						,Acct
						,PlayerId
						,IsEnroll = case when EnrollOption = 0 then 0 else IsEnroll end
						,EnrollOption = 2
						,CasinoId
						,GamingDt
						,ReasonDesc
						,Remarks
						,SignatureId
						,CreatedBy
						,ModifiedBy
						,CreatedDtm
						,ModifiedDtm
						,IsCurrent
						,InsertDtm
				FROM	 [qcic].[SGA_Dim_PlayerECreditEnrollment_History] WITH (NOLOCK)
				WHERE	 EnrollOption IN (0,1)
				UNION ALL
				SELECT	 EnrollmentId
						,Acct
						,PlayerId
						,IsEnroll = case when EnrollOption = 0 then 0 else IsEnroll end
						,EnrollOption = 3
						,CasinoId
						,GamingDt
						,ReasonDesc
						,Remarks
						,SignatureId
						,CreatedBy
						,ModifiedBy
						,CreatedDtm
						,ModifiedDtm
						,IsCurrent
						,InsertDtm
				FROM	 [qcic].[SGA_Dim_PlayerECreditEnrollment_History] WITH (NOLOCK)
				WHERE	 EnrollOption IN (0,1)
				) a
		JOIN	 [qcids].[App_Casino] b
		ON		(a.CasinoID = b.CasinoID AND b.SystemNameInt = 2)
		) x;
--------------------------------------------------
-- PASO 8 
--drop table  [tqcids].[Fact_PlayerDeviceRatingDay] ;
select * 
into [tqcids].[Fact_PlayerDeviceRatingDay]
from [qcids].[Fact_PlayerDeviceRatingDay]; -- vamos  2:48:40 y me fui a dormir como 5 hrs y termino pero como la VM se reinicio no supe en realidad cuanto se tardo 

CREATE NONCLUSTERED INDEX [test_sga_etl_Fact_PlayerDeviceRatingDay]
ON [tqcids].[Fact_PlayerDeviceRatingDay] ([SystemNameInt])
INCLUDE ([GamingDate]) --14:08


CREATE VIEW [tqcic].[uvw_SGA_ETL08_Fact_PlayerDeviceRatingDay]  AS
--Slot Ratings
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
        WHERE	 a.TranType IN ('Autoload','Coupon') AND a.ExpUsed > 0; 
-----------------------------------------------------------------------------------
-- PASO 9 
--drop table [tqcids].[App_DeviceManufacturerGroup];
select * 
into [tqcids].[App_DeviceManufacturerGroup]
from [qcids].[App_DeviceManufacturerGroup];

CREATE VIEW [tqcic].[uvw_SGA_ETL09_App_DeviceManufacturerGroup]  AS
--Slot Ratings
SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'					)								AS SystemName
		,TRY_CONVERT(INT			,2							)								AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,OrigMfg					)								AS OriginalManufacturer
		,TRY_CONVERT(NVARCHAR(100)	,Mfg						)								AS ManufacturerCleaned
		,TRY_CONVERT(NVARCHAR(100)	,Mfg						)								AS ManufacturerGroup
		,TRY_CONVERT(DATETIME		,GETUTCDATE()				)								AS InsertDateTime
FROM	 [qcic].[SGA_Dim_SlotMFGGroup] WITH (NOLOCK);
----------------------------------------------------------------------
--- Paso 10 
--drop table [tqcids].[App_DeviceGameTypeGroup];
select *
into [tqcids].[App_DeviceGameTypeGroup]
from [qcids].[App_DeviceGameTypeGroup];

CREATE VIEW [tqcic].[uvw_SGA_ETL10_App_DeviceGameTypeGroup]  AS
--Slot Ratings
SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'					)								AS SystemName
		,TRY_CONVERT(INT			,2							)								AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,OrigGameType				)								AS OriginalGameType
		,TRY_CONVERT(NVARCHAR(100)	,GameTypeCleaned			)								AS GameTypeCleaned
		,TRY_CONVERT(NVARCHAR(100)	,GameType					)								AS GameTypeGroup
		,TRY_CONVERT(NVARCHAR(100)	,GameTypeShortName			)								AS GameTypeShortName
		,TRY_CONVERT(NVARCHAR(100)	,GameTypeSummary			)								AS GameTypeSummary
		,TRY_CONVERT(DATETIME		,GETUTCDATE()				)								AS InsertDateTime
FROM	 [qcic].[SGA_Dim_SlotGameTypeGroup] WITH (NOLOCK);
-------------------------
------PASO 11 
--drop table [tqcids].[App_DeviceDenominationGroup];
select * 
into [tqcids].[App_DeviceDenominationGroup]
from [qcids].[App_DeviceDenominationGroup];

CREATE VIEW [tqcic].[uvw_SGA_ETL11_App_DeviceDenominationGroup]  AS
--Slot Ratings
SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'					)								AS SystemName
		,TRY_CONVERT(INT			,2							)								AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,OrigDenom					)								AS OriginalDenomination
		,TRY_CONVERT(NVARCHAR(100)	,Denom						)								AS DenominationCleaned
		,TRY_CONVERT(NVARCHAR(100)	,DenomSort					)								AS DenominationSort
		,TRY_CONVERT(NVARCHAR(100)	,DenomGroup					)								AS DenominationGroup
		,TRY_CONVERT(NVARCHAR(100)	,DenomGroupSort				)								AS DenominationGroupSort
		,TRY_CONVERT(DATETIME		,GETUTCDATE()				)								AS InsertDateTime
FROM	 [qcic].[SGA_Dim_SlotDenomGroup] WITH (NOLOCK);
---------------------------------------------------------
---- Paso 12
--drop table  [tqcids].[Fact_PlayerPrize] ;
select *
into [tqcids].[Fact_PlayerPrize]
from [qcids].[Fact_PlayerPrize]; -- 00:39

CREATE NONCLUSTERED INDEX [test_etl_sga_Fact_PlayerPrize]
ON [tqcids].[Fact_PlayerPrize] ([SystemNameInt])
INCLUDE ([PrizeStartDateTime]) -- 00:07

CREATE NONCLUSTERED INDEX [test2_etl_sga_Fact_PlayerPrize]
ON [tqcids].[Fact_PlayerPrize] ([SystemNameInt],[PrizeStartDateTime]) -- 00:09



--drop view [tqcic].[uvw_SGA_ETL12_Fact_PlayerPrize] 
CREATE VIEW [tqcic].[uvw_SGA_ETL12_Fact_PlayerPrize]  AS
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
ON		(a.IssueCasinoId = c.CasinoID AND c.SystemNameInt = 2);
-----------------------

-- Paso 14
select * 
into [tqcids].[Fact_PlayerEvaluationCurrent] 
from [qcids].[Fact_PlayerEvaluationCurrent]  ; -- 00:35

CREATE NONCLUSTERED INDEX [test_etl_sga_Fact_PlayerEvaluationCurrent]
ON [tqcids].[Fact_PlayerEvaluationCurrent] ([SystemNameInt])-- 00:10


CREATE VIEW [tqcic].[uvw_SGA_ETL14_Fact_PlayerEvaluationCurrent]  AS
--Slot Ratings
SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)								AS SystemName
		,TRY_CONVERT(INT			,2						)								AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,PlayerId				)								AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](PlayerID,1))		AS PlayerIDBigInt
		,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId				)								AS CasinoGroupID
		,TRY_CONVERT(INT			,b.CasinoGroupIDInt		)								AS CasinoGroupIDInt
		,TRY_CONVERT(INT			,ROUND(ValuationScore*100,0))							AS EvaluationScoreInt100
		,TRY_CONVERT(DATETIME		,GETUTCDATE()			)								AS InsertDateTime
FROM	 [qcic].[SGA_Fact_PlayerValuationScore] a WITH (NOLOCK)
JOIN	 [qcids].[App_CasinoGroup] b WITH (NOLOCK)
ON		(a.CasinoID = b.CasinoGroupID AND b.SystemNameInt = 2);

----------------------
---paso 15 
---drop table [tqcids].[Fact_DeviceDay];
select * 
into [tqcids].[Fact_DeviceDay]
from [qcids].[Fact_DeviceDay];  -- 00:24:09

CREATE NONCLUSTERED INDEX [test_etl_sga_Fact_DeviceDay]
ON [tqcids].[Fact_DeviceDay] ([SystemNameInt]) 
INCLUDE ([GamingDate]) -- 02:31

CREATE NONCLUSTERED INDEX [test2_etl_sga_Fact_DeviceDay]
ON [tqcids].[Fact_DeviceDay] ([SystemNameInt],[GamingDate]) -- 02:45



CREATE VIEW [tqcic].[uvw_SGA_ETL15_Fact_DeviceDay]  AS
--Slot Ratings
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
		,TRY_CONVERT(VARCHAR(100)	,concat('Reels:',a.Reels,
											'MaxBet:',a.MaxBet)	)								AS DeviceExtraAttributes	
		,TRY_CONVERT(INT			,1							)								AS IsSlotDevice
		,TRY_CONVERT(INT			,0							)								AS IsTableDevice
		,TRY_CONVERT(INT			,0							)								AS IsPokerDevice
		,TRY_CONVERT(INT			,0							)								AS IsOtherDevice
		,TRY_CONVERT(INT			,a.Active					)								AS IsActiveDevice
		,DeviceLocationBigInt = TRY_CONVERT(BIGINT			,CHECKSUM(a.MetaLocation))
		,TRY_CONVERT(DATETIME		,GETUTCDATE()				)								AS InsertDateTime
FROM	 [qcic].[SGA_Fact_SlotAnalysis_Staging] a WITH (NOLOCK)
JOIN	 [qcids].[App_Casino] b WITH (NOLOCK)
ON		(a.Casino = b.CasinoID AND b.SystemNameInt = 2);

---------------------------------
select * 
into [tqcids].[Dim_PlayerPhone_Current] 
from [qcids].[Dim_PlayerPhone_Current]; -- 00:16

CREATE NONCLUSTERED INDEX [test_etl_sga_Dim_PlayerPhone_Current]
ON [tqcids].[Dim_PlayerPhone_Current] ([SystemNameInt]) -- 00:04

CREATE VIEW [tqcic].[uvw_SGA_ETL18_Dim_PlayerPhone_Current]  AS
SELECT 
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)							AS SystemName
		,TRY_CONVERT(INT			,2						)							AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,PlayerId				)							AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](PlayerId,1))	AS PlayerIDBigInt
		,TRY_CONVERT(NVARCHAR(20)	,ContactType			)							AS TelephoneType
		,TRY_CONVERT(NVARCHAR(20)	,PhoneNumber			)							AS Telephone
		,TRY_CONVERT(INT			,IsCall					)							AS IsCall
		,GETUTCDATE()																	AS InsertDateTime
FROM	 [qcic].[SGA_uvw_PlayerContactPhone] WITH (NOLOCK);










