drop function [test].[udf_HRC_ETL01];
CREATE   FUNCTION [test].[udf_HRC_ETL01] ( )
RETURNS TABLE
AS
RETURN
	SELECT 
			 TRY_CONVERT(NVARCHAR(100),  'HRC'								)	    	AS SystemName  
			,TRY_CONVERT(INT		,  3								)			    AS SystemNameInt  
			,TRY_CONVERT(NVARCHAR(100), a.CasinoID							)			AS CasinoGroupID
			,TRY_CONVERT(INT		, b.CasinoIDInt						)				AS CasinoGroupIDInt
			,TRY_CONVERT(NVARCHAR(100), EmpID								)			AS HostID
			,TRY_CONVERT(BIGINT		, [qcids].[udf_ConvertStringToBigInt](EmpID,2))		AS HostIDBigInt
			,TRY_CONVERT(NVARCHAR(50), LTRIM(RTRIM(ISNULL(FirstName,'NoFirstName'))))	AS HostFirstName
				,TRY_CONVERT(NVARCHAR(50), LTRIM(RTRIM(ISNULL(LastName,'NoLastName'))))	AS HostLastName
			,TRY_CONVERT(NVARCHAR(100), ISNULL(Position,'NoPosition')		)			AS HostPosition
			,TRY_CONVERT(NVARCHAR(50), EmpNum								)			AS HostCode
			,TRY_CONVERT(DATETIME	, InsertDtm							)				AS StartDateTime
			,TRY_CONVERT(DATETIME	, ISNULL(LastUpdatedDtm,'2999-12-31'))				AS EndDateTime
			,TRY_CONVERT(INT		, isCurrent							)				AS IsCurrent
			,GETUTCDATE()																AS InsertDateTime
		FROM	 [qcic].[HRC_Dim_CMPEmployees_History] AS  a WITH (NOLOCK)	
		JOIN	 [qcids].[App_Casino] AS  b WITH (NOLOCK)  
			ON		(a.CasinoID = b.CasinoID AND b.SystemNameInt = 3)
		WHERE	 isHost = 1 and isInactive = 0 ;
/* testing 
select * from [test].[udf_HRC_ETL01]();
*/

drop function [test].[udf_HRC_ETL02];
CREATE  FUNCTION [test].[udf_HRC_ETL02] ( )
RETURNS TABLE
AS
RETURN
	SELECT 
			 TRY_CONVERT(NVARCHAR(100), 'HRC'					)							AS SystemName 
			,TRY_CONVERT(INT		,  3						)							AS SystemNameInt 
			,TRY_CONVERT(NVARCHAR(100), PlayerID				)							AS PlayerID
			,TRY_CONVERT(BIGINT		, [qcids].[udf_ConvertStringToBigInt](PlayerID,1))	AS PlayerIDBigInt
			,TRY_CONVERT(NVARCHAR(100), Acct					)							AS Account
			,TRY_CONVERT(NVARCHAR(20), title					)							AS Title
			,TRY_CONVERT(NVARCHAR(50), LTRIM(RTRIM(lastname))	)							AS LastName
			,TRY_CONVERT(NVARCHAR(50), LTRIM(RTRIM(firstname)))						        AS FirstName
			,TRY_CONVERT(NVARCHAR(50), ISNULL(displayname,concat(firstname,' ',lastname)))  AS DisplayName
			,TRY_CONVERT(NVARCHAR(50), NickNameAlias			)							AS NickNameAlias
			,TRY_CONVERT(NVARCHAR(100), HomeAddr01				)							AS HomeAddress1
			,TRY_CONVERT(NVARCHAR(100), HomeAddr02				)							AS HomeAddress2
			,TRY_CONVERT(NVARCHAR(100), HomeCity				)							AS HomeCity
			,TRY_CONVERT(NVARCHAR(50), HomeStateCode			)							AS HomeStateCode
			,TRY_CONVERT(NVARCHAR(50), HomeCountryCode		)							    AS HomeCountryCode
			,TRY_CONVERT(NVARCHAR(20), HomePostalCode			)							AS HomePostalCode
			,TRY_CONVERT(NVARCHAR(20), HomeTel01Type			)							AS HomeTelephone1Type
			,TRY_CONVERT(NVARCHAR(20), HomeTel01				)							AS HomeTelephone1
			,TRY_CONVERT(NVARCHAR(20), HomeTel02Type			)							AS HomeTelephone2Type
			,TRY_CONVERT(NVARCHAR(20), HomeTel02				)							AS HomeTelephone2
			,TRY_CONVERT(NVARCHAR(255), HomeEmail				)							AS HomeEmail
			,TRY_CONVERT(NVARCHAR(50), AltName				)							    AS AltName
			,TRY_CONVERT(NVARCHAR(100), AltAddr01				)							AS AltAddress1
			,TRY_CONVERT(NVARCHAR(100), AltAddr02				)							AS AltAddress2
			,TRY_CONVERT(NVARCHAR(100), AltCity				)							    AS AltCity
			,TRY_CONVERT(NVARCHAR(50), AltStateCode			)							    AS AltStateCode
			,TRY_CONVERT(NVARCHAR(50), AltCountryCode			)							AS AltCountryCode
			,TRY_CONVERT(NVARCHAR(20), AltPostalCode			)							AS AltPostalCode
			,TRY_CONVERT(NVARCHAR(20), AltTel01Type			)				    			AS AltTelephone1Type
			,TRY_CONVERT(NVARCHAR(20), AltTel01				)				    			AS AltTelephone1
			,TRY_CONVERT(NVARCHAR(20), AltTel02Type			)				    			AS AltTelephone2Type
			,TRY_CONVERT(NVARCHAR(20), AltTel02				)				    			AS AltTelephone2
			,TRY_CONVERT(NVARCHAR(255), AltEmail				)							AS AltEmail
			,TRY_CONVERT(DATE		, BirthDT				)					    		AS BirthDate
			,TRY_CONVERT(INT		, isPrivacy				)					    		AS IsPrivacy
			,TRY_CONVERT(INT		, isMailToAlt			)					    		AS IsMailToAlt
			,TRY_CONVERT(INT		, 1 - IsMailPlayer		)					    		AS IsNoMail
			,TRY_CONVERT(INT		, isReturnMail			)					    		AS IsReturnMail
			,TRY_CONVERT(INT		, isVIP					)					    		AS IsVIP
			,TRY_CONVERT(INT		, isLostAndFound			)							AS IsLostAndFound
			,TRY_CONVERT(INT		, isCreditAcct			)					    		AS IsCreditAccount
			,TRY_CONVERT(INT		, isBanned				)					    		AS IsBanned
			,TRY_CONVERT(INT		, isInactive				)							AS IsInactive
			,TRY_CONVERT(INT		, IsCallPlayer			)						    	AS IsCall
			,TRY_CONVERT(INT		, IsEmailPlayer			)						    	AS IsEmailSend
			,TRY_CONVERT(NVARCHAR(100), SetupCasinoID			)							AS SetupCasinoID
			,TRY_CONVERT(NVARCHAR(100), SetupEmpID				)							AS SetupEmployeeID
			,TRY_CONVERT(DATE		, SetupDTM				)					    		AS SetupDate
			,TRY_CONVERT(NVARCHAR(50), Gender					)							AS Gender
			,TRY_CONVERT(NVARCHAR(100), MailingAddr01			)							AS MailingAddress1
			,TRY_CONVERT(NVARCHAR(100), MailingAddr02			)							AS MailingAddress2
			,TRY_CONVERT(NVARCHAR(100), MailingCity			)				    			AS MailingCity
			,TRY_CONVERT(NVARCHAR(50), MailingStateCode		)				    			AS MailingStateCode
			,TRY_CONVERT(NVARCHAR(50), MailingCountryCode		)							AS MailingCountryCode
			,TRY_CONVERT(NVARCHAR(20), MailingPostalCode		)							AS MailingPostalCode
			,TRY_CONVERT(NVARCHAR(20), 'GeoCode'				)							AS Address_MatchType
			,TRY_CONVERT(REAL		, MailingLatitude		)					    		AS Address_Latitude
			,TRY_CONVERT(REAL		, MailingLongitude		)					    		AS Address_Longitude
			,TRY_CONVERT(DATETIME	, StartGamingDt			)					    		AS StartDateTime
			,TRY_CONVERT(DATETIME	, EndGamingDt			)					    		AS EndDateTime
			,TRY_CONVERT(INT		, isCurrent				)					    		AS IsCurrent
			,GETUTCDATE()				 										    		AS InsertDateTime
		FROM	 [qcic].[HRC_Dim_MarketablePlayersAttribute] WITH (NOLOCK)
		WHERE	 isBanned = 0 ;
/* testing 
select * from [test].[udf_HRC_ETL02]();

*/

drop function [test].[udf_HRC_ETL03];
CREATE FUNCTION [test].[udf_HRC_ETL03] ()
RETURNS TABLE
AS
RETURN
	SELECT	 	
			 TRY_CONVERT(NVARCHAR(100), 'HRC'					)							AS SystemName 
			,TRY_CONVERT(INT		, 3						)					    		AS SystemNameInt 
			,TRY_CONVERT(NVARCHAR(100), b.CasinoID				)							AS CasinoID
			,TRY_CONVERT(INT		, b.CasinoIDInt			)						    	AS CasinoIDInt
			,TRY_CONVERT(NVARCHAR(100), a.PlayerID				)							AS PlayerID
			,TRY_CONVERT(BIGINT		, [qcids].[udf_ConvertStringToBigInt](a.PlayerID,1))	AS PlayerIDBigInt
			,TRY_CONVERT(FLOAT		, DistanceToProperty		)							AS PlayerDistanceToCasino
			,TRY_CONVERT(DATETIME	, a.StartGamingDt		)						    	AS StartDateTime
			,TRY_CONVERT(DATETIME	, a.EndGamingDt			)						    	AS EndDateTime
			,TRY_CONVERT(INT		, a.IsCurrent			)						    	AS IsCurrent
			,GETUTCDATE()																	AS InsertDateTime
	FROM	 [qcic].[HRC_Dim_MarketablePlayersAttribute] a WITH (NOLOCK)
CROSS JOIN (
		SELECT	 SystemName
				,SystemNameInt
				,CasinoID
				,CasinoIDInt
		FROM	 [qcids].[App_Casino] WITH (NOLOCK)
		WHERE	 SystemNameInt = 3
		GROUP BY SystemName , SystemNameInt, CasinoID , CasinoIDInt) AS b
WHERE	 a.isBanned = 0 AND b.SystemName = 'HRC';
/*
-- unit testing 
select * from [test].[udf_HRC_ETL03]();


*/
drop function  [test].[udf_HRC_ETL04];
CREATE  FUNCTION [test].[udf_HRC_ETL04] ( )
RETURNS TABLE
AS
RETURN
	SELECT
			 TRY_CONVERT(NVARCHAR(100), 'HRC'					)						AS SystemName
			,TRY_CONVERT(INT		, 3						)							AS SystemNameInt
			,TRY_CONVERT(NVARCHAR(100), PlayerID				)						AS PlayerID
			,TRY_CONVERT(BIGINT		, [qcids].[udf_ConvertStringToBigInt](PlayerID,1))	AS PlayerIDBigInt
			,TRY_CONVERT(NVARCHAR(100), P_ClubStatus			)						AS LoyaltyCardLevel
			,TRY_CONVERT(DATETIME	, StartGamingDt			)							AS StartDateTime
			,TRY_CONVERT(DATETIME	, EndGamingDt			)							AS EndDateTime
			,TRY_CONVERT(INT		, isCurrent				)							AS IsCurrent
			,GETUTCDATE()																AS InsertDateTime
	FROM	 [qcic].[HRC_Dim_MarketablePlayersAttribute] WITH (NOLOCK);
/*
-- testing 
select * from [test].[udf_HRC_ETL04]();
*/

drop function  [test].[udf_HRC_ETL05];
CREATE   FUNCTION [test].[udf_HRC_ETL05] ( )
RETURNS TABLE
AS
RETURN
	SELECT	 	
			 TRY_CONVERT(NVARCHAR(100), 'HRC'	                                )	  AS SystemName
			,TRY_CONVERT(INT		, 3						)		                  AS SystemNameInt
			,TRY_CONVERT(NVARCHAR(100), a.CasinoID				)	                  AS CasinoID
			,TRY_CONVERT(INT		, b.CasinoIDInt			)		                  AS CasinoIDInt
			,TRY_CONVERT(NVARCHAR(100), PlayerID				)	                  AS PlayerID
			,TRY_CONVERT(BIGINT, [qcids].[udf_ConvertStringToBigInt]( PlayerID, 1 ) ) AS PlayerIDBigInt
			,TRY_CONVERT(NVARCHAR(100), HostEmpID)	                                  AS HostID
			,TRY_CONVERT(BIGINT, [qcids].[udf_ConvertStringToBigInt]( HostEmpID, 2) ) AS HostIDBigInt
			,TRY_CONVERT(NVARCHAR(100), 'Primary'				)		              AS PlayerHostType
			,TRY_CONVERT(DATETIME	, HostStartDTM			)	 	                  AS StartDateTime
			,TRY_CONVERT(DATETIME	, HostEndDTM				)	                  AS EndDateTime
			,TRY_CONVERT(INT		, isCurrent				)		                  AS IsCurrent
			,GETUTCDATE()											                  AS InsertDateTime
		FROM	 [qcic].[HRC_Fact_PlayerHostHistory_Primary] AS a WITH (NOLOCK)
		JOIN	 [qcids].[App_Casino] AS b WITH (NOLOCK)
 			ON		(a.CasinoID = b.CasinoID AND b.SystemNameInt = 3)
		GROUP BY a.CasinoID, b.CasinoIDInt, PlayerID, HostEmpID, HostStartDTM, HostEndDTM, isCurrent
	UNION ALL
	SELECT	 	
			 TRY_CONVERT(NVARCHAR(100), 'HRC'					)	                       AS SystemName
			,TRY_CONVERT(INT		, 3						)		                       AS SystemNameInt
			,TRY_CONVERT(NVARCHAR(100), a.CasinoID				)	                       AS CasinoID
			,TRY_CONVERT(INT		, b.CasinoIDInt			)		                       AS CasinoIDInt
			,TRY_CONVERT(NVARCHAR(100), PlayerID				)	                       AS PlayerID
			,TRY_CONVERT(BIGINT		, [qcids].[udf_ConvertStringToBigInt]( PlayerID, 1 ) ) AS PlayerIDBigInt
			,TRY_CONVERT(NVARCHAR(100), HostEmpID				)	                       AS HostID
			,TRY_CONVERT(BIGINT		, [qcids].[udf_ConvertStringToBigInt]( HostEmpID, 2) ) AS HostIDBigInt
			,TRY_CONVERT(NVARCHAR(100), 'Secondary'			)		                       AS PlayerHostType
			,TRY_CONVERT(DATETIME	, HostStartDTM			)		                       AS StartDateTime
			,TRY_CONVERT(DATETIME	, HostEndDTM				)                   	   AS EndDateTime
			,TRY_CONVERT(INT		, isCurrent				)	                           AS IsCurrent
			,GETUTCDATE()											      	               AS InsertDateTime
		FROM	 [qcic].[HRC_Fact_PlayerHostHistory_Secondary] AS a WITH (NOLOCK)
		JOIN	 [qcids].[App_Casino] AS  b WITH (NOLOCK)
			ON		(a.CasinoID = b.CasinoID AND b.SystemNameInt = 3)
		GROUP BY a.CasinoID, b.CasinoIDInt, PlayerID, HostEmpID , HostStartDTM , HostEndDTM, isCurrent;
/*
-- unit testing 
select * from [test].[udf_HRC_ETL05]( );
*/


drop function [test].[udf_HRC_ETL06];
CREATE  FUNCTION [test].[udf_HRC_ETL06] ( )
RETURNS TABLE
AS
RETURN
	SELECT	 	
			 TRY_CONVERT(NVARCHAR(100)	, 'HRC'					)		AS SystemName
			,TRY_CONVERT(INT			,3						)		AS SystemNameInt
			,TRY_CONVERT(NVARCHAR(100)	,TranCasinoID			)		AS CasinoID
			,TRY_CONVERT(INT			,b.CasinoIDInt			)		AS CasinoIDInt
			,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)		AS PlayerID
			,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt]( PlayerID, 1 )) AS PlayerIDBigInt
			,TRY_CONVERT(INT			,ROUND(CreditLimit * 100,0))	AS CreditLimitInt100
			,TRY_CONVERT(INT			,IsCreditPlayer			)		AS IsActiveCredit
			,TRY_CONVERT(NVARCHAR(100)	,SetupCasinoID			)		AS SetupCasinoID
			,TRY_CONVERT(NVARCHAR(4000)	,GroupCode				)		AS Note
			,TRY_CONVERT(DATETIME		,StartGamingDt			)		AS StartDateTime
			,TRY_CONVERT(DATETIME		,EndGamingDt			)		AS EndDateTime
			,TRY_CONVERT(INT			,isCurrent				)		AS IsCurrent
			,GETUTCDATE()												AS InsertDateTime
		FROM	 [qcic].[HRC_Dim_PlayerCreditLimit_History] a WITH (NOLOCK)
		JOIN	 [qcids].[App_Casino] b WITH (NOLOCK)
	ON		(a.TranCasinoID = b.CasinoID AND b.SystemNameInt = 3);
/* unit test 
select * from [test].[udf_HRC_ETL06]();
---- */


drop function  [test].[udf_HRC_ETL07];
CREATE  FUNCTION [test].[udf_HRC_ETL07] ( )
RETURNS TABLE
AS
RETURN
	SELECT	 
		SystemName
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
		,EndDateTime = LEAD(StartDateTime, 1, {d '2999-12-31'}) OVER ( PARTITION BY	 SystemName, CasinoID, PlayerID, ElectronicCreditType
				   														ORDER BY StartDateTime   )
		,IsCurrent
		,InsertDateTime
		FROM	(
			SELECT	 	
					 TRY_CONVERT(NVARCHAR(100)	,'HRC'					)		AS SystemName
					,TRY_CONVERT(INT			,3						)		AS SystemNameInt
					,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId				)		AS CasinoID
					,TRY_CONVERT(INT			,b.CasinoIDInt			)		AS CasinoIDInt
					,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)		AS PlayerID
					,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](PlayerID,1)) AS PlayerIDBigInt
					,TRY_CONVERT(NVARCHAR(100)	,
								case when EnrollOption = 2 then 'Power Bank'
									 else 'Self-Pay Jackpots' end						)		AS ElectronicCreditType
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
						FROM	 [qcic].[HRC_Dim_PlayerECreditEnrollment_History] WITH (NOLOCK)
						WHERE	 EnrollOption IN (2,3)
					UNION ALL
					SELECT	
						 EnrollmentId
						,Acct
						,PlayerId
						,IsEnroll = case when EnrollOption = 0 then 0 else IsEnroll end
						,EnrollOption = 2
						,CasinoId
						,GamingDt
						,EMAEmpID = 0
						,ReasonDesc
						,Remarks
						,SignatureId
						,CreatedBy
						,ModifiedBy
						,CreatedDtm
						,ModifiedDtm
						,IsCurrent
						,InsertDtm
						FROM	 [qcic].[HRC_Dim_PlayerECreditEnrollment_History] WITH (NOLOCK)
						WHERE	 EnrollOption IN (0,1)
					UNION ALL
					SELECT	
						 EnrollmentId
						,Acct
						,PlayerId
						,IsEnroll = case when EnrollOption = 0 then 0 else IsEnroll end
						,EnrollOption = 3
						,CasinoId
						,GamingDt
						,EMAEmpID = 0
						,ReasonDesc
						,Remarks
						,SignatureId
						,CreatedBy
						,ModifiedBy
						,CreatedDtm
						,ModifiedDtm
						,IsCurrent
						,InsertDtm
						FROM	 [qcic].[HRC_Dim_PlayerECreditEnrollment_History] WITH (NOLOCK)
						WHERE	 EnrollOption IN (0,1) ) AS  a
				JOIN	 [qcids].[App_Casino] AS b WITH (NOLOCK)
				ON		(a.CasinoID = b.CasinoID AND b.SystemNameInt = 3) ) x;
/* Unit testing
select * from  [test].[udf_HRC_ETL07]();
*/
drop function [test].[udf_HRC_ETL08];
CREATE FUNCTION [test].[udf_HRC_ETL08](  @MaxDate DATE,  @days int   )
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
/*  testing 
DECLARE @SystemNameInt int = 3, @MaxDate DATE,  @days int = -10;
SELECT @MaxDate = MAX(GamingDate) FROM [test].[Fact_PlayerDeviceRatingDay] WITH (NOLOCK) WHERE SystemNameInt = @SystemNameInt;
SELECT @MaxDate = ISNULL(@MaxDate,'2000-01-01');
print @MaxDate
print @days
select * 
from [test].[udf_HRC_ETL08](@MaxDate,@days );

*/


drop function  [test].[udf_HRC_ETL09];
CREATE  FUNCTION [test].[udf_HRC_ETL09]( )
RETURNS TABLE
AS
RETURN
--Slot Ratings
	SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'						)								AS SystemName
		,TRY_CONVERT(INT			,3							)								AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,OrigMfg					)								AS OriginalManufacturer
		,TRY_CONVERT(NVARCHAR(100)	,Mfg						)								AS ManufacturerCleaned
		,TRY_CONVERT(NVARCHAR(100)	,Mfg						)								AS ManufacturerGroup
		,TRY_CONVERT(DATETIME		,GETUTCDATE()				)								AS InsertDateTime
		FROM	 [qcic].[HRC_Dim_SlotMFGGroup] WITH (NOLOCK) ;
/* Unit testing 
select * from [test].[udf_HRC_ETL09] () ;
*/

drop function [test].[udf_HRC_ETL10];
CREATE FUNCTION [test].[udf_HRC_ETL10](   )
RETURNS TABLE
AS
RETURN
	SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'						)								AS SystemName
		,TRY_CONVERT(INT			,3							)								AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,OrigGameType				)								AS OriginalGameType
		,TRY_CONVERT(NVARCHAR(100)	,GameTypeCleaned			)								AS GameTypeCleaned
		,TRY_CONVERT(NVARCHAR(100)	,GameType					)								AS GameTypeGroup
		,TRY_CONVERT(NVARCHAR(100)	,GameTypeShortName			)								AS GameTypeShortName
		,TRY_CONVERT(NVARCHAR(100)	,GameTypeSummary			)								AS GameTypeSummary
		,TRY_CONVERT(DATETIME		,GETUTCDATE()				)								AS InsertDateTime
FROM	 [qcic].[HRC_Dim_SlotGameTypeGroup] WITH (NOLOCK);
/* unit testing 
select * from  [test].[udf_HRC_ETL10]();
*/

drop function  [test].[udf_HRC_ETL11];
CREATE  FUNCTION [test].[udf_HRC_ETL11](  )
RETURNS TABLE
AS
RETURN
	SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'					)								AS SystemName
		,TRY_CONVERT(INT			,3						)								AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,OrigDenom				)								AS OriginalDenomination
		,TRY_CONVERT(NVARCHAR(100)	,Denom					)								AS DenominationCleaned
		,TRY_CONVERT(NVARCHAR(100)	,DenomSort				)								AS DenominationSort
		,TRY_CONVERT(NVARCHAR(100)	,DenomGroup				)								AS DenominationGroup
		,TRY_CONVERT(NVARCHAR(100)	,DenomGroupSort			)								AS DenominationGroupSort
		,TRY_CONVERT(DATETIME		,GETUTCDATE()			)								AS InsertDateTime
	FROM	 [qcic].[HRC_Dim_SlotDenomGroup] WITH (NOLOCK); 
/*  testing
select * from  [test].[udf_HRC_ETL11]( );

*/

drop function  [test].[udf_HRC_ETL12] ; 
CREATE FUNCTION [test].[udf_HRC_ETL12](   )
RETURNS TABLE
AS
RETURN
	SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'					)								AS SystemName
		,TRY_CONVERT(INT			,3						)								AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId				)								AS IssueCasinoID
		,TRY_CONVERT(INT			,b.CasinoIDInt			)								AS IssueCasinoIDInt
		,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId				)								AS RedeemCasinoID
		,TRY_CONVERT(INT			,b.CasinoIDInt			)								AS RedeemCasinoIDInt
		,TRY_CONVERT(NVARCHAR(100)	,PlayerId				)								AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](PlayerID,1))		AS PlayerIDBigInt
		,TRY_CONVERT(DATETIME		,GamingDt				)								AS RedeemDateTime
		,TRY_CONVERT(NVARCHAR(100)	,RedeemPrizeId			)								AS PrizeID
		,TRY_CONVERT(NVARCHAR(100)	,TranType				)								AS PrizeType
		,TRY_CONVERT(INT			,ROUND(AmountAuth*100,0))								AS PrizeIssueAmountInt100
		,TRY_CONVERT(INT			,ROUND(AmountUsed*100,0))								AS PrizeRedeemAmountInt100
		,TRY_CONVERT(INT			,ROUND(RedeemPts*100,0)	)								AS PointForPrizeRedeemAmountInt100
		,TRY_CONVERT(DATETIME		,RedeemedDtm			)								AS PrizeStartDateTime
		,TRY_CONVERT(DATETIME		,RedeemedDtm			)								AS PrizeEndDateTime
		,TRY_CONVERT(NVARCHAR(100)	,''						)								AS PrizeCampaignID
		,TRY_CONVERT(NVARCHAR(100)	,''						)								AS PrizeCampaignSegmentID
		,TRY_CONVERT(DATETIME		,GETUTCDATE()			)								AS InsertDateTime
		FROM	 [qcic].[HRC_Fact_PlayerRedemption_Tran] AS a WITH (NOLOCK)
		JOIN	 [qcids].[App_Casino] b WITH (NOLOCK)
		ON		(a.CasinoId = b.CasinoID AND b.SystemNameInt = 3); 
/*  testing 
select * from [test].[udf_HRC_ETL12]();
*/

drop function  [test].[udf_HRC_ETL14];
CREATE   FUNCTION [test].[udf_HRC_ETL14](   )
RETURNS TABLE
AS
RETURN
	--Slot Ratings
	SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'					)								AS SystemName
		,TRY_CONVERT(INT			,3						)								AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,PlayerId				)								AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt]( PlayerID, 1 ) )		AS PlayerIDBigInt
		,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId				)								AS CasinoGroupID
		,TRY_CONVERT(INT			,b.CasinoGroupIDInt		)								AS CasinoGroupIDInt
		,TRY_CONVERT(INT			,ROUND(MAX(ValuationScore)*100,0))						AS EvaluationScoreInt100
		,TRY_CONVERT(DATETIME		,GETUTCDATE()			)								AS InsertDateTime
		FROM	 [qcic].[HRC_Fact_PlayerValuationScore] AS a WITH (NOLOCK)
		JOIN	 [qcids].[App_CasinoGroup] AS b WITH (NOLOCK)
		ON		(a.CasinoID = b.CasinoGroupID AND b.SystemNameInt = 3)
		GROUP BY PlayerID, a.CasinoId ,b.CasinoGroupIDInt ; 
/* unit testing 
select * from [test].[udf_HRC_ETL14]( );
*/

drop function [test].[udf_HRC_ETL15]; 
CREATE FUNCTION [test].[udf_HRC_ETL15](  @MaxDate DATE,  @days int   )
/*
  Inputs: @MaxDate  It is the maximum GamingDate from the table  [qcids].[Fact_DeviceDay] where SystemNameInt is equal to 3 (HRC) 
          @days Only for no hardcoding another value
*/
RETURNS TABLE
AS
RETURN
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
		WHERE a.GamingDt  >= DATEADD(DAY, @days, @MaxDate) ;
/* TESTING 
DECLARE @SystemNameInt int = 3, @MaxDate DATE, @SystemName nvarchar(100) = 'HRC', @days int=-10;
SELECT @MaxDate = MAX(GamingDate) FROM [qcids].[Fact_DeviceDay] WITH (NOLOCK) WHERE SystemNameInt = @SystemNameInt
SELECT @MaxDate = ISNULL(@MaxDate,'2000-01-01')
print @MaxDate
 SELECT * 
		FROM [test].[udf_HRC_ETL15](@MaxDate , @days)
		order by GamingDate
*/

