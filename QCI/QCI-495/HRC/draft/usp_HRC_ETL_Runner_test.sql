/****** Object:  StoredProcedure [test].[usp_HRC_ETL_Runner_test]    Script Date: 10/1/2020 6:40:16 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/***********************************************************************************************
Author:			Ralph Thomas

Date:			2020-04-19

Notes:			Runs the ETLs for HRC System to build the UDM Connection Layer as well as the base UDM Data Science Layer Tables

				Requires schemas qcic, qcids, and qciapi to be created first
				Requires qcic tables to be created second
				Requires qcic udf_HRC_ETL01, udf_HRC_ETL02, udf_HRC_ETL03, udf_HRC_ETL04, udf_HRC_ETL05, udf_HRC_ETL06
                              udf_HRC_ETL07, udf_HRC_ETL08, udf_HRC_ETL09, udf_HRC_ETL10, udf_HRC_ETL11, udf_HRC_ETL12  
                              udf_HRC_ETL14 functions to be created third
				This gets run fourth				
				
Modifictions:
Date			Name			Notes
<CurrentDate>	<Name>			<Desc>
***********************************************************************************************/
ALTER   PROCEDURE [test].[usp_HRC_ETL_Runner_test] -- change to [qcic].[usp_HRC_ETL_Runner]
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	DECLARE @SystemName nvarchar(100) = 'HRC'
			,@SystemNameInt int = 3
			,@MaxDate date
	DROP TABLE IF EXISTS [test].[HRC_ETL_Runner_Tracker] -- CHANGE TO [qcic]
	SELECT TimeStamp = GETDATE(),TableCompleted = convert(nvarchar(255),'Start'),RecordCount = 0 INTO [test].[HRC_ETL_Runner_Tracker] -- change to [qcic]

	/******************
	UDM-DS Steps
	******************/
    If(OBJECT_ID('tempdb..#HRC_ETL01') Is Not Null)
	Begin
		Drop Table #HRC_ETL01
	End	
	CREATE table #HRC_ETL01   ( SystemName NVARCHAR(100), SystemNameInt INT, CasinoGroupID NVARCHAR(100), CasinoGroupIDInt INT, HostID NVARCHAR(100),
                              HostIDBigInt  BIGINT, HostFirstName NVARCHAR(50), HostLastName NVARCHAR(50), HostPosition NVARCHAR(100) , HostCode  NVARCHAR(50), 
	                              StartDateTime DATETIME, EndDateTime  DATETIME, IsCurrent   INT, InsertDateTime DATETIME);
	INSERT INTO #HRC_ETL01 
	SELECT *  
		FROM [test].[udf_HRC_ETL01]( );
	--DECLARE @SystemNameInt int = 3
	BEGIN TRANSACTION
		--DELETE FROM [qcids].[Dim_Host_SCD] WHERE SystemName = @SystemName
		--INSERT INTO [qcids].[Dim_Host_SCD] SELECT * FROM [qcic].[uvw_HRC_ETL01_Dim_Host_SCD]
		MERGE [test].[Dim_Host_SCD] T -- CHANGE TO [qcids]
		USING #HRC_ETL01 AS S
		ON T.SystemNameInt = S.SystemNameInt AND T.CasinoGroupIDInt = S.CasinoGroupIDInt AND T.HostIDBigInt = S.HostIDBigInt AND T.StartDateTime = S.StartDateTime AND T.IsCurrent = S.IsCurrent
		WHEN MATCHED AND
			BINARY_CHECKSUM(T.SystemName, T.SystemNameInt, T.CasinoGroupID, T.CasinoGroupIDInt, T.HostID, T.HostIDBigInt, T.HostFirstName, T.HostLastName, T.HostPosition, T.HostCode, T.StartDateTime, T.EndDateTime, T.IsCurrent)
			<>
			BINARY_CHECKSUM(S.SystemName, S.SystemNameInt, S.CasinoGroupID, S.CasinoGroupIDInt, S.HostID, S.HostIDBigInt, S.HostFirstName, S.HostLastName, S.HostPosition, S.HostCode, S.StartDateTime, S.EndDateTime, S.IsCurrent)

			THEN UPDATE SET
                 T.[SystemName] = S.[SystemName]
                ,T.[SystemNameInt] = S.[SystemNameInt]
                ,T.[CasinoGroupID] = S.[CasinoGroupID]
                ,T.[CasinoGroupIDInt] = S.[CasinoGroupIDInt]
                ,T.[HostID] = S.[HostID]
                ,T.[HostIDBigInt] = S.[HostIDBigInt]
                ,T.[HostFirstName] = S.[HostFirstName]
                ,T.[HostLastName] = S.[HostLastName]
                ,T.[HostPosition] = S.[HostPosition]
                ,T.[HostCode] = S.[HostCode]
                ,T.[StartDateTime] = S.[StartDateTime]
                ,T.[EndDateTime] = S.[EndDateTime]
                ,T.[IsCurrent] = S.[IsCurrent]
                ,T.[InsertDateTime] = S.[InsertDateTime]
		WHEN NOT MATCHED
			THEN INSERT (SystemName, SystemNameInt, CasinoGroupID, CasinoGroupIDInt, HostID, HostIDBigInt
						, HostFirstName, HostLastName, HostPosition, HostCode, StartDateTime, EndDateTime, IsCurrent, InsertDateTime)
				 VALUES (S.SystemName, S.SystemNameInt, S.CasinoGroupID, S.CasinoGroupIDInt, S.HostID, S.HostIDBigInt
						, S.HostFirstName, S.HostLastName, S.HostPosition, S.HostCode, S.StartDateTime, S.EndDateTime, S.IsCurrent, S.InsertDateTime)
		WHEN NOT MATCHED BY SOURCE AND T.SystemNameInt = @SystemNameInt
			THEN DELETE;
	COMMIT TRANSACTION
		INSERT INTO [test].[HRC_ETL_Runner_Tracker] -- CHANGE to [qcic]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[qcids].[Dim_Host_SCD]',RecordCount = count(*) FROM [qcids].[Dim_Host_SCD]
		WHERE SystemNameInt = @SystemNameInt

	If(OBJECT_ID('tempdb..#HRC_ETL02') Is Not Null)
	Begin
		Drop Table #HRC_ETL02;
	End
	CREATE TABLE #HRC_ETL02  ( SystemName  NVARCHAR(100), SystemNameInt INT, PlayerID NVARCHAR(100), PlayerIDBigInt BIGINT, Account NVARCHAR(100), Title NVARCHAR(20), 
                                LastName NVARCHAR(50), FirstName NVARCHAR(50), DisplayName NVARCHAR(50), NickNameAlias NVARCHAR(50), HomeAddress1 NVARCHAR(100), HomeAddress2 NVARCHAR(100)   , 
                                HomeCity NVARCHAR(100), HomeStateCode NVARCHAR(50) , HomeCountryCode NVARCHAR(50), HomePostalCode NVARCHAR(20), HomeTelephone1Type NVARCHAR(20), HomeTelephone1 NVARCHAR(20), 
                                HomeTelephone2Type NVARCHAR(20), HomeTelephone2  NVARCHAR(20), HomeEmail NVARCHAR(255), AltName NVARCHAR(50), AltAddress1 NVARCHAR(100),  AltAddress2 NVARCHAR(100), 
                                AltCity NVARCHAR(100), AltStateCode NVARCHAR(50),AltCountryCode NVARCHAR(50), AltPostalCode NVARCHAR(20),  AltTelephone1Type NVARCHAR(20), AltTelephone1 NVARCHAR(20),
                                AltTelephone2Type NVARCHAR(20), AltTelephone2 NVARCHAR(20), AltEmail NVARCHAR(255), BirthDate DATE, IsPrivacy INT, IsMailToAlt INT, IsNoMail INT, IsReturnMail INT, IsVIP INT, IsLostAndFound INT, IsCreditAccount INT, 
                                IsBanned INT, IsInactive INT, IsCall INT, IsEmailSend INT, SetupCasinoID NVARCHAR(100), SetupEmployeeID NVARCHAR(100), SetupDate DATE, Gender NVARCHAR(50) , MailingAddress1 NVARCHAR(100), MailingAddress2 NVARCHAR(100), MailingCity NVARCHAR(100), 
                                MailingStateCode NVARCHAR(50), MailingCountryCode NVARCHAR(50), MailingPostalCode NVARCHAR(20), Address_MatchType NVARCHAR(20), Address_Latitude REAL, 
                                Address_Longitude REAL, StartDateTime DATETIME , EndDateTime DATETIME, IsCurrent INT, InsertDateTime DATETIME );
	INSERT INTO #HRC_ETL02 
	SELECT *  
	FROM [test].[udf_HRC_ETL02]( );
	--DECLARE @SystemNameInt int = 3
   BEGIN TRANSACTION
		--DELETE FROM [qcids].[Dim_Player_SCD] WHERE SystemNameInt = @SystemNameInt
		--INSERT INTO [qcids].[Dim_Player_SCD] SELECT * FROM [qcic].[uvw_HRC_ETL02_Dim_Player_SCD]
		MERGE [test].[Dim_Player_SCD] T -- CHANGE TO [qcids]
		USING #HRC_ETL02  AS S
		ON T.SystemNameInt = S.SystemNameInt AND T.PlayerIDBigInt = S.PlayerIDBigInt AND T.StartDateTime = S.StartDateTime AND T.IsCurrent = S.IsCurrent
		WHEN MATCHED AND 
			BINARY_CHECKSUM(T.SystemName, T.SystemNameInt, T.PlayerID, T.PlayerIDBigInt, T.Account, T.Title, T.LastName, T.FirstName, T.DisplayName, T.NickNameAlias, T.HomeAddress1, T.HomeAddress2, T.HomeCity, T.HomeStateCode, T.HomeCountryCode, T.HomePostalCode, T.HomeTelephone1Type, T.HomeTelephone1, T.HomeTelephone2Type, T.HomeTelephone2, T.HomeEmail, T.AltName, T.AltAddress1, T.AltAddress2, T.AltCity, T.AltStateCode, T.AltCountryCode, T.AltPostalCode, T.AltTelephone1Type, T.AltTelephone1, T.AltTelephone2Type, T.AltTelephone2, T.AltEmail, T.BirthDate, T.IsPrivacy, T.IsMailToAlt, T.IsNoMail, T.IsReturnMail, T.IsVIP, T.IsLostAndFound, T.IsCreditAccount, T.IsBanned, T.IsInactive, T.IsCall, T.IsEmailSend, T.SetupCasinoID, T.SetupEmployeeID, T.SetupDate, T.Gender, T.MailingAddress1, T.MailingAddress2, T.MailingCity, T.MailingStateCode, T.MailingCountryCode, T.MailingPostalCode, T.Address_MatchType, T.Address_Latitude, T.Address_Longitude, T.StartDateTime, T.EndDateTime, T.IsCurrent)
			<>
			BINARY_CHECKSUM(S.SystemName, S.SystemNameInt, S.PlayerID, S.PlayerIDBigInt, S.Account, S.Title, S.LastName, S.FirstName, S.DisplayName, S.NickNameAlias, S.HomeAddress1, S.HomeAddress2, S.HomeCity, S.HomeStateCode, S.HomeCountryCode, S.HomePostalCode, S.HomeTelephone1Type, S.HomeTelephone1, S.HomeTelephone2Type, S.HomeTelephone2, S.HomeEmail, S.AltName, S.AltAddress1, S.AltAddress2, S.AltCity, S.AltStateCode, S.AltCountryCode, S.AltPostalCode, S.AltTelephone1Type, S.AltTelephone1, S.AltTelephone2Type, S.AltTelephone2, S.AltEmail, S.BirthDate, S.IsPrivacy, S.IsMailToAlt, S.IsNoMail, S.IsReturnMail, S.IsVIP, S.IsLostAndFound, S.IsCreditAccount, S.IsBanned, S.IsInactive, S.IsCall, S.IsEmailSend, S.SetupCasinoID, S.SetupEmployeeID, S.SetupDate, S.Gender, S.MailingAddress1, S.MailingAddress2, S.MailingCity, S.MailingStateCode, S.MailingCountryCode, S.MailingPostalCode, S.Address_MatchType, S.Address_Latitude, S.Address_Longitude, S.StartDateTime, S.EndDateTime, S.IsCurrent)
			THEN UPDATE SET
					 T.[SystemName] = S.[SystemName]
					,T.[SystemNameInt] = S.[SystemNameInt]
					,T.[PlayerID] = S.[PlayerID]
					,T.[PlayerIDBigInt] = S.[PlayerIDBigInt]
					,T.[Account] = S.[Account]
					,T.[Title] = S.[Title]
					,T.[LastName] = S.[LastName]
					,T.[FirstName] = S.[FirstName]
					,T.[DisplayName] = S.[DisplayName]
					,T.[NickNameAlias] = S.[NickNameAlias]
					,T.[HomeAddress1] = S.[HomeAddress1]
					,T.[HomeAddress2] = S.[HomeAddress2]
					,T.[HomeCity] = S.[HomeCity]
					,T.[HomeStateCode] = S.[HomeStateCode]
					,T.[HomeCountryCode] = S.[HomeCountryCode]
					,T.[HomePostalCode] = S.[HomePostalCode]
					,T.[HomeTelephone1Type] = S.[HomeTelephone1Type]
					,T.[HomeTelephone1] = S.[HomeTelephone1]
					,T.[HomeTelephone2Type] = S.[HomeTelephone2Type]
					,T.[HomeTelephone2] = S.[HomeTelephone2]
					,T.[HomeEmail] = S.[HomeEmail]
					,T.[AltName] = S.[AltName]
					,T.[AltAddress1] = S.[AltAddress1]
					,T.[AltAddress2] = S.[AltAddress2]
					,T.[AltCity] = S.[AltCity]
					,T.[AltStateCode] = S.[AltStateCode]
					,T.[AltCountryCode] = S.[AltCountryCode]
					,T.[AltPostalCode] = S.[AltPostalCode]
					,T.[AltTelephone1Type] = S.[AltTelephone1Type]
					,T.[AltTelephone1] = S.[AltTelephone1]
					,T.[AltTelephone2Type] = S.[AltTelephone2Type]
					,T.[AltTelephone2] = S.[AltTelephone2]
					,T.[AltEmail] = S.[AltEmail]
					,T.[BirthDate] = S.[BirthDate]
					,T.[IsPrivacy] = S.[IsPrivacy]
					,T.[IsMailToAlt] = S.[IsMailToAlt]
					,T.[IsNoMail] = S.[IsNoMail]
					,T.[IsReturnMail] = S.[IsReturnMail]
					,T.[IsVIP] = S.[IsVIP]
					,T.[IsLostAndFound] = S.[IsLostAndFound]
					,T.[IsCreditAccount] = S.[IsCreditAccount]
					,T.[IsBanned] = S.[IsBanned]
					,T.[IsInactive] = S.[IsInactive]
					,T.[IsCall] = S.[IsCall]
					,T.[IsEmailSend] = S.[IsEmailSend]
					,T.[SetupCasinoID] = S.[SetupCasinoID]
					,T.[SetupEmployeeID] = S.[SetupEmployeeID]
					,T.[SetupDate] = S.[SetupDate]
					,T.[Gender] = S.[Gender]
					,T.[MailingAddress1] = S.[MailingAddress1]
					,T.[MailingAddress2] = S.[MailingAddress2]
					,T.[MailingCity] = S.[MailingCity]
					,T.[MailingStateCode] = S.[MailingStateCode]
					,T.[MailingCountryCode] = S.[MailingCountryCode]
					,T.[MailingPostalCode] = S.[MailingPostalCode]
					,T.[Address_MatchType] = S.[Address_MatchType]
					,T.[Address_Latitude] = S.[Address_Latitude]
					,T.[Address_Longitude] = S.[Address_Longitude]
					,T.[StartDateTime] = S.[StartDateTime]
					,T.[EndDateTime] = S.[EndDateTime]
					,T.[IsCurrent] = S.[IsCurrent]
					,T.[InsertDateTime] = S.[InsertDateTime]
		WHEN NOT MATCHED
			THEN INSERT (SystemName, SystemNameInt, PlayerID, PlayerIDBigInt, Account, Title, LastName, FirstName, DisplayName, NickNameAlias, HomeAddress1, HomeAddress2, HomeCity, HomeStateCode, HomeCountryCode, HomePostalCode, HomeTelephone1Type, HomeTelephone1, HomeTelephone2Type, HomeTelephone2, HomeEmail, AltName, AltAddress1, AltAddress2, AltCity, AltStateCode, AltCountryCode, AltPostalCode, AltTelephone1Type, AltTelephone1, AltTelephone2Type, AltTelephone2, AltEmail, BirthDate, IsPrivacy, IsMailToAlt, IsNoMail, IsReturnMail, IsVIP, IsLostAndFound, IsCreditAccount, IsBanned, IsInactive, IsCall, IsEmailSend, SetupCasinoID, SetupEmployeeID, SetupDate, Gender, MailingAddress1, MailingAddress2, MailingCity, MailingStateCode, MailingCountryCode, MailingPostalCode, Address_MatchType, Address_Latitude, Address_Longitude, StartDateTime, EndDateTime, IsCurrent, InsertDateTime)
			VALUES (S.SystemName, S.SystemNameInt, S.PlayerID, S.PlayerIDBigInt, S.Account, S.Title, S.LastName, S.FirstName, S.DisplayName, S.NickNameAlias, S.HomeAddress1, S.HomeAddress2, S.HomeCity, S.HomeStateCode, S.HomeCountryCode, S.HomePostalCode, S.HomeTelephone1Type, S.HomeTelephone1, S.HomeTelephone2Type, S.HomeTelephone2, S.HomeEmail, S.AltName, S.AltAddress1, S.AltAddress2, S.AltCity, S.AltStateCode, S.AltCountryCode, S.AltPostalCode, S.AltTelephone1Type, S.AltTelephone1, S.AltTelephone2Type, S.AltTelephone2, S.AltEmail, S.BirthDate, S.IsPrivacy, S.IsMailToAlt, S.IsNoMail, S.IsReturnMail, S.IsVIP, S.IsLostAndFound, S.IsCreditAccount, S.IsBanned, S.IsInactive, S.IsCall, S.IsEmailSend, S.SetupCasinoID, S.SetupEmployeeID, S.SetupDate, S.Gender, S.MailingAddress1, S.MailingAddress2, S.MailingCity, S.MailingStateCode, S.MailingCountryCode, S.MailingPostalCode, S.Address_MatchType, S.Address_Latitude, S.Address_Longitude, S.StartDateTime, S.EndDateTime, S.IsCurrent, S.InsertDateTime)
		WHEN NOT MATCHED BY SOURCE AND T.SystemNameInt = @SystemNameInt
			THEN DELETE;
    COMMIT TRANSACTION
		INSERT INTO [test].[HRC_ETL_Runner_Tracker] --CHANGE TO [qcic]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[qcids].[Dim_Player_SCD]',RecordCount = count(*) FROM [qcids].[Dim_Player_SCD]
		WHERE SystemNameInt = @SystemNameInt

	If(OBJECT_ID('tempdb..#HRC_ETL03') Is Not Null)
	Begin
		Drop Table #HRC_ETL03
	End
	CREATE TABLE #HRC_ETL03   ( SystemName NVARCHAR(100), SystemNameInt INT, CasinoID NVARCHAR(100), CasinoIDInt INT, PlayerID NVARCHAR(100), PlayerIDBigInt BIGINT, 
                                 PlayerDistanceToCasino FLOAT, StartDateTime DATETIME , EndDateTime DATETIME, IsCurrent INT, InsertDateTime DATETIME) ; 
	INSERT INTO #HRC_ETL03
	SELECT * 
		FROM [test].[udf_HRC_ETL03]( ) ;
	--DECLARE @SystemNameInt int = 3
    BEGIN TRANSACTION
		--DELETE FROM [qcids].[Dim_PlayerDistanceToCasino_SCD] WHERE SystemNameInt = @SystemNameInt
		--INSERT INTO [qcids].[Dim_PlayerDistanceToCasino_SCD] SELECT * FROM [qcic].[uvw_HRC_ETL03_Dim_PlayerDistanceToCasino_SCD]
		MERGE [test].[Dim_PlayerDistanceToCasino_SCD] T -- CHANGE TO [qcids]
		USING  #HRC_ETL03 AS S
		ON T.SystemNameInt = S.SystemNameInt AND T.CasinoIDInt = S.CasinoIDInt AND T.PlayerIDBigInt = S.PlayerIDBigInt AND T.StartDateTime = S.StartDateTime AND T.IsCurrent = S.IsCurrent
		WHEN MATCHED AND 
			BINARY_CHECKSUM(T.SystemName, T.SystemNameInt, T.CasinoID, T.CasinoIDInt, T.PlayerID, T.PlayerIDBigInt, T.PlayerDistanceToCasino, T.StartDateTime, T.EndDateTime, T.IsCurrent)
			<>
			BINARY_CHECKSUM(S.SystemName, S.SystemNameInt, S.CasinoID, S.CasinoIDInt, S.PlayerID, S.PlayerIDBigInt, S.PlayerDistanceToCasino, S.StartDateTime, S.EndDateTime, S.IsCurrent)
			THEN UPDATE SET
					 T.[SystemName] = S.[SystemName]
					,T.[SystemNameInt] = S.[SystemNameInt]
					,T.[CasinoID] = S.[CasinoID]
					,T.[CasinoIDInt] = S.[CasinoIDInt]
					,T.[PlayerID] = S.[PlayerID]
					,T.[PlayerIDBigInt] = S.[PlayerIDBigInt]
					,T.[PlayerDistanceToCasino] = S.[PlayerDistanceToCasino]
					,T.[StartDateTime] = S.[StartDateTime]
					,T.[EndDateTime] = S.[EndDateTime]
					,T.[IsCurrent] = S.[IsCurrent]
					,T.[InsertDateTime] = S.[InsertDateTime]
		WHEN NOT MATCHED
			THEN INSERT (SystemName, SystemNameInt, CasinoID, CasinoIDInt, PlayerID, PlayerIDBigInt, PlayerDistanceToCasino, StartDateTime, EndDateTime, IsCurrent, InsertDateTime)
			VALUES (S.SystemName, S.SystemNameInt, S.CasinoID, S.CasinoIDInt, S.PlayerID, S.PlayerIDBigInt, S.PlayerDistanceToCasino, S.StartDateTime, S.EndDateTime, S.IsCurrent, S.InsertDateTime)
		WHEN NOT MATCHED BY SOURCE AND T.SystemNameInt = @SystemNameInt
			THEN DELETE;
    COMMIT TRANSACTION
		INSERT INTO [test].[HRC_ETL_Runner_Tracker] -- CHANGE TO [qcic]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[qcids].[Dim_PlayerDistanceToCasino_SCD]',RecordCount = count(*) FROM [qcids].[Dim_PlayerDistanceToCasino_SCD]
		WHERE SystemNameInt = @SystemNameInt


	If(OBJECT_ID('tempdb..#HRC_ETL04') Is Not Null)
	Begin
	Drop Table #HRC_ETL04
	End
	CREATE TABLE #HRC_ETL04  (  SystemName NVARCHAR(100), SystemNameInt INT , PlayerID NVARCHAR(100), PlayerIDBigInt BIGINT, LoyaltyCardLevel NVARCHAR(100), 
									StartDateTime DATETIME, EndDateTime DATETIME, IsCurrent INT , InsertDateTime DATETIME );
	INSERT INTO #HRC_ETL04
	SELECT * 
		FROM [test].[udf_HRC_ETL04]() ;
	--DECLARE @SystemNameInt int = 3
    BEGIN TRANSACTION
		--DELETE FROM [qcids].[Dim_PlayerLoyalty_SCD] WHERE SystemNameInt = @SystemNameInt
		--INSERT INTO [qcids].[Dim_PlayerLoyalty_SCD] SELECT * FROM [qcic].[uvw_HRC_ETL04_Dim_PlayerLoyalty_SCD]
		MERGE [test].[Dim_PlayerLoyalty_SCD] T --change to [qcids]
		USING #HRC_ETL04 AS S
		ON T.SystemNameInt = S.SystemNameInt AND T.PlayerIDBigInt = S.PlayerIDBigInt AND T.StartDateTime = S.StartDateTime AND T.IsCurrent = S.IsCurrent
		WHEN MATCHED AND 
			BINARY_CHECKSUM(T.SystemName, T.SystemNameInt, T.PlayerID, T.PlayerIDBigInt, T.LoyaltyCardLevel, T.StartDateTime, T.EndDateTime, T.IsCurrent)
			<>
			BINARY_CHECKSUM(S.SystemName, S.SystemNameInt, S.PlayerID, S.PlayerIDBigInt, S.LoyaltyCardLevel, S.StartDateTime, S.EndDateTime, S.IsCurrent)
			THEN UPDATE SET
					 T.[SystemName] = S.[SystemName]
					,T.[SystemNameInt] = S.[SystemNameInt]
					,T.[PlayerID] = S.[PlayerID]
					,T.[PlayerIDBigInt] = S.[PlayerIDBigInt]
					,T.[LoyaltyCardLevel] = S.[LoyaltyCardLevel]
					,T.[StartDateTime] = S.[StartDateTime]
					,T.[EndDateTime] = S.[EndDateTime]
					,T.[IsCurrent] = S.[IsCurrent]
					,T.[InsertDateTime] = S.[InsertDateTime]
		WHEN NOT MATCHED
			THEN INSERT (SystemName ,SystemNameInt ,PlayerID ,PlayerIDBigInt ,LoyaltyCardLevel ,StartDateTime ,EndDateTime ,IsCurrent ,InsertDateTime)
			VALUES (S.SystemName, S.SystemNameInt, S.PlayerID, S.PlayerIDBigInt, S.LoyaltyCardLevel, S.StartDateTime, S.EndDateTime, S.IsCurrent, S.InsertDateTime)
		WHEN NOT MATCHED BY SOURCE AND T.SystemNameInt = @SystemNameInt
			THEN DELETE;
    COMMIT TRANSACTION
		INSERT INTO [test].[HRC_ETL_Runner_Tracker] -- change to [qcic]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[qcids].[Dim_PlayerLoyalty_SCD]',RecordCount = count(*) FROM [qcids].[Dim_PlayerLoyalty_SCD]
		WHERE SystemNameInt = @SystemNameInt


	If(OBJECT_ID('tempdb..#HRC_ETL05') Is Not Null)
	Begin
		Drop Table #HRC_ETL05;
	End
	CREATE TABLE #HRC_ETL05  ( SystemName  NVARCHAR(100),  SystemNameInt INT,  CasinoID NVARCHAR(100), CasinoIDInt INT, PlayerID NVARCHAR(100), 
								PlayerIDBigInt BIGINT , HostID NVARCHAR(100), HostIDBigInt BIGINT,  PlayerHostType NVARCHAR(100),  StartDateTime DATETIME, EndDateTime DATETIME,  IsCurrent INT,  InsertDateTime DATETIME); 
	INSERT INTO #HRC_ETL05
	SELECT * 
	FROM [test].[udf_HRC_ETL05]( ) ;
	--DECLARE @SystemNameInt int = 3
     BEGIN TRANSACTION
		--DELETE FROM [qcids].[Dim_PlayerHost_SCD] WHERE SystemNameInt = @SystemNameInt
		--INSERT INTO [qcids].[Dim_PlayerHost_SCD] SELECT * FROM [qcic].[uvw_HRC_ETL05_Dim_PlayerHost_SCD]
		MERGE [test].[Dim_PlayerHost_SCD] T -- change to [qcids]
		USING #HRC_ETL05 AS S
		ON T.SystemNameInt = S.SystemNameInt AND T.CasinoIDInt = S.CasinoIDInt AND T.PlayerIDBigInt = S.PlayerIDBigInt AND T.PlayerHostType = S.PlayerHostType AND T.HostIDBigInt = S.HostIDBigInt AND T.StartDateTime = S.StartDateTime AND T.IsCurrent = S.IsCurrent
		WHEN MATCHED AND 
			BINARY_CHECKSUM(T.SystemName, T.SystemNameInt, T.CasinoID, T.CasinoIDInt, T.PlayerID, T.PlayerIDBigInt, T.HostID, T.HostIDBigInt, T.PlayerHostType, T.StartDateTime, T.EndDateTime, T.IsCurrent)
			<>
			BINARY_CHECKSUM(S.SystemName, S.SystemNameInt, S.CasinoID, S.CasinoIDInt, S.PlayerID, S.PlayerIDBigInt, S.HostID, S.HostIDBigInt, S.PlayerHostType, S.StartDateTime, S.EndDateTime, S.IsCurrent)
			THEN UPDATE SET
					 T.[SystemName] = S.[SystemName]
					,T.[SystemNameInt] = S.[SystemNameInt]
					,T.[CasinoID] = S.[CasinoID]
					,T.[CasinoIDInt] = S.[CasinoIDInt]
					,T.[PlayerID] = S.[PlayerID]
					,T.[PlayerIDBigInt] = S.[PlayerIDBigInt]
					,T.[HostID] = S.[HostID]
					,T.[HostIDBigInt] = S.[HostIDBigInt]
					,T.[PlayerHostType] = S.[PlayerHostType]
					,T.[StartDateTime] = S.[StartDateTime]
					,T.[EndDateTime] = S.[EndDateTime]
					,T.[IsCurrent] = S.[IsCurrent]
					,T.[InsertDateTime] = S.[InsertDateTime]
		WHEN NOT MATCHED
			THEN INSERT (SystemName, SystemNameInt, CasinoID, CasinoIDInt, PlayerID, PlayerIDBigInt, HostID, HostIDBigInt, PlayerHostType, StartDateTime, EndDateTime, IsCurrent, InsertDateTime)
			VALUES (S.SystemName, S.SystemNameInt, S.CasinoID, S.CasinoIDInt, S.PlayerID, S.PlayerIDBigInt, S.HostID, S.HostIDBigInt, S.PlayerHostType, S.StartDateTime, S.EndDateTime, S.IsCurrent, S.InsertDateTime)
		WHEN NOT MATCHED BY SOURCE AND T.SystemNameInt = @SystemNameInt
			THEN DELETE;
    COMMIT TRANSACTION
		INSERT INTO [test].[HRC_ETL_Runner_Tracker] -- change to [qcic]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[qcids].[Dim_PlayerHost_SCD]',RecordCount = count(*) FROM [qcids].[Dim_PlayerHost_SCD]
		WHERE SystemNameInt = @SystemNameInt


	If(OBJECT_ID('tempdb..#HRC_ETL06') Is Not Null)
	Begin
		Drop Table #HRC_ETL06
	End
	CREATE table #HRC_ETL06  ( SystemName NVARCHAR(100),  SystemNameInt INT, CasinoID  NVARCHAR(100), CasinoIDInt  INT, PlayerID NVARCHAR(100),  PlayerIDBigInt BIGINT,  CreditLimitInt100 INT, IsActiveCredit INT,  
                               SetupCasinoID NVARCHAR(100), Note NVARCHAR(4000), StartDateTime DATETIME, EndDateTime DATETIME, IsCurrent INT , InsertDateTime DATETIME   );
	INSERT INTO #HRC_ETL06	
	SELECT * 
		FROM [test].[udf_HRC_ETL06]( ) ;
	--DECLARE @SystemNameInt int = 3
    BEGIN TRANSACTION
		--DELETE FROM [qcids].[Dim_PlayerCredit_SCD] WHERE SystemNameInt = @SystemNameInt
		--INSERT INTO [qcids].[Dim_PlayerCredit_SCD] SELECT * FROM [qcic].[uvw_HRC_ETL06_Dim_PlayerCredit_SCD]
		MERGE [test].[Dim_PlayerCredit_SCD] T -- change to [qcids]
		USING #HRC_ETL06 AS S
		ON  T.SystemNameInt = S.SystemNameInt AND T.CasinoIDInt = S.CasinoIDInt AND T.PlayerIDBigInt = S.PlayerIDBigInt AND T.StartDateTime = S.StartDateTime AND T.IsCurrent = S.IsCurrent
		WHEN MATCHED AND 
			BINARY_CHECKSUM(T.SystemName, T.SystemNameInt, T.CasinoID, T.CasinoIDInt, T.PlayerID, T.PlayerIDBigInt, T.CreditLimitInt100, T.IsActiveCredit, T.SetupCasinoID, T.Note, T.StartDateTime, T.EndDateTime, T.IsCurrent)
			<>
			BINARY_CHECKSUM(S.SystemName, S.SystemNameInt, S.CasinoID, S.CasinoIDInt, S.PlayerID, S.PlayerIDBigInt, S.CreditLimitInt100, S.IsActiveCredit, S.SetupCasinoID, S.Note, S.StartDateTime, S.EndDateTime, S.IsCurrent)
			THEN UPDATE SET
					 T.[SystemName] = S.[SystemName]
					,T.[SystemNameInt] = S.[SystemNameInt]
					,T.[CasinoID] = S.[CasinoID]
					,T.[CasinoIDInt] = S.[CasinoIDInt]
					,T.[PlayerID] = S.[PlayerID]
					,T.[PlayerIDBigInt] = S.[PlayerIDBigInt]
					,T.[CreditLimitInt100] = S.[CreditLimitInt100]
					,T.[IsActiveCredit] = S.[IsActiveCredit]
					,T.[SetupCasinoID] = S.[SetupCasinoID]
					,T.[Note] = S.[Note]
					,T.[StartDateTime] = S.[StartDateTime]
					,T.[EndDateTime] = S.[EndDateTime]
					,T.[IsCurrent] = S.[IsCurrent]
					,T.[InsertDateTime] = S.[InsertDateTime]
		WHEN NOT MATCHED
			THEN INSERT (SystemName, SystemNameInt, CasinoID, CasinoIDInt, PlayerID, PlayerIDBigInt, CreditLimitInt100, IsActiveCredit, SetupCasinoID, Note, StartDateTime, EndDateTime, IsCurrent, InsertDateTime)
			VALUES (S.SystemName, S.SystemNameInt, S.CasinoID, S.CasinoIDInt, S.PlayerID, S.PlayerIDBigInt, S.CreditLimitInt100, S.IsActiveCredit, S.SetupCasinoID, S.Note, S.StartDateTime, S.EndDateTime, S.IsCurrent, S.InsertDateTime)
		WHEN NOT MATCHED BY SOURCE AND T.SystemNameInt = @SystemNameInt
			THEN DELETE;
    COMMIT TRANSACTION
		INSERT INTO [test].[HRC_ETL_Runner_Tracker] -- change to [qcic]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[qcids].[Dim_PlayerCredit_SCD]',RecordCount = count(*) FROM [qcids].[Dim_PlayerCredit_SCD]
		WHERE SystemNameInt = @SystemNameInt


	If(OBJECT_ID('tempdb..#HRC_ETL07') Is Not Null)
	Begin
		Drop Table #HRC_ETL07;
	End
	CREATE TABLE #HRC_ETL07 ( SystemName NVARCHAR(100), SystemNameInt INT, CasinoID NVARCHAR(100), CasinoIDInt INT, PlayerID NVARCHAR(100), PlayerIDBigInt BIGINT,
								ElectronicCreditType NVARCHAR(100), IsEnrolled INT, ElectronicCreditAccountID NVARCHAR(100), SignatureID NVARCHAR(100),
								CreatedByID NVARCHAR(100), Note NVARCHAR(4000), StartDateTime DATETIME, EndDateTime DATETIME, IsCurrent INT , InsertDateTime DATETIME);
	INSERT INTO #HRC_ETL07
	SELECT * 
		FROM [test].[udf_HRC_ETL07]( ) ;
	--DECLARE @SystemNameInt int = 3
   BEGIN TRANSACTION
		--DELETE FROM [qcids].[Dim_PlayerCreditElectronic_SCD] WHERE SystemNameInt = @SystemNameInt
		--INSERT INTO [qcids].[Dim_PlayerCreditElectronic_SCD] SELECT * FROM [qcic].[uvw_HRC_ETL07_Dim_PlayerCreditElectronic_SCD]
		MERGE [test].[Dim_PlayerCreditElectronic_SCD] T -- change to [qcids]
		USING #HRC_ETL07 AS S
		ON T.SystemNameInt = S.SystemNameInt AND T.CasinoIDInt = S.CasinoIDInt AND T.PlayerIDBigInt = S.PlayerIDBigInt AND T.ElectronicCreditType = S.ElectronicCreditType AND T.IsEnrolled = S.IsEnrolled AND T.StartDateTime = S.StartDateTime AND T.IsCurrent = S.IsCurrent
		WHEN MATCHED AND 
			BINARY_CHECKSUM(T.SystemName, T.SystemNameInt, T.CasinoID, T.CasinoIDInt, T.PlayerID, T.PlayerIDBigInt, T.ElectronicCreditType, T.IsEnrolled, T.ElectronicCreditAccountID, T.SignatureID, T.CreatedByID, T.Note, T.StartDateTime, T.EndDateTime, T.IsCurrent)
			<>
			BINARY_CHECKSUM(S.SystemName, S.SystemNameInt, S.CasinoID, S.CasinoIDInt, S.PlayerID, S.PlayerIDBigInt, S.ElectronicCreditType, S.IsEnrolled, S.ElectronicCreditAccountID, S.SignatureID, S.CreatedByID, S.Note, S.StartDateTime, S.EndDateTime, S.IsCurrent)
			THEN UPDATE SET
					 T.[SystemName] = S.[SystemName]
					,T.[SystemNameInt] = S.[SystemNameInt]
					,T.[CasinoID] = S.[CasinoID]
					,T.[CasinoIDInt] = S.[CasinoIDInt]
					,T.[PlayerID] = S.[PlayerID]
					,T.[PlayerIDBigInt] = S.[PlayerIDBigInt]
					,T.[ElectronicCreditType] = S.[ElectronicCreditType]
					,T.[IsEnrolled] = S.[IsEnrolled]
					,T.[ElectronicCreditAccountID] = S.[ElectronicCreditAccountID]
					,T.[SignatureID] = S.[SignatureID]
					,T.[CreatedByID] = S.[CreatedByID]
					,T.[Note] = S.[Note]
					,T.[StartDateTime] = S.[StartDateTime]
					,T.[EndDateTime] = S.[EndDateTime]
					,T.[IsCurrent] = S.[IsCurrent]
				   , T.[InsertDateTime] = S.[InsertDateTime]
		WHEN NOT MATCHED
			THEN INSERT (SystemName, SystemNameInt, CasinoID, CasinoIDInt, PlayerID, PlayerIDBigInt, ElectronicCreditType, IsEnrolled, ElectronicCreditAccountID, SignatureID, CreatedByID, Note, StartDateTime, EndDateTime, IsCurrent, InsertDateTime)
			VALUES (S.SystemName, S.SystemNameInt, S.CasinoID, S.CasinoIDInt, S.PlayerID, S.PlayerIDBigInt, S.ElectronicCreditType, S.IsEnrolled, S.ElectronicCreditAccountID, S.SignatureID, S.CreatedByID, S.Note, S.StartDateTime, S.EndDateTime, S.IsCurrent, S.InsertDateTime)
		WHEN NOT MATCHED BY SOURCE AND T.SystemNameInt = @SystemNameInt
			THEN DELETE;
    COMMIT TRANSACTION
		INSERT INTO [test].[HRC_ETL_Runner_Tracker] -- change to [qcic]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[qcids].[Dim_PlayerCreditElectronic_SCD]',RecordCount = count(*) FROM [qcids].[Dim_PlayerCreditElectronic_SCD]
		WHERE SystemNameInt = @SystemNameInt

	If(OBJECT_ID('tempdb..#HRC_ETL08') Is Not Null)
	Begin
		Drop Table #HRC_ETL08;
	End
	CREATE TABLE #HRC_ETL08 ( SystemName NVARCHAR(100), [SystemNameInt] INT,  [CasinoID]  NVARCHAR(100), [CasinoIDInt]  INT
    						,[DeviceID]  NVARCHAR(100), [DeviceIDBigInt] BIGINT, [PlayerID]  NVARCHAR(100), [PlayerIDBigInt] BIGINT  
							 ,[GamingDate]  DATE, [StartDateTime]  DATETIME , [EndDateTime]  DATETIME, [CoinInInt100]  BIGINT, [CoinOutInt100]  BIGINT ,
	                         [ActualWinWithFreeplayInt100]  INT, [ActualWinNetFreeplayInt100]  INT, [NumberGamesPlayed]  INT, [JackpotInt100]  INT, [FreeplayInt100]  INT, [TheoWinWithFreeplayInt100]  INT,
	                         [TheoWinNetFreeplayInt100]  INT, [TimePlayedInt100]  INT, [CashInInt100]  BIGINT, [ChipsInInt100]  BIGINT,
	                         [CreditInInt100]  BIGINT, [WalkedWithInt100]  BIGINT, [PlayerZipCode]  VARCHAR(100), [PlayerGender]  VARCHAR(50), [PlayerEnrollmentDate]  DATE, [PlayerBirthDate]  DATE, 
	                         [PlayerAddressDistance]  FLOAT, [GameTheme]  NVARCHAR(100), [GameThemeCount]  NVARCHAR(100), [GameTheoHold]  FLOAT, 
	                         [DeviceTheoHoldPercent]  FLOAT, [DeviceManufacturer]  NVARCHAR(200),  [Denomination]  FLOAT, [DeviceGameType]   NVARCHAR(200), 
	                         [DeviceCabinetType]  NVARCHAR(100), [DeviceCabinetModel]  NVARCHAR(200), [DeviceLocation]  NVARCHAR(20), [DeviceLocationArea]  NVARCHAR(20), 
	                         [DeviceLocationBank]  NVARCHAR(20) , [DeviceLocationStand]  NVARCHAR(20), [DeviceStartDate]  DATE, [DeviceSerial]  NVARCHAR(50) , [DeviceGameClass]  INT, [DeviceLeaseFeeInt100]  INT
                            ,[DeviceLeaseAttributes]  NVARCHAR(4000)	, [DeviceExtraAttributes]  NVARCHAR(4000) ,[IsSlotDevice]  INT , [IsTableDevice]  INT, [IsPokerDevice]  INT, [IsOtherDevice]  INT			       
                            ,[IsActiveDevice]  INT , [DeviceLocationBigInt]  BIGINT, [InsertDateTime]  DATETIME );
	DECLARE @days int = -10;
	SELECT @MaxDate = MAX(GamingDate) FROM [qcids].[Fact_PlayerDeviceRatingDay] WITH (NOLOCK) WHERE SystemNameInt = @SystemNameInt
	SELECT @MaxDate = ISNULL(@MaxDate,'2000-01-01')
	-- warning with @MaxDate
	INSERT INTO  #HRC_ETL08
    SELECT *
	    FROM [test].[udf_HRC_ETL08](@MaxDate,@days );
	--DECLARE @SystemNameInt int = 3, @MaxDate DATE
    BEGIN TRANSACTION
		--SELECT @MaxDate = MAX(GamingDate) FROM [qcids].[Fact_PlayerDeviceRatingDay] WITH (NOLOCK) WHERE SystemNameInt = @SystemNameInt
		--SELECT @MaxDate = ISNULL(@MaxDate,'2000-01-01')
		DELETE FROM [test].[Fact_PlayerDeviceRatingDay] WHERE SystemNameInt = @SystemNameInt AND GamingDate >= DATEADD(DAY,-10,@MaxDate) -- change to [qcids]
		INSERT INTO [test].[Fact_PlayerDeviceRatingDay] SELECT *  FROM #HRC_ETL08 -- WHERE  GamingDate >= DATEADD(DAY,-10,@MaxDate) >> inside udf_HRF_ETL08  --change first to [qcids], 
    COMMIT TRANSACTION
		INSERT INTO [test].[HRC_ETL_Runner_Tracker] -- change to [qcic]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[qcids].[Fact_PlayerDeviceRatingDay]',RecordCount = count(*) FROM [qcids].[Fact_PlayerDeviceRatingDay]
		WHERE SystemNameInt = @SystemNameInt


	If(OBJECT_ID('tempdb..#HRC_ETL09') Is Not Null)
	Begin
		Drop Table #HRC_ETL09;
	End
	CREATE TABLE #HRC_ETL09 ( SystemName NVARCHAR(100), SystemNameInt INT, OriginalManufacturer NVARCHAR(100), ManufacturerCleaned NVARCHAR(100), 
                                                  ManufacturerGroup NVARCHAR(100), InsertDateTime DATETIME); 
	INSERT INTO #HRC_ETL09
	SELECT * 
		FROM [test].[udf_HRC_ETL09]();
	--DECLARE @SystemNameInt int = 3
    BEGIN TRANSACTION
		DELETE FROM [test].[App_DeviceManufacturerGroup] WHERE SystemNameInt = @SystemNameInt -- change to [qcids]
		INSERT INTO [test].[App_DeviceManufacturerGroup] SELECT * FROM #HRC_ETL09 --change to [qcids]
    COMMIT TRANSACTION
		INSERT INTO [test].[HRC_ETL_Runner_Tracker] -- change to [qcic]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[qcids].[App_DeviceManufacturerGroup]',RecordCount = count(*) FROM [qcids].[App_DeviceManufacturerGroup]
		WHERE SystemNameInt = @SystemNameInt


	If(OBJECT_ID('tempdb..#HRC_ETL10') Is Not Null)
	Begin
		Drop Table #HRC_ETL10;
	End
	CREATE TABLE #HRC_ETL10 ( SystemName NVARCHAR(100), SystemNameInt INT, OriginalGameType NVARCHAR(100), GameTypeCleaned NVARCHAR(100) , GameTypeGroup NVARCHAR(100),
	                         GameTypeShortName NVARCHAR(100), GameTypeSummary NVARCHAR(100),  InsertDateTime DATETIME ) ;
	INSERT INTO #HRC_ETL10
	SELECT * 
	FROM  [test].[udf_HRC_ETL10]();
	--DECLARE @SystemNameInt int = 3
    BEGIN TRANSACTION
		DELETE FROM [test].[App_DeviceGameTypeGroup] WHERE SystemNameInt = @SystemNameInt -- change to [qcids]
		INSERT INTO [test].[App_DeviceGameTypeGroup] SELECT * FROM #HRC_ETL10 -- change first to [qcids]
    COMMIT TRANSACTION
		INSERT INTO [test].[HRC_ETL_Runner_Tracker] -- change to [qcic]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[qcids].[App_DeviceGameTypeGroup]',RecordCount = count(*) FROM [qcids].[App_DeviceGameTypeGroup]
		WHERE SystemNameInt = @SystemNameInt


	If(OBJECT_ID('tempdb..#HRC_ETL11') Is Not Null)
	Begin
		Drop Table #HRC_ETL11;	
	End
	CREATE TABLE #HRC_ETL11  (SystemName NVARCHAR(100), SystemNameInt INT, OriginalDenomination NVARCHAR(100),  DenominationCleaned NVARCHAR(100),
								DenominationSort NVARCHAR(100), DenominationGroup NVARCHAR(100) , DenominationGroupSort NVARCHAR(100),  InsertDateTime DATETIME );
	INSERT INTO #HRC_ETL11 
	SELECT * 
	FROM   [test].[udf_HRC_ETL11]();

	--DECLARE @SystemNameInt int = 3
    BEGIN TRANSACTION
		DELETE FROM [test].[App_DeviceDenominationGroup] WHERE SystemNameInt = @SystemNameInt --change to [qcids]
		INSERT INTO [test].[App_DeviceDenominationGroup] SELECT * FROM #HRC_ETL11  --change first to [qcids]
    COMMIT TRANSACTION
		INSERT INTO [test].[HRC_ETL_Runner_Tracker] -- change to [qcic]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[qcids].[App_DeviceDenominationGroup]',RecordCount = count(*) FROM [qcids].[App_DeviceDenominationGroup]
		WHERE SystemNameInt = @SystemNameInt

    If(OBJECT_ID('tempdb..#HRC_ETL12') Is Not Null)
	Begin
		Drop Table #HRC_ETL12;
	End
	CREATE TABLE #HRC_ETL12  (SystemName NVARCHAR(100), SystemNameInt INT, IssueCasinoID NVARCHAR(100), 
							 IssueCasinoIDInt INT, RedeemCasinoID  NVARCHAR(100), RedeemCasinoIDInt INT, PlayerID  NVARCHAR(100), PlayerIDBigInt BIGINT, 
							  RedeemDateTime DATETIME, PrizeID  NVARCHAR(100), PrizeType  NVARCHAR(100), PrizeIssueAmountInt100 INT, PrizeRedeemAmountInt100 INT, 
							   PointForPrizeRedeemAmountInt100 INT, PrizeStartDateTime DATETIME, PrizeEndDateTime DATETIME, PrizeCampaignID  NVARCHAR(100), 
								   PrizeCampaignSegmentID  NVARCHAR(100), InsertDateTime DATETIME ) ;
	
	SELECT @MaxDate = MAX(PrizeStartDateTime) FROM [qcids].[Fact_PlayerPrize] WITH (NOLOCK) WHERE SystemNameInt = @SystemNameInt
	SELECT @MaxDate = ISNULL(@MaxDate,'2000-01-01')
	
	INSERT INTO #HRC_ETL12
	SELECT * 
	FROM [test].[udf_HRC_ETL12]();
	--DECLARE @SystemNameInt int = 3, @MaxDate DATE
    BEGIN TRANSACTION
		--SELECT @MaxDate = MAX(PrizeStartDateTime) FROM [qcids].[Fact_PlayerPrize] WITH (NOLOCK) WHERE SystemNameInt = @SystemNameInt
		--SELECT @MaxDate = ISNULL(@MaxDate,'2000-01-01')
		DELETE FROM [test].[Fact_PlayerPrize] WHERE SystemNameInt = @SystemNameInt AND PrizeStartDateTime >= DATEADD(DAY,-10,@MaxDate) -- change to [qcids]
		INSERT INTO [test].[Fact_PlayerPrize] SELECT * FROM #HRC_ETL12 WHERE PrizeStartDateTime >= DATEADD(DAY,-10,@MaxDate) -- change first o [qcids]
    COMMIT TRANSACTION
		INSERT INTO [test].[HRC_ETL_Runner_Tracker] -- change to [qcic]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[qcids].[Fact_PlayerPrize]',RecordCount = count(*) FROM [qcids].[Fact_PlayerPrize]
		WHERE SystemNameInt = @SystemNameInt


	If(OBJECT_ID('tempdb..#HRC_ETL14') Is Not Null)
	Begin
		Drop Table #HRC_ETL14;
	End
	CREATE TABLE #HRC_ETL14( SystemName NVARCHAR(100), SystemNameInt INT, PlayerID NVARCHAR(100),  PlayerIDBigInt BIGINT, CasinoGroupID NVARCHAR(100), CasinoGroupIDInt INT, 
                                                 EvaluationScoreInt100 INT, InsertDateTime DATETIME ) ;
	INSERT INTO #HRC_ETL14
	SELECT * FROM [test].[udf_HRC_ETL14]();
	--DECLARE @SystemNameInt int = 3, @MaxDate DATE
    BEGIN TRANSACTION
		--DELETE FROM [qcids].[Fact_PlayerEvaluationCurrent] WHERE SystemNameInt = @SystemNameInt
		--INSERT INTO [qcids].[Fact_PlayerEvaluationCurrent] SELECT * FROM [qcic].[uvw_HRC_ETL14_Fact_PlayerEvaluationCurrent]
		MERGE [test].[Fact_PlayerEvaluationCurrent] T --change to [qcids]
		USING #HRC_ETL14 AS S
		ON T.SystemNameInt = S.SystemNameInt AND T.PlayerIDBigInt = S.PlayerIDBigInt AND T.CasinoGroupIDInt = S.CasinoGroupIDInt 
		WHEN MATCHED AND 
			BINARY_CHECKSUM(T.SystemName, T.SystemNameInt, T.PlayerID, T.PlayerIDBigInt, T.CasinoGroupID, T.CasinoGroupIDInt, T.EvaluationScoreInt100)
			<>
			BINARY_CHECKSUM(S.SystemName, S.SystemNameInt, S.PlayerID, S.PlayerIDBigInt, S.CasinoGroupID, S.CasinoGroupIDInt, S.EvaluationScoreInt100)
			THEN UPDATE SET
					 T.[SystemName] = S.[SystemName]
					,T.[SystemNameInt] = S.[SystemNameInt]
					,T.[PlayerID] = S.[PlayerID]
					,T.[PlayerIDBigInt] = S.[PlayerIDBigInt]
					,T.[CasinoGroupID] = S.[CasinoGroupID]
					,T.[CasinoGroupIDInt] = S.[CasinoGroupIDInt]
					,T.[EvaluationScoreInt100] = S.[EvaluationScoreInt100]
					,T.[InsertDateTime] = S.[InsertDateTime]
		WHEN NOT MATCHED
			THEN INSERT (SystemName, SystemNameInt, PlayerID, PlayerIDBigInt, CasinoGroupID, CasinoGroupIDInt, EvaluationScoreInt100, InsertDateTime)
			VALUES (S.SystemName, S.SystemNameInt, S.PlayerID, S.PlayerIDBigInt, S.CasinoGroupID, S.CasinoGroupIDInt, S.EvaluationScoreInt100, S.InsertDateTime)
		WHEN NOT MATCHED BY SOURCE AND T.SystemNameInt = @SystemNameInt
			THEN DELETE;
    COMMIT TRANSACTION
		INSERT INTO [test].[HRC_ETL_Runner_Tracker] -- change to [qcic]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[qcids].[Fact_PlayerEvaluationCurrent]',RecordCount = count(*) FROM [qcids].[Fact_PlayerEvaluationCurrent]
		WHERE SystemNameInt = @SystemNameInt


	If(OBJECT_ID('tempdb..#HRC_ETL15') Is Not Null)
	Begin
		Drop Table #HRC_ETL15;
	End
	CREATE TABLE #HRC_ETL15( SystemName NVARCHAR(100), SystemNameInt INT, CasinoID NVARCHAR(100),
							 CasinoIDInt INT, DeviceID NVARCHAR(100), DeviceIDBigInt BIGINT, GamingDate DATE, CoinIn BIGINT, CoinOut BIGINT, ActualWinWithFreeplay INT, ActualWinNetFreeplay INT , 
								NumberGamesPlayed INT, Jackpot INT, Freeplay INT, TheoWinWithFreeplay  INT, TheoWinNetFreeplay  INT, TimePlayed INT, CashIn   BIGINT, ChipsIn BIGINT, 
							 CreditIn   BIGINT, WalkedWith BIGINT, GameTheme NVARCHAR(100),  GameThemeCount	NVARCHAR(100), GameTheoHold FLOAT, 
							  DeviceTheoHoldPercent FLOAT, DeviceManufacturer NVARCHAR(200),  Denomination FLOAT, DeviceGameType	 NVARCHAR(200), 
						     DeviceCabinetType NVARCHAR(100), DeviceCabinetModel NVARCHAR(200), DeviceLocation NVARCHAR(20), DeviceLocationArea NVARCHAR(20), 
							DeviceLocationBank NVARCHAR(20), DeviceLocationStand NVARCHAR(20), DeviceStartDate DATE, DeviceSerial	NVARCHAR(50), 
					      DeviceGameClass	 INT, DeviceLeaseFeeInt100 MONEY,  DeviceLeaseAttributes NVARCHAR(4000), DeviceExtraAttributes NVARCHAR(4000), 	
						IsSlotDevice INT , IsTableDevice INT , IsPokerDevice INT , IsOtherDevice INT , IsActiveDevice INT , DeviceLocationBigInt BIGINT, InsertDateTime  DATETIME) ;
	 
	SELECT @MaxDate = MAX(GamingDate) FROM [qcids].[Fact_DeviceDay] WITH (NOLOCK) WHERE SystemNameInt = @SystemNameInt
	SELECT @MaxDate = ISNULL(@MaxDate,'2000-01-01')
	
	 INSERT INTO #HRC_ETL15
	 SELECT * 
		FROM [test].[udf_HRC_ETL15](@MaxDate , @days) ;
	--DECLARE @SystemNameInt int = 3, @MaxDate DATE
    BEGIN TRANSACTION
	--	SELECT @MaxDate = MAX(GamingDate) FROM [qcids].[Fact_DeviceDay] WITH (NOLOCK) WHERE SystemNameInt = @SystemNameInt
	--	SELECT @MaxDate = ISNULL(@MaxDate,'2000-01-01')
		DELETE FROM [test].[Fact_DeviceDay] WHERE SystemNameInt = @SystemNameInt AND GamingDate >= DATEADD(DAY,-10,@MaxDate) --change to [qcids]
		INSERT INTO [test].[Fact_DeviceDay] SELECT * FROM #HRC_ETL15 --<< inside  udf WHERE GamingDate >= DATEADD(DAY,-10,@MaxDate) --change to [qcids]
    COMMIT TRANSACTION
		INSERT INTO [test].[HRC_ETL_Runner_Tracker] --change to [qcic]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[qcids].[Fact_DeviceDay]',RecordCount = count(*) FROM [qcids].[Fact_DeviceDay]
		WHERE SystemNameInt = @SystemNameInt

END
/* testing
EXEC [test].[usp_HRC_ETL_Runner_test] 

- WARNING -EXEC [qcic].[usp_HRC_ETL_Runner]
*/