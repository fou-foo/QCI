
declare    @RunNumber INT -- DO NOT set a default value.  Needs to be explicit.

	DECLARE @SystemName nvarchar(100) = 'SGAHRI'
			,@SystemNameInt int = 2
			,@MaxDate date
            ,@Days int
	DROP TABLE IF EXISTS [tqcic].[SGA_ETL_Runner_Tracker]
	SELECT TimeStamp = GETDATE(),TableCompleted = convert(nvarchar(255),'Start'),RecordCount = 0 INTO [tqcic].[SGA_ETL_Runner_Tracker]

    IF @RunNumber = 1
    BEGIN
        SET @Days = 10
    END
    ELSE IF @RunNumber = 2
    BEGIN
        SET @Days = 2
    END

	/******************
	UDM-DS Steps
	******************/
	--DECLARE @SystemNameInt int = 2
	--BEGIN TRANSACTION
		--DELETE FROM [tqcids].[Dim_Host_SCD] WHERE SystemName = @SystemName
		--INSERT INTO [tqcids].[Dim_Host_SCD] SELECT * FROM [tqcic].[uvw_SGA_ETL01_Dim_Host_SCD] WITH (NOLOCK)
    If(OBJECT_ID('tempdb..#temp1_SGA') Is Not Null)
	Begin
		Drop Table #temp1_SGA
	End	
	CREATE table #temp1_SGA  ( SystemName NVARCHAR(100), SystemNameInt INT, CasinoGroupID NVARCHAR(100), CasinoGroupIDInt INT, HostID NVARCHAR(100), HostIDBigInt  BIGINT, 
	                  HostFirstName NVARCHAR(50), HostLastName NVARCHAR(50), HostPosition NVARCHAR(100) , HostCode  NVARCHAR(50), StartDateTime DATETIME, EndDateTime  DATETIME, IsCurrent   INT, InsertDateTime DATETIME);
	INSERT INTO #temp1_SGA EXEC   [tqcic].[usp_SGA_ETL01_Dim_Host_SCD] 


		MERGE [tqcids].[Dim_Host_SCD] T 
		USING #temp1_SGA  AS  S WITH (NOLOCK)
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
	--COMMIT TRANSACTION
		INSERT INTO [tqcic].[SGA_ETL_Runner_Tracker]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[Dim_Host_SCD]',RecordCount = count(*) FROM [tqcids].[Dim_Host_SCD] WITH (NOLOCK)
		WHERE SystemNameInt = @SystemNameInt

	--DECLARE @SystemNameInt int = 2
	declare    @RunNumber INT -- DO NOT set a default value.  Needs to be explicit.

	DECLARE @SystemName nvarchar(100) = 'SGAHRI'
			,@SystemNameInt int = 2
			,@MaxDate date
            ,@Days int
	
	--BEGIN TRANSACTION
		--DELETE FROM [tqcids].[Dim_Employee_SCD] WHERE SystemName = @SystemName
		--INSERT INTO [tqcids].[Dim_Employee_SCD] SELECT * FROM [tqcic].[uvw_SGA_ETL01a_Dim_Employee_SCD] WITH (NOLOCK)
		MERGE [tqcids].[Dim_Employee_SCD] T 
		USING [tqcic].[uvw_SGA_ETL01a_Dim_Employee_SCD] S
		ON T.SystemNameInt = S.SystemNameInt AND T.CasinoGroupIDInt = S.CasinoGroupIDInt AND T.EmployeeIDBigInt = S.EmployeeIDBigInt AND T.StartDateTime = S.StartDateTime AND T.IsCurrent = S.IsCurrent
		WHEN MATCHED AND
			BINARY_CHECKSUM(T.SystemName, T.SystemNameInt, T.CasinoGroupID, T.CasinoGroupIDInt, T.EmployeeID, T.EmployeeIDBigInt, T.EmployeeFirstName, T.EmployeeLastName, T.EmployeePosition, T.EmployeeCode, T.StartDateTime, T.EndDateTime, T.IsCurrent)
			<>
			BINARY_CHECKSUM(S.SystemName, S.SystemNameInt, S.CasinoGroupID, S.CasinoGroupIDInt, S.EmployeeID, S.EmployeeIDBigInt, S.EmployeeFirstName, S.EmployeeLastName, S.EmployeePosition, S.EmployeeCode, S.StartDateTime, S.EndDateTime, S.IsCurrent)

			THEN UPDATE SET
                 T.[SystemName] = S.[SystemName]
                ,T.[SystemNameInt] = S.[SystemNameInt]
                ,T.[CasinoGroupID] = S.[CasinoGroupID]
                ,T.[CasinoGroupIDInt] = S.[CasinoGroupIDInt]
                ,T.[EmployeeID] = S.[EmployeeID]
                ,T.[EmployeeIDBigInt] = S.[EmployeeIDBigInt]
                ,T.[EmployeeFirstName] = S.[EmployeeFirstName]
                ,T.[EmployeeLastName] = S.[EmployeeLastName]
                ,T.[EmployeePosition] = S.[EmployeePosition]
                ,T.[EmployeeCode] = S.[EmployeeCode]
                ,T.[StartDateTime] = S.[StartDateTime]
                ,T.[EndDateTime] = S.[EndDateTime]
                ,T.[IsCurrent] = S.[IsCurrent]
                ,T.[InsertDateTime] = S.[InsertDateTime]
		WHEN NOT MATCHED
			THEN INSERT (SystemName, SystemNameInt, CasinoGroupID, CasinoGroupIDInt, EmployeeID, EmployeeIDBigInt
						, EmployeeFirstName, EmployeeLastName, EmployeePosition, EmployeeCode, StartDateTime, EndDateTime, IsCurrent, InsertDateTime)
				 VALUES (S.SystemName, S.SystemNameInt, S.CasinoGroupID, S.CasinoGroupIDInt, S.EmployeeID, S.EmployeeIDBigInt
						, S.EmployeeFirstName, S.EmployeeLastName, S.EmployeePosition, S.EmployeeCode, S.StartDateTime, S.EndDateTime, S.IsCurrent, S.InsertDateTime)
		WHEN NOT MATCHED BY SOURCE AND T.SystemNameInt = @SystemNameInt
			THEN DELETE;
	COMMIT TRANSACTION
		INSERT INTO [tqcic].[SGA_ETL_Runner_Tracker]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[Dim_Employee_SCD]',RecordCount = count(*) FROM [tqcids].[Dim_Employee_SCD]
		WHERE SystemNameInt = @SystemNameInt

	--DECLARE @SystemNameInt int = 2
    IF @RunNumber = 1
    BEGIN
        BEGIN TRANSACTION
    		--DELETE FROM [tqcids].[Dim_Player_SCD] WHERE SystemNameInt = @SystemNameInt
    		--INSERT INTO [tqcids].[Dim_Player_SCD] SELECT * FROM [tqcic].[uvw_SGA_ETL02_Dim_Player_SCD]
    		MERGE [tqcids].[Dim_Player_SCD] T
    		USING [tqcic].[uvw_SGA_ETL02_Dim_Player_SCD] S WITH (NOLOCK)
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
    END
	ELSE -- This picks up players that signed up the previous day.  Necessary for reports.
	BEGIN
        BEGIN TRANSACTION
    		DELETE FROM [tqcids].[Dim_Player_SCD] WHERE SystemNameInt = @SystemNameInt AND SetupDate >= DATEADD(DAY, -1, CONVERT(DATE, GETDATE()))
    		INSERT INTO [tqcids].[Dim_Player_SCD] SELECT * FROM [tqcic].[uvw_SGA_ETL02_Dim_Player_SCD] WHERE SystemNameInt = @SystemNameInt AND SetupDate >= DATEADD(DAY, -1, CONVERT(DATE, GETDATE()))
		COMMIT TRANSACTION
	END -- IF @RunNumber = 1


	INSERT INTO [tqcic].[SGA_ETL_Runner_Tracker]
	SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[Dim_Player_SCD]',RecordCount = count(*) FROM [tqcids].[Dim_Player_SCD] WITH (NOLOCK)
	WHERE SystemNameInt = @SystemNameInt

	--DECLARE @SystemNameInt int = 2
    IF @RunNumber = 1
    BEGIN
        BEGIN TRANSACTION
    		--DELETE FROM [tqcids].[Dim_PlayerDistanceToCasino_SCD] WHERE SystemNameInt = @SystemNameInt
    		--INSERT INTO [tqcids].[Dim_PlayerDistanceToCasino_SCD] SELECT * FROM [tqcic].[uvw_SGA_ETL03_Dim_PlayerDistanceToCasino_SCD] WITH (NOLOCK)
    		MERGE [tqcids].[Dim_PlayerDistanceToCasino_SCD] T 
    		USING [tqcic].[uvw_SGA_ETL03_Dim_PlayerDistanceToCasino_SCD] S WITH (NOLOCK)
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
    END -- IF @RunNumber = 1

	INSERT INTO [tqcic].[SGA_ETL_Runner_Tracker]
	SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[Dim_PlayerDistanceToCasino_SCD]',RecordCount = count(*) FROM [tqcids].[Dim_PlayerDistanceToCasino_SCD] WITH (NOLOCK)
	WHERE SystemNameInt = @SystemNameInt

	--DECLARE @SystemNameInt int = 2
    BEGIN TRANSACTION
		--DELETE FROM [tqcids].[Dim_PlayerLoyalty_SCD] WHERE SystemNameInt = @SystemNameInt
		--INSERT INTO [tqcids].[Dim_PlayerLoyalty_SCD] SELECT * FROM [tqcic].[uvw_SGA_ETL04_Dim_PlayerLoyalty_SCD] WITH (NOLOCK)
		MERGE [tqcids].[Dim_PlayerLoyalty_SCD] T
		USING [tqcic].[uvw_SGA_ETL04_Dim_PlayerLoyalty_SCD] S WITH (NOLOCK)
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
		INSERT INTO [tqcic].[SGA_ETL_Runner_Tracker]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[Dim_PlayerLoyalty_SCD]',RecordCount = count(*) FROM [tqcids].[Dim_PlayerLoyalty_SCD] WITH (NOLOCK)
		WHERE SystemNameInt = @SystemNameInt

	--DECLARE @SystemNameInt int = 2
    BEGIN TRANSACTION
		--DELETE FROM [tqcids].[Dim_PlayerHost_SCD] WHERE SystemNameInt = @SystemNameInt
		--INSERT INTO [tqcids].[Dim_PlayerHost_SCD] SELECT * FROM [tqcic].[uvw_SGA_ETL05_Dim_PlayerHost_SCD] WITH (NOLOCK)
		MERGE [tqcids].[Dim_PlayerHost_SCD] T 
		USING [tqcic].[uvw_SGA_ETL05_Dim_PlayerHost_SCD] S WITH (NOLOCK)
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
	INSERT INTO [tqcic].[SGA_ETL_Runner_Tracker]
	SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[Dim_PlayerHost_SCD]',RecordCount = count(*) FROM [tqcids].[Dim_PlayerHost_SCD] WITH (NOLOCK)
	WHERE SystemNameInt = @SystemNameInt

	--DECLARE @SystemNameInt int = 2
    BEGIN TRANSACTION
		--DELETE FROM [tqcids].[Dim_PlayerCredit_SCD] WHERE SystemNameInt = @SystemNameInt
		--INSERT INTO [tqcids].[Dim_PlayerCredit_SCD] SELECT * FROM [tqcic].[uvw_SGA_ETL06_Dim_PlayerCredit_SCD] WITH (NOLOCK)
		MERGE [tqcids].[Dim_PlayerCredit_SCD] T 
		USING [tqcic].[uvw_SGA_ETL06_Dim_PlayerCredit_SCD] S WITH (NOLOCK)
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
		INSERT INTO [tqcic].[SGA_ETL_Runner_Tracker]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[Dim_PlayerCredit_SCD]',RecordCount = count(*) FROM [tqcids].[Dim_PlayerCredit_SCD] WITH (NOLOCK)
		WHERE SystemNameInt = @SystemNameInt

	--DECLARE @SystemNameInt int = 2
    BEGIN TRANSACTION
		--DELETE FROM [tqcids].[Dim_PlayerCreditElectronic_SCD] WHERE SystemNameInt = @SystemNameInt
		--INSERT INTO [tqcids].[Dim_PlayerCreditElectronic_SCD] SELECT * FROM [tqcic].[uvw_SGA_ETL07_Dim_PlayerCreditElectronic_SCD] WITH (NOLOCK)
		MERGE [tqcids].[Dim_PlayerCreditElectronic_SCD] T 
		USING [tqcic].[uvw_SGA_ETL07_Dim_PlayerCreditElectronic_SCD] S WITH (NOLOCK)
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
	INSERT INTO [tqcic].[SGA_ETL_Runner_Tracker]
	SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[Dim_PlayerCreditElectronic_SCD]',RecordCount = count(*) FROM [tqcids].[Dim_PlayerCreditElectronic_SCD] WITH (NOLOCK)
	WHERE SystemNameInt = @SystemNameInt

	--DECLARE @SystemNameInt int = 2, @MaxDate DATE
    BEGIN TRANSACTION
		SELECT @MaxDate = MAX(GamingDate) FROM [tqcids].[Fact_PlayerDeviceRatingDay] WITH (NOLOCK) WHERE SystemNameInt = @SystemNameInt
		SELECT @MaxDate = ISNULL(@MaxDate,'2000-01-01')
		DELETE FROM [tqcids].[Fact_PlayerDeviceRatingDay] WHERE SystemNameInt = @SystemNameInt AND GamingDate >= DATEADD(DAY,0-@Days,@MaxDate)
		INSERT INTO [tqcids].[Fact_PlayerDeviceRatingDay] EXEC [tqcic].[usp_SGA_ETL08_Fact_PlayerDeviceRatingDay] @MaxDate, @Days
    COMMIT TRANSACTION
		INSERT INTO [tqcic].[SGA_ETL_Runner_Tracker]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[Fact_PlayerDeviceRatingDay]',RecordCount = count(*) FROM [tqcids].[Fact_PlayerDeviceRatingDay] WITH (NOLOCK)
		WHERE SystemNameInt = @SystemNameInt

	--DECLARE @SystemNameInt int = 2
    BEGIN TRANSACTION
		DELETE FROM [tqcids].[App_DeviceManufacturerGroup] WHERE SystemNameInt = @SystemNameInt
		INSERT INTO [tqcids].[App_DeviceManufacturerGroup] SELECT * FROM [tqcic].[uvw_SGA_ETL09_App_DeviceManufacturerGroup] WITH (NOLOCK)
    COMMIT TRANSACTION
		INSERT INTO [tqcic].[SGA_ETL_Runner_Tracker]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[App_DeviceManufacturerGroup]',RecordCount = count(*) FROM [tqcids].[App_DeviceManufacturerGroup] WITH (NOLOCK)
		WHERE SystemNameInt = @SystemNameInt

	--DECLARE @SystemNameInt int = 2
    BEGIN TRANSACTION
		DELETE FROM [tqcids].[App_DeviceGameTypeGroup] WHERE SystemNameInt = @SystemNameInt
		INSERT INTO [tqcids].[App_DeviceGameTypeGroup] SELECT * FROM [tqcic].[uvw_SGA_ETL10_App_DeviceGameTypeGroup] WITH (NOLOCK)
    COMMIT TRANSACTION
		INSERT INTO [tqcic].[SGA_ETL_Runner_Tracker]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[App_DeviceGameTypeGroup]',RecordCount = count(*) FROM [tqcids].[App_DeviceGameTypeGroup] WITH (NOLOCK)
		WHERE SystemNameInt = @SystemNameInt

	--DECLARE @SystemNameInt int = 2
    BEGIN TRANSACTION
		DELETE FROM [tqcids].[App_DeviceDenominationGroup] WHERE SystemNameInt = @SystemNameInt
		INSERT INTO [tqcids].[App_DeviceDenominationGroup] SELECT * FROM [tqcic].[uvw_SGA_ETL11_App_DeviceDenominationGroup] WITH (NOLOCK)
    COMMIT TRANSACTION
		INSERT INTO [tqcic].[SGA_ETL_Runner_Tracker]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[App_DeviceDenominationGroup]',RecordCount = count(*) FROM [tqcids].[App_DeviceDenominationGroup] WITH (NOLOCK)
		WHERE SystemNameInt = @SystemNameInt

	--DECLARE @SystemNameInt int = 2, @MaxDate DATE
    BEGIN TRANSACTION
		SELECT @MaxDate = MAX(PrizeStartDateTime) FROM [tqcids].[Fact_PlayerPrize] WITH (NOLOCK) WHERE SystemNameInt = @SystemNameInt
		SELECT @MaxDate = ISNULL(@MaxDate,'2000-01-01')
		DELETE FROM [tqcids].[Fact_PlayerPrize] WHERE SystemNameInt = @SystemNameInt AND PrizeStartDateTime >= DATEADD(DAY,0-@Days,@MaxDate)
		INSERT INTO [tqcids].[Fact_PlayerPrize] SELECT * FROM [tqcic].[uvw_SGA_ETL12_Fact_PlayerPrize] WITH (NOLOCK) WHERE PrizeStartDateTime >= DATEADD(DAY,0-@Days,@MaxDate)
    COMMIT TRANSACTION
		INSERT INTO [tqcic].[SGA_ETL_Runner_Tracker]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[Fact_PlayerPrize]',RecordCount = count(*) FROM [tqcids].[Fact_PlayerPrize] WITH (NOLOCK)
		WHERE SystemNameInt = @SystemNameInt

	--DECLARE @SystemNameInt int = 2, @MaxDate DATE
    BEGIN TRANSACTION
		--DELETE FROM [tqcids].[Fact_PlayerEvaluationCurrent] WHERE SystemNameInt = @SystemNameInt
		--INSERT INTO [tqcids].[Fact_PlayerEvaluationCurrent] SELECT * FROM [tqcic].[uvw_SGA_ETL14_Fact_PlayerEvaluationCurrent] WITH (NOLOCK)
		MERGE [tqcids].[Fact_PlayerEvaluationCurrent] T 
		USING [tqcic].[uvw_SGA_ETL14_Fact_PlayerEvaluationCurrent] S
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
		INSERT INTO [tqcic].[SGA_ETL_Runner_Tracker]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[Fact_PlayerEvaluationCurrent]',RecordCount = count(*) FROM [tqcids].[Fact_PlayerEvaluationCurrent] WITH (NOLOCK)
		WHERE SystemNameInt = @SystemNameInt

	--DECLARE @SystemNameInt int = 2, @MaxDate DATE
    BEGIN TRANSACTION
		SELECT @MaxDate = MAX(GamingDate) FROM [tqcids].[Fact_DeviceDay] WITH (NOLOCK) WHERE SystemNameInt = @SystemNameInt
		SELECT @MaxDate = ISNULL(@MaxDate,'2000-01-01')
		DELETE FROM [tqcids].[Fact_DeviceDay] WHERE SystemNameInt = @SystemNameInt AND GamingDate >= DATEADD(DAY,0-@Days,@MaxDate)
		INSERT INTO [tqcids].[Fact_DeviceDay] SELECT * FROM [tqcic].[uvw_SGA_ETL15_Fact_DeviceDay] WITH (NOLOCK) WHERE GamingDate >= DATEADD(DAY,0-@Days,@MaxDate)
    COMMIT TRANSACTION
		INSERT INTO [tqcic].[SGA_ETL_Runner_Tracker]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[Fact_DeviceDay]',RecordCount = count(*) FROM [tqcids].[Fact_DeviceDay] WITH (NOLOCK)
		WHERE SystemNameInt = @SystemNameInt

	--DECLARE @SystemNameInt int = 2, @MaxDate DATE
    BEGIN TRANSACTION
		DELETE FROM [tqcids].[Dim_PlayerPhone_Current] WHERE SystemNameInt = @SystemNameInt
		INSERT INTO [tqcids].[Dim_PlayerPhone_Current] SELECT * FROM [tqcic].[uvw_SGA_ETL18_Dim_PlayerPhone_Current] WITH (NOLOCK)
    COMMIT TRANSACTION
		INSERT INTO [tqcic].[SGA_ETL_Runner_Tracker]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[Dim_PlayerPhone_Current]',RecordCount = count(*) FROM [tqcids].[Dim_PlayerPhone_Current] WITH (NOLOCK)
		WHERE SystemNameInt = @SystemNameInt

	--DECLARE @SystemName nvarchar(100) = 'SGAHRI',@SystemNameInt int = 2
    DROP TABLE IF EXISTS #tqcids_Fact_PlayerTargetPerDay
    SELECT	 [SystemName]
			,[SystemNameInt]
			,[CasinoID]
			,[CasinoIDInt]
			,[PlayerID]
			,[PlayerIDBigInt]
			,[TargetStartDate]
			,[TargetEndDate]
			,[TargetValuePerDay]
			,DeleteDate = CAST([InsertDateTime] AS DATE)
    INTO #tqcids_Fact_PlayerTargetPerDay
    FROM tqcids.Fact_PlayerTargetPerDay
    WHERE 1 <> 1

	--FY2020 only gets loaded once
	IF (SELECT Count(*) FROM [tqcids].[Fact_PlayerTargetPerDay] WITH (NOLOCK) WHERE SystemNameInt = @SystemNameInt AND TargetStartDate = '2019-10-01' AND TargetEndDate = '2020-09-30') = 0
	BEGIN
		TRUNCATE TABLE #tqcids_Fact_PlayerTargetPerDay
		INSERT INTO #tqcids_Fact_PlayerTargetPerDay
		EXEC [tqcids].[usp_Host_PlayerTargetPerDayFixedPlayerTarget] 2,4.5,'2019-10-01','2020-09-30','[tqcic].[SGA_tbl_SGAVIPHostCodeBenchmarkUniverse]','C12_TotalTheo - C12_FreePlay',1
		INSERT INTO #tqcids_Fact_PlayerTargetPerDay
		EXEC [tqcids].[usp_Host_PlayerTargetPerDayFixedPlayerTarget] 2,1,'2019-10-01','2020-09-30','[tqcic].[SGA_tbl_SGAVIPHostCodeBenchmarkUniverse]','C12_TotalTheo - C12_FreePlay',2
		INSERT INTO #tqcids_Fact_PlayerTargetPerDay
		EXEC [tqcids].[usp_Host_PlayerTargetPerDayFixedPlayerTarget] 2,32.8,'2019-10-01','2020-09-30','[tqcic].[SGA_tbl_SGAVIPHostCodeBenchmarkUniverse]','C12_TotalTheo - C12_FreePlay',3
		INSERT INTO #tqcids_Fact_PlayerTargetPerDay
		EXEC [tqcids].[usp_Host_PlayerTargetPerDayFixedPlayerTarget] 2,2.4,'2019-10-01','2020-09-30','[tqcic].[SGA_tbl_SGAVIPHostCodeBenchmarkUniverse]','C12_TotalTheo - C12_FreePlay',4
		INSERT INTO #tqcids_Fact_PlayerTargetPerDay
		EXEC [tqcids].[usp_Host_PlayerTargetPerDayFixedPlayerTarget] 2,1,'2019-10-01','2020-09-30','[tqcic].[SGA_tbl_SGAVIPHostCodeBenchmarkUniverse]','C12_TotalTheo - C12_FreePlay',5
		INSERT INTO #tqcids_Fact_PlayerTargetPerDay
		EXEC [tqcids].[usp_Host_PlayerTargetPerDayFixedPlayerTarget] 2,12.9,'2019-10-01','2020-09-30','[tqcic].[SGA_tbl_SGAVIPHostCodeBenchmarkUniverse]','C12_TotalTheo - C12_FreePlay',6
	END

	--FY2021 only gets loaded once
	IF (SELECT Count(*) FROM [tqcids].[Fact_PlayerTargetPerDay] WITH (NOLOCK) WHERE SystemNameInt = @SystemNameInt AND TargetStartDate = '2020-10-01' AND TargetEndDate = '2021-09-30') = 0
	BEGIN
		TRUNCATE TABLE #tqcids_Fact_PlayerTargetPerDay
		INSERT INTO #tqcids_Fact_PlayerTargetPerDay
		EXEC [tqcids].[usp_Host_PlayerTargetPerDayFixedPlayerTarget] 2,2,'2020-10-01','2021-09-30','[tqcic].[SGA_tbl_SGAVIPHostCodeBenchmarkUniverse21]','C12_TotalTheo - C12_FreePlay',1
		INSERT INTO #tqcids_Fact_PlayerTargetPerDay
		EXEC [tqcids].[usp_Host_PlayerTargetPerDayFixedPlayerTarget] 2,1,'2020-10-01','2021-09-30','[tqcic].[SGA_tbl_SGAVIPHostCodeBenchmarkUniverse21]','C12_TotalTheo - C12_FreePlay',2
		INSERT INTO #tqcids_Fact_PlayerTargetPerDay
		EXEC [tqcids].[usp_Host_PlayerTargetPerDayFixedPlayerTarget] 2,23,'2020-10-01','2021-09-30','[tqcic].[SGA_tbl_SGAVIPHostCodeBenchmarkUniverse21]','C12_TotalTheo - C12_FreePlay',3
		INSERT INTO #tqcids_Fact_PlayerTargetPerDay
		EXEC [tqcids].[usp_Host_PlayerTargetPerDayFixedPlayerTarget] 2,1,'2020-10-01','2021-09-30','[tqcic].[SGA_tbl_SGAVIPHostCodeBenchmarkUniverse21]','C12_TotalTheo - C12_FreePlay',4
		INSERT INTO #tqcids_Fact_PlayerTargetPerDay
		EXEC [tqcids].[usp_Host_PlayerTargetPerDayFixedPlayerTarget] 2,1,'2020-10-01','2021-09-30','[tqcic].[SGA_tbl_SGAVIPHostCodeBenchmarkUniverse21]','C12_TotalTheo - C12_FreePlay',5
		INSERT INTO #tqcids_Fact_PlayerTargetPerDay
		EXEC [tqcids].[usp_Host_PlayerTargetPerDayFixedPlayerTarget] 2,8,'2020-10-01','2021-09-30','[tqcic].[SGA_tbl_SGAVIPHostCodeBenchmarkUniverse21]','C12_TotalTheo - C12_FreePlay',6
	END

	INSERT INTO [tqcids].[Fact_PlayerTargetPerDay]
	SELECT	 [SystemName]
			,[SystemNameInt]
			,[CasinoID]
			,[CasinoIDInt]
			,[PlayerID]
			,[PlayerIDBigInt]
			,[TargetStartDate]
			,[TargetEndDate]
			,[TargetValuePerDay]
			,[InsertDateTime] = GETUTCDATE()
	FROM	 #tqcids_Fact_PlayerTargetPerDay WITH (NOLOCK)

    DROP TABLE IF EXISTS #tqcids_Fact_PlayerTargetPerDay
	INSERT INTO [tqcic].[SGA_ETL_Runner_Tracker]
	SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[Fact_PlayerTargetPerDay]',RecordCount = count(*) FROM [tqcids].[Fact_PlayerTargetPerDay] WITH (NOLOCK)
	WHERE SystemNameInt = @SystemNameInt

