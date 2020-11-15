SET STATISTICS TIME ON 

DECLARE @SystemName nvarchar(100) = 'HRC'
			,@SystemNameInt int = 3
			,@MaxDate date

DROP TABLE IF EXISTS [tqcic].[HRC_ETL_Runner_Tracker]
SELECT TimeStamp = GETDATE(),TableCompleted = convert(nvarchar(255),'Start'),RecordCount = 0 INTO [tqcic].[HRC_ETL_Runner_Tracker]

	print 'START MERGE 1' 
	--BEGIN TRANSACTION
		--DELETE FROM [qcids].[Dim_Host_SCD] WHERE SystemName = @SystemName
		--INSERT INTO [qcids].[Dim_Host_SCD] SELECT * FROM [qcic].[uvw_HRC_ETL01_Dim_Host_SCD]
		MERGE [tqcids].[Dim_Host_SCD] T 
		USING [tqcic].[uvw_HRC_ETL01_Dim_Host_SCD] S
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
		INSERT INTO [tqcic].[HRC_ETL_Runner_Tracker]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcic].[Dim_Host_SCD]',RecordCount = count(*) FROM [tqcids].[Dim_Host_SCD]
		WHERE SystemNameInt = @SystemNameInt
	print 'FINISH MERGE 1' 
	print 'START MERGE 2' 

  -- BEGIN TRANSACTION
		--DELETE FROM [qcids].[Dim_Player_SCD] WHERE SystemNameInt = @SystemNameInt
		--INSERT INTO [qcids].[Dim_Player_SCD] SELECT * FROM [qcic].[uvw_HRC_ETL02_Dim_Player_SCD]
		MERGE [tqcids].[Dim_Player_SCD] T
		USING [tqcic].[uvw_HRC_ETL02_Dim_Player_SCD] S
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
 --   COMMIT TRANSACTION
		INSERT INTO [tqcic].[HRC_ETL_Runner_Tracker]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[Dim_Player_SCD]',RecordCount = count(*) FROM [tqcids].[Dim_Player_SCD]
		WHERE SystemNameInt = @SystemNameInt
	print 'FINISH MERGE 2' 
	print 'START MERGE 3' 

 --   BEGIN TRANSACTION
		--DELETE FROM [qcids].[Dim_PlayerDistanceToCasino_SCD] WHERE SystemNameInt = @SystemNameInt
		--INSERT INTO [qcids].[Dim_PlayerDistanceToCasino_SCD] SELECT * FROM [qcic].[uvw_HRC_ETL03_Dim_PlayerDistanceToCasino_SCD]
		MERGE  [tqcids].[Dim_PlayerDistanceToCasino_SCD] T 
		USING [tqcic].[uvw_HRC_ETL03_Dim_PlayerDistanceToCasino_SCD] S
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
  --  COMMIT TRANSACTION
		INSERT INTO [tqcic].[HRC_ETL_Runner_Tracker]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[Dim_PlayerDistanceToCasino_SCD]',RecordCount = count(*) FROM [tqcids].[Dim_PlayerDistanceToCasino_SCD]
		WHERE SystemNameInt = @SystemNameInt
	print 'FINISH MERGE 3' 
	print 'START MERGE 4' 

 -- BEGIN TRANSACTION
		--DELETE FROM [qcids].[Dim_PlayerLoyalty_SCD] WHERE SystemNameInt = @SystemNameInt
		--INSERT INTO [qcids].[Dim_PlayerLoyalty_SCD] SELECT * FROM [qcic].[uvw_HRC_ETL04_Dim_PlayerLoyalty_SCD]
		MERGE [tqcids].[Dim_PlayerLoyalty_SCD] T
		USING [tqcic].[uvw_HRC_ETL04_Dim_PlayerLoyalty_SCD] S
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
  --  COMMIT TRANSACTION
		INSERT INTO [tqcic].[HRC_ETL_Runner_Tracker]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[Dim_PlayerLoyalty_SCD]',RecordCount = count(*) FROM [tqcids].[Dim_PlayerLoyalty_SCD]
		WHERE SystemNameInt = @SystemNameInt
	print 'FINISH MERGE 4' 
	print 'START MERGE 5'


  --   BEGIN TRANSACTION
		--DELETE FROM [qcids].[Dim_PlayerHost_SCD] WHERE SystemNameInt = @SystemNameInt
		--INSERT INTO [qcids].[Dim_PlayerHost_SCD] SELECT * FROM [qcic].[uvw_HRC_ETL05_Dim_PlayerHost_SCD]
		MERGE [tqcids].[Dim_PlayerHost_SCD] T 
		USING [tqcic].[uvw_HRC_ETL05_Dim_PlayerHost_SCD] S
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
 --   COMMIT TRANSACTION
		INSERT INTO [tqcic].[HRC_ETL_Runner_Tracker]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[Dim_PlayerHost_SCD]',RecordCount = count(*) FROM [tqcids].[Dim_PlayerHost_SCD]
		WHERE SystemNameInt = @SystemNameInt

	print 'FINISH MERGE 5' 
	print 'START MERGE 6'

--    BEGIN TRANSACTION
		--DELETE FROM [qcids].[Dim_PlayerCredit_SCD] WHERE SystemNameInt = @SystemNameInt
		--INSERT INTO [qcids].[Dim_PlayerCredit_SCD] SELECT * FROM [qcic].[uvw_HRC_ETL06_Dim_PlayerCredit_SCD]
		MERGE [tqcids].[Dim_PlayerCredit_SCD] T 
		USING [tqcic].[uvw_HRC_ETL06_Dim_PlayerCredit_SCD] S
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
 --   COMMIT TRANSACTION
		INSERT INTO [tqcic].[HRC_ETL_Runner_Tracker]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[Dim_PlayerCredit_SCD]',RecordCount = count(*) FROM [tqcids].[Dim_PlayerCredit_SCD]
		WHERE SystemNameInt = @SystemNameInt
	print 'FINISH MERGE 6' 
	print 'START MERGE 7'

  -- BEGIN TRANSACTION
		--DELETE FROM [qcids].[Dim_PlayerCreditElectronic_SCD] WHERE SystemNameInt = @SystemNameInt
		--INSERT INTO [qcids].[Dim_PlayerCreditElectronic_SCD] SELECT * FROM [qcic].[uvw_HRC_ETL07_Dim_PlayerCreditElectronic_SCD]
		MERGE [tqcids].[Dim_PlayerCreditElectronic_SCD] T 
		USING [tqcic].[uvw_HRC_ETL07_Dim_PlayerCreditElectronic_SCD] S
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
  --  COMMIT TRANSACTION
		INSERT INTO [tqcic].[HRC_ETL_Runner_Tracker]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[Dim_PlayerCreditElectronic_SCD]',RecordCount = count(*) FROM [tqcids].[Dim_PlayerCreditElectronic_SCD]
		WHERE SystemNameInt = @SystemNameInt
	print 'FINISH MERGE 7' 
	print 'START MERGE 8'

    DECLARE @days int = -10
  --  BEGIN TRANSACTION
		SELECT @MaxDate = MAX(GamingDate) FROM [qcids].[Fact_PlayerDeviceRatingDay] WITH (NOLOCK) WHERE SystemNameInt = @SystemNameInt
		SELECT @MaxDate = ISNULL(@MaxDate,'2000-01-01')
		DELETE FROM [tqcids].[Fact_PlayerDeviceRatingDay] WHERE SystemNameInt = @SystemNameInt AND GamingDate >= DATEADD(DAY,-10,@MaxDate)
		INSERT INTO [tqcids].[Fact_PlayerDeviceRatingDay] EXEC [tqcic].[usp_HRC_ETL08_Fact_PlayerDeviceRatingDay] @MaxDate, @days
  --  COMMIT TRANSACTION
		INSERT INTO [tqcic].[HRC_ETL_Runner_Tracker]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[Fact_PlayerDeviceRatingDay]',RecordCount = count(*) FROM [tqcids].[Fact_PlayerDeviceRatingDay]
		WHERE SystemNameInt = @SystemNameInt
	print 'FINISH MERGE 8'
	
	print 'START MERGE 9'	
   -- BEGIN TRANSACTION
		DELETE FROM [tqcids].[App_DeviceManufacturerGroup] WHERE SystemNameInt = @SystemNameInt
		INSERT INTO [tqcids].[App_DeviceManufacturerGroup] SELECT * FROM  [tqcic].[uvw_HRC_ETL09_App_DeviceManufacturerGroup]
  --  COMMIT TRANSACTION
		INSERT INTO [tqcic].[HRC_ETL_Runner_Tracker]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[test].[App_DeviceManufacturerGroup]',RecordCount = count(*) FROM [tqcids].[App_DeviceManufacturerGroup]
		WHERE SystemNameInt = @SystemNameInt
	print 'FINISH MERGE 9'
	
	print 'START MERGE 10'	
   -- BEGIN TRANSACTION
		DELETE FROM [tqcids].[App_DeviceGameTypeGroup] WHERE SystemNameInt = @SystemNameInt
		INSERT INTO [tqcids].[App_DeviceGameTypeGroup] SELECT * FROM  [tqcic].[uvw_HRC_ETL10_App_DeviceGameTypeGroup]
  --  COMMIT TRANSACTION
		INSERT INTO [tqcic].[HRC_ETL_Runner_Tracker]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[App_DeviceGameTypeGroup]',RecordCount = count(*) FROM [tqcids].[App_DeviceGameTypeGroup]
		WHERE SystemNameInt = @SystemNameInt
	print 'FINISH MERGE 10'
	
	print 'START MERGE 11'	
  --  BEGIN TRANSACTION
		DELETE FROM [tqcids].[App_DeviceDenominationGroup] WHERE SystemNameInt = @SystemNameInt
		INSERT INTO [tqcids].[App_DeviceDenominationGroup] SELECT * FROM  [tqcic].[uvw_HRC_ETL11_App_DeviceDenominationGroup]
  --  COMMIT TRANSACTION
		INSERT INTO [tqcic].[HRC_ETL_Runner_Tracker]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[App_DeviceDenominationGroup]',RecordCount = count(*) FROM [tqcids].[App_DeviceDenominationGroup]
		WHERE SystemNameInt = @SystemNameInt
	print 'FINISH MERGE 11'
	
	print 'START MERGE 12'	
  --  BEGIN TRANSACTION
		SELECT @MaxDate = MAX(PrizeStartDateTime) FROM [tqcids].[Fact_PlayerPrize] WITH (NOLOCK) WHERE SystemNameInt = @SystemNameInt
		SELECT @MaxDate = ISNULL(@MaxDate,'2000-01-01')
		DELETE FROM [tqcids].[Fact_PlayerPrize] WHERE SystemNameInt = @SystemNameInt AND PrizeStartDateTime >= DATEADD(DAY,-10,@MaxDate)
		INSERT INTO [tqcids].[Fact_PlayerPrize]  EXEC [tqcic].[usp_HRC_ETL12_Fact_PlayerPrize] @MaxDate, @days
  --  COMMIT TRANSACTION
		INSERT INTO [tqcic].[HRC_ETL_Runner_Tracker]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[test].[Fact_PlayerPrize]',RecordCount = count(*) FROM [tqcids].[Fact_PlayerPrize]
		WHERE SystemNameInt = @SystemNameInt
	print 'FINISH MERGE 12'
	
	print 'START MERGE 13'	 
   -- BEGIN TRANSACTION
		--DELETE FROM [qcids].[Fact_PlayerEvaluationCurrent] WHERE SystemNameInt = @SystemNameInt
		--INSERT INTO [qcids].[Fact_PlayerEvaluationCurrent] SELECT * FROM [qcic].[uvw_HRC_ETL14_Fact_PlayerEvaluationCurrent]
		MERGE [tqcids].[Fact_PlayerEvaluationCurrent] T 
		USING [tqcic].[uvw_HRC_ETL14_Fact_PlayerEvaluationCurrent] S
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
   -- COMMIT TRANSACTION
		INSERT INTO [tqcic].[HRC_ETL_Runner_Tracker]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[Fact_PlayerEvaluationCurrent]',RecordCount = count(*) FROM [tqcids].[Fact_PlayerEvaluationCurrent]
		WHERE SystemNameInt = @SystemNameInt
	print 'FINISH MERGE 13'
	
	print 'START MERGE 14'	 
   -- BEGIN TRANSACTION
		SELECT @MaxDate = MAX(GamingDate) FROM [qcids].[Fact_DeviceDay] WITH (NOLOCK) WHERE SystemNameInt = @SystemNameInt
		SELECT @MaxDate = ISNULL(@MaxDate,'2000-01-01')
		DELETE FROM [tqcids].[Fact_DeviceDay] WHERE SystemNameInt = @SystemNameInt AND GamingDate >= DATEADD(DAY,-10,@MaxDate)
		INSERT INTO [tqcids].[Fact_DeviceDay] EXEC  [tqcic].[usp_HRC_ETL15_Fact_DeviceDay] @MaxDate, @days
   -- COMMIT TRANSACTION
		INSERT INTO [tqcic].[HRC_ETL_Runner_Tracker]
		SELECT TimeStamp = GETDATE(),TableCompleted = '[tqcids].[Fact_DeviceDay]',RecordCount = count(*) FROM [tqcids].[Fact_DeviceDay]
		WHERE SystemNameInt = @SystemNameInt
		print 'FINISH MERGE 14'





