select  [SystemName] ,[SystemNameInt] , [CasinoGroupID], [CasinoGroupIDInt] ,[HostID], [HostIDBigInt], [HostFirstName], [HostLastName]
      ,[HostPosition], [HostCode], [StartDateTime], [EndDateTime], [IsCurrent], [InsertDateTime],   
	  -- first assumption
	  CASE WHEN RowNumber = 1 THEN 'OK' 
	       WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) < 0 THEN 'Overlap'
		   WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) >= 0 THEN 'Gap' end as Flag
	  FROM (
		select * , 
		ROW_NUMBER()  OVER (PARTITION BY SystemNameInt, CasinoGroupIDInt, HostIDBigInt ORDER BY StartDateTime  ) AS RowNumber,
		LAG(EndDateTime, 1) OVER (PARTITION BY SystemNameInt, CasinoGroupIDInt, HostIDBigInt ORDER BY StartDateTime ) AS LagEndDateTime
		from  [qcids].[Dim_Host_SCD] ) as A 
    order by SystemNameInt, CasinoGroupIDInt, HostIDBigInt , StartDateTime
---------------------------------
-- second merge 
SELECT [SystemName], [SystemNameInt], [PlayerID], [PlayerIDBigInt], [Account], [Title], [LastName], [FirstName]
      ,[DisplayName], [NickNameAlias], [HomeAddress1], [HomeAddress2], [HomeCity] , [HomeStateCode], [HomeCountryCode], [HomePostalCode]
      ,[HomeTelephone1Type], [HomeTelephone1], [HomeTelephone2Type], [HomeTelephone2], [HomeEmail], [AltName], [AltAddress1], [AltAddress2]
      ,[AltCity], [AltStateCode], [AltCountryCode], [AltPostalCode], [AltTelephone1Type], [AltTelephone1], [AltTelephone2Type], [AltTelephone2]
      ,[AltEmail], [BirthDate], [IsPrivacy], [IsMailToAlt], [IsNoMail], [IsReturnMail], [IsVIP], [IsLostAndFound]
      ,[IsCreditAccount], [IsBanned], [IsInactive], [IsCall], [IsEmailSend], [SetupCasinoID], [SetupEmployeeID], [SetupDate]
      ,[Gender], [MailingAddress1], [MailingAddress2], [MailingCity], [MailingStateCode], [MailingCountryCode], [MailingPostalCode], [Address_MatchType]
      ,[Address_Latitude], [Address_Longitude], [StartDateTime], [EndDateTime],[IsCurrent], [InsertDateTime], 
	  -- first assumption
	  CASE WHEN RowNumber = 1 THEN 'OK' 
	       WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) < 0 THEN 'Overlap'
		   WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) >= 0 THEN 'Gap' end as Flag
	  FROM (
		select * , 
		ROW_NUMBER()  OVER (PARTITION BY  SystemNameInt, PlayerIDBigInt ORDER BY StartDateTime  ) AS RowNumber,
		LAG(EndDateTime, 1) OVER (PARTITION BY  SystemNameInt, PlayerIDBigInt ORDER BY StartDateTime ) AS LagEndDateTime
		from  [qcids].[Dim_Player_SCD] ) as A 
        order by  SystemNameInt, PlayerIDBigInt , StartDateTime
-------------------------------
--- third merge 

SELECT  [SystemName] ,[SystemNameInt], [CasinoID], [CasinoIDInt], [PlayerID], [PlayerIDBigInt], [PlayerDistanceToCasino]
      ,[StartDateTime], [EndDateTime], [IsCurrent], [InsertDateTime] ,
	  -- first assumption
	  CASE WHEN RowNumber = 1 THEN 'OK' 
	       WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) < 0 THEN 'Overlap'
		   WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) >= 0 THEN 'Gap' end as Flag
	  FROM (
		select * , 
		ROW_NUMBER()  OVER (PARTITION BY  SystemNameInt, CasinoIDInt, PlayerIDBigInt ORDER BY StartDateTime  ) AS RowNumber,
		LAG(EndDateTime, 1) OVER (PARTITION BY  SystemNameInt, CasinoIDInt, PlayerIDBigInt ORDER BY StartDateTime ) AS LagEndDateTime
		from   [qcids].[Dim_PlayerDistanceToCasino_SCD] ) as A 
        order by  SystemNameInt, CasinoIDInt, PlayerIDBigInt, StartDateTime
-----------fourth merge
SELECT   [SystemName], [SystemNameInt], [PlayerID], [PlayerIDBigInt],[LoyaltyCardLevel], [StartDateTime], [EndDateTime], [IsCurrent] ,[InsertDateTime], 
	  -- first assumption
	  CASE WHEN RowNumber = 1 THEN 'OK' 
	       WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) < 0 THEN 'Overlap'
		   WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) >= 0 THEN 'Gap' end as Flag
	  FROM (
		select * , 
		ROW_NUMBER()  OVER (PARTITION BY SystemNameInt, PlayerIDBigInt ORDER BY StartDateTime  ) AS RowNumber,
		LAG(EndDateTime, 1) OVER (PARTITION BY  SystemNameInt, PlayerIDBigInt ORDER BY StartDateTime ) AS LagEndDateTime
		from    [qcids].[Dim_PlayerLoyalty_SCD]  ) as A 
        order by  SystemNameInt, PlayerIDBigInt, StartDateTime


--- fifth merge
SELECT  [SystemName], [SystemNameInt] ,[CasinoID], [CasinoIDInt], [PlayerID], [PlayerIDBigInt],[HostID], [HostIDBigInt]
      ,[PlayerHostType], [StartDateTime],[EndDateTime], [IsCurrent], [InsertDateTime],
	  CASE WHEN RowNumber = 1 THEN 'OK' 
	       WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) < 0 THEN 'Overlap'
		   WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) >= 0 THEN 'Gap' end as Flag
	  FROM (
		select * , 
		ROW_NUMBER()  OVER (PARTITION BY SystemNameInt, CasinoIDInt, PlayerIDBigInt, PlayerHostType, HostIDBigInt ORDER BY StartDateTime  ) AS RowNumber,
		LAG(EndDateTime, 1) OVER (PARTITION BY   SystemNameInt, CasinoIDInt, PlayerIDBigInt, PlayerHostType, HostIDBigInt ORDER BY StartDateTime ) AS LagEndDateTime
		from    [qcids].[Dim_PlayerHost_SCD] ) as A 
        order by  SystemNameInt, CasinoIDInt, PlayerIDBigInt, PlayerHostType, HostIDBigInt, StartDateTime
------ sixth merge 
 SELECT [SystemName], [SystemNameInt], [CasinoID], [CasinoIDInt], [PlayerID], [PlayerIDBigInt], [CreditLimitInt100], [IsActiveCredit]
      ,[SetupCasinoID], [Note], [StartDateTime], [EndDateTime], [IsCurrent], [InsertDateTime], 
	  	  CASE WHEN RowNumber = 1 THEN 'OK' 
	       WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) < 0 THEN 'Overlap'
		   WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) >= 0 THEN 'Gap' end as Flag
	  FROM (
		select * , 
		ROW_NUMBER()  OVER (PARTITION BY SystemNameInt, CasinoIDInt, PlayerIDBigInt  ORDER BY StartDateTime  ) AS RowNumber,
		LAG(EndDateTime, 1) OVER (PARTITION BY   SystemNameInt, CasinoIDInt, PlayerIDBigInt  ORDER BY StartDateTime ) AS LagEndDateTime
		from    [qcids].[Dim_PlayerCredit_SCD] ) as A 
        order by  SystemNameInt, CasinoIDInt, PlayerIDBigInt , StartDateTime
--- seventh merge 
SELECT [SystemName], [SystemNameInt] , [CasinoID], [CasinoIDInt] , [PlayerID], [PlayerIDBigInt], [ElectronicCreditType], [IsEnrolled]
      ,[ElectronicCreditAccountID], [SignatureID], [CreatedByID], [Note] ,[StartDateTime], [EndDateTime], [IsCurrent], [InsertDateTime], 
	  	  CASE WHEN RowNumber = 1 THEN 'OK' 
	       WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) < 0 THEN 'Overlap'
		   WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) >= 0 THEN 'Gap' end as Flag
	  FROM (
		select * , 
		ROW_NUMBER()  OVER (PARTITION BY SystemNameInt, CasinoIDInt, PlayerIDBigInt, ElectronicCreditType, IsEnrolled ORDER BY StartDateTime  ) AS RowNumber,
		LAG(EndDateTime, 1) OVER (PARTITION BY    SystemNameInt, CasinoIDInt, PlayerIDBigInt, ElectronicCreditType, IsEnrolled  ORDER BY StartDateTime ) AS LagEndDateTime
		from   [qcids].[Dim_PlayerCreditElectronic_SCD] ) as A 
        order by  SystemNameInt, CasinoIDInt, PlayerIDBigInt, ElectronicCreditType, IsEnrolled, StartDateTime


