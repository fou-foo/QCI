--merge 1 

  select * from   [tqcids].[Dim_Host_SCD] as t 
    where t.SystemName='HRC'
  order by t.HostIDBigInt -- before 55 after usp 22


/****** Script for SelectTopNRows command from SSMS  ******/
SELECT  [SystemName]   ,[SystemNameInt] ,[PlayerID]     ,[PlayerIDBigInt]
      ,[Account]      ,[Title] ,[LastName]      ,[FirstName]
      ,[DisplayName]      ,[NickNameAlias],[HomeAddress1]      ,[HomeAddress2]
      ,[HomeCity]      ,[HomeStateCode],[HomeCountryCode]      ,[HomePostalCode]
      ,[HomeTelephone1Type]      ,[HomeTelephone1],[HomeTelephone2Type]      ,[HomeTelephone2]
      ,[HomeEmail]     ,[AltName],[AltAddress1]      ,[AltAddress2]
      ,[AltCity]      ,[AltStateCode],[AltCountryCode]      ,[AltPostalCode]
      ,[AltTelephone1Type]      ,[AltTelephone1] ,[AltTelephone2Type]      ,[AltTelephone2]
      ,[AltEmail]      ,[BirthDate],[IsPrivacy]      ,[IsMailToAlt]
      ,[IsNoMail]      ,[IsReturnMail] ,[IsVIP]      ,[IsLostAndFound]
      ,[IsCreditAccount]      ,[IsBanned],[IsInactive]      ,[IsCall]
      ,[IsEmailSend]      ,[SetupCasinoID] ,[SetupEmployeeID]      ,[SetupDate]
      ,[Gender]      ,[MailingAddress1]  ,[MailingAddress2]      ,[MailingCity]
      ,[MailingStateCode]      ,[MailingCountryCode]   ,[MailingPostalCode]      ,[Address_MatchType]
      ,[Address_Latitude]      ,[Address_Longitude]  ,[StartDateTime]      ,[EndDateTime]
      ,[IsCurrent]      ,[InsertDateTime]   ,[bandera]
  FROM [tqcids].[Dim_Player_SCD] as t 
  
  where t.SystemName='HRC'
  order by t.PlayerIDBigInt --  before   606,356 after usp 151,589
  

  --paso 3 
  select * 
  from [tqcids].[Dim_PlayerDistanceToCasino_SCD] as t
  where t.SystemName='HRC'
  order by t.PlayerIDBigInt -- before    505, 218      after usp   151, 589

  --- paso 4 
  select *
  from [tqcids].[Dim_PlayerLoyalty_SCD] as t 
  where t.SystemName='HRC'
  order by t.PlayerIDBigInt -- before     505, 218      after usp   151, 589


  -- paso 5 
  select * 
  from [tqcids].[Dim_PlayerHost_SCD] as t 
  where t.SystemName='HRC'
  order by SystemNameInt, CasinoIDInt, PlayerIDBigInt , HostIDBigInt-- before   9,177     after usp  4,837


  --paso 6 

  select * 
  from [tqcids].[Dim_PlayerCredit_SCD] as t 
  where t.SystemName='HRC'
  order by SystemNameInt, CasinoIDInt, PlayerIDBigInt -- before 42,009         after usp   22,944


  --paso 7 

  select * 
  from [tqcids].[Dim_PlayerCreditElectronic_SCD]  as t 
  where t.SystemName='HRC'
  order by SystemNameInt, CasinoIDInt, PlayerIDBigInt -- before  33,006     after usp  18,411

