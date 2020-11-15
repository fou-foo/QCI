----------------copy 
--merge 1 
-----------------------Paso 1 
drop table  [tqcids].[Dim_Host_SCD] ;
select *
into [tqcids].[Dim_Host_SCD]
from [qcids].[Dim_Host_SCD]; --742

ALTER TABLE  [tqcids].[Dim_Host_SCD]
ADD  bandera varchar(80);

update [tqcids].[Dim_Host_SCD]
set bandera ='original';

--- insert gaps 
with gaps as ( 
select top 300  [SystemName], [SystemNameInt], [CasinoGroupID], [CasinoGroupIDInt] , [HostID], [HostIDBigInt], [HostFirstName], [HostLastName] , [HostPosition], [HostCode]
				,DATEADD( day, -40, t.[StartDateTime]) as [StartDateTime]      ,  DATEADD( day, -10, t.[StartDateTime])  as [EndDateTime] 
		       ,[IsCurrent] , [InsertDateTime], ' gaps' AS  [bandera]
                 from [tqcids].[Dim_Host_SCD] as t
                  order by  t.[HostIDBigInt]  ) 

insert into  [tqcids].[Dim_Host_SCD]
select * from  gaps;

with overlaps as ( 
select top 300  [SystemName], [SystemNameInt], [CasinoGroupID], [CasinoGroupIDInt] , [HostID], [HostIDBigInt], [HostFirstName], [HostLastName] , [HostPosition], [HostCode]
				  , DATEADD( day, 30, t.[StartDateTime]) as [StartDateTime]      ,  [EndDateTime] 
		    ,[IsCurrent], [InsertDateTime], 'overlaps' AS [bandera]
                 from  [tqcids].[Dim_Host_SCD] as t
                  order by  t.[HostIDBigInt]   ) 

insert into [tqcids].[Dim_Host_SCD]
select * from  overlaps;
---------------------------------------
select  [SystemName] ,[SystemNameInt] , [CasinoGroupID], [CasinoGroupIDInt] ,[HostID], [HostIDBigInt], [HostFirstName], [HostLastName]
      ,[HostPosition], [HostCode], [StartDateTime], [EndDateTime], [IsCurrent], [InsertDateTime], [bandera], 
	  -- first assumption
	  CASE WHEN RowNumber = 1 THEN 'OK' 
	       WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) < 0 THEN 'Overlap'
		   WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) >= 0 THEN 'Gap' end as Flag
	  FROM (
		select * , 
		ROW_NUMBER()  OVER (PARTITION BY SystemNameInt, CasinoGroupIDInt, HostIDBigInt ORDER BY StartDateTime  ) AS RowNumber,
		LAG(EndDateTime, 1) OVER (PARTITION BY SystemNameInt, CasinoGroupIDInt, HostIDBigInt ORDER BY StartDateTime ) AS LagEndDateTime
		from  [tqcids].[Dim_Host_SCD] ) as A 
    order by SystemNameInt, CasinoGroupIDInt, HostIDBigInt , StartDateTime
---------------------------------------------
--merge 2 
drop table [tqcids].[Dim_Player_SCD] ;
select *
into [tqcids].[Dim_Player_SCD]
from [qcids].[Dim_Player_SCD]; --6,234,999
CREATE CLUSTERED COLUMNSTORE INDEX [cci_tqcids_Dim_Player_SCD] ON [tqcids].[Dim_Player_SCD] WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [PRIMARY]

ALTER TABLE  [tqcids].[Dim_Player_SCD]
ADD  bandera varchar(80);

update [tqcids].[Dim_Player_SCD]
set bandera ='original';

--- insert gaps 
with gaps as ( 
select top 3000000  [SystemName] , [SystemNameInt] ,[PlayerID], [PlayerIDBigInt], [Account], [Title], [LastName] , [FirstName], [DisplayName] ,[NickNameAlias], [HomeAddress1]
      ,[HomeAddress2], [HomeCity], [HomeStateCode], [HomeCountryCode]
      ,[HomePostalCode], [HomeTelephone1Type], [HomeTelephone1], [HomeTelephone2Type]
      ,[HomeTelephone2], [HomeEmail], [AltName], [AltAddress1]
      ,[AltAddress2] ,[AltCity], [AltStateCode], [AltCountryCode]
      ,[AltPostalCode], [AltTelephone1Type], [AltTelephone1], [AltTelephone2Type]
      ,[AltTelephone2], [AltEmail], [BirthDate], [IsPrivacy]
      ,[IsMailToAlt], [IsNoMail], [IsReturnMail], [IsVIP]
      ,[IsLostAndFound] , [IsCreditAccount], [IsBanned], [IsInactive]
      ,[IsCall], [IsEmailSend], [SetupCasinoID] , [SetupEmployeeID]
      ,[SetupDate], [Gender], [MailingAddress1]  ,[MailingAddress2]
      ,[MailingCity] ,[MailingStateCode], [MailingCountryCode]
      ,[MailingPostalCode], [Address_MatchType]
      ,[Address_Latitude], [Address_Longitude]
	  ,DATEADD( day, -40, t.[StartDateTime]) as [StartDateTime]      ,  DATEADD( day, -10, t.[StartDateTime])  as [EndDateTime] 
      ,[IsCurrent],[InsertDateTime], ' gaps' AS  [bandera]
                 from [tqcids].[Dim_Player_SCD] as t
                  order by  t.[PlayerIDBigInt]  ) 

insert into  [tqcids].[Dim_Player_SCD]
select * from  gaps;

with overlaps as ( 
select top 3000000  [SystemName] , [SystemNameInt] ,[PlayerID], [PlayerIDBigInt], [Account], [Title], [LastName] , [FirstName], [DisplayName] ,[NickNameAlias], [HomeAddress1]
      ,[HomeAddress2], [HomeCity], [HomeStateCode], [HomeCountryCode]
      ,[HomePostalCode], [HomeTelephone1Type], [HomeTelephone1], [HomeTelephone2Type]
      ,[HomeTelephone2], [HomeEmail], [AltName], [AltAddress1]
      ,[AltAddress2] ,[AltCity], [AltStateCode], [AltCountryCode]
      ,[AltPostalCode], [AltTelephone1Type], [AltTelephone1], [AltTelephone2Type]
      ,[AltTelephone2], [AltEmail], [BirthDate], [IsPrivacy]
      ,[IsMailToAlt], [IsNoMail], [IsReturnMail], [IsVIP]
      ,[IsLostAndFound] , [IsCreditAccount], [IsBanned], [IsInactive]
      ,[IsCall], [IsEmailSend], [SetupCasinoID] , [SetupEmployeeID]
      ,[SetupDate], [Gender], [MailingAddress1]  ,[MailingAddress2]
      ,[MailingCity] ,[MailingStateCode], [MailingCountryCode]
      ,[MailingPostalCode], [Address_MatchType]
      ,[Address_Latitude], [Address_Longitude]
				  , DATEADD( day, 30, t.[StartDateTime]) as [StartDateTime]      ,  [EndDateTime] 
		    ,[IsCurrent], [InsertDateTime], 'overlaps' AS [bandera]
                 from [tqcids].[Dim_Player_SCD] as t
                  order by  t.[PlayerIDBigInt]   ) 

insert into [tqcids].[Dim_Player_SCD]
select * from  overlaps;
----------------------
SELECT [SystemName], [SystemNameInt], [PlayerID], [PlayerIDBigInt], [Account], [Title], [LastName], [FirstName]
      ,[DisplayName], [NickNameAlias], [HomeAddress1], [HomeAddress2], [HomeCity] , [HomeStateCode], [HomeCountryCode], [HomePostalCode]
      ,[HomeTelephone1Type], [HomeTelephone1], [HomeTelephone2Type], [HomeTelephone2], [HomeEmail], [AltName], [AltAddress1], [AltAddress2]
      ,[AltCity], [AltStateCode], [AltCountryCode], [AltPostalCode], [AltTelephone1Type], [AltTelephone1], [AltTelephone2Type], [AltTelephone2]
      ,[AltEmail], [BirthDate], [IsPrivacy], [IsMailToAlt], [IsNoMail], [IsReturnMail], [IsVIP], [IsLostAndFound]
      ,[IsCreditAccount], [IsBanned], [IsInactive], [IsCall], [IsEmailSend], [SetupCasinoID], [SetupEmployeeID], [SetupDate]
      ,[Gender], [MailingAddress1], [MailingAddress2], [MailingCity], [MailingStateCode], [MailingCountryCode], [MailingPostalCode], [Address_MatchType]
      ,[Address_Latitude], [Address_Longitude], [StartDateTime], [EndDateTime],[IsCurrent], [InsertDateTime], [bandera], 
	  -- first assumption
	  CASE WHEN RowNumber = 1 THEN 'OK' 
	       WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) < 0 THEN 'Overlap'
		   WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) >= 0 THEN 'Gap' end as Flag
	  FROM (
		select * , 
		ROW_NUMBER()  OVER (PARTITION BY  SystemNameInt, PlayerIDBigInt ORDER BY StartDateTime  ) AS RowNumber,
		LAG(EndDateTime, 1) OVER (PARTITION BY  SystemNameInt, PlayerIDBigInt ORDER BY StartDateTime ) AS LagEndDateTime
		from  [tqcids].[Dim_Player_SCD] ) as A 
        order by  SystemNameInt, PlayerIDBigInt , StartDateTime
------------------
--merge 3 --Paso 3 

drop table  [tqcids].[Dim_PlayerDistanceToCasino_SCD] ;
select *
into [tqcids].[Dim_PlayerDistanceToCasino_SCD]
from [qcids].[Dim_PlayerDistanceToCasino_SCD]; -- 14,607,847
--CREATE CLUSTERED COLUMNSTORE INDEX [cci_tqcids_Dim_PlayerDistanceToCasino_SCD] ON [tqcids].[Dim_PlayerDistanceToCasino_SCD] WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [PRIMARY]

ALTER TABLE  [tqcids].[Dim_PlayerDistanceToCasino_SCD]
ADD  bandera varchar(80);

update [tqcids].[Dim_PlayerDistanceToCasino_SCD]
set bandera ='original';

--- insert gaps 
with gaps as ( 
select top 7000000   [SystemName] , [SystemNameInt], [CasinoID], [CasinoIDInt],[PlayerID], [PlayerIDBigInt], [PlayerDistanceToCasino],
                  DATEADD( day, -40, t.[StartDateTime]) as [StartDateTime] , DATEADD( day, -10, t.[StartDateTime])  as [EndDateTime]
                , [IsCurrent], [InsertDateTime], ' gaps' AS  [bandera]
                 from [tqcids].[Dim_PlayerDistanceToCasino_SCD] as t
                  order by  t.[PlayerIDBigInt] ) 

insert into  [tqcids].[Dim_PlayerDistanceToCasino_SCD]
select * from  gaps;

with overlaps as ( 
select top 7000000   [SystemName] , [SystemNameInt], [CasinoID], [CasinoIDInt],[PlayerID], [PlayerIDBigInt], [PlayerDistanceToCasino],
       		   DATEADD( day, 30, t.[StartDateTime]) as [StartDateTime]      ,  [EndDateTime] 
		    ,[IsCurrent], [InsertDateTime], 'overlaps' AS [bandera]
                 from  [tqcids].[Dim_PlayerDistanceToCasino_SCD] as t
                  order by  t.[PlayerIDBigInt]   ) 

insert into [tqcids].[Dim_PlayerDistanceToCasino_SCD]
select * from  overlaps;
-------------------------------------------
-- paso 3
SELECT  [SystemName] ,[SystemNameInt], [CasinoID], [CasinoIDInt], [PlayerID], [PlayerIDBigInt], [PlayerDistanceToCasino]
      ,[StartDateTime], [EndDateTime], [IsCurrent], [InsertDateTime], [bandera], 
	  -- first assumption
	  CASE WHEN RowNumber = 1 THEN 'OK' 
	       WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) < 0 THEN 'Overlap'
		   WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) >= 0 THEN 'Gap' end as Flag
	  FROM (
		select * , 
		ROW_NUMBER()  OVER (PARTITION BY  SystemNameInt, CasinoIDInt, PlayerIDBigInt ORDER BY StartDateTime  ) AS RowNumber,
		LAG(EndDateTime, 1) OVER (PARTITION BY  SystemNameInt, CasinoIDInt, PlayerIDBigInt ORDER BY StartDateTime ) AS LagEndDateTime
		from   [tqcids].[Dim_PlayerDistanceToCasino_SCD] ) as A 
        order by  SystemNameInt, CasinoIDInt, PlayerIDBigInt, StartDateTime

-------------Paso 4 
drop table [tqcids].[Dim_PlayerLoyalty_SCD] ;
select *
into [tqcids].[Dim_PlayerLoyalty_SCD] 
from [qcids].[Dim_PlayerLoyalty_SCD] ; -- 6,220,193
/****** Object:  Index [cci_qcids_Dim_PlayerLoyalty_SCD]    Script Date: 10/22/2020 8:28:21 AM ******/
--CREATE CLUSTERED COLUMNSTORE INDEX [cci_tqcids_Dim_PlayerLoyalty_SCD] ON [tqcids].[Dim_PlayerLoyalty_SCD] WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [PRIMARY]

ALTER TABLE  [tqcids].[Dim_PlayerLoyalty_SCD] 
ADD  bandera varchar(80);

update [tqcids].[Dim_PlayerLoyalty_SCD]
set bandera ='original';


--- insert gaps 
with gaps as ( 
select top 3000000  [SystemName] ,[SystemNameInt], [PlayerID], [PlayerIDBigInt] , [LoyaltyCardLevel]
	  ,DATEADD( day, -40, t.[StartDateTime]) as [StartDateTime]      ,  DATEADD( day, -10, t.[StartDateTime])  as [EndDateTime] 
      ,[IsCurrent],[InsertDateTime], ' gaps' AS  [bandera]
                 from [tqcids].[Dim_PlayerLoyalty_SCD]  as t
                  order by  t.[PlayerIDBigInt]  ) 

insert into  [tqcids].[Dim_PlayerLoyalty_SCD] 
select * from  gaps;

with overlaps as ( 
select top 3000000  [SystemName] ,[SystemNameInt], [PlayerID], [PlayerIDBigInt] , [LoyaltyCardLevel]
           , DATEADD( day, 30, t.[StartDateTime]) as [StartDateTime]      ,  [EndDateTime] 
		    ,[IsCurrent], [InsertDateTime], 'overlaps' AS [bandera]
                 from [tqcids].[Dim_PlayerLoyalty_SCD]  as t
                  order by  t.[PlayerIDBigInt]  ) 

insert into [tqcids].[Dim_PlayerLoyalty_SCD] 
select * from  overlaps;
---------------cuarto paso 
SELECT   [SystemName], [SystemNameInt], [PlayerID], [PlayerIDBigInt],[LoyaltyCardLevel], [StartDateTime], [EndDateTime], [IsCurrent] ,[InsertDateTime], [bandera], 
	  -- first assumption
	  CASE WHEN RowNumber = 1 THEN 'OK' 
	       WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) < 0 THEN 'Overlap'
		   WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) >= 0 THEN 'Gap' end as Flag
	  FROM (
		select * , 
		ROW_NUMBER()  OVER (PARTITION BY SystemNameInt, PlayerIDBigInt ORDER BY StartDateTime  ) AS RowNumber,
		LAG(EndDateTime, 1) OVER (PARTITION BY  SystemNameInt, PlayerIDBigInt ORDER BY StartDateTime ) AS LagEndDateTime
		from    [tqcids].[Dim_PlayerLoyalty_SCD]  ) as A 
        order by  SystemNameInt, PlayerIDBigInt, StartDateTime

----------------------Paso 5 
 drop table  [tqcids].[Dim_PlayerHost_SCD] ;
select *
into  [tqcids].[Dim_PlayerHost_SCD]
from  [qcids].[Dim_PlayerHost_SCD]; -- 36,019

ALTER TABLE  [tqcids].[Dim_PlayerHost_SCD]
ADD  bandera varchar(80);

update [tqcids].[Dim_PlayerHost_SCD]
set bandera ='original';

--- insert gaps 
with gaps as ( 
select top 15000  [SystemName] , [SystemNameInt] , [CasinoID], [CasinoIDInt], [PlayerID] , [PlayerIDBigInt], [HostID], [HostIDBigInt], [PlayerHostType]  
                 ,DATEADD( day, -40, t.[StartDateTime]) as [StartDateTime]      ,  DATEADD( day, -10, t.[StartDateTime])  as [EndDateTime] 
		       ,[IsCurrent] , [InsertDateTime], ' gaps' AS  [bandera]
                 from [tqcids].[Dim_PlayerHost_SCD] as t
                  order by  t.[PlayerIDBigInt]  ) 

insert into  [tqcids].[Dim_PlayerHost_SCD]
select * from  gaps;

with overlaps as ( 
select top 15000  [SystemName] , [SystemNameInt] , [CasinoID], [CasinoIDInt], [PlayerID] , [PlayerIDBigInt], [HostID], [HostIDBigInt], [PlayerHostType]  
				  , DATEADD( day, 30, t.[StartDateTime]) as [StartDateTime]      ,  [EndDateTime] 
		    ,[IsCurrent], [InsertDateTime], 'overlaps' AS [bandera]
                 from [tqcids].[Dim_PlayerHost_SCD] as t
                  order by  t.[PlayerIDBigInt]   ) 

insert into [tqcids].[Dim_PlayerHost_SCD]
select * from  overlaps;
-------------------------------------- merge 5 
SELECT  [SystemName], [SystemNameInt] ,[CasinoID], [CasinoIDInt], [PlayerID], [PlayerIDBigInt],[HostID], [HostIDBigInt]
      ,[PlayerHostType], [StartDateTime],[EndDateTime], [IsCurrent], [InsertDateTime],[bandera],
	  CASE WHEN RowNumber = 1 THEN 'OK' 
	       WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) < 0 THEN 'Overlap'
		   WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) >= 0 THEN 'Gap' end as Flag
	  FROM (
		select * , 
		ROW_NUMBER()  OVER (PARTITION BY SystemNameInt, CasinoIDInt, PlayerIDBigInt, PlayerHostType, HostIDBigInt ORDER BY StartDateTime  ) AS RowNumber,
		LAG(EndDateTime, 1) OVER (PARTITION BY   SystemNameInt, CasinoIDInt, PlayerIDBigInt, PlayerHostType, HostIDBigInt ORDER BY StartDateTime ) AS LagEndDateTime
		from    [tqcids].[Dim_PlayerHost_SCD] ) as A 
        order by  SystemNameInt, CasinoIDInt, PlayerIDBigInt, PlayerHostType, HostIDBigInt, StartDateTime
--------
----------------------Paso 6
 drop table [tqcids].[Dim_PlayerCredit_SCD] ;
select *
into  [tqcids].[Dim_PlayerCredit_SCD]
from  [qcids].[Dim_PlayerCredit_SCD]; -- 144,168

ALTER TABLE  [tqcids].[Dim_PlayerCredit_SCD]
ADD  bandera varchar(80);

update [tqcids].[Dim_PlayerCredit_SCD]
set bandera ='original';

--- insert gaps 
with gaps as ( 
select top 60000  [SystemName],[SystemNameInt]   ,[CasinoID]    ,[CasinoIDInt] ,[PlayerID]   ,[PlayerIDBigInt]  ,[CreditLimitInt100]   ,[IsActiveCredit],[SetupCasinoID]     ,[Note]  ,
	              DATEADD( day, -40, t.[StartDateTime]) as [StartDateTime]      ,  DATEADD( day, -10, t.[StartDateTime])  as [EndDateTime] 
		       ,[IsCurrent] , [InsertDateTime], ' gaps' AS  [bandera]
                 from [tqcids].[Dim_PlayerCredit_SCD] as t
                  order by  t.[PlayerIDBigInt]  ) 

insert into [tqcids].[Dim_PlayerCredit_SCD]
select * from  gaps;

with overlaps as ( 
select top 60000  [SystemName],[SystemNameInt]   ,[CasinoID]    ,[CasinoIDInt] ,[PlayerID]   ,[PlayerIDBigInt]  ,[CreditLimitInt100]   ,[IsActiveCredit],[SetupCasinoID]     ,[Note]  ,
				   DATEADD( day, 30, t.[StartDateTime]) as [StartDateTime]      ,  [EndDateTime] 
		    ,[IsCurrent], [InsertDateTime], 'overlaps' AS [bandera]
                 from [tqcids].[Dim_PlayerCredit_SCD] as t
                  order by  t.[PlayerIDBigInt]   ) 

insert into [tqcids].[Dim_PlayerCredit_SCD]
select * from  overlaps;

 ---------------- paso 6 
 SELECT [SystemName], [SystemNameInt], [CasinoID], [CasinoIDInt], [PlayerID], [PlayerIDBigInt], [CreditLimitInt100], [IsActiveCredit]
      ,[SetupCasinoID], [Note], [StartDateTime], [EndDateTime], [IsCurrent], [InsertDateTime], [bandera], 
	  	  CASE WHEN RowNumber = 1 THEN 'OK' 
	       WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) < 0 THEN 'Overlap'
		   WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) >= 0 THEN 'Gap' end as Flag
	  FROM (
		select * , 
		ROW_NUMBER()  OVER (PARTITION BY SystemNameInt, CasinoIDInt, PlayerIDBigInt  ORDER BY StartDateTime  ) AS RowNumber,
		LAG(EndDateTime, 1) OVER (PARTITION BY   SystemNameInt, CasinoIDInt, PlayerIDBigInt  ORDER BY StartDateTime ) AS LagEndDateTime
		from    [tqcids].[Dim_PlayerCredit_SCD] ) as A 
        order by  SystemNameInt, CasinoIDInt, PlayerIDBigInt , StartDateTime
----------------------Paso 7
 drop table  [tqcids].[Dim_PlayerCreditElectronic_SCD] ;
select *
into   [tqcids].[Dim_PlayerCreditElectronic_SCD]
from  [qcids].[Dim_PlayerCreditElectronic_SCD]; -- 126,148

ALTER TABLE  [tqcids].[Dim_PlayerCreditElectronic_SCD]
ADD  bandera varchar(80);

update  [tqcids].[Dim_PlayerCreditElectronic_SCD]
set bandera ='original';

--- insert gaps 
with gaps as ( 
select top 50000  [SystemName] ,[SystemNameInt]  ,[CasinoID]  ,[CasinoIDInt] ,[PlayerID]  ,[PlayerIDBigInt]   ,[ElectronicCreditType]   ,[IsEnrolled]
      ,[ElectronicCreditAccountID] ,[SignatureID] ,[CreatedByID] ,[Note] ,
	              DATEADD( day, -40, t.[StartDateTime]) as [StartDateTime]      ,  DATEADD( day, -10, t.[StartDateTime])  as [EndDateTime] 
		       ,[IsCurrent] , [InsertDateTime], ' gaps' AS  [bandera]
                 from  [tqcids].[Dim_PlayerCreditElectronic_SCD] as t
                  order by  t.[PlayerIDBigInt]  ) 

insert into  [tqcids].[Dim_PlayerCreditElectronic_SCD]
select * from  gaps;

with overlaps as ( 
select top 50000  [SystemName] ,[SystemNameInt]  ,[CasinoID]  ,[CasinoIDInt] ,[PlayerID]  ,[PlayerIDBigInt]   ,[ElectronicCreditType]   ,[IsEnrolled]
      ,[ElectronicCreditAccountID] ,[SignatureID] ,[CreatedByID] ,[Note] ,
				   DATEADD( day, 30, t.[StartDateTime]) as [StartDateTime]      ,  [EndDateTime] 
		    ,[IsCurrent], [InsertDateTime], 'overlaps' AS [bandera]
                 from  [tqcids].[Dim_PlayerCreditElectronic_SCD]as t
                  order by  t.[PlayerIDBigInt]   ) 

insert into  [tqcids].[Dim_PlayerCreditElectronic_SCD]
select * from  overlaps;

-------------------------------
SELECT [SystemName], [SystemNameInt] , [CasinoID], [CasinoIDInt] , [PlayerID], [PlayerIDBigInt], [ElectronicCreditType], [IsEnrolled]
      ,[ElectronicCreditAccountID], [SignatureID], [CreatedByID], [Note] ,[StartDateTime], [EndDateTime], [IsCurrent], [InsertDateTime]
      ,[bandera], 
	  	  CASE WHEN RowNumber = 1 THEN 'OK' 
	       WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) < 0 THEN 'Overlap'
		   WHEN RowNumber <> 1 AND DATEDIFF(minute,  EndDateTime , LagEndDateTime) >= 0 THEN 'Gap' end as Flag
	  FROM (
		select * , 
		ROW_NUMBER()  OVER (PARTITION BY SystemNameInt, CasinoIDInt, PlayerIDBigInt, ElectronicCreditType, IsEnrolled ORDER BY StartDateTime  ) AS RowNumber,
		LAG(EndDateTime, 1) OVER (PARTITION BY    SystemNameInt, CasinoIDInt, PlayerIDBigInt, ElectronicCreditType, IsEnrolled  ORDER BY StartDateTime ) AS LagEndDateTime
		from   [tqcids].[Dim_PlayerCreditElectronic_SCD] ) as A 
        order by  SystemNameInt, CasinoIDInt, PlayerIDBigInt, ElectronicCreditType, IsEnrolled, StartDateTime




