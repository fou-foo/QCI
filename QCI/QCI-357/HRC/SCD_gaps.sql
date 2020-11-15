-----------------------Paso 1 
drop table  [tqcids].[Dim_Host_SCD] ;
select *
into [tqcids].[Dim_Host_SCD]
from [qcids].[Dim_Host_SCD]; --742


select   [SystemName], [CasinoGroupIDInt], [HostIDBigInt],  count(*) 
	  from  [tqcids].[Dim_Host_SCD]
	  group by [SystemName], [CasinoGroupIDInt], [HostIDBigInt]
	  having count(*) > 1 -- not repeated

ALTER TABLE  [tqcids].[Dim_Host_SCD]
ADD  bandera varchar(80);

update [tqcids].[Dim_Host_SCD]
set bandera ='original';

--- insert gaps 
with gaps as ( 
select top 350  [SystemName], [SystemNameInt], [CasinoGroupID], [CasinoGroupIDInt] , [HostID], [HostIDBigInt], [HostFirstName], [HostLastName] , [HostPosition], [HostCode]
				,DATEADD( day, -40, t.[StartDateTime]) as [StartDateTime]      ,  DATEADD( day, -10, t.[StartDateTime])  as [EndDateTime] 
		       ,[IsCurrent] , [InsertDateTime], ' gaps' AS  [bandera]
                 from [tqcids].[Dim_Host_SCD] as t
                  order by  t.[HostIDBigInt]  ) 

insert into  [tqcids].[Dim_Host_SCD]
select * from  gaps;

with overlaps as ( 
select top 350  [SystemName], [SystemNameInt], [CasinoGroupID], [CasinoGroupIDInt] , [HostID], [HostIDBigInt], [HostFirstName], [HostLastName] , [HostPosition], [HostCode]
				  , DATEADD( day, 30, t.[StartDateTime]) as [StartDateTime]      ,  [EndDateTime] 
		    ,[IsCurrent], [InsertDateTime], 'overlaps' AS [bandera]
                 from  [tqcids].[Dim_Host_SCD] as t
                  order by  t.[HostIDBigInt]   ) 

insert into [tqcids].[Dim_Host_SCD]
select * from  overlaps;
--------------------
select * from [tqcids].[Dim_Host_SCD]  where HostIDBigInt in (
	   select  HostIDBigInt
	  from  [tqcids].[Dim_Host_SCD] 
	  group by  [SystemName], [CasinoGroupIDInt], [HostIDBigInt]
	  having count(*) > 1 ) -- 700 repeated
///////////////////
drop table  [tqcic].[HRC_Dim_CMPEmployees_History] ;
select *
into [tqcic].[HRC_Dim_CMPEmployees_History]
from [qcic].[HRC_Dim_CMPEmployees_History]; --22

select   [CasinoID],  [EmpID],   count(*) 
	  from  [tqcic].[HRC_Dim_CMPEmployees_History]
	  group by   [CasinoID],  [EmpID]
	  having count(*) > 1 -- not repeated

--- insert gaps 
with gaps as ( 
select top 9   [CMPEmployeeHistoryId], [EmpID], [EmpNum], [CardNum], [Title], [FirstName], [LastName], [Position], [EmpDefaultLocnID], [isHost]
                ,[CasinoID], [isInactive] 
                ,[isCurrent]
				,DATEADD( day, -40, t.[InsertDtm]) as [InsertDtm]      ,  DATEADD( day, -10, t.[InsertDtm])  as [LastUpdatedDtm] 
                 from [tqcic].[HRC_Dim_CMPEmployees_History] as t
                  order by  t.[EmpID]  ) 

insert into  [tqcic].[HRC_Dim_CMPEmployees_History]
select * from  gaps;

with overlaps as ( 
select top 9   [CMPEmployeeHistoryId], [EmpID], [EmpNum], [CardNum], [Title], [FirstName], [LastName], [Position], [EmpDefaultLocnID], [isHost]
                ,[CasinoID], [isInactive] 
                ,[isCurrent]
				  , DATEADD( day, 30, t.[InsertDtm]) as [InsertDtm]      ,  [LastUpdatedDtm] 
                 from  [tqcic].[HRC_Dim_CMPEmployees_History]  as t
                  order by  t.[EmpID]   ) 

insert into  [tqcic].[HRC_Dim_CMPEmployees_History]
select * from  overlaps;

select * from [tqcic].[HRC_Dim_CMPEmployees_History]   where [EmpID] in (
	   select  [EmpID]
	  from  [tqcic].[HRC_Dim_CMPEmployees_History] 
	  group by [CasinoID],  [EmpID]
	  having count(*) > 1 ) -- 18 repeated
---------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------
-- PASO 2
drop table [tqcids].[Dim_Player_SCD] ;
select *
into [tqcids].[Dim_Player_SCD]
from [qcids].[Dim_Player_SCD]; --6,234,999
CREATE CLUSTERED COLUMNSTORE INDEX [cci_tqcids_Dim_Player_SCD] ON [tqcids].[Dim_Player_SCD] WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [PRIMARY]

select   [SystemName] ,[SystemNameInt],[PlayerID], [PlayerIDBigInt] , count(*) 
	  from  [tqcids].[Dim_Player_SCD]
	  group by [SystemName] ,[SystemNameInt],[PlayerID] ,[PlayerIDBigInt]
	  having count(*) > 1 -- not repeated

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
--------------------
select * from [tqcids].[Dim_Player_SCD]  where [PlayerIDBigInt] in (
	   select  [PlayerIDBigInt]
	  from  [tqcids].[Dim_Player_SCD]
	  group by   [SystemName] ,[SystemNameInt],[PlayerID], [PlayerIDBigInt]
	  having count(*) > 1 ) -- 6000000 repeated
///////////////////
drop table   [tqcic].[HRC_Dim_MarketablePlayersAttribute] ;
select *
into [tqcic].[HRC_Dim_MarketablePlayersAttribute] 
from  [qcic].[HRC_Dim_MarketablePlayersAttribute] ; -- 151,589

select   PlayerID,   count(*) 
	  from   [tqcic].[HRC_Dim_MarketablePlayersAttribute] 
	  group by  PlayerID
	  having count(*) > 1 -- not repeated

--- insert gaps 
with gaps as ( 
select top 70000   [PlayerHistoryId], [PlayerId], [Acct], [Title], [LastName], [FirstName], [DisplayName], [NickNameAlias]
      ,[Gender], [BirthDt] , [HomeAddr01] ,[HomeAddr02], [HomeCity], [HomeStateCode] , [HomeCountryCode] , [HomePostalCode]
      ,[HomeTel01Type], [HomeTel01], [HomeTel02Type], [HomeTel02], [HomeEmail], [HomeAddrSinceDtm], [LastHomeAddrChangeDtm] , [AltName]
      ,[AltAddr01], [AltAddr02], [AltCity], [AltStateCode], [AltCountryCode] , [AltPostalCode] ,[AltTel01Type], [AltTel01]
      ,[AltTel02Type], [AltTel02], [AltEmail], [LastAltAddrChangeDtm], [isMailToAlt] , [IsMailPlayer], [IsCallPlayer], [IsTextPlayer]
      ,[IsEmailPlayer], [IsTemplate], [IsCashOnly], [IsCreditAcct], [isBanned], [isVIP], [isPrivacy],[isReturnMail]
      ,[IsCreditPlayer], [IsNewPlayer] , [IsAutoDepositJackPot], [isInactive] ,[isLostAndFound], [PlayerType], [SetupDtm], [SetupCasinoId]
      ,[SetupEmpId], [MailingAddr01], [MailingAddr02] , [MailingCity] ,[MailingStateCode] , [MailingCountryCode] , [MailingPostalCode], [MailingCounty]
      ,[MailingLatitude], [MailingLongitude], [DistanceToProperty], [P_Age] ,[P_Qualify], [P_MailTo], [P_ClubStatus]
	  	,DATEADD( day, -40, t.[StartGamingDt]) as [StartGamingDt]      ,  DATEADD( day, -10, t.[StartGamingDt])  as [EndGamingDt] 
      ,[isCurrent] ,[InsertDtm],[LastUpdateDtm]
                 from  [tqcic].[HRC_Dim_MarketablePlayersAttribute]  as t
                  order by  t.[PlayerId]  ) 

insert into   [tqcic].[HRC_Dim_MarketablePlayersAttribute] 
select * from  gaps;

with overlaps as ( 
select   top 70000   [PlayerHistoryId], [PlayerId], [Acct], [Title], [LastName], [FirstName], [DisplayName], [NickNameAlias]
      ,[Gender], [BirthDt] , [HomeAddr01] ,[HomeAddr02], [HomeCity], [HomeStateCode] , [HomeCountryCode] , [HomePostalCode]
      ,[HomeTel01Type], [HomeTel01], [HomeTel02Type], [HomeTel02], [HomeEmail], [HomeAddrSinceDtm], [LastHomeAddrChangeDtm] , [AltName]
      ,[AltAddr01], [AltAddr02], [AltCity], [AltStateCode], [AltCountryCode] , [AltPostalCode] ,[AltTel01Type], [AltTel01]
      ,[AltTel02Type], [AltTel02], [AltEmail], [LastAltAddrChangeDtm], [isMailToAlt] , [IsMailPlayer], [IsCallPlayer], [IsTextPlayer]
      ,[IsEmailPlayer], [IsTemplate], [IsCashOnly], [IsCreditAcct], [isBanned], [isVIP], [isPrivacy],[isReturnMail]
      ,[IsCreditPlayer], [IsNewPlayer] , [IsAutoDepositJackPot], [isInactive] ,[isLostAndFound], [PlayerType], [SetupDtm], [SetupCasinoId]
      ,[SetupEmpId], [MailingAddr01], [MailingAddr02] , [MailingCity] ,[MailingStateCode] , [MailingCountryCode] , [MailingPostalCode], [MailingCounty]
      ,[MailingLatitude], [MailingLongitude], [DistanceToProperty], [P_Age] ,[P_Qualify], [P_MailTo], [P_ClubStatus]
	   , DATEADD( day, 30, t.[StartGamingDt]) as [StartGamingDt]      ,  [EndGamingDt] 
	     ,[isCurrent] ,[InsertDtm],[LastUpdateDtm]
                 from   [tqcic].[HRC_Dim_MarketablePlayersAttribute]  as t
                  order by  t.[PlayerId] desc ) 

insert into  [tqcic].[HRC_Dim_MarketablePlayersAttribute] 
select * from  overlaps;

select * from  [tqcic].[HRC_Dim_MarketablePlayersAttribute]    where [PlayerId] in (
	   select  [PlayerId]
	  from  [tqcic].[HRC_Dim_MarketablePlayersAttribute] 
	  group by PlayerID
	  having count(*) > 1 ) -- 140000 repeated
-----------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------
--Paso 3 

drop table  [tqcids].[Dim_PlayerDistanceToCasino_SCD] ;
select *
into [tqcids].[Dim_PlayerDistanceToCasino_SCD]
from [qcids].[Dim_PlayerDistanceToCasino_SCD]; -- 14,607,847
CREATE CLUSTERED COLUMNSTORE INDEX [cci_tqcids_Dim_PlayerDistanceToCasino_SCD] ON [tqcids].[Dim_PlayerDistanceToCasino_SCD] WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [PRIMARY]



select   SystemNameInt,   CasinoIDInt, PlayerIDBigInt ,  count(*) 
	  from  [tqcids].[Dim_PlayerDistanceToCasino_SCD]
	  group by SystemNameInt,   CasinoIDInt, PlayerIDBigInt 
	  having count(*) > 1 -- not repeated

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
--------------------
select * from [tqcids].[Dim_PlayerDistanceToCasino_SCD]  where [PlayerIDBigInt] in (
	   select  [PlayerIDBigInt]
	  from  [tqcids].[Dim_PlayerDistanceToCasino_SCD]
	  group by  SystemNameInt,   CasinoIDInt, PlayerIDBigInt
	  having count(*) > 1 ) -- 14000000 repeated
///////////////////
-- no hizo falta se utiliza la misma tabla que en el usp anterior
---------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------
--Paso 4 
drop table [tqcids].[Dim_PlayerLoyalty_SCD] ;
select *
into [tqcids].[Dim_PlayerLoyalty_SCD] 
from [qcids].[Dim_PlayerLoyalty_SCD] ; -- 6,220,193
/****** Object:  Index [cci_qcids_Dim_PlayerLoyalty_SCD]    Script Date: 10/22/2020 8:28:21 AM ******/
CREATE CLUSTERED COLUMNSTORE INDEX [cci_tqcids_Dim_PlayerLoyalty_SCD] ON [tqcids].[Dim_PlayerLoyalty_SCD] WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [PRIMARY]

select  [SystemNameInt]   ,[PlayerID] ,[PlayerIDBigInt] , count(*) 
	  from  [tqcids].[Dim_PlayerLoyalty_SCD] 
	  group by [SystemNameInt]   ,[PlayerID] ,[PlayerIDBigInt]
	  having count(*) > 1 -- not repeated

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
                  order by  t.[PlayerIDBigInt]   ) 

insert into [tqcids].[Dim_PlayerLoyalty_SCD] 
select * from  overlaps;
--------------------
select * from [tqcids].[Dim_PlayerLoyalty_SCD]   where [PlayerIDBigInt] in (
	   select  [PlayerIDBigInt]
	  from  [tqcids].[Dim_PlayerLoyalty_SCD] 
	  group by   [SystemNameInt]   ,[PlayerID] ,[PlayerIDBigInt]
	  having count(*) > 1 ) -- 6000000 repeated
///////////////////
-- no hizo falta se utiliza la misma tabla que en el usp anterior
--------------------------------------------------------------------------------
-----------------------------------------------------
----------------------Paso 5 
 drop table  [tqcids].[Dim_PlayerHost_SCD] ;
select *
into  [tqcids].[Dim_PlayerHost_SCD]
from  [qcids].[Dim_PlayerHost_SCD]; -- 36,019

select * 
from [tqcids].[Dim_PlayerHost_SCD]
WHERE [PlayerIDBigInt] =130619
order by SystemNameInt, CasinoIDInt, PlayerIDBigInt , HostIDBigInt


select   SystemNameInt, CasinoIDInt, PlayerIDBigInt , HostIDBigInt,  count(*) 
	  from  [tqcids].[Dim_PlayerHost_SCD]
	  group by  SystemNameInt, CasinoIDInt, PlayerIDBigInt , HostIDBigInt
	  having count(*) > 1 -- not repeated

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

select * from [tqcids].[Dim_PlayerHost_SCD] where PlayerIDBigInt in (
	   select  PlayerIDBigInt
	  from [tqcids].[Dim_PlayerHost_SCD]
	  group by SystemNameInt, CasinoIDInt, PlayerIDBigInt , HostIDBigInt
	  having count(*) > 1 ) -- 30000 repeated
///////////////////
drop table  [tqcic].[HRC_Fact_PlayerHostHistory_Primary] ;
select *
into  [tqcic].[HRC_Fact_PlayerHostHistory_Primary] 
from  [qcic].[HRC_Fact_PlayerHostHistory_Primary] ; --3898

select [PrimaryHostID] ,[PlayerID] ,[CasinoID],[HostEmpID],  count(*) 
	  from   [tqcic].[HRC_Fact_PlayerHostHistory_Primary] 
	  group by [PrimaryHostID] ,[PlayerID] ,[CasinoID],[HostEmpID]
	  having count(*) > 1 -- not repeated

--- insert gaps 
with gaps as ( 
select top 1500 [PrimaryHostID] ,[PlayerID], [CasinoID], [HostEmpID], [ModifiedBy]
	           ,DATEADD( day, -40, t.[HostStartDTM]) as [HostStartDTM]      ,  DATEADD( day, -10, t.[HostStartDTM])  as [HostEndDTM] ,
                [isCurrent],[CreatedDtm], [CreatedBy], [ModifiedDtm]  ,[InsertDtm]
	             from  [tqcic].[HRC_Fact_PlayerHostHistory_Primary]  as t
                  order by  t.[PlayerID]  ) 

insert into   [tqcic].[HRC_Fact_PlayerHostHistory_Primary] 
select * from  gaps;

with overlaps as ( 
select top 1500 [PrimaryHostID] ,[PlayerID], [CasinoID], [HostEmpID], [ModifiedBy]
				  , DATEADD( day, 30, t.[HostStartDTM]) as [HostStartDTM]      ,  [HostEndDTM] , 
                   [isCurrent],[CreatedDtm], [CreatedBy], [ModifiedDtm]  ,[InsertDtm]
	             from  [tqcic].[HRC_Fact_PlayerHostHistory_Primary]  as t
                  order by  t.[PlayerID]   ) 

insert into  [tqcic].[HRC_Fact_PlayerHostHistory_Primary]
select * from  overlaps;

select * from  [tqcic].[HRC_Fact_PlayerHostHistory_Primary]   where [PlayerID] in (
	   select  [PlayerID]
	  from   [tqcic].[HRC_Fact_PlayerHostHistory_Primary] 
	  group by [PrimaryHostID] ,[PlayerID] ,[CasinoID],[HostEmpID]
	  having count(*) > 1 ) -- 3000 repeated
--- second part 
drop table  [tqcic].[HRC_Fact_PlayerHostHistory_Secondary] ;
select *
into  [tqcic].[HRC_Fact_PlayerHostHistory_Secondary] 
from  [qcic].[HRC_Fact_PlayerHostHistory_Secondary] ; --939

select [SecondaryHostID] ,[PlayerID] ,[CasinoID],[HostEmpID],  count(*) 
	  from   [tqcic].[HRC_Fact_PlayerHostHistory_Secondary] 
	  group by [SecondaryHostID] ,[PlayerID] ,[CasinoID],[HostEmpID]
	  having count(*) > 1 -- not repeated

--- insert gaps 
with gaps as ( 
select top 400 [SecondaryHostID] ,[PlayerID], [CasinoID], [HostEmpID], [ModifiedBy]
	           ,DATEADD( day, -40, t.[HostStartDTM]) as [HostStartDTM]      ,  DATEADD( day, -10, t.[HostStartDTM])  as [HostEndDTM] ,
                [isCurrent],[CreatedDtm], [CreatedBy], [ModifiedDtm]  ,[InsertDtm]
	             from [tqcic].[HRC_Fact_PlayerHostHistory_Secondary]   as t
                  order by  t.[PlayerID]  ) 

insert into  [tqcic].[HRC_Fact_PlayerHostHistory_Secondary] 
select * from  gaps;

with overlaps as ( 
select top 400 [SecondaryHostID] ,[PlayerID], [CasinoID], [HostEmpID], [ModifiedBy]
				  , DATEADD( day, 30, t.[HostStartDTM]) as [HostStartDTM]      ,  [HostEndDTM] , 
                   [isCurrent],[CreatedDtm], [CreatedBy], [ModifiedDtm]  ,[InsertDtm]
	             from [tqcic].[HRC_Fact_PlayerHostHistory_Secondary]  as t
                  order by  t.[PlayerID]   ) 

insert into [tqcic].[HRC_Fact_PlayerHostHistory_Secondary] 
select * from  overlaps;

select * from [tqcic].[HRC_Fact_PlayerHostHistory_Secondary]   where [PlayerID] in (
	   select  [PlayerID]
	  from   [tqcic].[HRC_Fact_PlayerHostHistory_Secondary] 
	  group by [SecondaryHostID] ,[PlayerID] ,[CasinoID],[HostEmpID]
	  having count(*) > 1 ) -- 800 repeated

--------------------------------------------------------------------------------
-----------------------------------------------------
----------------------Paso 6
 drop table [tqcids].[Dim_PlayerCredit_SCD] ;
select *
into  [tqcids].[Dim_PlayerCredit_SCD]
from  [qcids].[Dim_PlayerCredit_SCD]; -- 144,168


select    SystemNameInt , CasinoIDInt, PlayerIDBigInt , count(*) 
	  from  [tqcids].[Dim_PlayerCredit_SCD]
	  group by SystemNameInt , CasinoIDInt, PlayerIDBigInt
	  having count(*) > 1 -- not repeated

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

select * from [tqcids].[Dim_PlayerCredit_SCD] where PlayerIDBigInt in (
	   select  PlayerIDBigInt
	  from [tqcids].[Dim_PlayerCredit_SCD]
	  group by SystemNameInt , CasinoIDInt, PlayerIDBigInt
	  having count(*) > 1 ) --120,000 repeated
///////////////////
drop table  [tqcic].[HRC_Dim_PlayerCreditLimit_History];
select *
into  [tqcic].[HRC_Dim_PlayerCreditLimit_History]
from  [qcic].[HRC_Dim_PlayerCreditLimit_History] ; --22,944

select  [PlayerID] ,[TranCasinoID] ,[SetupCasinoID],  count(*) 
	  from   [tqcic].[HRC_Dim_PlayerCreditLimit_History]
	  group by [PlayerID] ,[TranCasinoID] ,[SetupCasinoID]
	  having count(*) > 1 -- not repeated

--- insert gaps 
with gaps as ( 
select top 10000  [PlayerID] ,[GroupCode] ,[CreditLimit] ,[TranCasinoID] ,[SetupCasinoID] ,[SetupDt] 
	           ,DATEADD( day, -40, t.[StartGamingDt]) as [StartGamingDt]      ,  DATEADD( day, -10, t.[StartGamingDt])  as [EndGamingDt] 
                 ,[IsCreditPlayer] ,[IsCurrent],[InsertDtm]
	             from  [tqcic].[HRC_Dim_PlayerCreditLimit_History]  as t
                  order by  t.[PlayerID]  ) 

insert into [tqcic].[HRC_Dim_PlayerCreditLimit_History]
select * from  gaps;

with overlaps as ( 
select top 10000  [PlayerID] ,[GroupCode] ,[CreditLimit] ,[TranCasinoID] ,[SetupCasinoID] ,[SetupDt] 
				  , DATEADD( day, 30, t.[StartGamingDt]) as [StartGamingDt]      ,  [EndGamingDt] 
                    ,[IsCreditPlayer] ,[IsCurrent],[InsertDtm]
	             from [tqcic].[HRC_Dim_PlayerCreditLimit_History] as t
                  order by  t.[PlayerID]   ) 

insert into  [tqcic].[HRC_Dim_PlayerCreditLimit_History]
select * from  overlaps;

select * from [tqcic].[HRC_Dim_PlayerCreditLimit_History]  where [PlayerID] in (
	   select  [PlayerID]
	  from  [tqcic].[HRC_Dim_PlayerCreditLimit_History]
	  group by [PlayerID] ,[TranCasinoID] ,[SetupCasinoID]
	  having count(*) > 1 ) -- 20,000 repeated
--------------------------------------------------------------------------------
-----------------------------------------------------
----------------------Paso 7
 drop table  [tqcids].[Dim_PlayerCreditElectronic_SCD] ;
select *
into   [tqcids].[Dim_PlayerCreditElectronic_SCD]
from  [qcids].[Dim_PlayerCreditElectronic_SCD]; -- 126,148


select  SystemNameInt ,CasinoIDInt , PlayerIDBigInt   , [ElectronicCreditType], count(*) 
	  from   [tqcids].[Dim_PlayerCreditElectronic_SCD]
	  group by SystemNameInt , CasinoIDInt, PlayerIDBigInt, [ElectronicCreditType]
	  having count(*) > 1 -- not repeated

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

select * from  [tqcids].[Dim_PlayerCreditElectronic_SCD] where PlayerIDBigInt in (
	   select  PlayerIDBigInt
	  from   [tqcids].[Dim_PlayerCreditElectronic_SCD]
	  group by SystemNameInt , CasinoIDInt, PlayerIDBigInt, [ElectronicCreditType]
	  having count(*) > 1 ) --100,000 repeated
///////////////////
drop table   [tqcic].[HRC_Dim_PlayerECreditEnrollment_History] ;
select *
into  [tqcic].[HRC_Dim_PlayerECreditEnrollment_History] 
from  [qcic].[HRC_Dim_PlayerECreditEnrollment_History]  ; --18,411

select  CasinoID, PlayerID , count(*) 
	  from  [tqcic].[HRC_Dim_PlayerECreditEnrollment_History] 
	  group by  CasinoID, PlayerID
	  having count(*) > 1 -- not repeated

--- insert gaps 
with gaps as ( 
select top 8000  [EnrollmentId] ,[Acct],[PlayerId] ,[IsEnroll] ,[EnrollOption] ,[CasinoId]
      ,[GamingDt] ,[EMAEmpID] ,[ReasonDesc] ,[Remarks] ,[SignatureId],[CreatedBy] ,[ModifiedBy]
	           ,DATEADD( day, -40, t.[CreatedDtm]) as [CreatedDtm]  
      ,[ModifiedDtm],[IsCurrent] ,[InsertDtm] 
	             from   [tqcic].[HRC_Dim_PlayerECreditEnrollment_History]  as t
                  order by  t.[PlayerID]  ) 

insert into  [tqcic].[HRC_Dim_PlayerECreditEnrollment_History] 
select * from  gaps;

with overlaps as ( 
select top 8000  [EnrollmentId] ,[Acct],[PlayerId] ,[IsEnroll] ,[EnrollOption] ,[CasinoId]
      ,[GamingDt] ,[EMAEmpID] ,[ReasonDesc] ,[Remarks] ,[SignatureId],[CreatedBy] ,[ModifiedBy]
				  , DATEADD( day, 30, t.[CreatedDtm]) as [CreatedDtm]    
                   ,[ModifiedDtm],[IsCurrent] ,[InsertDtm] 
	             from  [tqcic].[HRC_Dim_PlayerECreditEnrollment_History]  as t
                  order by  t.[PlayerID]   ) 

insert into  [tqcic].[HRC_Dim_PlayerECreditEnrollment_History] 
select * from  overlaps;

select * from [tqcic].[HRC_Dim_PlayerECreditEnrollment_History]  where [PlayerID] in (
	   select  [PlayerID]
	  from  [tqcic].[HRC_Dim_PlayerECreditEnrollment_History] 
	  group by CasinoID, PlayerID
	  having count(*) > 1 ) -- 16,000 repeated