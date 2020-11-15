-----------------------Paso 1 
/*drop table  [tqcids].[Dim_Host_SCD] ;
select *
into [tqcids].[Dim_Host_SCD]
from [qcids].[Dim_Host_SCD];


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
                  order by  t.[HostIDBigInt]  desc ) 

insert into [tqcids].[Dim_Host_SCD]
select * from  overlaps;
--------------------
select * from [tqcids].[Dim_Host_SCD]  where HostIDBigInt in (
	   select  HostIDBigInt
	  from  [tqcids].[Dim_Host_SCD] 
	  group by  [SystemName], [CasinoGroupIDInt], [HostIDBigInt]
	  having count(*) > 1 ) -- 700 repeated
///////////////////
*/
drop table  [tqcic].[SGA_Dim_CMPEmployees];
select *
into  [tqcic].[SGA_Dim_CMPEmployees]
from  [qcic].[SGA_Dim_CMPEmployees]; --80

select   [CasinoID],  [EmpID],   count(*) 
	  from [tqcic].[SGA_Dim_CMPEmployees]
	  group by   [CasinoID],  [EmpID]
	  having count(*) > 1 -- not repeated

--- insert gaps 
with gaps as ( 
select top 35    [EmpID], [EmpNum], [CardNum], [Title], [FirstName], [LastName], [Position], [EmpDefaultLocnID]
      ,[isHost], [CasinoID], [isInactive], [isCurrent]  	  
				,DATEADD( day, -40, t.[InsertDT]) as [InsertDT]      ,  DATEADD( day, -10, t.[InsertDT])  as [RetireDT] 
                 from [tqcic].[SGA_Dim_CMPEmployees] as t
                  order by  t.[EmpID]  ) 

insert into [tqcic].[SGA_Dim_CMPEmployees]
select * from  gaps;

with overlaps as ( 
select top 35   [EmpID], [EmpNum], [CardNum], [Title], [FirstName], [LastName], [Position], [EmpDefaultLocnID]
      ,[isHost], [CasinoID], [isInactive], [isCurrent]  
				  , DATEADD( day, 30, t.[InsertDT]) as [InsertDT]      ,  [RetireDT] 
                 from  [tqcic].[SGA_Dim_CMPEmployees] as t
                  order by  t.[EmpID]   ) 

insert into  [tqcic].[SGA_Dim_CMPEmployees]
select * from  overlaps;

select * from [tqcic].[SGA_Dim_CMPEmployees]  where [EmpID] in (
	   select  [EmpID]
	  from [tqcic].[SGA_Dim_CMPEmployees]
	  group by [CasinoID],  [EmpID]
	  having count(*) > 1 ) -- 70 repeated
---------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------
-- PASO 2
drop table [tqcids].[Dim_Player_SCD] ;
select *
into [tqcids].[Dim_Player_SCD]
from [qcids].[Dim_Player_SCD];
CREATE CLUSTERED COLUMNSTORE INDEX [cci_tqcids_Dim_Player_SCD] ON [tqcids].[Dim_Player_SCD] WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [PRIMARY]

select count (*) from [tqcids].[Dim_Player_SCD];


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
                  order by  t.[PlayerIDBigInt]  desc ) 

insert into [tqcids].[Dim_Player_SCD]
select * from  overlaps;
--------------------
select * from [tqcids].[Dim_Player_SCD]  where [PlayerIDBigInt] in (
	   select  [PlayerIDBigInt]
	  from  [tqcids].[Dim_Player_SCD]
	  group by   [SystemName] ,[SystemNameInt],[PlayerID], [PlayerIDBigInt]
	  having count(*) > 1 ) -- 6000000 repeated
///////////////////
drop table  [tqcic].[SGA_uvw_MarketablePlayersAttributes] ;
select *
into [tqcic].[SGA_uvw_MarketablePlayersAttributes] 
from  [qcic].[SGA_uvw_MarketablePlayersAttributes] ; -- 454,916

select   PlayerID,   count(*) 
	  from   [tqcic].[SGA_uvw_MarketablePlayersAttributes]
	  group by  PlayerID
	  having count(*) > 1 -- not repeated

--- insert gaps 
with gaps as ( 
select top 200000   
	  [PlayerHistoryID] ,[PlayerID]  ,[acct] ,[title]
      ,[lastname],[firstname]  ,[displayname],[NickNameAlias]
      ,[HomeAddr1] ,[HomeAddr2]   ,[HomeCity], [HomeStateCode]
      ,[HomeCountryCode] ,[HomePostalCode]  ,[HomeTel1Type],[HomeTel1]
      ,[HomeTel2Type],[HomeTel2]   ,[HomeEmail], [HomeAddrSinceDt]
      ,[LastHomeAddrChangeDt]   ,[AltName]  ,[AltAddr1] ,[AltAddr2]
      ,[AltCity] ,[AltStateCode]  ,[AltCountryCode]  ,[AltPostalCode]
      ,[AltTel1Type]  ,[AltTel1]   ,[AltTel2Type] ,[AltTel2]
      ,[AltEmail] ,[LastAltAddrChangeDt]  ,[BirthDT] ,[isPrivacy]
      ,[isMailToAlt]  ,[isNoMail] ,[isReturnMail]  ,[isVIP]
      ,[isLostAndFound] ,[isCreditAcct] ,[isBanned],[isInactive]
      ,[TypeID] ,[isCall]  ,[isEmailSend]   
	  	,DATEADD( day, -40, t.[InsertDT]) as [InsertDT]      ,  DATEADD( day, -10, t.[InsertDT])  as [RetireDT] 
       ,[isCurrent]  ,[isTemplate]   ,[SetupDTM]
      ,[SetupCasinoID] ,[SetupEmpID]  ,[Sex] ,[MailingAddr1]
      ,[MailingAddr2]  ,[MailingCity] ,[MailingStateCode]   ,[MailingCountryCode]
      ,[MailingPostalCode]  ,[P_Age]  ,[P_Qualify]  ,[P_MailTo]
      ,[P_ClubStatus]  ,[Addr_MatchType]  ,[Addr_LAT]  ,[Addr_LONG]
      ,[P_DistanceTo_BSG]   ,[P_DistanceTo_CSG]  ,[P_DistanceTo_HSG]  ,[P_DistanceTo_ISG]
      ,[P_DistanceTo_SHC]  ,[P_DistanceTo_TSG]
      ,[P_County]
	 
	   from  [tqcic].[SGA_uvw_MarketablePlayersAttributes] as t
                  order by  t.[PlayerId]  ) 

insert into    [tqcic].[SGA_uvw_MarketablePlayersAttributes]
select * from  gaps;

with overlaps as ( 
select 
top 200000   
	  [PlayerHistoryID] ,[PlayerID]  ,[acct] ,[title]
      ,[lastname],[firstname]  ,[displayname],[NickNameAlias]
      ,[HomeAddr1] ,[HomeAddr2]   ,[HomeCity], [HomeStateCode]
      ,[HomeCountryCode] ,[HomePostalCode]  ,[HomeTel1Type],[HomeTel1]
      ,[HomeTel2Type],[HomeTel2]   ,[HomeEmail], [HomeAddrSinceDt]
      ,[LastHomeAddrChangeDt]   ,[AltName]  ,[AltAddr1] ,[AltAddr2]
      ,[AltCity] ,[AltStateCode]  ,[AltCountryCode]  ,[AltPostalCode]
      ,[AltTel1Type]  ,[AltTel1]   ,[AltTel2Type] ,[AltTel2]
      ,[AltEmail] ,[LastAltAddrChangeDt]  ,[BirthDT] ,[isPrivacy]
      ,[isMailToAlt]  ,[isNoMail] ,[isReturnMail]  ,[isVIP]
      ,[isLostAndFound] ,[isCreditAcct] ,[isBanned],[isInactive]
      ,[TypeID] ,[isCall]  ,[isEmailSend]   
	  	,DATEADD( day, -40, t.[InsertDT]) as [InsertDT]      ,  [RetireDT] 
       ,[isCurrent]  ,[isTemplate]   ,[SetupDTM]
      ,[SetupCasinoID] ,[SetupEmpID]  ,[Sex] ,[MailingAddr1]
      ,[MailingAddr2]  ,[MailingCity] ,[MailingStateCode]   ,[MailingCountryCode]
      ,[MailingPostalCode]  ,[P_Age]  ,[P_Qualify]  ,[P_MailTo]
      ,[P_ClubStatus]  ,[Addr_MatchType]  ,[Addr_LAT]  ,[Addr_LONG]
      ,[P_DistanceTo_BSG]   ,[P_DistanceTo_CSG]  ,[P_DistanceTo_HSG]  ,[P_DistanceTo_ISG]
      ,[P_DistanceTo_SHC]  ,[P_DistanceTo_TSG]
      ,[P_County]
                 from     [tqcic].[SGA_uvw_MarketablePlayersAttributes]  as t
                  order by  t.[PlayerId] desc ) 

insert into    [tqcic].[SGA_uvw_MarketablePlayersAttributes]
select * from  overlaps;

select * from   [tqcic].[SGA_uvw_MarketablePlayersAttributes]   where [PlayerId] in (
	   select  [PlayerId]
	  from   [tqcic].[SGA_uvw_MarketablePlayersAttributes]
	  group by PlayerID
	  having count(*) > 1 ) -- 400,000 repeated
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
                  order by  t.[PlayerIDBigInt]  desc ) 

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
--Paso 4  aqui VAS CUIDADO CON VALIDAR QUE NO HAYAS BORRADO LAS TABLAS NCESARIAS 
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
                  order by  t.[PlayerIDBigInt]  desc ) 

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
                  order by  t.[PlayerIDBigInt]  desc ) 

insert into [tqcids].[Dim_PlayerHost_SCD]
select * from  overlaps;

select * from [tqcids].[Dim_PlayerHost_SCD] where PlayerIDBigInt in (
	   select  PlayerIDBigInt
	  from [tqcids].[Dim_PlayerHost_SCD]
	  group by SystemNameInt, CasinoIDInt, PlayerIDBigInt , HostIDBigInt
	  having count(*) > 1 ) -- 30000 repeated
///////////////////
drop table  [tqcic].[SGA_Fact_PlayerHostHistory_Primary]  ;
select *
into   [tqcic].[SGA_Fact_PlayerHostHistory_Primary]
from   [qcic].[SGA_Fact_PlayerHostHistory_Primary] ; --11,586

select [PrimaryHostID] ,[PlayerID] ,[CasinoID],[HostEmpID],  count(*) 
	  from    [tqcic].[SGA_Fact_PlayerHostHistory_Primary]
	  group by [PrimaryHostID] ,[PlayerID] ,[CasinoID],[HostEmpID]
	  having count(*) > 1 -- not repeated

--- insert gaps 
with gaps as ( 
select top 1500 [PrimaryHostID] ,[PlayerID], [CasinoID], [HostEmpID], [ModifiedBy]
	           ,DATEADD( day, -40, t.[HostStartDTM]) as [HostStartDTM]      ,  DATEADD( day, -10, t.[HostStartDTM])  as [HostEndDTM] ,
                [isCurrent],[CreatedDtm], [CreatedBy], [ModifiedDtm]  ,[InsertDtm]
	             from  [tqcic].[SGA_Fact_PlayerHostHistory_Primary]  as t
                  order by  t.[PlayerID]  ) 

insert into    [tqcic].[SGA_Fact_PlayerHostHistory_Primary]
select * from  gaps;

with overlaps as ( 
select top 1500 [PrimaryHostID] ,[PlayerID], [CasinoID], [HostEmpID], [ModifiedBy]
				  , DATEADD( day, 30, t.[HostStartDTM]) as [HostStartDTM]      ,  [HostEndDTM] , 
                   [isCurrent],[CreatedDtm], [CreatedBy], [ModifiedDtm]  ,[InsertDtm]
	             from  [tqcic].[SGA_Fact_PlayerHostHistory_Primary]as t
                  order by  t.[PlayerID]  desc ) 

insert into  [tqcic].[SGA_Fact_PlayerHostHistory_Primary]
select * from  overlaps;

select * from  [tqcic].[SGA_Fact_PlayerHostHistory_Primary]   where [PlayerID] in (
	   select  [PlayerID]
	  from   [tqcic].[SGA_Fact_PlayerHostHistory_Primary]
	  group by [PrimaryHostID] ,[PlayerID] ,[CasinoID],[HostEmpID]
	  having count(*) > 1 ) -- 3001 repeated
--- second part 
drop table [tqcic].[SGA_Fact_PlayerHostHistory_Secondary];
select *
into [tqcic].[SGA_Fact_PlayerHostHistory_Secondary] 
from  [qcic].[SGA_Fact_PlayerHostHistory_Secondary]; -- 2,925

select [SecondaryHostID] ,[PlayerID] ,[CasinoID],[HostEmpID],  count(*) 
	  from  [tqcic].[SGA_Fact_PlayerHostHistory_Secondary] 
	  group by [SecondaryHostID] ,[PlayerID] ,[CasinoID],[HostEmpID]
	  having count(*) > 1 -- not repeated

--- insert gaps 
with gaps as ( 
select top 1000 [SecondaryHostID] ,[PlayerID], [CasinoID], [HostEmpID], [ModifiedBy]
	           ,DATEADD( day, -40, t.[HostStartDTM]) as [HostStartDTM]      ,  DATEADD( day, -10, t.[HostStartDTM])  as [HostEndDTM] ,
                [isCurrent],[CreatedDtm], [CreatedBy], [ModifiedDtm]  ,[InsertDtm]
	             from [tqcic].[SGA_Fact_PlayerHostHistory_Secondary]   as t
                  order by  t.[PlayerID]  ) 

insert into  [tqcic].[SGA_Fact_PlayerHostHistory_Secondary] 
select * from  gaps;

with overlaps as ( 
select top 1000 [SecondaryHostID] ,[PlayerID], [CasinoID], [HostEmpID], [ModifiedBy]
				  , DATEADD( day, 30, t.[HostStartDTM]) as [HostStartDTM]      ,  [HostEndDTM] , 
                   [isCurrent],[CreatedDtm], [CreatedBy], [ModifiedDtm]  ,[InsertDtm]
	             from [tqcic].[SGA_Fact_PlayerHostHistory_Secondary]  as t
                  order by  t.[PlayerID]  desc ) 

insert into [tqcic].[SGA_Fact_PlayerHostHistory_Secondary] 
select * from  overlaps;

select * from [tqcic].[SGA_Fact_PlayerHostHistory_Secondary]    where [PlayerID] in (
	   select  [PlayerID]
	  from  [tqcic].[SGA_Fact_PlayerHostHistory_Secondary] 
	  group by [SecondaryHostID] ,[PlayerID] ,[CasinoID],[HostEmpID]
	  having count(*) > 1 ) -- 2,000 repeated

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
                  order by  t.[PlayerIDBigInt]  desc ) 

insert into [tqcids].[Dim_PlayerCredit_SCD]
select * from  overlaps;

select * from [tqcids].[Dim_PlayerCredit_SCD] where PlayerIDBigInt in (
	   select  PlayerIDBigInt
	  from [tqcids].[Dim_PlayerCredit_SCD]
	  group by SystemNameInt , CasinoIDInt, PlayerIDBigInt
	  having count(*) > 1 ) --120,000 repeated
///////////////////
drop table   [tqcic].[SGA_Dim_PlayerCreditLimit_History];
select *
into  [tqcic].[SGA_Dim_PlayerCreditLimit_History]
from   [qcic].[SGA_Dim_PlayerCreditLimit_History] ; --69,370

select  [PlayerID] ,[TranCasinoID] ,[SetupCasinoID],  count(*) 
	  from    [tqcic].[SGA_Dim_PlayerCreditLimit_History]
	  group by [PlayerID] ,[TranCasinoID] ,[SetupCasinoID]
	  having count(*) > 1 -- not repeated

--- insert gaps 
with gaps as ( 
select top 30000  [PlayerID] ,[GroupCode] ,[CreditLimit] ,[TranCasinoID] ,[SetupCasinoID] ,[SetupDt] 
	           ,DATEADD( day, -40, t.[StartGamingDt]) as [StartGamingDt]      ,  DATEADD( day, -10, t.[StartGamingDt])  as [EndGamingDt] 
                 ,[IsCreditPlayer] ,[IsCurrent],[InsertDtm]
	             from   [tqcic].[SGA_Dim_PlayerCreditLimit_History]  as t
                  order by  t.[PlayerID]  ) 

insert into  [tqcic].[SGA_Dim_PlayerCreditLimit_History]
select * from  gaps;

with overlaps as ( 
select top 30000  [PlayerID] ,[GroupCode] ,[CreditLimit] ,[TranCasinoID] ,[SetupCasinoID] ,[SetupDt] 
				  , DATEADD( day, 30, t.[StartGamingDt]) as [StartGamingDt]      ,  [EndGamingDt] 
                    ,[IsCreditPlayer] ,[IsCurrent],[InsertDtm]
	             from  [tqcic].[SGA_Dim_PlayerCreditLimit_History] as t
                  order by  t.[PlayerID]  desc ) 

insert into   [tqcic].[SGA_Dim_PlayerCreditLimit_History]
select * from  overlaps;

select * from  [tqcic].[SGA_Dim_PlayerCreditLimit_History] where [PlayerID] in (
	   select  [PlayerID]
	  from   [tqcic].[SGA_Dim_PlayerCreditLimit_History] 
	  group by [PlayerID] ,[TranCasinoID] ,[SetupCasinoID]
	  having count(*) > 1 ) -- 60,001 repeated
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
                  order by  t.[PlayerIDBigInt]  desc ) 

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
                  order by  t.[PlayerID]  desc ) 

insert into  [tqcic].[HRC_Dim_PlayerECreditEnrollment_History] 
select * from  overlaps;

select * from [tqcic].[HRC_Dim_PlayerECreditEnrollment_History]  where [PlayerID] in (
	   select  [PlayerID]
	  from  [tqcic].[HRC_Dim_PlayerECreditEnrollment_History] 
	  group by CasinoID, PlayerID
	  having count(*) > 1 ) -- 16,000 repeated