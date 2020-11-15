drop table  [tqcids].[Dim_Host_SCD] ;
select *
into [tqcids].[Dim_Host_SCD]
from [qcids].[Dim_Host_SCD]; -- 742

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

select count(*) from  [tqcids].[Dim_Host_SCD];

with overlaps as ( 
select top 350  [SystemName], [SystemNameInt], [CasinoGroupID], [CasinoGroupIDInt] , [HostID], [HostIDBigInt], [HostFirstName], [HostLastName] , [HostPosition], [HostCode]
				  , DATEADD( day, 30, t.[StartDateTime]) as [StartDateTime]      ,  [EndDateTime] 
		    ,[IsCurrent], [InsertDateTime], 'overlaps' AS [bandera]
                 from  [tqcids].[Dim_Host_SCD] as t
                  order by  t.[HostIDBigInt]   ) 

insert into [tqcids].[Dim_Host_SCD]
select * from  overlaps;

select * 
from  [tqcids].[Dim_Host_SCD]
where SystemName  = 'HRC'
AND HostIDBigInt=-1945838999
order by SystemNameInt, HostIDBigInt
--------------------
--DEMO MERGE 2 

----------------------------------------------------
-- demo merge 5
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
select top 30000  [SystemName] , [SystemNameInt] , [CasinoID], [CasinoIDInt], [PlayerID] , [PlayerIDBigInt], [HostID], [HostIDBigInt], [PlayerHostType]  
				  , DATEADD( day, 30, t.[StartDateTime]) as [StartDateTime]      ,  [EndDateTime] 
		    ,[IsCurrent], [InsertDateTime], 'overlaps' AS [bandera]
                 from [tqcids].[Dim_PlayerHost_SCD] as t
                  order by  t.[PlayerIDBigInt]   ) 

insert into [tqcids].[Dim_PlayerHost_SCD]
select * from  overlaps;
/////////////////////

drop table  [tqcic].[HRC_Fact_PlayerHostHistory_Primary] ;
select *
into  [tqcic].[HRC_Fact_PlayerHostHistory_Primary] 
from  [qcic].[HRC_Fact_PlayerHostHistory_Primary] ; --3898

--- insert gaps 
with gaps as ( 
select [PrimaryHostID] ,[PlayerID], [CasinoID], [HostEmpID], [ModifiedBy]
	           ,DATEADD( day, -40, t.[HostStartDTM]) as [HostStartDTM]      ,  DATEADD( day, -10, t.[HostStartDTM])  as [HostEndDTM] ,
                [isCurrent],[CreatedDtm], [CreatedBy], [ModifiedDtm]  ,[InsertDtm]
	             from  [tqcic].[HRC_Fact_PlayerHostHistory_Primary]  as t
                   ) 

insert into   [tqcic].[HRC_Fact_PlayerHostHistory_Primary] 
select * from  gaps;

with overlaps as ( 
select [PrimaryHostID] ,[PlayerID], [CasinoID], [HostEmpID], [ModifiedBy]
				  , DATEADD( day, 30, t.[HostStartDTM]) as [HostStartDTM]      ,  [HostEndDTM] , 
                   [isCurrent],[CreatedDtm], [CreatedBy], [ModifiedDtm]  ,[InsertDtm]
	             from  [tqcic].[HRC_Fact_PlayerHostHistory_Primary]  as t
                   ) 

insert into  [tqcic].[HRC_Fact_PlayerHostHistory_Primary]
select * from  overlaps;


select * from [tqcic].[HRC_Fact_PlayerHostHistory_Primary] 
where  PlayerID in (138, 5109) 

--- second part 
drop table  [tqcic].[HRC_Fact_PlayerHostHistory_Secondary] ;
select *
into  [tqcic].[HRC_Fact_PlayerHostHistory_Secondary] 
from  [qcic].[HRC_Fact_PlayerHostHistory_Secondary] ; --939

--- insert gaps 
with gaps as ( 
select   [SecondaryHostID] ,[PlayerID], [CasinoID], [HostEmpID], [ModifiedBy]
	           ,DATEADD( day, -40, t.[HostStartDTM]) as [HostStartDTM]      ,  DATEADD( day, -10, t.[HostStartDTM])  as [HostEndDTM] ,
                [isCurrent],[CreatedDtm], [CreatedBy], [ModifiedDtm]  ,[InsertDtm]
	             from [tqcic].[HRC_Fact_PlayerHostHistory_Secondary]   as t
                    ) 

insert into  [tqcic].[HRC_Fact_PlayerHostHistory_Secondary] 
select * from  gaps;

with overlaps as ( 
select   [SecondaryHostID] ,[PlayerID], [CasinoID], [HostEmpID], [ModifiedBy]
				  , DATEADD( day, 30, t.[HostStartDTM]) as [HostStartDTM]      ,  [HostEndDTM] , 
                   [isCurrent],[CreatedDtm], [CreatedBy], [ModifiedDtm]  ,[InsertDtm]
	             from [tqcic].[HRC_Fact_PlayerHostHistory_Secondary]  as t
                     ) 

insert into [tqcic].[HRC_Fact_PlayerHostHistory_Secondary] 
select * from  overlaps;


select * from [tqcic].[HRC_Fact_PlayerHostHistory_Secondary] 
where  PlayerID in (138, 5109) 

select * 
from [tqcids].[Dim_PlayerHost_SCD]
where SystemName  = 'HRC'
AND PlayerIDBigInt in (138, 5109) 
order by SystemNameInt, CasinoIDInt, PlayerIDBigInt , HostIDBigInt, StartDateTime

------------------------------------------------------
delete from [tqcids].[Dim_PlayerHost_SCD]
where SystemName  = 'HRC'
AND PlayerIDBigInt=138 and StartDateTime <='2016-12-22' and bandera ='overlaps'



