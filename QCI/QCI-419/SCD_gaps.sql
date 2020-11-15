

	   select  [CasinoID],  [PlayerID] , count(*) 
	  from   [qcids].[Dim_PlayerHost_SCD]
	  group by [CasinoID],  [PlayerID] 
	  having count(*) > 1 -- not repeated

  select count (*)   FROM [qcids].[Dim_PlayerHost_SCD]


  --------------------- 
  -------------------- no pues que yo haga la muestra 
  drop table [temp].[Dim_PlayerHost_SCD];
  select *
  into  [temp].[Dim_PlayerHost_SCD]
  from  [qcids].[Dim_PlayerHost_SCD];
  CREATE CLUSTERED COLUMNSTORE INDEX [cci_temp_Dim_PlayerHost_SCD] ON [temp].[Dim_PlayerHost_SCD] WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [PRIMARY];
  -------------------------------
---------------------- usp 
	   select  [CasinoID],  [PlayerID] , count(*) 
	  from   [temp].[Dim_PlayerHost_SCD]
	  group by [CasinoID],  [PlayerID] 
	  having count(*) > 1 -- not repeated
-------------------------------------
--- insert gaps 
with gaps as ( 
select top 3000 [SystemName]   ,  [SystemNameInt]  ,[CasinoID]    ,[CasinoIDInt]
      ,[PlayerID]    ,[PlayerIDBigInt]   , '' as [HostID]
      ,0 as [HostIDBigInt]   ,[PlayerHostType]
      --,[StartDateTime]   ,[EndDateTime]
	   ,DATEADD( day, -40, t.[StartDateTime]) as [StartDateTime]      ,  DATEADD( day, -10, t.[StartDateTime])  as[EndDateTime] 
      ,[IsCurrent]  ,[InsertDateTime] 
from [temp].[Dim_PlayerHost_SCD] as t
order by  t.PlayerIDBigInt  ) 

insert into [temp].[Dim_PlayerHost_SCD]
select * from  gaps

with overlaps as ( 
select top 3000 [SystemName]   ,  [SystemNameInt]  ,[CasinoID]    ,[CasinoIDInt]
      ,[PlayerID]    ,[PlayerIDBigInt]   , '' as [HostID]
      ,0 as [HostIDBigInt]   ,[PlayerHostType]
      --,[StartDateTime]   ,[EndDateTime]
	  , DATEADD( day, 30, t.[StartDateTime]) as [StartDateTime]      ,  [EndDateTime]
      ,[IsCurrent]  ,[InsertDateTime] 
from [temp].[Dim_PlayerHost_SCD] as t
order by  t.PlayerIDBigInt  desc ) 

insert into [temp].[Dim_PlayerHost_SCD]
select * from  overlaps
--------------------
	   select  [CasinoID],  [PlayerID] , count(*) 
	  from   temp.[Dim_PlayerHost_SCD]
	  group by [CasinoID],  [PlayerID] 
	  having count(*) > 1 -- 6000 repeated
-----------------------

with fix_scd as (
SELECT  [SystemName] , [SystemNameInt]  , [CasinoID] , [CasinoIDInt] , [PlayerID]
      ,[PlayerIDBigInt] , [HostID] , [HostIDBigInt] , [PlayerHostType] 
	        ,[IsCurrent] , [InsertDateTime], 
	  [StartDateTime]  as t1, [EndDateTime] as t2, 
	  MAX( [StartDateTime]) over ( partition by   [SystemName], [PlayerIDBigInt],  [CasinoID] , [IsCurrent] -- is current somethings ==0
	                                order by    [SystemName], [PlayerIDBigInt],  [CasinoID] , [IsCurrent] ) as t3, 
 
	  MAX( [EndDateTime]) over ( partition by   [SystemName],[PlayerIDBigInt],  [CasinoID] , [IsCurrent] -- is current somethings ==0
	                                order by    [SystemName], [PlayerIDBigInt],  [CasinoID] , [IsCurrent]  ) as t4, 
     ROW_NUMBER() over ( partition by   [SystemName],[PlayerIDBigInt],  [CasinoID] , [IsCurrent] -- is current somethings ==0
	                                order by    [SystemName], [PlayerIDBigInt],  [CasinoID] , [IsCurrent]  ) as flag
  FROM [temp].[Dim_PlayerHost_SCD] ) ,

--  select *, DATEDIFF( DAY, CAST(t2 as DATE), CAST(t3 as DATE) ) from fix_scd
-- THE CORE 
  res as (
 select [SystemName] , [SystemNameInt]  , [CasinoID] , [CasinoIDInt] , [PlayerID] ,[PlayerIDBigInt] , [HostID] , [HostIDBigInt] , [PlayerHostType] , 
	        
	 -- lthe imits fixed
	 t3 as [StartDateTime], t4 as  [EndDateTime], flag, 
	 LAST_VALUE(flag) over ( partition by   [SystemName],[PlayerIDBigInt],  [CasinoID] , [IsCurrent] -- is current somethings ==0
	                                order by    [SystemName], [PlayerIDBigInt],  [CasinoID] , [IsCurrent]  ) flag2
  ,[IsCurrent] , [InsertDateTime]
   from fix_scd )

   select *

   from res 
   order by [PlayerIDBigInt],  [CasinoID]

   -------------------





