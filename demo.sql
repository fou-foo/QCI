----------------------Paso 5 
 drop table  [tqcids].[Dim_PlayerHost_SCD] ;
select *
into  [tqcids].[Dim_PlayerHost_SCD]
from  [qcids].[Dim_PlayerHost_SCD]; -- 36,019

select * 
from [tqcids].[Dim_PlayerHost_SCD]
WHERE [PlayerIDBigInt] =130619
AND    SystemName='HRC'
order by SystemNameInt, CasinoIDInt, PlayerIDBigInt , HostIDBigInt,  StartDateTime DESC, EndDateTime DESC


