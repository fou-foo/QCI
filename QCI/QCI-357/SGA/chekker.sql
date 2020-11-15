-- merge 1
  select * from   [tqcids].[Dim_Host_SCD] as t 
    where t.SystemName='SGAHRI'
  order by t.HostIDBigInt -- before 139  after usp 61


----merge 2 
SELECT  * 
  FROM [tqcids].[Dim_Player_SCD] as t 
  
  where t.SystemName='SGAHRI'
  order by t.PlayerIDBigInt -- before 1,819,664  after usp   454,916

  --paso 3 
  select * 
  from [tqcids].[Dim_PlayerDistanceToCasino_SCD] as t
  where t.SystemName='SGAHRI'
  order by t.PlayerIDBigInt -- before     9,942,732    after usp 
  --- paso 4 
  select *
  from [tqcids].[Dim_PlayerLoyalty_SCD] as t 
  where t.SystemName='SGAHRI'
  order by t.PlayerIDBigInt -- before            after usp  


  -- paso 5 
  select * 
  from [tqcids].[Dim_PlayerHost_SCD] as t 
  where t.SystemName='SGAHRI'
  order by SystemNameInt, CasinoIDInt, PlayerIDBigInt , HostIDBigInt-- before         after usp  


  --paso 6 

  select * 
  from [tqcids].[Dim_PlayerCredit_SCD] as t 
  where t.SystemName='SGAHRI'
  order by SystemNameInt, CasinoIDInt, PlayerIDBigInt -- before     after usp   


  --paso 7 

  select * 
  from [tqcids].[Dim_PlayerCreditElectronic_SCD]  as t 
  where t.SystemName='SGAHRI'
  order by SystemNameInt, CasinoIDInt, PlayerIDBigInt -- before         after usp  

