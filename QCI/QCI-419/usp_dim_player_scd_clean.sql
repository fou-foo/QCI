/****** Object:  StoredProcedure [test].[usp_Dim_PlayerHost_SCD_clean]    Script Date: 16/10/2020 04:35:05 p. m. ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
	CREATE OR ALTER PROCEDURE  [test].[usp_Dim_PlayerHost_SCD_clean]
	AS
		BEGIN
		SET NOCOUNT ON;
		DROP TABLE IF EXISTS  [temp].[Dim_PlayerHost_SCD_clean] ;
		WITH fix_scd as (
		SELECT  [SystemName], [SystemNameInt], [CasinoID], [CasinoIDInt], [PlayerID], [PlayerIDBigInt], [HostID], [HostIDBigInt], [PlayerHostType], [IsCurrent], [InsertDateTime], 
				[StartDateTime]  as t1, [EndDateTime] as t2, 
	  MAX( [StartDateTime]) over ( partition by   [SystemName], [PlayerIDBigInt],  [CasinoID] , [IsCurrent] -- is current somethings ==0
	                                order by    [SystemName], [PlayerIDBigInt],  [CasinoID] , [IsCurrent] ) as t3, 
	  MAX( [EndDateTime]) over ( partition by   [SystemName],[PlayerIDBigInt],  [CasinoID] , [IsCurrent] -- is current somethings ==0
	                                order by    [SystemName], [PlayerIDBigInt],  [CasinoID] , [IsCurrent]  ) as t4, 
     ROW_NUMBER() over ( partition by   [SystemName],[PlayerIDBigInt],  [CasinoID] , [IsCurrent] -- is current somethings ==0
	                                order by    [SystemName], [PlayerIDBigInt],  [CasinoID] , [IsCurrent]  ) as flag
			  FROM [temp].[Dim_PlayerHost_SCD] ) ,

	-- THE CORE 
	temp as (
		select [SystemName] , [SystemNameInt]  , [CasinoID] , [CasinoIDInt] , [PlayerID] ,[PlayerIDBigInt] , [HostID] , [HostIDBigInt] , [PlayerHostType] ,     
	 -- the Limits fixed
				t3 as [StartDateTime], t4 as  [EndDateTime], flag, 
				LAST_VALUE(flag) over ( partition by   [SystemName],[PlayerIDBigInt],  [CasinoID] , [IsCurrent] -- is current somethings ==0
						                    order by    [SystemName], [PlayerIDBigInt],  [CasinoID] , [IsCurrent]  ) flag2
				,[IsCurrent] , [InsertDateTime]
			from fix_scd ), 

   res_clean as (
	select [SystemName] , [SystemNameInt]  , [CasinoID] , [CasinoIDInt] , [PlayerID] ,[PlayerIDBigInt] , [HostID] , [HostIDBigInt] , [PlayerHostType] ,
		 [StartDateTime],  [EndDateTime], [IsCurrent], [InsertDateTime]
			from temp 
			WHERE flag =  flag2 ) 

   select *
   into  [temp].[Dim_PlayerHost_SCD_clean] 
   from res_clean
END



