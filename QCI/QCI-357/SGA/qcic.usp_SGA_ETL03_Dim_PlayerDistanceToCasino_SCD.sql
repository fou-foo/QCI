
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
	CREATE OR ALTER PROCEDURE  [tqcic].[usp_SGA_ETL03_Dim_PlayerDistanceToCasino_SCD]
	AS
		BEGIN
		SET NOCOUNT ON;
        WITH temp as (		
        SELECT	 	
		     TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)							AS SystemName
		    ,TRY_CONVERT(INT			,2						)							AS SystemNameInt
		    ,TRY_CONVERT(NVARCHAR(100)	,b.CasinoID				)							AS CasinoID
		    ,TRY_CONVERT(INT			,b.CasinoIDInt			)							AS CasinoIDInt
		    ,TRY_CONVERT(NVARCHAR(100)	,a.PlayerID				)							AS PlayerID
		    ,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](a.PlayerID,1))	AS PlayerIDBigInt
		    ,TRY_CONVERT(FLOAT			,case when b.CasinoID = 'BSG' then a.P_DistanceTo_BSG
										  when b.CasinoID = 'CSG' then a.P_DistanceTo_CSG
										  when b.CasinoID = 'HSG' then a.P_DistanceTo_HSG
										  when b.CasinoID = 'ISG' then a.P_DistanceTo_ISG
										  when b.CasinoID = 'SHC' then a.P_DistanceTo_SHC
										  when b.CasinoID = 'TSG' then a.P_DistanceTo_TSG
										  else 0 end		)							AS PlayerDistanceToCasino
		    ,TRY_CONVERT(DATETIME		,a.InsertDT				)							AS StartDateTime
		    ,TRY_CONVERT(DATETIME		,a.RetireDt				)							AS EndDateTime
		    ,TRY_CONVERT(INT			,a.IsCurrent			)							AS IsCurrent
		    ,GETUTCDATE()																	AS InsertDateTime
            FROM	 [tqcic].[SGA_uvw_MarketablePlayersAttributes] a WITH (NOLOCK)
            CROSS JOIN (
		    SELECT	 SystemName, SystemNameInt, CasinoID, CasinoIDInt
		    FROM	 [qcids].[App_Casino] WITH (NOLOCK)
		    WHERE	 SystemNameInt = 2
		    GROUP BY SystemName, SystemNameInt, CasinoID, CasinoIDInt) b
        WHERE	 a.isBanned = 0 AND b.SystemNameInt = 2 )
  --select * from temp order  by SystemNameInt, CasinoIDInt, PlayerIDBigInt 
  ,     
        fix_temp AS (
        SELECT     IIF (RowNumber = 1, '2999-12-31', LagStartDateTime) as QCIEndDateTime,     a.*
            FROM (    SELECT *,
                        ROW_NUMBER()  OVER (PARTITION BY SystemNameInt, CasinoIDInt, PlayerIDBigInt  ORDER BY StartDateTime DESC, EndDateTime DESC) AS RowNumber,
                        LAG(StartDateTime, 1) OVER (PARTITION BY   SystemNameInt, CasinoIDInt, PlayerIDBigInt  ORDER BY StartDateTime DESC, EndDateTime DESC) AS LagStartDateTime
    	                    FROM temp ) AS a ), 
		final AS (        
		SELECT  SystemName, SystemNameInt , CasinoID , CasinoIDInt, PlayerID, PlayerIDBigInt, PlayerDistanceToCasino, StartDateTime , QCIEndDateTime AS EndDateTime  ,
        		   CASE WHEN RowNumber = 1 THEN  IsCurrent ELSE 0 END IsCurrent, InsertDateTime
                   FROM fix_temp ) 
		SELECT SystemName, SystemNameInt , CasinoID , CasinoIDInt, PlayerID, PlayerIDBigInt, PlayerDistanceToCasino, 
		       		         MIN(StartDateTime) AS StartDateTime , MAX(EndDateTime) AS EndDateTime , 1 AS IsCurrent, MAX(InsertDateTime)  AS InsertDateTime
			   FROM final
			   GROUP BY  SystemName, SystemNameInt , CasinoID , CasinoIDInt, PlayerID, PlayerIDBigInt, PlayerDistanceToCasino
END