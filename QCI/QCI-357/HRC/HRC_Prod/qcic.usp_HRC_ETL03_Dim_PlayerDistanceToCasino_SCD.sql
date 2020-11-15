/****** Object:  StoredProcedure [qcic].[usp_HRC_ETL03_Dim_PlayerDistanceToCasino_SCD]    Script Date: 10/22/2020 5:29:30 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
	CREATE OR ALTER   PROCEDURE  [qcic].[usp_HRC_ETL03_Dim_PlayerDistanceToCasino_SCD]
	AS
		BEGIN
		SET NOCOUNT ON;
        WITH temp as (
        SELECT	 	
		     TRY_CONVERT(NVARCHAR(100)	,'HRC'					)							AS SystemName
		    ,TRY_CONVERT(INT			,3						)							AS SystemNameInt
		    ,TRY_CONVERT(NVARCHAR(100)	,b.CasinoID				)							AS CasinoID
		    ,TRY_CONVERT(INT			,b.CasinoIDInt			)							AS CasinoIDInt
		    ,TRY_CONVERT(NVARCHAR(100)	,a.PlayerID				)							AS PlayerID
		    ,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](a.PlayerID,1))	AS PlayerIDBigInt
		    ,TRY_CONVERT(FLOAT			,DistanceToProperty		)							AS PlayerDistanceToCasino
		    ,TRY_CONVERT(DATETIME		,a.StartGamingDt		)							AS StartDateTime
		    ,TRY_CONVERT(DATETIME		,a.EndGamingDt			)							AS EndDateTime
		    ,TRY_CONVERT(INT			,a.IsCurrent			)							AS IsCurrent
		    ,GETUTCDATE()																	AS InsertDateTime
            FROM	 [qcic].[HRC_Dim_MarketablePlayersAttribute] a WITH (NOLOCK)
            CROSS JOIN (
        		SELECT	 SystemName, SystemNameInt ,CasinoID , CasinoIDInt
                        FROM	 [qcids].[App_Casino] WITH (NOLOCK)
        	        	WHERE	 SystemNameInt = 3
	        	        GROUP BY SystemName	,SystemNameInt, CasinoID, CasinoIDInt) b 
            WHERE	 a.isBanned = 0 AND b.SystemName = 'HRC'), 
        fix_temp AS (
    		  SELECT
                 IIF (RowNumber = 1, '2999-12-31', LagStartDateTime) as QCIEndDateTime,     a.*
                 FROM (
                     SELECT *,
                            ROW_NUMBER()  OVER (PARTITION BY SystemNameInt, CasinoIDInt, PlayerIDBigInt    ORDER BY StartDateTime DESC, EndDateTime DESC) AS RowNumber,
                            LAG(StartDateTime, 1) OVER (PARTITION BY SystemNameInt, CasinoIDInt, PlayerIDBigInt    ORDER BY StartDateTime DESC, EndDateTime DESC) AS LagStartDateTime
                            FROM temp ) AS a ), 
        final AS (
				SELECT SystemName, SystemNameInt , CasinoID , CasinoIDInt, PlayerID, PlayerIDBigInt, PlayerDistanceToCasino, StartDateTime , QCIEndDateTime AS EndDateTime  ,
        		   CASE WHEN RowNumber = 1 THEN  IsCurrent ELSE 0 END IsCurrent, InsertDateTime
                   FROM  fix_temp)

			SELECT SystemName, SystemNameInt , CasinoID , CasinoIDInt, PlayerID, PlayerIDBigInt, PlayerDistanceToCasino,
		       MIN(StartDateTime) AS StartDateTime , MAX(EndDateTime) AS EndDateTime , 1 AS IsCurrent, MAX(InsertDateTime)  AS InsertDateTime
			   FROM final
			   GROUP BY  SystemName, SystemNameInt , CasinoID , CasinoIDInt, PlayerID, PlayerIDBigInt, PlayerDistanceToCasino
END

