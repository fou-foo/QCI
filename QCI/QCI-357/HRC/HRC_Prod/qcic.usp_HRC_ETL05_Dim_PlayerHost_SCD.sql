/****** Object:  StoredProcedure [qcic].[usp_HRC_ETL05_Dim_PlayerHost_SCD]    Script Date: 10/22/2020 5:29:30 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
	CREATE OR ALTER   PROCEDURE  [qcic].[usp_HRC_ETL05_Dim_PlayerHost_SCD]
	AS
		BEGIN
		SET NOCOUNT ON;
        WITH temp as (
	        SELECT	 	
		         TRY_CONVERT(NVARCHAR(100)	,'HRC'					)		AS SystemName
		        ,TRY_CONVERT(INT			,3						)		AS SystemNameInt
		        ,TRY_CONVERT(NVARCHAR(100)	,a.CasinoID				)		AS CasinoID
		        ,TRY_CONVERT(INT			,b.CasinoIDInt			)		AS CasinoIDInt
		        ,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)		AS PlayerID
		        ,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](PlayerID,1)) AS PlayerIDBigInt
		        ,TRY_CONVERT(NVARCHAR(100)	,HostEmpID				)		AS HostID
		        ,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](HostEmpID,2)) AS HostIDBigInt
		        ,TRY_CONVERT(NVARCHAR(100)	,'Primary'				)		AS PlayerHostType
		        ,TRY_CONVERT(DATETIME		,HostStartDTM			)		AS StartDateTime
		        ,TRY_CONVERT(DATETIME		,HostEndDTM				)		AS EndDateTime
		        ,TRY_CONVERT(INT			,isCurrent				)		AS IsCurrent
		        ,GETUTCDATE()												AS InsertDateTime
                FROM	 [qcic].[HRC_Fact_PlayerHostHistory_Primary] a WITH (NOLOCK)
                JOIN	 [qcids].[App_Casino] b WITH (NOLOCK)
                ON		(a.CasinoID = b.CasinoID AND b.SystemNameInt = 3)
                GROUP BY a.CasinoID, b.CasinoIDInt, PlayerID, HostEmpID, HostStartDTM , HostEndDTM, isCurrent
            UNION ALL
            SELECT	 	
		         TRY_CONVERT(NVARCHAR(100)	,'HRC'					)		AS SystemName
		        ,TRY_CONVERT(INT			,3						)		AS SystemNameInt
		        ,TRY_CONVERT(NVARCHAR(100)	,a.CasinoID				)		AS CasinoID
		        ,TRY_CONVERT(INT			,b.CasinoIDInt			)		AS CasinoIDInt
		        ,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)		AS PlayerID
		        ,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](PlayerID,1)) AS PlayerIDBigInt
		        ,TRY_CONVERT(NVARCHAR(100)	,HostEmpID				)		AS HostID
		        ,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](HostEmpID,2)) AS HostIDBigInt
		        ,TRY_CONVERT(NVARCHAR(100)	,'Secondary'			)		AS PlayerHostType
		        ,TRY_CONVERT(DATETIME		,HostStartDTM			)		AS StartDateTime
		        ,TRY_CONVERT(DATETIME		,HostEndDTM				)		AS EndDateTime
		        ,TRY_CONVERT(INT			,isCurrent				)		AS IsCurrent
		        ,GETUTCDATE()												AS InsertDateTime
                FROM	 [qcic].[HRC_Fact_PlayerHostHistory_Secondary] a WITH (NOLOCK)
                JOIN	 [qcids].[App_Casino] b WITH (NOLOCK)
                ON		(a.CasinoID = b.CasinoID AND b.SystemNameInt = 3)
                GROUP BY a.CasinoID, b.CasinoIDInt, PlayerID, HostEmpID, HostStartDTM, HostEndDTM, isCurrent ), 
        fix_temp AS (
    		SELECT
                IIF (RowNumber = 1, '2999-12-31', LagStartDateTime) as QCIEndDateTime,     a.*
                FROM (
                     SELECT *,
                            ROW_NUMBER()  OVER (PARTITION BY  SystemNameInt, CasinoIDInt, PlayerIDBigInt, PlayerHostType, HostIDBigInt
							                         ORDER BY StartDateTime DESC, EndDateTime DESC) AS RowNumber,
                            LAG(StartDateTime, 1) OVER (PARTITION BY  SystemNameInt, CasinoIDInt, PlayerIDBigInt, PlayerHostType, HostIDBigInt
							                                       ORDER BY StartDateTime DESC, EndDateTime DESC) AS LagStartDateTime
                            FROM temp ) AS a ), 
        final AS (
            SELECT 	SystemName,  SystemNameInt,  CasinoID, CasinoIDInt, PlayerID, PlayerIDBigInt, HostID, HostIDBigInt,  PlayerHostType,  StartDateTime, 
			        QCIEndDateTime AS EndDateTime  ,
        		    CASE WHEN RowNumber = 1 THEN  IsCurrent ELSE 0 END IsCurrent, InsertDateTime
                    FROM fix_temp) 
	  SELECT SystemName,  SystemNameInt,  CasinoID, CasinoIDInt, PlayerID, PlayerIDBigInt, HostID, HostIDBigInt,  PlayerHostType, 
			 MIN( StartDateTime) AS StartDateTime ,  MAX(EndDateTime) AS EndDateTime, 1 AS IsCurrent, MAX(InsertDateTime) AS InsertDateTime 
			 FROM final 
			 GROUP BY  SystemName,  SystemNameInt,  CasinoID, CasinoIDInt, PlayerID, PlayerIDBigInt, HostID, HostIDBigInt,  PlayerHostType
END

