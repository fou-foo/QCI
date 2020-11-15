
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
	CREATE OR ALTER PROCEDURE  [tqcic].[usp_SGA_ETL05_Dim_PlayerHost_SCD]
	AS
		BEGIN
		SET NOCOUNT ON;
        WITH temp as (		
        SELECT	 	
		     TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)		AS SystemName
		    ,TRY_CONVERT(INT			,2						)		AS SystemNameInt
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
            FROM	 [tqcic].[SGA_Fact_PlayerHostHistory_Primary] AS a WITH (NOLOCK)
            JOIN	 [qcids].[App_Casino] AS b WITH (NOLOCK)
            ON		(a.CasinoID = b.CasinoID AND b.SystemNameInt = 2)
            GROUP BY a.CasinoID, b.CasinoIDInt, PlayerID , HostEmpID , HostStartDTM, HostEndDTM, isCurrent
        UNION ALL
        SELECT	 	
		     TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)		AS SystemName
		    ,TRY_CONVERT(INT			,2						)		AS SystemNameInt
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
            FROM	 [tqcic].[SGA_Fact_PlayerHostHistory_Secondary] AS a WITH (NOLOCK)
            JOIN	 [qcids].[App_Casino] AS b WITH (NOLOCK)
            ON		(a.CasinoID = b.CasinoID AND b.SystemNameInt = 2)
            GROUP BY a.CasinoID , b.CasinoIDInt , PlayerID, HostEmpID, HostStartDTM , HostEndDTM, isCurrent )       ,     
        
        fix_temp AS (
        SELECT     IIF (RowNumber = 1, '2999-12-31', LagStartDateTime) as QCIEndDateTime,     a.*
            FROM (    SELECT *,
                        ROW_NUMBER()  OVER (PARTITION BY  SystemNameInt, CasinoIDInt, PlayerIDBigInt, PlayerHostType, HostIDBigInt
						                                  ORDER BY StartDateTime DESC, EndDateTime DESC) AS RowNumber,
                        LAG(StartDateTime, 1) OVER (PARTITION BY    SystemNameInt, CasinoIDInt, PlayerIDBigInt, PlayerHostType, HostIDBigInt
						                                            ORDER BY StartDateTime DESC, EndDateTime DESC) AS LagStartDateTime
    	                    FROM temp ) AS a )
        
		SELECT SystemName,  SystemNameInt,  CasinoID, CasinoIDInt, PlayerID, PlayerIDBigInt, HostID, HostIDBigInt,  PlayerHostType,  StartDateTime, 
			        QCIEndDateTime AS EndDateTime  ,
        		   CASE WHEN RowNumber = 1 THEN  IsCurrent ELSE 0 END IsCurrent, InsertDateTime
                    from fix_temp
		            order by  SystemNameInt, CasinoIDInt, PlayerIDBigInt, PlayerHostType, HostIDBigInt
END