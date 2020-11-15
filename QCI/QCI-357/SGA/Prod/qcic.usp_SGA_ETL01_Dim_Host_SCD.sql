SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
	CREATE OR ALTER PROCEDURE  [qcic].[usp_SGA_ETL01_Dim_Host_SCD]
	AS
		BEGIN
		SET NOCOUNT ON;
        WITH temp as (		
        SELECT 
		     TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'																)		AS SystemName
		    ,TRY_CONVERT(INT			,2																		)		AS SystemNameInt
		    ,TRY_CONVERT(NVARCHAR(100)	,ISNULL(c.CasinoGroupID,a.CasinoID)										)		AS CasinoGroupID
		    ,TRY_CONVERT(INT			,ISNULL(c.CasinoGroupIDInt,b.CasinoIDInt)								)		AS CasinoGroupIDInt
		    ,TRY_CONVERT(NVARCHAR(100)	,a.EmpID																)		AS HostID
		    ,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](a.EmpID,2)							)		AS HostIDBigInt
		    ,TRY_CONVERT(NVARCHAR(50)	,ISNULL(c.HostFirstName,LTRIM(RTRIM(ISNULL(a.FirstName,'NoFirstName')))))		AS HostFirstName
		    ,TRY_CONVERT(NVARCHAR(50)	,ISNULL(c.HostLastName,LTRIM(RTRIM(ISNULL(a.LastName,'NoLastName'))))	)		AS HostLastName
		    ,TRY_CONVERT(NVARCHAR(100)	,ISNULL(c.HostPosition,ISNULL(a.Position,'NoPosition'))					)		AS HostPosition
		    ,TRY_CONVERT(NVARCHAR(50)	,ISNULL(c.HostCode,a.EmpNum)											)		AS HostCode
		    ,TRY_CONVERT(DATETIME		,ISNULL(c.StartDateTime,a.InsertDT)										)		AS StartDateTime
		    ,TRY_CONVERT(DATETIME		,ISNULL(c.EndDateTime,ISNULL(a.RetireDT,'2999-12-31'))					)		AS EndDateTime
		    ,TRY_CONVERT(INT			,ISNULL(c.IsCurrent,a.isCurrent)										)		AS IsCurrent
		    ,GETUTCDATE()																								AS InsertDateTime
            FROM	 [qcic].[SGA_Dim_CMPEmployees] AS a WITH (NOLOCK)
            JOIN	 [qcids].[App_Casino] AS b WITH (NOLOCK)
            ON	(a.CasinoID = b.CasinoID AND b.SystemNameInt = 2)
            LEFT JOIN [qcids].[App_Dim_Host_SCD_Override] AS c WITH (NOLOCK)
            ON		(a.EmpID = c.HostID AND c.SystemNameInt = 2 AND c.OverrideIsActive = 1)
            WHERE	 a.isHost = 1 AND a.isInactive = 0 ),
         fix_temp AS (
            SELECT     IIF (RowNumber = 1, '2999-12-31', LagStartDateTime) as QCIEndDateTime,     a.*
              FROM (    SELECT *,
                            ROW_NUMBER()  OVER (PARTITION BY SystemNameInt, CasinoGroupIDInt, HostIDBigInt ORDER BY StartDateTime DESC, EndDateTime DESC) AS RowNumber,
                            LAG(StartDateTime, 1) OVER (PARTITION BY SystemNameInt, CasinoGroupIDInt, HostIDBigInt ORDER BY StartDateTime DESC, EndDateTime DESC) AS LagStartDateTime
                            FROM temp ) AS a ), 
		final AS(
			SELECT SystemName, SystemNameInt, CasinoGroupID, CasinoGroupIDInt, HostID, HostIDBigInt, HostFirstName, HostLastName, HostPosition,HostCode , StartDateTime, QCIEndDateTime AS EndDateTime, 
        		CASE WHEN RowNumber = 1 THEN  IsCurrent ELSE 0 END IsCurrent, InsertDateTime
			FROM  fix_temp) 

			SELECT SystemName, SystemNameInt, CasinoGroupID, CasinoGroupIDInt, HostID, HostIDBigInt, HostFirstName, HostLastName, HostPosition,HostCode , 
			         MIN(StartDateTime) AS StartDateTime , MAX(EndDateTime) AS EndDateTime , 1 AS IsCurrent, MAX(InsertDateTime)  AS InsertDateTime
			   FROM final
			   GROUP BY SystemName, SystemNameInt, CasinoGroupID, CasinoGroupIDInt, HostID, HostIDBigInt, HostFirstName, HostLastName, HostPosition,HostCode 
END