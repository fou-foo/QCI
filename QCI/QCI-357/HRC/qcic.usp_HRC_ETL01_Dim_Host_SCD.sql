/****** Object:  StoredProcedure [tqcic].[usp_HRC_ETL01_Dim_Player_SCD]    Script Date: 16/10/2020 04:35:05 p. m. ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
	CREATE OR ALTER PROCEDURE  [tqcic].[usp_HRC_ETL01_Dim_Player_SCD]
	AS
		BEGIN
		SET NOCOUNT ON;
        WITH temp as (
            SELECT 
        		 TRY_CONVERT(NVARCHAR(100)	,'HRC'								)				AS SystemName
        		,TRY_CONVERT(INT			,3									)				AS SystemNameInt
        		,TRY_CONVERT(NVARCHAR(100)	,a.CasinoID							)				AS CasinoGroupID
        		,TRY_CONVERT(INT			,b.CasinoIDInt						)				AS CasinoGroupIDInt
        		,TRY_CONVERT(NVARCHAR(100)	,EmpID								)				AS HostID
        		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](EmpID,2))		AS HostIDBigInt
        		,TRY_CONVERT(NVARCHAR(50)	,LTRIM(RTRIM(ISNULL(FirstName,'NoFirstName'))))		AS HostFirstName
        		,TRY_CONVERT(NVARCHAR(50)	,LTRIM(RTRIM(ISNULL(LastName,'NoLastName'))))		AS HostLastName
        		,TRY_CONVERT(NVARCHAR(100)	,ISNULL(Position,'NoPosition')		)				AS HostPosition
        		,TRY_CONVERT(NVARCHAR(50)	,EmpNum								)				AS HostCode
        		,TRY_CONVERT(DATETIME		,InsertDtm							)				AS StartDateTime
        		,TRY_CONVERT(DATETIME		,ISNULL(LastUpdatedDtm,'2999-12-31'))				AS EndDateTime
        		,TRY_CONVERT(INT			,isCurrent							)				AS IsCurrent
        		,GETUTCDATE()																	AS InsertDateTime
            FROM	 [tqcic].[HRC_Dim_CMPEmployees_History] a WITH (NOLOCK)
            JOIN	 [qcids].[App_Casino] b WITH (NOLOCK)
            ON		(a.CasinoID = b.CasinoID AND b.SystemNameInt = 3)
            WHERE	 isHost = 1 and isInactive = 0), 
        fix_temp AS (
        		  SELECT     IIF (RowNumber = 1, '2999-12-31', LagStartDateTime) as QCIEndDateTime,     a.*
                    FROM (
                        SELECT *,
                            ROW_NUMBER()  OVER (PARTITION BY SystemNameInt, CasinoGroupIDInt, HostIDBigInt ORDER BY StartDateTime DESC, EndDateTime DESC) AS RowNumber,
                            LAG(StartDateTime, 1) OVER (PARTITION BY SystemNameInt, CasinoGroupIDInt, HostIDBigInt ORDER BY StartDateTime DESC, EndDateTime DESC) AS LagStartDateTime
                            FROM temp ) AS a ), 
		final AS (
        SELECT SystemName, SystemNameInt, CasinoGroupID, CasinoGroupIDInt, HostID, HostIDBigInt, HostFirstName, HostLastName, HostPosition,HostCode , StartDateTime, QCIEndDateTime AS EndDateTime, 
        		CASE WHEN RowNumber = 1 THEN  IsCurrent ELSE 0 END IsCurrent, InsertDateTime
				FROM  fix_temp )

		SELECT SystemName, SystemNameInt, CasinoGroupID, CasinoGroupIDInt, HostID, HostIDBigInt, HostFirstName, HostLastName, HostPosition,HostCode , 
		       MIN(StartDateTime) AS StartDateTime , MAX(EndDateTime) AS EndDateTime , 1 AS IsCurrent, MAX(InsertDateTime)  AS InsertDateTime
			   FROM final
			   GROUP BY SystemNameInt, CasinoGroupIDInt, HostIDBigInt, StartDateTime
END