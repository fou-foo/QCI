/****** Object:  UserDefinedFunction [qcic].[udf_HRC_ETL05]    Script Date: 10/2/2020 5:26:45 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   FUNCTION [qcic].[udf_HRC_ETL05] ( )
RETURNS TABLE
AS
RETURN
	SELECT	 	
			 TRY_CONVERT(NVARCHAR(100), 'HRC'	                                )	  AS SystemName
			,TRY_CONVERT(INT		, 3						)		                  AS SystemNameInt
			,TRY_CONVERT(NVARCHAR(100), a.CasinoID				)	                  AS CasinoID
			,TRY_CONVERT(INT		, b.CasinoIDInt			)		                  AS CasinoIDInt
			,TRY_CONVERT(NVARCHAR(100), PlayerID				)	                  AS PlayerID
			,TRY_CONVERT(BIGINT, [qcids].[udf_ConvertStringToBigInt]( PlayerID, 1 ) ) AS PlayerIDBigInt
			,TRY_CONVERT(NVARCHAR(100), HostEmpID)	                                  AS HostID
			,TRY_CONVERT(BIGINT, [qcids].[udf_ConvertStringToBigInt]( HostEmpID, 2) ) AS HostIDBigInt
			,TRY_CONVERT(NVARCHAR(100), 'Primary'				)		              AS PlayerHostType
			,TRY_CONVERT(DATETIME	, HostStartDTM			)	 	                  AS StartDateTime
			,TRY_CONVERT(DATETIME	, HostEndDTM				)	                  AS EndDateTime
			,TRY_CONVERT(INT		, isCurrent				)		                  AS IsCurrent
			,GETUTCDATE()											                  AS InsertDateTime
		FROM	 [qcic].[HRC_Fact_PlayerHostHistory_Primary] AS a WITH (NOLOCK)
		JOIN	 [qcids].[App_Casino] AS b WITH (NOLOCK)
 			ON		(a.CasinoID = b.CasinoID AND b.SystemNameInt = 3)
		GROUP BY a.CasinoID, b.CasinoIDInt, PlayerID, HostEmpID, HostStartDTM, HostEndDTM, isCurrent
	UNION ALL
	SELECT	 	
			 TRY_CONVERT(NVARCHAR(100), 'HRC'					)	                       AS SystemName
			,TRY_CONVERT(INT		, 3						)		                       AS SystemNameInt
			,TRY_CONVERT(NVARCHAR(100), a.CasinoID				)	                       AS CasinoID
			,TRY_CONVERT(INT		, b.CasinoIDInt			)		                       AS CasinoIDInt
			,TRY_CONVERT(NVARCHAR(100), PlayerID				)	                       AS PlayerID
			,TRY_CONVERT(BIGINT		, [qcids].[udf_ConvertStringToBigInt]( PlayerID, 1 ) ) AS PlayerIDBigInt
			,TRY_CONVERT(NVARCHAR(100), HostEmpID				)	                       AS HostID
			,TRY_CONVERT(BIGINT		, [qcids].[udf_ConvertStringToBigInt]( HostEmpID, 2) ) AS HostIDBigInt
			,TRY_CONVERT(NVARCHAR(100), 'Secondary'			)		                       AS PlayerHostType
			,TRY_CONVERT(DATETIME	, HostStartDTM			)		                       AS StartDateTime
			,TRY_CONVERT(DATETIME	, HostEndDTM				)                   	   AS EndDateTime
			,TRY_CONVERT(INT		, isCurrent				)	                           AS IsCurrent
			,GETUTCDATE()											      	               AS InsertDateTime
		FROM	 [qcic].[HRC_Fact_PlayerHostHistory_Secondary] AS a WITH (NOLOCK)
		JOIN	 [qcids].[App_Casino] AS  b WITH (NOLOCK)
			ON		(a.CasinoID = b.CasinoID AND b.SystemNameInt = 3)
		GROUP BY a.CasinoID, b.CasinoIDInt, PlayerID, HostEmpID , HostStartDTM , HostEndDTM, isCurrent;
