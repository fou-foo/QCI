/****** Object:  UserDefinedFunction [qcic].[udf_HRC_ETL01]    Script Date: 10/2/2020 5:18:11 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   FUNCTION [qcic].[udf_HRC_ETL01] ( )
RETURNS TABLE
AS
RETURN
	SELECT 
			 TRY_CONVERT(NVARCHAR(100),  'HRC'								)	    	AS SystemName  
			,TRY_CONVERT(INT		,  3								)			    AS SystemNameInt  
			,TRY_CONVERT(NVARCHAR(100), a.CasinoID							)			AS CasinoGroupID
			,TRY_CONVERT(INT		, b.CasinoIDInt						)				AS CasinoGroupIDInt
			,TRY_CONVERT(NVARCHAR(100), EmpID								)			AS HostID
			,TRY_CONVERT(BIGINT		, [qcids].[udf_ConvertStringToBigInt](EmpID,2))		AS HostIDBigInt
			,TRY_CONVERT(NVARCHAR(50), LTRIM(RTRIM(ISNULL(FirstName,'NoFirstName'))))	AS HostFirstName
				,TRY_CONVERT(NVARCHAR(50), LTRIM(RTRIM(ISNULL(LastName,'NoLastName'))))	AS HostLastName
			,TRY_CONVERT(NVARCHAR(100), ISNULL(Position,'NoPosition')		)			AS HostPosition
			,TRY_CONVERT(NVARCHAR(50), EmpNum								)			AS HostCode
			,TRY_CONVERT(DATETIME	, InsertDtm							)				AS StartDateTime
			,TRY_CONVERT(DATETIME	, ISNULL(LastUpdatedDtm,'2999-12-31'))				AS EndDateTime
			,TRY_CONVERT(INT		, isCurrent							)				AS IsCurrent
			,GETUTCDATE()																AS InsertDateTime
		FROM	 [qcic].[HRC_Dim_CMPEmployees_History] AS  a WITH (NOLOCK)	
		JOIN	 [qcids].[App_Casino] AS  b WITH (NOLOCK)  
			ON		(a.CasinoID = b.CasinoID AND b.SystemNameInt = 3)
		WHERE	 isHost = 1 and isInactive = 0 ;
