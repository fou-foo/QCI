/****** Object:  UserDefinedFunction [qcic].[udf_HRC_ETL14]    Script Date: 10/2/2020 6:00:37 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   FUNCTION [qcic].[udf_HRC_ETL14](   )
RETURNS TABLE
AS
RETURN
	--Slot Ratings
	SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'					)								AS SystemName
		,TRY_CONVERT(INT			,3						)								AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,PlayerId				)								AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt]( PlayerID, 1 ) )		AS PlayerIDBigInt
		,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId				)								AS CasinoGroupID
		,TRY_CONVERT(INT			,b.CasinoGroupIDInt		)								AS CasinoGroupIDInt
		,TRY_CONVERT(INT			,ROUND(MAX(ValuationScore)*100,0))						AS EvaluationScoreInt100
		,TRY_CONVERT(DATETIME		,GETUTCDATE()			)								AS InsertDateTime
		FROM	 [qcic].[HRC_Fact_PlayerValuationScore] AS a WITH (NOLOCK)
		JOIN	 [qcids].[App_CasinoGroup] AS b WITH (NOLOCK)
		ON		(a.CasinoID = b.CasinoGroupID AND b.SystemNameInt = 3)
		GROUP BY PlayerID, a.CasinoId ,b.CasinoGroupIDInt ; 
GO


