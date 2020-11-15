/****** Object:  UserDefinedFunction [qcic].[udf_HRC_ETL11]    Script Date: 10/2/2020 5:58:05 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE  FUNCTION [qcic].[udf_HRC_ETL11](  )
RETURNS TABLE
AS
RETURN
	SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'					)								AS SystemName
		,TRY_CONVERT(INT			,3						)								AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,OrigDenom				)								AS OriginalDenomination
		,TRY_CONVERT(NVARCHAR(100)	,Denom					)								AS DenominationCleaned
		,TRY_CONVERT(NVARCHAR(100)	,DenomSort				)								AS DenominationSort
		,TRY_CONVERT(NVARCHAR(100)	,DenomGroup				)								AS DenominationGroup
		,TRY_CONVERT(NVARCHAR(100)	,DenomGroupSort			)								AS DenominationGroupSort
		,TRY_CONVERT(DATETIME		,GETUTCDATE()			)								AS InsertDateTime
	FROM	 [qcic].[HRC_Dim_SlotDenomGroup] WITH (NOLOCK); 



