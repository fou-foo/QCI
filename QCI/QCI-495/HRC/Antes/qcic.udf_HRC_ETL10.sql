/****** Object:  UserDefinedFunction [qcic].[udf_HRC_ETL10]    Script Date: 10/2/2020 5:41:44 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [qcic].[udf_HRC_ETL10](   )
RETURNS TABLE
AS
RETURN
	SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'						)								AS SystemName
		,TRY_CONVERT(INT			,3							)								AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,OrigGameType				)								AS OriginalGameType
		,TRY_CONVERT(NVARCHAR(100)	,GameTypeCleaned			)								AS GameTypeCleaned
		,TRY_CONVERT(NVARCHAR(100)	,GameType					)								AS GameTypeGroup
		,TRY_CONVERT(NVARCHAR(100)	,GameTypeShortName			)								AS GameTypeShortName
		,TRY_CONVERT(NVARCHAR(100)	,GameTypeSummary			)								AS GameTypeSummary
		,TRY_CONVERT(DATETIME		,GETUTCDATE()				)								AS InsertDateTime
FROM	 [qcic].[HRC_Dim_SlotGameTypeGroup] WITH (NOLOCK);
GO


