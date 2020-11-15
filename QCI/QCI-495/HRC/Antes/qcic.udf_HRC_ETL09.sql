/****** Object:  UserDefinedFunction [qcic].[udf_HRC_ETL09]    Script Date: 10/2/2020 5:40:01 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE  FUNCTION [qcic].[udf_HRC_ETL09]( )
RETURNS TABLE
AS
RETURN
--Slot Ratings
	SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'						)								AS SystemName
		,TRY_CONVERT(INT			,3							)								AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,OrigMfg					)								AS OriginalManufacturer
		,TRY_CONVERT(NVARCHAR(100)	,Mfg						)								AS ManufacturerCleaned
		,TRY_CONVERT(NVARCHAR(100)	,Mfg						)								AS ManufacturerGroup
		,TRY_CONVERT(DATETIME		,GETUTCDATE()				)								AS InsertDateTime
		FROM	 [qcic].[HRC_Dim_SlotMFGGroup] WITH (NOLOCK) ;


