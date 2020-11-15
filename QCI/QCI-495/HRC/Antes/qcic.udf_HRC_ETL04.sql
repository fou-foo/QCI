/****** Object:  UserDefinedFunction [qcic].[udf_HRC_ETL04]    Script Date: 10/2/2020 5:25:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE  FUNCTION [qcic].[udf_HRC_ETL04] ( )
RETURNS TABLE
AS
RETURN
	SELECT
			 TRY_CONVERT(NVARCHAR(100), 'HRC'					)						AS SystemName
			,TRY_CONVERT(INT		, 3						)							AS SystemNameInt
			,TRY_CONVERT(NVARCHAR(100), PlayerID				)						AS PlayerID
			,TRY_CONVERT(BIGINT		, [qcids].[udf_ConvertStringToBigInt](PlayerID,1))	AS PlayerIDBigInt
			,TRY_CONVERT(NVARCHAR(100), P_ClubStatus			)						AS LoyaltyCardLevel
			,TRY_CONVERT(DATETIME	, StartGamingDt			)							AS StartDateTime
			,TRY_CONVERT(DATETIME	, EndGamingDt			)							AS EndDateTime
			,TRY_CONVERT(INT		, isCurrent				)							AS IsCurrent
			,GETUTCDATE()																AS InsertDateTime
	FROM	 [qcic].[HRC_Dim_MarketablePlayersAttribute] WITH (NOLOCK);
