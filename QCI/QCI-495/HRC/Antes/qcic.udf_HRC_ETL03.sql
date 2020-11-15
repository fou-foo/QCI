/****** Object:  UserDefinedFunction [qcic].[udf_HRC_ETL03]    Script Date: 10/2/2020 5:23:14 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE FUNCTION [qcic].[udf_HRC_ETL03] ()
RETURNS TABLE
AS
RETURN
	SELECT	 	
			 TRY_CONVERT(NVARCHAR(100), 'HRC'					)							AS SystemName 
			,TRY_CONVERT(INT		, 3						)					    		AS SystemNameInt 
			,TRY_CONVERT(NVARCHAR(100), b.CasinoID				)							AS CasinoID
			,TRY_CONVERT(INT		, b.CasinoIDInt			)						    	AS CasinoIDInt
			,TRY_CONVERT(NVARCHAR(100), a.PlayerID				)							AS PlayerID
			,TRY_CONVERT(BIGINT		, [qcids].[udf_ConvertStringToBigInt](a.PlayerID,1))	AS PlayerIDBigInt
			,TRY_CONVERT(FLOAT		, DistanceToProperty		)							AS PlayerDistanceToCasino
			,TRY_CONVERT(DATETIME	, a.StartGamingDt		)						    	AS StartDateTime
			,TRY_CONVERT(DATETIME	, a.EndGamingDt			)						    	AS EndDateTime
			,TRY_CONVERT(INT		, a.IsCurrent			)						    	AS IsCurrent
			,GETUTCDATE()																	AS InsertDateTime
	FROM	 [qcic].[HRC_Dim_MarketablePlayersAttribute] a WITH (NOLOCK)
CROSS JOIN (
		SELECT	 SystemName
				,SystemNameInt
				,CasinoID
				,CasinoIDInt
		FROM	 [qcids].[App_Casino] WITH (NOLOCK)
		WHERE	 SystemNameInt = 3
		GROUP BY SystemName , SystemNameInt, CasinoID , CasinoIDInt) AS b
WHERE	 a.isBanned = 0 AND b.SystemName = 'HRC';
