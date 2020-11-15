/****** Object:  StoredProcedure [tqcic].[usp_HRC_ETL04_Dim_PlayerLoyalty_SCD]    Script Date: 10/22/2020 5:29:30 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
	CREATE OR ALTER   PROCEDURE  [tqcic].[usp_HRC_ETL04_Dim_PlayerLoyalty_SCD]
	AS
		BEGIN
		SET NOCOUNT ON;
        WITH temp as (
		SELECT
			 TRY_CONVERT(NVARCHAR(100)	,'HRC'					)							AS SystemName
			,TRY_CONVERT(INT			,3						)							AS SystemNameInt
			,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)							AS PlayerID
			,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](PlayerID,1))	AS PlayerIDBigInt
			,TRY_CONVERT(NVARCHAR(100)	,P_ClubStatus			)							AS LoyaltyCardLevel
			,TRY_CONVERT(DATETIME		,StartGamingDt			)							AS StartDateTime
			,TRY_CONVERT(DATETIME		,EndGamingDt			)							AS EndDateTime
			,TRY_CONVERT(INT			,isCurrent				)							AS IsCurrent
			,GETUTCDATE()																	AS InsertDateTime
			FROM	 [tqcic].[HRC_Dim_MarketablePlayersAttribute] WITH (NOLOCK)   ), 
        fix_temp AS (
    		  SELECT
                 IIF (RowNumber = 1, '2999-12-31', LagStartDateTime) as QCIEndDateTime,     a.*
                 FROM (
                     SELECT *,
                            ROW_NUMBER()  OVER (PARTITION BY SystemNameInt, PlayerIDBigInt  ORDER BY StartDateTime DESC, EndDateTime DESC) AS RowNumber,
                            LAG(StartDateTime, 1) OVER (PARTITION BY SystemNameInt, PlayerIDBigInt  ORDER BY StartDateTime DESC, EndDateTime DESC) AS LagStartDateTime
                            FROM temp ) AS a ), 
	   final AS (
            SELECT SystemName, SystemNameInt , PlayerID , PlayerIDBigInt, LoyaltyCardLevel, StartDateTime , QCIEndDateTime AS EndDateTime  ,
        		   CASE WHEN RowNumber = 1 THEN  IsCurrent ELSE 0 END IsCurrent, InsertDateTime
                   FROM  fix_temp )
	   SELECT SystemName, SystemNameInt , PlayerID , PlayerIDBigInt, LoyaltyCardLevel, 
	           MIN(StartDateTime) AS StartDateTime , MAX(EndDateTime) AS EndDateTime , 1 AS IsCurrent, MAX(InsertDateTime)  AS InsertDateTime
			   FROM final 
			   GROUP BY SystemName, SystemNameInt , PlayerID , PlayerIDBigInt, LoyaltyCardLevel

END

