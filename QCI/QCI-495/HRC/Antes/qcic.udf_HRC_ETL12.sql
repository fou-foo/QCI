/****** Object:  UserDefinedFunction [qcic].[udf_HRC_ETL12]    Script Date: 10/2/2020 5:59:15 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [qcic].[udf_HRC_ETL12](   )
RETURNS TABLE
AS
RETURN
	SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'					)								AS SystemName
		,TRY_CONVERT(INT			,3						)								AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId				)								AS IssueCasinoID
		,TRY_CONVERT(INT			,b.CasinoIDInt			)								AS IssueCasinoIDInt
		,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId				)								AS RedeemCasinoID
		,TRY_CONVERT(INT			,b.CasinoIDInt			)								AS RedeemCasinoIDInt
		,TRY_CONVERT(NVARCHAR(100)	,PlayerId				)								AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](PlayerID,1))		AS PlayerIDBigInt
		,TRY_CONVERT(DATETIME		,GamingDt				)								AS RedeemDateTime
		,TRY_CONVERT(NVARCHAR(100)	,RedeemPrizeId			)								AS PrizeID
		,TRY_CONVERT(NVARCHAR(100)	,TranType				)								AS PrizeType
		,TRY_CONVERT(INT			,ROUND(AmountAuth*100,0))								AS PrizeIssueAmountInt100
		,TRY_CONVERT(INT			,ROUND(AmountUsed*100,0))								AS PrizeRedeemAmountInt100
		,TRY_CONVERT(INT			,ROUND(RedeemPts*100,0)	)								AS PointForPrizeRedeemAmountInt100
		,TRY_CONVERT(DATETIME		,RedeemedDtm			)								AS PrizeStartDateTime
		,TRY_CONVERT(DATETIME		,RedeemedDtm			)								AS PrizeEndDateTime
		,TRY_CONVERT(NVARCHAR(100)	,''						)								AS PrizeCampaignID
		,TRY_CONVERT(NVARCHAR(100)	,''						)								AS PrizeCampaignSegmentID
		,TRY_CONVERT(DATETIME		,GETUTCDATE()			)								AS InsertDateTime
		FROM	 [qcic].[HRC_Fact_PlayerRedemption_Tran] AS a WITH (NOLOCK)
		JOIN	 [qcids].[App_Casino] b WITH (NOLOCK)
		ON		(a.CasinoId = b.CasinoID AND b.SystemNameInt = 3); 


