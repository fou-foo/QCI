/****** Object:  StoredProcedure [tqcic].[usp_HRC_ETL12_Fact_PlayerPrize]   Script Date: 10/2/2020 5:29:43 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [tqcic].[usp_HRC_ETL12_Fact_PlayerPrize]
@MaxDate DATE,  @days int
/*
  Inputs: @MaxDate  It is the maximum GamingDate from the table  [qcids].[Fact_PlayerPrize] where SystemNameInt is equal to 3 (HRC) 
          @days Only for no hardcoding another value
*/
AS
 SET NOCOUNT ON;  
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
			, NULL AS [IsComp]
	        , NULL AS [PrizeDescription]
	        , NULL  AS [EmployeeID]
	        , NULL AS [EmployeeIDBigInt]
		FROM	 [qcic].[HRC_Fact_PlayerRedemption_Tran] AS a WITH (NOLOCK)
		JOIN	 [qcids].[App_Casino] b WITH (NOLOCK)
		ON		(a.CasinoId = b.CasinoID AND b.SystemNameInt = 3)
		WHERE RedeemedDtm  >= DATEADD(DAY, @days ,@MaxDate) 
	RETURN; 


