/****** Object:  StoredProcedure [qcic].[usp_SGA_ETL12_Fact_PlayerPrize]   Script Date: 10/2/2020 5:29:43 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [qcic].usp_SGA_ETL12_Fact_PlayerPrize 
@MaxDate DATE,  @days int
/*
  Inputs: @MaxDate  It is the maximum PrizeStartDateTime from the table   [tqcids].[Fact_PlayerPrize] where SystemNameInt is equal to 2 (SGA) 
          @days Only for no hardcoding another value

  the last four columns  (NULLs) is to take care of the integrity with the table [qcids].[Fact_PlayerPrize]
*/
AS
 SET NOCOUNT ON;  
	--Slot Ratings
	SELECT 
			--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
			 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)								AS SystemName
			,TRY_CONVERT(INT			,2						)								AS SystemNameInt
			,TRY_CONVERT(NVARCHAR(100)	,a.IssueCasinoId		)								AS IssueCasinoID
			,TRY_CONVERT(INT			,c.CasinoIDInt			)								AS IssueCasinoIDInt
			,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId				)								AS RedeemCasinoID
			,TRY_CONVERT(INT			,b.CasinoIDInt			)								AS RedeemCasinoIDInt
			,TRY_CONVERT(NVARCHAR(100)	,PlayerId				)								AS PlayerID
			,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](PlayerID,1))		AS PlayerIDBigInt
			,TRY_CONVERT(DATETIME		,GamingDt				)								AS RedeemDateTime
			,TRY_CONVERT(NVARCHAR(100)	,PrizeId				)								AS PrizeID
			,TRY_CONVERT(NVARCHAR(100)	,TranType				)								AS PrizeType
			,TRY_CONVERT(INT			,ROUND(ExpIssued*100,0)	)								AS PrizeIssueAmountInt100
			,TRY_CONVERT(INT			,ROUND(ExpUsed*100,0)	)								AS PrizeRedeemAmountInt100
			,TRY_CONVERT(INT			,ROUND(PtsUsed*100,0)	)								AS PointForPrizeRedeemAmountInt100
			,TRY_CONVERT(DATETIME		,FirstRedemptionDtm		)								AS PrizeStartDateTime
			,TRY_CONVERT(DATETIME		,LastRedemptionDtm		)								AS PrizeEndDateTime
			,TRY_CONVERT(NVARCHAR(100)	,''						)								AS PrizeCampaignID
			,TRY_CONVERT(NVARCHAR(100)	,''						)								AS PrizeCampaignSegmentID
			,TRY_CONVERT(DATETIME		,GETUTCDATE()			)								AS InsertDateTime
			, NULL AS [IsComp]
	        , NULL AS [PrizeDescription]
	        , NULL  AS [EmployeeID]
	        , NULL AS [EmployeeIDBigInt]
	FROM	 [qcic].[SGA_Fact_PlayerPrizeExpense_Day] a WITH (NOLOCK)
	JOIN	 [qcids].[App_Casino] b WITH (NOLOCK)
	ON		(a.CasinoID = b.CasinoID AND b.SystemNameInt = 2)
	JOIN	 [qcids].[App_Casino] c WITH (NOLOCK)
	ON		(a.IssueCasinoId = c.CasinoID AND c.SystemNameInt = 2)
	WHERE FirstRedemptionDtm  >= DATEADD(DAY, @days ,@MaxDate) 
RETURN ;
