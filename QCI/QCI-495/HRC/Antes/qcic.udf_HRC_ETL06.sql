/****** Object:  UserDefinedFunction [qcic].[udf_HRC_ETL06]    Script Date: 10/2/2020 5:28:00 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE  FUNCTION [qcic].[udf_HRC_ETL06] ( )
RETURNS TABLE
AS
RETURN
	SELECT	 	
			 TRY_CONVERT(NVARCHAR(100)	, 'HRC'					)		AS SystemName
			,TRY_CONVERT(INT			,3						)		AS SystemNameInt
			,TRY_CONVERT(NVARCHAR(100)	,TranCasinoID			)		AS CasinoID
			,TRY_CONVERT(INT			,b.CasinoIDInt			)		AS CasinoIDInt
			,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)		AS PlayerID
			,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt]( PlayerID, 1 )) AS PlayerIDBigInt
			,TRY_CONVERT(INT			,ROUND(CreditLimit * 100,0))	AS CreditLimitInt100
			,TRY_CONVERT(INT			,IsCreditPlayer			)		AS IsActiveCredit
			,TRY_CONVERT(NVARCHAR(100)	,SetupCasinoID			)		AS SetupCasinoID
			,TRY_CONVERT(NVARCHAR(4000)	,GroupCode				)		AS Note
			,TRY_CONVERT(DATETIME		,StartGamingDt			)		AS StartDateTime
			,TRY_CONVERT(DATETIME		,EndGamingDt			)		AS EndDateTime
			,TRY_CONVERT(INT			,isCurrent				)		AS IsCurrent
			,GETUTCDATE()												AS InsertDateTime
		FROM	 [qcic].[HRC_Dim_PlayerCreditLimit_History] a WITH (NOLOCK)
		JOIN	 [qcids].[App_Casino] b WITH (NOLOCK)
	ON		(a.TranCasinoID = b.CasinoID AND b.SystemNameInt = 3);
