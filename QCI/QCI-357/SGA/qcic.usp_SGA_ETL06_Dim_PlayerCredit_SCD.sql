
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
	CREATE OR ALTER PROCEDURE  [tqcic].[usp_SGA_ETL06_Dim_PlayerCredit_SCD]
	AS
		BEGIN
		SET NOCOUNT ON;
        WITH temp as (		
        SELECT	 	
		     TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)		AS SystemName
		    ,TRY_CONVERT(INT			,2						)		AS SystemNameInt
		    ,TRY_CONVERT(NVARCHAR(100)	,TranCasinoID			)		AS CasinoID
		    ,TRY_CONVERT(INT			,b.CasinoIDInt			)		AS CasinoIDInt
		    ,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)		AS PlayerID
		    ,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](PlayerID,1)) AS PlayerIDBigInt
		    ,TRY_CONVERT(INT			,ROUND(CreditLimit * 100,0))	AS CreditLimitInt100
		    ,TRY_CONVERT(INT			,IsCreditPlayer			)		AS IsActiveCredit
		    ,TRY_CONVERT(NVARCHAR(100)	,SetupCasinoID			)		AS SetupCasinoID
		    ,TRY_CONVERT(NVARCHAR(4000)	,GroupCode				)		AS Note
		    ,TRY_CONVERT(DATETIME		,StartGamingDt			)		AS StartDateTime
		    ,TRY_CONVERT(DATETIME		,EndGamingDt			)		AS EndDateTime
		    ,TRY_CONVERT(INT			,isCurrent				)		AS IsCurrent
		    ,GETUTCDATE()												AS InsertDateTime
            FROM	 [tqcic].[SGA_Dim_PlayerCreditLimit_History] AS a WITH (NOLOCK)
            JOIN	 [qcids].[App_Casino] AS b WITH (NOLOCK)
            ON		(a.TranCasinoID = b.CasinoID AND b.SystemNameInt = 2)   )       ,     
        
        fix_temp AS (
        SELECT     IIF (RowNumber = 1, '2999-12-31', LagStartDateTime) as QCIEndDateTime,     a.*
            FROM (    SELECT *,
                        ROW_NUMBER()  OVER (PARTITION BY   SystemNameInt, CasinoIDInt, PlayerIDBigInt
						                                  ORDER BY StartDateTime DESC, EndDateTime DESC) AS RowNumber,
                        LAG(StartDateTime, 1) OVER (PARTITION BY     SystemNameInt, CasinoIDInt, PlayerIDBigInt
						                                            ORDER BY StartDateTime DESC, EndDateTime DESC) AS LagStartDateTime
    	                    FROM temp ) AS a )
        
		SELECT SystemName,  SystemNameInt, CasinoID, CasinoIDInt, PlayerID,  PlayerIDBigInt,  CreditLimitInt100, IsActiveCredit, SetupCasinoID, Note, StartDateTime,  
			        QCIEndDateTime AS EndDateTime  ,
        		   CASE WHEN RowNumber = 1 THEN  IsCurrent ELSE 0 END IsCurrent, InsertDateTime
                    from fix_temp
		            order by  SystemNameInt, CasinoIDInt, PlayerIDBigInt
END