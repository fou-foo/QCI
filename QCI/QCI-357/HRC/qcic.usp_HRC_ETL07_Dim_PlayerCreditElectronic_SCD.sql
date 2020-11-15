/****** Object:  StoredProcedure [tqcic].[usp_HRC_ETL07_Dim_PlayerCreditElectronic_SCD]    Script Date: 10/22/2020 5:29:30 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
	CREATE OR ALTER   PROCEDURE  [tqcic].[usp_HRC_ETL07_Dim_PlayerCreditElectronic_SCD]
	AS
		BEGIN
		SET NOCOUNT ON;
        WITH temp as (	 
        SELECT	 
            SystemName 
		    ,SystemNameInt
		    ,CasinoID
		    ,CasinoIDInt
		    ,PlayerID
		    ,PlayerIDBigInt
		    ,ElectronicCreditType
		    ,IsEnrolled
		    ,ElectronicCreditAccountID
		    ,SignatureID
		    ,CreatedByID
		    ,Note
		    ,StartDateTime
		    ,EndDateTime = LEAD(StartDateTime, 1, {d '2999-12-31'}) OVER (  PARTITION BY SystemName, CasinoID, PlayerID, ElectronicCreditType   ORDER BY StartDateTime  )
		   ,IsCurrent
		   ,InsertDateTime
        FROM	(
		    SELECT	 	
				 TRY_CONVERT(NVARCHAR(100)	,'HRC'					)		AS SystemName
				,TRY_CONVERT(INT			,3						)		AS SystemNameInt
				,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId				)		AS CasinoID
				,TRY_CONVERT(INT			,b.CasinoIDInt			)		AS CasinoIDInt
				,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)		AS PlayerID
				,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](PlayerID,1)) AS PlayerIDBigInt
				,TRY_CONVERT(NVARCHAR(100)	,
							case when EnrollOption = 2 then 'Power Bank'
							else 'Self-Pay Jackpots' end
																	)		AS ElectronicCreditType
				,TRY_CONVERT(INT			,IsEnroll				)		AS IsEnrolled
				,TRY_CONVERT(NVARCHAR(100)	,Acct					)		AS ElectronicCreditAccountID
				,TRY_CONVERT(NVARCHAR(100)	,SignatureId			)		AS SignatureID
				,TRY_CONVERT(NVARCHAR(100)	,CreatedBy				)		AS CreatedByID
				,TRY_CONVERT(NVARCHAR(4000)	,concat(ReasonDesc,' ',Remarks)
																	)		AS Note
				,TRY_CONVERT(DATETIME		,CreatedDtm				)		AS StartDateTime
				,TRY_CONVERT(INT			,isCurrent				)		AS IsCurrent
				,GETUTCDATE()												AS InsertDateTime
		        FROM	 (
				    SELECT	 *
				        FROM	 [tqcic].[HRC_Dim_PlayerECreditEnrollment_History] WITH (NOLOCK)
				        WHERE	 EnrollOption IN (2,3)
				    UNION ALL
				    SELECT	 EnrollmentId
						,Acct
						,PlayerId
						,IsEnroll = case when EnrollOption = 0 then 0 else IsEnroll end
						,EnrollOption = 2
						,CasinoId
						,GamingDt
						,EMAEmpID = 0
						,ReasonDesc
						,Remarks
						,SignatureId
						,CreatedBy
						,ModifiedBy
						,CreatedDtm
						,ModifiedDtm
						,IsCurrent
						,InsertDtm
				        FROM	 [tqcic].[HRC_Dim_PlayerECreditEnrollment_History] WITH (NOLOCK)
				        WHERE	 EnrollOption IN (0,1)
				    UNION ALL
				    SELECT	 EnrollmentId
						,Acct
						,PlayerId
						,IsEnroll = case when EnrollOption = 0 then 0 else IsEnroll end
						,EnrollOption = 3
						,CasinoId
						,GamingDt
						,EMAEmpID = 0
						,ReasonDesc
						,Remarks
						,SignatureId
						,CreatedBy
						,ModifiedBy
						,CreatedDtm
						,ModifiedDtm
						,IsCurrent
						,InsertDtm
				    FROM	 [tqcic].[HRC_Dim_PlayerECreditEnrollment_History] WITH (NOLOCK)
				    WHERE	 EnrollOption IN (0,1) ) a
		        JOIN	 [qcids].[App_Casino] b WITH (NOLOCK)
		        ON		(a.CasinoID = b.CasinoID AND b.SystemNameInt = 3) ) x ), 
        fix_temp AS (
        	SELECT
                IIF (RowNumber = 1, '2999-12-31', LagStartDateTime) as QCIEndDateTime,     a.*
                FROM (  SELECT *,
                            ROW_NUMBER()  OVER (PARTITION BY  SystemNameInt, CasinoIDInt, PlayerIDBigInt, ElectronicCreditType, IsEnrolled
							                         ORDER BY StartDateTime DESC, EndDateTime DESC) AS RowNumber,
                            LAG(StartDateTime, 1) OVER (PARTITION BY  SystemNameInt, CasinoIDInt, PlayerIDBigInt, ElectronicCreditType, IsEnrolled
							                                       ORDER BY StartDateTime DESC, EndDateTime DESC) AS LagStartDateTime
                            FROM temp ) AS a ), 
		final AS (
            SELECT  SystemName, SystemNameInt, CasinoID, CasinoIDInt, PlayerID, PlayerIDBigInt, ElectronicCreditType, IsEnrolled, ElectronicCreditAccountID, SignatureID,
								CreatedByID, Note, StartDateTime,
			        QCIEndDateTime AS EndDateTime  ,
        		    CASE WHEN RowNumber = 1 THEN  IsCurrent ELSE 0 END IsCurrent, InsertDateTime
                    FROM fix_temp ) 
		SELECT SystemName, SystemNameInt, CasinoID, CasinoIDInt, PlayerID, PlayerIDBigInt, ElectronicCreditType, IsEnrolled, ElectronicCreditAccountID, SignatureID,
								CreatedByID, Note, 
					   MIN( StartDateTime) AS StartDateTime ,  MAX(EndDateTime) AS EndDateTime, 1 AS IsCurrent, MAX(InsertDateTime) AS InsertDateTime 
			    FROM final 
			   GROUP BY  SystemName, SystemNameInt, CasinoID, CasinoIDInt, PlayerID, PlayerIDBigInt, ElectronicCreditType, IsEnrolled, ElectronicCreditAccountID, SignatureID,
								CreatedByID, Note
END

