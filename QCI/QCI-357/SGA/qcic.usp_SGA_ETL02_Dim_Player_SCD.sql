SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
	CREATE OR ALTER PROCEDURE  [tqcic].[usp_SGA_ETL02_Dim_Player_SCD]
	AS
		BEGIN
		SET NOCOUNT ON;
        WITH temp as (		
     SELECT 
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)							AS SystemName
		,TRY_CONVERT(INT			,2						)							AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)							AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](PlayerID,1))	AS PlayerIDBigInt
		,TRY_CONVERT(NVARCHAR(100)	,Acct					)							AS Account
		,TRY_CONVERT(NVARCHAR(20)	,title					)							AS Title
		,TRY_CONVERT(NVARCHAR(50)	,LTRIM(RTRIM(lastname))	)							AS LastName
		,TRY_CONVERT(NVARCHAR(50)	,LTRIM(RTRIM(firstname)))							AS FirstName
		,TRY_CONVERT(NVARCHAR(50)	,ISNULL(displayname,concat(firstname,' ',lastname)))AS DisplayName
		,TRY_CONVERT(NVARCHAR(50)	,NickNameAlias			)							AS NickNameAlias
		,TRY_CONVERT(NVARCHAR(100)	,HomeAddr1				)							AS HomeAddress1
		,TRY_CONVERT(NVARCHAR(100)	,HomeAddr2				)							AS HomeAddress2
		,TRY_CONVERT(NVARCHAR(100)	,HomeCity				)							AS HomeCity
		,TRY_CONVERT(NVARCHAR(50)	,HomeStateCode			)							AS HomeStateCode
		,TRY_CONVERT(NVARCHAR(50)	,HomeCountryCode		)							AS HomeCountryCode
		,TRY_CONVERT(NVARCHAR(20)	,HomePostalCode			)							AS HomePostalCode
		,TRY_CONVERT(NVARCHAR(20)	,HomeTel1Type			)							AS HomeTelephone1Type
		,TRY_CONVERT(NVARCHAR(20)	,HomeTel1				)							AS HomeTelephone1
		,TRY_CONVERT(NVARCHAR(20)	,HomeTel2Type			)							AS HomeTelephone2Type
		,TRY_CONVERT(NVARCHAR(20)	,HomeTel2				)							AS HomeTelephone2
		,TRY_CONVERT(NVARCHAR(255)	,HomeEmail				)							AS HomeEmail
		,TRY_CONVERT(NVARCHAR(50)	,AltName				)							AS AltName
		,TRY_CONVERT(NVARCHAR(100)	,AltAddr1				)							AS AltAddress1
		,TRY_CONVERT(NVARCHAR(100)	,AltAddr2				)							AS AltAddress2
		,TRY_CONVERT(NVARCHAR(100)	,AltCity				)							AS AltCity
		,TRY_CONVERT(NVARCHAR(50)	,AltStateCode			)							AS AltStateCode
		,TRY_CONVERT(NVARCHAR(50)	,AltCountryCode			)							AS AltCountryCode
		,TRY_CONVERT(NVARCHAR(20)	,AltPostalCode			)							AS AltPostalCode
		,TRY_CONVERT(NVARCHAR(20)	,AltTel1Type			)							AS AltTelephone1Type
		,TRY_CONVERT(NVARCHAR(20)	,AltTel1				)							AS AltTelephone1
		,TRY_CONVERT(NVARCHAR(20)	,AltTel2Type			)							AS AltTelephone2Type
		,TRY_CONVERT(NVARCHAR(20)	,AltTel2				)							AS AltTelephone2
		,TRY_CONVERT(NVARCHAR(255)	,AltEmail				)							AS AltEmail
		,TRY_CONVERT(DATE			,BirthDT				)							AS BirthDate
		,TRY_CONVERT(INT			,isPrivacy				)							AS IsPrivacy
		,TRY_CONVERT(INT			,isMailToAlt			)							AS IsMailToAlt
		,TRY_CONVERT(INT			,isNoMail				)							AS IsNoMail
		,TRY_CONVERT(INT			,isReturnMail			)							AS IsReturnMail
		,TRY_CONVERT(INT			,isVIP					)							AS IsVIP
		,TRY_CONVERT(INT			,isLostAndFound			)							AS IsLostAndFound
		,TRY_CONVERT(INT			,isCreditAcct			)							AS IsCreditAccount
		,TRY_CONVERT(INT			,isBanned				)							AS IsBanned
		,TRY_CONVERT(INT			,isInactive				)							AS IsInactive
		,TRY_CONVERT(INT			,isCall					)							AS IsCall
		,TRY_CONVERT(INT			,isEmailSend			)							AS IsEmailSend
		,TRY_CONVERT(NVARCHAR(100)	,SetupCasinoID			)							AS SetupCasinoID
		,TRY_CONVERT(NVARCHAR(100)	,SetupEmpID				)							AS SetupEmployeeID
		,TRY_CONVERT(DATE			,SetupDTM				)							AS SetupDate
		,TRY_CONVERT(NVARCHAR(50)	,case when Sex = 0 then 'F' when Sex = 1 then 'M' else 'O' end)	AS Gender
		,TRY_CONVERT(NVARCHAR(100)	,MailingAddr1			)							AS MailingAddress1
		,TRY_CONVERT(NVARCHAR(100)	,MailingAddr2			)							AS MailingAddress2
		,TRY_CONVERT(NVARCHAR(100)	,MailingCity			)							AS MailingCity
		,TRY_CONVERT(NVARCHAR(50)	,MailingStateCode		)							AS MailingStateCode
		,TRY_CONVERT(NVARCHAR(50)	,MailingCountryCode		)							AS MailingCountryCode
		,TRY_CONVERT(NVARCHAR(20)	,MailingPostalCode		)							AS MailingPostalCode
		,TRY_CONVERT(NVARCHAR(20)	,Addr_MatchType			)							AS Address_MatchType
		,TRY_CONVERT(REAL			,Addr_LAT				)							AS Address_Latitude
		,TRY_CONVERT(REAL			,Addr_LONG				)							AS Address_Longitude
		,TRY_CONVERT(DATETIME		,InsertDT				)							AS StartDateTime
		,TRY_CONVERT(DATETIME		,RetireDT				)							AS EndDateTime
		,TRY_CONVERT(INT			,isCurrent				)							AS IsCurrent
		,GETUTCDATE()																	AS InsertDateTime
		FROM	 [tqcic].[SGA_uvw_MarketablePlayersAttributes] WITH (NOLOCK)
		WHERE	 isBanned = 0)   
		--select * from temp order by  SystemNameInt , PlayerIDBigInt
		,
        fix_temp AS (
        SELECT     IIF (RowNumber = 1, '2999-12-31', LagStartDateTime) as QCIEndDateTime,     a.*
              FROM (    SELECT *,
                        ROW_NUMBER()  OVER (PARTITION BY SystemNameInt, PlayerIDBigInt ORDER BY StartDateTime DESC, EndDateTime DESC) AS RowNumber,
                        LAG(StartDateTime, 1) OVER (PARTITION BY   SystemNameInt, PlayerIDBigInt ORDER BY StartDateTime DESC, EndDateTime DESC) AS LagStartDateTime
    	                    FROM temp ) AS a ), 
		final AS ( 
			SELECT SystemName , SystemNameInt , PlayerID , PlayerIDBigInt , Account , Title , 
                                LastName  , FirstName  , DisplayName  , NickNameAlias  , HomeAddress1  , HomeAddress2     , 
                                HomeCity  , HomeStateCode  , HomeCountryCode  , HomePostalCode  , HomeTelephone1Type  , HomeTelephone1  , 
                                HomeTelephone2Type  , HomeTelephone2   , HomeEmail  , AltName  , AltAddress1  ,  AltAddress2  , 
                                AltCity  , AltStateCode  ,AltCountryCode  , AltPostalCode  ,  AltTelephone1Type  , AltTelephone1  ,
                                AltTelephone2Type  , AltTelephone2  , AltEmail  , BirthDate  , IsPrivacy  , IsMailToAlt  , IsNoMail  , IsReturnMail  , IsVIP  , IsLostAndFound  , IsCreditAccount  , 
                                IsBanned  , IsInactive  , IsCall  , IsEmailSend  , SetupCasinoID  , SetupEmployeeID  , SetupDate  , Gender   , MailingAddress1  , MailingAddress2  , MailingCity  , 
                                MailingStateCode  , MailingCountryCode  , MailingPostalCode  , Address_MatchType  , Address_Latitude  , 
                                Address_Longitude  , StartDateTime   , QCIEndDateTime AS EndDateTime  ,
        		CASE WHEN RowNumber = 1 THEN  IsCurrent ELSE 0 END IsCurrent, InsertDateTime
				FROM fix_temp )
			SELECT SystemName , SystemNameInt , PlayerID , PlayerIDBigInt , Account , Title , 
                                LastName  , FirstName  , DisplayName  , NickNameAlias  , HomeAddress1  , HomeAddress2     , 
                                HomeCity  , HomeStateCode  , HomeCountryCode  , HomePostalCode  , HomeTelephone1Type  , HomeTelephone1  , 
                                HomeTelephone2Type  , HomeTelephone2   , HomeEmail  , AltName  , AltAddress1  ,  AltAddress2  , 
                                AltCity  , AltStateCode  ,AltCountryCode  , AltPostalCode  ,  AltTelephone1Type  , AltTelephone1  ,
                                AltTelephone2Type  , AltTelephone2  , AltEmail  , BirthDate  , IsPrivacy  , IsMailToAlt  , IsNoMail  , IsReturnMail  , IsVIP  , IsLostAndFound  , IsCreditAccount  , 
                                IsBanned  , IsInactive  , IsCall  , IsEmailSend  , SetupCasinoID  , SetupEmployeeID  , SetupDate  , Gender   , MailingAddress1  , MailingAddress2  , MailingCity  , 
                                MailingStateCode  , MailingCountryCode  , MailingPostalCode  , Address_MatchType  , Address_Latitude  , 
                                Address_Longitude  ,
					         MIN(StartDateTime) AS StartDateTime , MAX(EndDateTime) AS EndDateTime , 1 AS IsCurrent, MAX(InsertDateTime)  AS InsertDateTime
			   FROM final
			   GROUP BY SystemName , SystemNameInt , PlayerID , PlayerIDBigInt , Account , Title , 
                                LastName  , FirstName  , DisplayName  , NickNameAlias  , HomeAddress1  , HomeAddress2     , 
                                HomeCity  , HomeStateCode  , HomeCountryCode  , HomePostalCode  , HomeTelephone1Type  , HomeTelephone1  , 
                                HomeTelephone2Type  , HomeTelephone2   , HomeEmail  , AltName  , AltAddress1  ,  AltAddress2  , 
                                AltCity  , AltStateCode  ,AltCountryCode  , AltPostalCode  ,  AltTelephone1Type  , AltTelephone1  ,
                                AltTelephone2Type  , AltTelephone2  , AltEmail  , BirthDate  , IsPrivacy  , IsMailToAlt  , IsNoMail  , IsReturnMail  , IsVIP  , IsLostAndFound  , IsCreditAccount  , 
                                IsBanned  , IsInactive  , IsCall  , IsEmailSend  , SetupCasinoID  , SetupEmployeeID  , SetupDate  , Gender   , MailingAddress1  , MailingAddress2  , MailingCity  , 
                                MailingStateCode  , MailingCountryCode  , MailingPostalCode  , Address_MatchType  , Address_Latitude  , 
                                Address_Longitude 
END