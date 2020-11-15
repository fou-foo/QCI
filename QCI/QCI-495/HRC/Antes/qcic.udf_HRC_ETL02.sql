/****** Object:  UserDefinedFunction [qcic].[udf_HRC_ETL02]    Script Date: 10/2/2020 5:22:00 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE  FUNCTION [qcic].[udf_HRC_ETL02] ( )
RETURNS TABLE
AS
RETURN
	SELECT 
			 TRY_CONVERT(NVARCHAR(100), 'HRC'					)							AS SystemName 
			,TRY_CONVERT(INT		,  3						)							AS SystemNameInt 
			,TRY_CONVERT(NVARCHAR(100), PlayerID				)							AS PlayerID
			,TRY_CONVERT(BIGINT		, [qcids].[udf_ConvertStringToBigInt](PlayerID,1))	AS PlayerIDBigInt
			,TRY_CONVERT(NVARCHAR(100), Acct					)							AS Account
			,TRY_CONVERT(NVARCHAR(20), title					)							AS Title
			,TRY_CONVERT(NVARCHAR(50), LTRIM(RTRIM(lastname))	)							AS LastName
			,TRY_CONVERT(NVARCHAR(50), LTRIM(RTRIM(firstname)))						        AS FirstName
			,TRY_CONVERT(NVARCHAR(50), ISNULL(displayname,concat(firstname,' ',lastname)))  AS DisplayName
			,TRY_CONVERT(NVARCHAR(50), NickNameAlias			)							AS NickNameAlias
			,TRY_CONVERT(NVARCHAR(100), HomeAddr01				)							AS HomeAddress1
			,TRY_CONVERT(NVARCHAR(100), HomeAddr02				)							AS HomeAddress2
			,TRY_CONVERT(NVARCHAR(100), HomeCity				)							AS HomeCity
			,TRY_CONVERT(NVARCHAR(50), HomeStateCode			)							AS HomeStateCode
			,TRY_CONVERT(NVARCHAR(50), HomeCountryCode		)							    AS HomeCountryCode
			,TRY_CONVERT(NVARCHAR(20), HomePostalCode			)							AS HomePostalCode
			,TRY_CONVERT(NVARCHAR(20), HomeTel01Type			)							AS HomeTelephone1Type
			,TRY_CONVERT(NVARCHAR(20), HomeTel01				)							AS HomeTelephone1
			,TRY_CONVERT(NVARCHAR(20), HomeTel02Type			)							AS HomeTelephone2Type
			,TRY_CONVERT(NVARCHAR(20), HomeTel02				)							AS HomeTelephone2
			,TRY_CONVERT(NVARCHAR(255), HomeEmail				)							AS HomeEmail
			,TRY_CONVERT(NVARCHAR(50), AltName				)							    AS AltName
			,TRY_CONVERT(NVARCHAR(100), AltAddr01				)							AS AltAddress1
			,TRY_CONVERT(NVARCHAR(100), AltAddr02				)							AS AltAddress2
			,TRY_CONVERT(NVARCHAR(100), AltCity				)							    AS AltCity
			,TRY_CONVERT(NVARCHAR(50), AltStateCode			)							    AS AltStateCode
			,TRY_CONVERT(NVARCHAR(50), AltCountryCode			)							AS AltCountryCode
			,TRY_CONVERT(NVARCHAR(20), AltPostalCode			)							AS AltPostalCode
			,TRY_CONVERT(NVARCHAR(20), AltTel01Type			)				    			AS AltTelephone1Type
			,TRY_CONVERT(NVARCHAR(20), AltTel01				)				    			AS AltTelephone1
			,TRY_CONVERT(NVARCHAR(20), AltTel02Type			)				    			AS AltTelephone2Type
			,TRY_CONVERT(NVARCHAR(20), AltTel02				)				    			AS AltTelephone2
			,TRY_CONVERT(NVARCHAR(255), AltEmail				)							AS AltEmail
			,TRY_CONVERT(DATE		, BirthDT				)					    		AS BirthDate
			,TRY_CONVERT(INT		, isPrivacy				)					    		AS IsPrivacy
			,TRY_CONVERT(INT		, isMailToAlt			)					    		AS IsMailToAlt
			,TRY_CONVERT(INT		, 1 - IsMailPlayer		)					    		AS IsNoMail
			,TRY_CONVERT(INT		, isReturnMail			)					    		AS IsReturnMail
			,TRY_CONVERT(INT		, isVIP					)					    		AS IsVIP
			,TRY_CONVERT(INT		, isLostAndFound			)							AS IsLostAndFound
			,TRY_CONVERT(INT		, isCreditAcct			)					    		AS IsCreditAccount
			,TRY_CONVERT(INT		, isBanned				)					    		AS IsBanned
			,TRY_CONVERT(INT		, isInactive				)							AS IsInactive
			,TRY_CONVERT(INT		, IsCallPlayer			)						    	AS IsCall
			,TRY_CONVERT(INT		, IsEmailPlayer			)						    	AS IsEmailSend
			,TRY_CONVERT(NVARCHAR(100), SetupCasinoID			)							AS SetupCasinoID
			,TRY_CONVERT(NVARCHAR(100), SetupEmpID				)							AS SetupEmployeeID
			,TRY_CONVERT(DATE		, SetupDTM				)					    		AS SetupDate
			,TRY_CONVERT(NVARCHAR(50), Gender					)							AS Gender
			,TRY_CONVERT(NVARCHAR(100), MailingAddr01			)							AS MailingAddress1
			,TRY_CONVERT(NVARCHAR(100), MailingAddr02			)							AS MailingAddress2
			,TRY_CONVERT(NVARCHAR(100), MailingCity			)				    			AS MailingCity
			,TRY_CONVERT(NVARCHAR(50), MailingStateCode		)				    			AS MailingStateCode
			,TRY_CONVERT(NVARCHAR(50), MailingCountryCode		)							AS MailingCountryCode
			,TRY_CONVERT(NVARCHAR(20), MailingPostalCode		)							AS MailingPostalCode
			,TRY_CONVERT(NVARCHAR(20), 'GeoCode'				)							AS Address_MatchType
			,TRY_CONVERT(REAL		, MailingLatitude		)					    		AS Address_Latitude
			,TRY_CONVERT(REAL		, MailingLongitude		)					    		AS Address_Longitude
			,TRY_CONVERT(DATETIME	, StartGamingDt			)					    		AS StartDateTime
			,TRY_CONVERT(DATETIME	, EndGamingDt			)					    		AS EndDateTime
			,TRY_CONVERT(INT		, isCurrent				)					    		AS IsCurrent
			,GETUTCDATE()				 										    		AS InsertDateTime
		FROM	 [qcic].[HRC_Dim_MarketablePlayersAttribute] WITH (NOLOCK)
		WHERE	 isBanned = 0 ;
