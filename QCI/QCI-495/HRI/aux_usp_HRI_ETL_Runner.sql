CREATE   FUNCTION [tqcic].[udf_SGA_ETL01] ( )
RETURNS TABLE
AS
RETURN
	SELECT 
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'							)				AS SystemName
		,TRY_CONVERT(NVARCHAR(100)	,CasinoID							)				AS CasinoGroupID
		,TRY_CONVERT(NVARCHAR(100)	,EmpID								)				AS HostID
		,TRY_CONVERT(NVARCHAR(50)	,ISNULL(FirstName,'NoFirstName')	)				AS HostFirstName
		,TRY_CONVERT(NVARCHAR(50)	,ISNULL(LastName,'NoLastName')		)				AS HostLastName
		,TRY_CONVERT(NVARCHAR(100)	,ISNULL(Position,'NoPosition')		)				AS HostPosition
		,TRY_CONVERT(NVARCHAR(50)	,EmpNum								)				AS HostCode
		,TRY_CONVERT(DATETIME		,InsertDT							)				AS StartDateTime
		,TRY_CONVERT(DATETIME		,ISNULL(RetireDT,'2999-12-31')		)				AS EndDateTime
		,TRY_CONVERT(INT			,isCurrent							)				AS IsCurrent
		,GETUTCDATE()																	AS InsertDateTime
	FROM	 [qcic].[SGA_Dim_CMPEmployees]
	WHERE	 isHost = 1 and isInactive = 0;
-- testing select * from [tqcic].[udf_SGA_ETL01] ( )

CREATE   FUNCTION [tqcic].[udf_SGA_ETL02] ( )
RETURNS TABLE
AS
RETURN
	SELECT 
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)							AS SystemName
		,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)							AS PlayerID
		,TRY_CONVERT(NVARCHAR(20)	,title					)							AS Title
		,TRY_CONVERT(NVARCHAR(50)	,lastname				)							AS LastName
		,TRY_CONVERT(NVARCHAR(50)	,firstname				)							AS FirstName
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
		,TRY_CONVERT(INT			,isCall					)								AS IsCall
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
	FROM	 [qcic].[SGA_uvw_MarketablePlayersAttributes]
	WHERE	 isBanned = 0;
-- testing select * from [tqcic].[udf_SGA_ETL02] ( )

CREATE   FUNCTION [tqcic].[udf_SGA_ETL03] ( )
RETURNS TABLE
AS
RETURN
	SELECT	 	
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)							AS SystemName
		,TRY_CONVERT(NVARCHAR(100)	,b.CasinoID				)							AS CasinoID
		,TRY_CONVERT(NVARCHAR(100)	,a.PlayerID				)							AS PlayerID
		,TRY_CONVERT(FLOAT			,case when b.CasinoID = 'BSG' then a.P_DistanceTo_BSG
										  when b.CasinoID = 'CSG' then a.P_DistanceTo_CSG
										  when b.CasinoID = 'HSG' then a.P_DistanceTo_HSG
										  when b.CasinoID = 'ISG' then a.P_DistanceTo_ISG
										  when b.CasinoID = 'SHC' then a.P_DistanceTo_SHC
										  when b.CasinoID = 'TSG' then a.P_DistanceTo_TSG
										  else 0 end		)							AS PlayerDistanceToCasino
		,TRY_CONVERT(DATETIME		,a.InsertDT				)							AS StartDateTime
		,TRY_CONVERT(DATETIME		,a.RetireDt				)							AS EndDateTime
		,TRY_CONVERT(INT			,a.IsCurrent			)							AS IsCurrent
		,GETUTCDATE()																	AS InsertDateTime
		FROM	 [qcic].[SGA_uvw_MarketablePlayersAttributes] a
		CROSS JOIN (
			SELECT	 a.SystemName
					,CasinoID = a.CasinoGroupID
					FROM	 [qcids].[App_CasinoGroup] AS a
					JOIN	 [qcids].[App_CasinoGroupCasino] AS b
					ON		(a.SystemName = b.SystemName AND a.CasinoGroupID = b.CasinoGroupID AND a.CasinoGroupID = b.CasinoID)
					WHERE	 a.SystemName = 'SGAHRI') b
		WHERE	 a.isBanned = 0 AND b.SystemName = 'SGAHRI';
--- TESTIG SELECT * FROM   [tqcic].[udf_SGA_ETL03] ( )

CREATE   FUNCTION [tqcic].[udf_SGA_ETL04] ( )
RETURNS TABLE
AS
RETURN
	SELECT
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)							AS SystemName
		,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)							AS PlayerID
		,TRY_CONVERT(NVARCHAR(100)	,P_ClubStatus			)							AS LoyaltyCardLevel
		,TRY_CONVERT(DATETIME		,InsertDT				)							AS StartDateTime
		,TRY_CONVERT(DATETIME		,RetireDT				)							AS EndDateTime
		,TRY_CONVERT(INT			,isCurrent				)							AS IsCurrent
		,GETUTCDATE()																	AS InsertDateTime
	FROM	 [qcic].[SGA_uvw_MarketablePlayersAttributes];
-- TESTING SELECT * FROM [tqcic].[udf_SGA_ETL04] ( )


CREATE   FUNCTION [tqcic].[udf_SGA_ETL05] ( )
RETURNS TABLE
AS
RETURN
	SELECT	 	
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)		AS SystemName
		,TRY_CONVERT(NVARCHAR(100)	,CasinoID				)		AS CasinoID
		,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)		AS PlayerID
		,TRY_CONVERT(NVARCHAR(100)	,HostEmpID				)		AS HostID
		,TRY_CONVERT(NVARCHAR(100)	,'Primary'				)		AS PlayerHostType
		,TRY_CONVERT(DATETIME		,HostStartDTM			)		AS StartDateTime
		,TRY_CONVERT(DATETIME		,HostEndDTM				)		AS EndDateTime
		,TRY_CONVERT(INT			,isCurrent				)		AS IsCurrent
		,GETUTCDATE()												AS InsertDateTime
		FROM	 [qcic].[SGA_Fact_PlayerHostHistory_Primary]
	UNION ALL
	SELECT	 	
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)		AS SystemName
		,TRY_CONVERT(NVARCHAR(100)	,CasinoID				)		AS CasinoID
		,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)		AS PlayerID
		,TRY_CONVERT(NVARCHAR(100)	,HostEmpID				)		AS HostID
		,TRY_CONVERT(NVARCHAR(100)	,'Secondary'			)		AS PlayerHostType
		,TRY_CONVERT(DATETIME		,HostStartDTM			)		AS StartDateTime
		,TRY_CONVERT(DATETIME		,HostEndDTM				)		AS EndDateTime
		,TRY_CONVERT(INT			,isCurrent				)		AS IsCurrent
		,GETUTCDATE()												AS InsertDateTime
		FROM	 [qcic].[SGA_Fact_PlayerHostHistory_Secondary];

CREATE   FUNCTION [tqcic].[udf_SGA_ETL06] ( )
RETURNS TABLE
AS
RETURN
	SELECT	 	
		 TRY_CONVERT(NVARCHAR(100)	,'SGAHRI'				)		AS SystemName
		,TRY_CONVERT(NVARCHAR(100)	,TranCasinoID			)		AS CasinoID
		,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)		AS PlayerID
		,TRY_CONVERT(MONEY			,CreditLimit			)		AS CreditLimit
		,TRY_CONVERT(INT			,IsCreditPlayer			)		AS IsActiveCredit
		,TRY_CONVERT(NVARCHAR(100)	,SetupCasinoID			)		AS SetupCasinoID
		,TRY_CONVERT(NVARCHAR(4000)	,GroupCode				)		AS Note
		,TRY_CONVERT(DATETIME		,StartGamingDt			)		AS StartDateTime
		,TRY_CONVERT(DATETIME		,EndGamingDt			)		AS EndDateTime
		,TRY_CONVERT(INT			,isCurrent				)		AS IsCurrent
		,GETUTCDATE()												AS InsertDateTime
	FROM	 [qcic].[SGA_Dim_PlayerCreditLimit_History];
-- testing  select * from  [tqcic].[udf_SGA_ETL06] ( )

CREATE   FUNCTION [tqcic].[udf_SGA_ETL07] ( )
RETURNS TABLE
AS
RETURN



CREATE   FUNCTION [tqcic].[udf_SGA_ETL08] ( )
RETURNS TABLE
AS
RETURN

