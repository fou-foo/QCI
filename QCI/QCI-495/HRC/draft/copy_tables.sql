

CREATE TABLE [test].[HRC_ETL_Runner_Tracker](
	[TimeStamp] [datetime] NOT NULL,
	[TableCompleted] [nvarchar](255) NULL,
	[RecordCount] [int] NOT NULL
) ON [PRIMARY] ;



CREATE TABLE [test].[Dim_Host_SCD](
	[SystemName] [nvarchar](100) NULL,
	[SystemNameInt] [int] NULL,
	[CasinoGroupID] [nvarchar](100) NULL,
	[CasinoGroupIDInt] [int] NULL,
	[HostID] [nvarchar](100) NULL,
	[HostIDBigInt] [bigint] NULL,
	[HostFirstName] [nvarchar](50) NULL,
	[HostLastName] [nvarchar](50) NULL,
	[HostPosition] [nvarchar](100) NULL,
	[HostCode] [nvarchar](50) NULL,
	[StartDateTime] [datetime] NULL,
	[EndDateTime] [datetime] NULL,
	[IsCurrent] [int] NULL,
	[InsertDateTime] [datetime] NULL
) ON [PRIMARY] ;

INSERT INTO [test].[Dim_Host_SCD]
SELECT * FROM  [qcids].[Dim_Host_SCD]; 

----VIEWS 
CREATE VIEW  [test].[uvw_HRC_ETL01_Dim_Host_SCD]  AS (

SELECT 
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'								)				AS SystemName  --parameter SP 
		,TRY_CONVERT(INT			,3									)				AS SystemNameInt --parameter SP 
		,TRY_CONVERT(NVARCHAR(100)	,a.CasinoID							)				AS CasinoGroupID
		,TRY_CONVERT(INT			,b.CasinoIDInt						)				AS CasinoGroupIDInt
		,TRY_CONVERT(NVARCHAR(100)	,EmpID								)				AS HostID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](EmpID,2))		AS HostIDBigInt
		,TRY_CONVERT(NVARCHAR(50)	,LTRIM(RTRIM(ISNULL(FirstName,'NoFirstName'))))		AS HostFirstName
		,TRY_CONVERT(NVARCHAR(50)	,LTRIM(RTRIM(ISNULL(LastName,'NoLastName'))))		AS HostLastName
		,TRY_CONVERT(NVARCHAR(100)	,ISNULL(Position,'NoPosition')		)				AS HostPosition
		,TRY_CONVERT(NVARCHAR(50)	,EmpNum								)				AS HostCode
		,TRY_CONVERT(DATETIME		,InsertDtm							)				AS StartDateTime
		,TRY_CONVERT(DATETIME		,ISNULL(LastUpdatedDtm,'2999-12-31'))				AS EndDateTime
		,TRY_CONVERT(INT			,isCurrent							)				AS IsCurrent
		,GETUTCDATE()																	AS InsertDateTime
FROM	 [qcic].[HRC_Dim_CMPEmployees_History] a WITH (NOLOCK)
JOIN	 [qcids].[App_Casino] b WITH (NOLOCK)
ON		(a.CasinoID = b.CasinoID AND b.SystemNameInt = 3)
WHERE	 isHost = 1 and isInactive = 0 )

CREATE table [test].[uvw_HRC_ETL01_Dim_Host_SCD]  AS (
        SystemName NVARCHAR(100), 
		SystemNameInt INT, 
		CasinoGroupID NVARCHAR(100), 
		CasinoGroupIDInt INT, 
		HostID NVARCHAR(100),
		HostIDBigInt  BIGINT, 
		HostFirstName NVARCHAR(50), 
		HostLastName NVARCHAR(50), 
		HostPosition NVARCHAR(100) , 
		HostCode  NVARCHAR(50), 
		StartDateTime DATETIME, 
		EndDateTime  DATETIME, 
		IsCurrent   INT, 
		InsertDateTime DATETIME);

INSERT INTO [test].[uvw_HRC_ETL01_Dim_Host_SCD] 
SELECT 
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'								)				AS SystemName  --parameter SP 
		,TRY_CONVERT(INT			,3									)				AS SystemNameInt --parameter SP 
		,TRY_CONVERT(NVARCHAR(100)	,a.CasinoID							)				AS CasinoGroupID
		,TRY_CONVERT(INT			,b.CasinoIDInt						)				AS CasinoGroupIDInt
		,TRY_CONVERT(NVARCHAR(100)	,EmpID								)				AS HostID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](EmpID,2))		AS HostIDBigInt
		,TRY_CONVERT(NVARCHAR(50)	,LTRIM(RTRIM(ISNULL(FirstName,'NoFirstName'))))		AS HostFirstName
		,TRY_CONVERT(NVARCHAR(50)	,LTRIM(RTRIM(ISNULL(LastName,'NoLastName'))))		AS HostLastName
		,TRY_CONVERT(NVARCHAR(100)	,ISNULL(Position,'NoPosition')		)				AS HostPosition
		,TRY_CONVERT(NVARCHAR(50)	,EmpNum								)				AS HostCode
		,TRY_CONVERT(DATETIME		,InsertDtm							)				AS StartDateTime
		,TRY_CONVERT(DATETIME		,ISNULL(LastUpdatedDtm,'2999-12-31'))				AS EndDateTime
		,TRY_CONVERT(INT			,isCurrent							)				AS IsCurrent
		,GETUTCDATE()																	AS InsertDateTime
FROM	 [qcic].[HRC_Dim_CMPEmployees_History] a WITH (NOLOCK)
JOIN	 [qcids].[App_Casino] b WITH (NOLOCK)
ON		(a.CasinoID = b.CasinoID AND b.SystemNameInt = 3)
WHERE	 isHost = 1 and isInactive = 0 ;


-----------


CREATE TABLE [test].[Dim_Player_SCD](
	[SystemName] [nvarchar](100) NOT NULL,
	[SystemNameInt] [int] NOT NULL,
	[PlayerID] [nvarchar](100) NOT NULL,
	[PlayerIDBigInt] [bigint] NOT NULL,
	[Account] [nvarchar](100) NOT NULL,
	[Title] [nvarchar](20) NULL,
	[LastName] [nvarchar](50) NULL,
	[FirstName] [nvarchar](50) NULL,
	[DisplayName] [nvarchar](50) NULL,
	[NickNameAlias] [nvarchar](50) NULL,
	[HomeAddress1] [nvarchar](100) NULL,
	[HomeAddress2] [nvarchar](100) NULL,
	[HomeCity] [nvarchar](100) NULL,
	[HomeStateCode] [nvarchar](50) NULL,
	[HomeCountryCode] [nvarchar](50) NULL,
	[HomePostalCode] [nvarchar](20) NULL,
	[HomeTelephone1Type] [nvarchar](20) NULL,
	[HomeTelephone1] [nvarchar](20) NULL,
	[HomeTelephone2Type] [nvarchar](20) NULL,
	[HomeTelephone2] [nvarchar](20) NULL,
	[HomeEmail] [nvarchar](255) NULL,
	[AltName] [nvarchar](50) NULL,
	[AltAddress1] [nvarchar](100) NULL,
	[AltAddress2] [nvarchar](100) NULL,
	[AltCity] [nvarchar](100) NULL,
	[AltStateCode] [nvarchar](50) NULL,
	[AltCountryCode] [nvarchar](50) NULL,
	[AltPostalCode] [nvarchar](20) NULL,
	[AltTelephone1Type] [nvarchar](20) NULL,
	[AltTelephone1] [nvarchar](20) NULL,
	[AltTelephone2Type] [nvarchar](20) NULL,
	[AltTelephone2] [nvarchar](20) NULL,
	[AltEmail] [nvarchar](255) NULL,
	[BirthDate] [date] NULL,
	[IsPrivacy] [int] NULL,
	[IsMailToAlt] [int] NULL,
	[IsNoMail] [int] NULL,
	[IsReturnMail] [int] NULL,
	[IsVIP] [int] NULL,
	[IsLostAndFound] [int] NULL,
	[IsCreditAccount] [int] NULL,
	[IsBanned] [int] NULL,
	[IsInactive] [int] NULL,
	[IsCall] [int] NULL,
	[IsEmailSend] [int] NULL,
	[SetupCasinoID] [nvarchar](100) NULL,
	[SetupEmployeeID] [nvarchar](100) NULL,
	[SetupDate] [date] NULL,
	[Gender] [nvarchar](50) NULL,
	[MailingAddress1] [nvarchar](100) NULL,
	[MailingAddress2] [nvarchar](100) NULL,
	[MailingCity] [nvarchar](100) NULL,
	[MailingStateCode] [nvarchar](50) NULL,
	[MailingCountryCode] [nvarchar](50) NULL,
	[MailingPostalCode] [nvarchar](20) NULL,
	[Address_MatchType] [varchar](20) NULL,
	[Address_Latitude] [real] NULL,
	[Address_Longitude] [real] NULL,
	[StartDateTime] [datetime] NOT NULL,
	[EndDateTime] [datetime] NULL,
	[IsCurrent] [int] NOT NULL,
	[InsertDateTime] [datetime] NULL,
PRIMARY KEY NONCLUSTERED 
(
	[SystemNameInt] ASC,
	[PlayerIDBigInt] ASC,
	[IsCurrent] ASC,
	[StartDateTime] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY];

INSERT INTO [test].[Dim_Player_SCD]
select * from [qcids].[Dim_Player_SCD];

--segunda vista 
CREATE VIEW [test].[uvw_HRC_ETL02_Dim_Player_SCD]  AS
SELECT 
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'					)							AS SystemName --parameter
		,TRY_CONVERT(INT			,3						)							AS SystemNameInt --parameter
		,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)							AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](PlayerID,1))	AS PlayerIDBigInt
		,TRY_CONVERT(NVARCHAR(100)	,Acct					)							AS Account
		,TRY_CONVERT(NVARCHAR(20)	,title					)							AS Title
		,TRY_CONVERT(NVARCHAR(50)	,LTRIM(RTRIM(lastname))	)							AS LastName
		,TRY_CONVERT(NVARCHAR(50)	,LTRIM(RTRIM(firstname)))							AS FirstName
		,TRY_CONVERT(NVARCHAR(50)	,ISNULL(displayname,concat(firstname,' ',lastname)))AS DisplayName
		,TRY_CONVERT(NVARCHAR(50)	,NickNameAlias			)							AS NickNameAlias
		,TRY_CONVERT(NVARCHAR(100)	,HomeAddr01				)							AS HomeAddress1
		,TRY_CONVERT(NVARCHAR(100)	,HomeAddr02				)							AS HomeAddress2
		,TRY_CONVERT(NVARCHAR(100)	,HomeCity				)							AS HomeCity
		,TRY_CONVERT(NVARCHAR(50)	,HomeStateCode			)							AS HomeStateCode
		,TRY_CONVERT(NVARCHAR(50)	,HomeCountryCode		)							AS HomeCountryCode
		,TRY_CONVERT(NVARCHAR(20)	,HomePostalCode			)							AS HomePostalCode
		,TRY_CONVERT(NVARCHAR(20)	,HomeTel01Type			)							AS HomeTelephone1Type
		,TRY_CONVERT(NVARCHAR(20)	,HomeTel01				)							AS HomeTelephone1
		,TRY_CONVERT(NVARCHAR(20)	,HomeTel02Type			)							AS HomeTelephone2Type
		,TRY_CONVERT(NVARCHAR(20)	,HomeTel02				)							AS HomeTelephone2
		,TRY_CONVERT(NVARCHAR(255)	,HomeEmail				)							AS HomeEmail
		,TRY_CONVERT(NVARCHAR(50)	,AltName				)							AS AltName
		,TRY_CONVERT(NVARCHAR(100)	,AltAddr01				)							AS AltAddress1
		,TRY_CONVERT(NVARCHAR(100)	,AltAddr02				)							AS AltAddress2
		,TRY_CONVERT(NVARCHAR(100)	,AltCity				)							AS AltCity
		,TRY_CONVERT(NVARCHAR(50)	,AltStateCode			)							AS AltStateCode
		,TRY_CONVERT(NVARCHAR(50)	,AltCountryCode			)							AS AltCountryCode
		,TRY_CONVERT(NVARCHAR(20)	,AltPostalCode			)							AS AltPostalCode
		,TRY_CONVERT(NVARCHAR(20)	,AltTel01Type			)							AS AltTelephone1Type
		,TRY_CONVERT(NVARCHAR(20)	,AltTel01				)							AS AltTelephone1
		,TRY_CONVERT(NVARCHAR(20)	,AltTel02Type			)							AS AltTelephone2Type
		,TRY_CONVERT(NVARCHAR(20)	,AltTel02				)							AS AltTelephone2
		,TRY_CONVERT(NVARCHAR(255)	,AltEmail				)							AS AltEmail
		,TRY_CONVERT(DATE			,BirthDT				)							AS BirthDate
		,TRY_CONVERT(INT			,isPrivacy				)							AS IsPrivacy
		,TRY_CONVERT(INT			,isMailToAlt			)							AS IsMailToAlt
		,TRY_CONVERT(INT			,1 - IsMailPlayer		)							AS IsNoMail
		,TRY_CONVERT(INT			,isReturnMail			)							AS IsReturnMail
		,TRY_CONVERT(INT			,isVIP					)							AS IsVIP
		,TRY_CONVERT(INT			,isLostAndFound			)							AS IsLostAndFound
		,TRY_CONVERT(INT			,isCreditAcct			)							AS IsCreditAccount
		,TRY_CONVERT(INT			,isBanned				)							AS IsBanned
		,TRY_CONVERT(INT			,isInactive				)							AS IsInactive
		,TRY_CONVERT(INT			,IsCallPlayer			)							AS IsCall
		,TRY_CONVERT(INT			,IsEmailPlayer			)							AS IsEmailSend
		,TRY_CONVERT(NVARCHAR(100)	,SetupCasinoID			)							AS SetupCasinoID
		,TRY_CONVERT(NVARCHAR(100)	,SetupEmpID				)							AS SetupEmployeeID
		,TRY_CONVERT(DATE			,SetupDTM				)							AS SetupDate
		,TRY_CONVERT(NVARCHAR(50)	,Gender					)							AS Gender
		,TRY_CONVERT(NVARCHAR(100)	,MailingAddr01			)							AS MailingAddress1
		,TRY_CONVERT(NVARCHAR(100)	,MailingAddr02			)							AS MailingAddress2
		,TRY_CONVERT(NVARCHAR(100)	,MailingCity			)							AS MailingCity
		,TRY_CONVERT(NVARCHAR(50)	,MailingStateCode		)							AS MailingStateCode
		,TRY_CONVERT(NVARCHAR(50)	,MailingCountryCode		)							AS MailingCountryCode
		,TRY_CONVERT(NVARCHAR(20)	,MailingPostalCode		)							AS MailingPostalCode
		,TRY_CONVERT(NVARCHAR(20)	,'GeoCode'				)							AS Address_MatchType
		,TRY_CONVERT(REAL			,MailingLatitude		)							AS Address_Latitude
		,TRY_CONVERT(REAL			,MailingLongitude		)							AS Address_Longitude
		,TRY_CONVERT(DATETIME		,StartGamingDt			)							AS StartDateTime
		,TRY_CONVERT(DATETIME		,EndGamingDt			)							AS EndDateTime
		,TRY_CONVERT(INT			,isCurrent				)							AS IsCurrent
		,GETUTCDATE()																	AS InsertDateTime
FROM	 [qcic].[HRC_Dim_MarketablePlayersAttribute] WITH (NOLOCK)
WHERE	 isBanned = 0;

---


CREATE table [test].[uvw_HRC_ETL02_Dim_Player_SCD]  ( SystemName  NVARCHAR(100), SystemNameInt INT, PlayerID NVARCHAR(100), PlayerIDBigInt BIGINT, Account NVARCHAR(100), Title NVARCHAR(20), 
  LastName NVARCHAR(50), FirstName NVARCHAR(50), DisplayName NVARCHAR(50), NickNameAlias NVARCHAR(50), HomeAddress1 NVARCHAR(100), HomeAddress2 NVARCHAR(100)   , 
  HomeCity NVARCHAR(100), HomeStateCode NVARCHAR(50) , HomeCountryCode NVARCHAR(50), HomePostalCode NVARCHAR(20), HomeTelephone1Type NVARCHAR(20), HomeTelephone1 NVARCHAR(20), 
  HomeTelephone2Type NVARCHAR(20), HomeTelephone2  NVARCHAR(20), HomeEmail NVARCHAR(255), AltName NVARCHAR(50), AltAddress1 NVARCHAR(100),  AltAddress2 NVARCHAR(100), 
  AltCity NVARCHAR(100), AltStateCode NVARCHAR(50), AltTelephone1Type NVARCHAR(20), AltTelephone1 NVARCHAR(20), AltTelephone2Type NVARCHAR(20), AltTelephone2 NVARCHAR(20), 
  AltEmail NVARCHAR(20), BirthDate DATE, IsPrivacy INT, IsMailToAlt INT, IsNoMail INT, IsReturnMail INT, IsVIP INT, IsLostAndFound INT, IsCreditAccount INT, 
  IsBanned INT, IsInactive INT, IsCall INT, IsEmailSend INT, SetupCasinoID NVARCHAR(100), 
  SetupEmployeeID NVARCHAR(100), SetupDate DATE, Gender NVARCHAR(50) , MailingAddress1 NVARCHAR(100), MailingAddress2 NVARCHAR(100), MailingCity NVARCHAR(100), 
MailingStateCode NVARCHAR(50), MailingCountryCode NVARCHAR(50), MailingPostalCode NVARCHAR(20), Address_MatchType NVARCHAR(20), Address_Latitude REAL, 
Address_Longitude REAL, StartDateTime DATETIME , EndDateTime DATETIME, IsCurrent INT, InsertDateTime DATETIME );

INSERT INTO [test].[uvw_HRC_ETL02_Dim_Player_SCD] 
SELECT 
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'					)							AS SystemName --param
		,TRY_CONVERT(INT			,3						)							AS SystemNameInt --param
		,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)							AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](PlayerID,1))	AS PlayerIDBigInt
		,TRY_CONVERT(NVARCHAR(100)	,Acct					)							AS Account
		,TRY_CONVERT(NVARCHAR(20)	,title					)							AS Title
		,TRY_CONVERT(NVARCHAR(50)	,LTRIM(RTRIM(lastname))	)							AS LastName
		,TRY_CONVERT(NVARCHAR(50)	,LTRIM(RTRIM(firstname)))							AS FirstName
		,TRY_CONVERT(NVARCHAR(50)	,ISNULL(displayname,concat(firstname,' ',lastname)))AS DisplayName
		,TRY_CONVERT(NVARCHAR(50)	,NickNameAlias			)							AS NickNameAlias
		,TRY_CONVERT(NVARCHAR(100)	,HomeAddr01				)							AS HomeAddress1
		,TRY_CONVERT(NVARCHAR(100)	,HomeAddr02				)							AS HomeAddress2
		,TRY_CONVERT(NVARCHAR(100)	,HomeCity				)							AS HomeCity
		,TRY_CONVERT(NVARCHAR(50)	,HomeStateCode			)							AS HomeStateCode
		,TRY_CONVERT(NVARCHAR(50)	,HomeCountryCode		)							AS HomeCountryCode
		,TRY_CONVERT(NVARCHAR(20)	,HomePostalCode			)							AS HomePostalCode
		,TRY_CONVERT(NVARCHAR(20)	,HomeTel01Type			)							AS HomeTelephone1Type
		,TRY_CONVERT(NVARCHAR(20)	,HomeTel01				)							AS HomeTelephone1
		,TRY_CONVERT(NVARCHAR(20)	,HomeTel02Type			)							AS HomeTelephone2Type
		,TRY_CONVERT(NVARCHAR(20)	,HomeTel02				)							AS HomeTelephone2
		,TRY_CONVERT(NVARCHAR(255)	,HomeEmail				)							AS HomeEmail
		,TRY_CONVERT(NVARCHAR(50)	,AltName				)							AS AltName
		,TRY_CONVERT(NVARCHAR(100)	,AltAddr01				)							AS AltAddress1
		,TRY_CONVERT(NVARCHAR(100)	,AltAddr02				)							AS AltAddress2
		,TRY_CONVERT(NVARCHAR(100)	,AltCity				)							AS AltCity
		,TRY_CONVERT(NVARCHAR(50)	,AltStateCode			)							AS AltStateCode
		,TRY_CONVERT(NVARCHAR(50)	,AltCountryCode			)							AS AltCountryCode
		,TRY_CONVERT(NVARCHAR(20)	,AltPostalCode			)							AS AltPostalCode
		,TRY_CONVERT(NVARCHAR(20)	,AltTel01Type			)							AS AltTelephone1Type
		,TRY_CONVERT(NVARCHAR(20)	,AltTel01				)							AS AltTelephone1
		,TRY_CONVERT(NVARCHAR(20)	,AltTel02Type			)							AS AltTelephone2Type
		,TRY_CONVERT(NVARCHAR(20)	,AltTel02				)							AS AltTelephone2
		,TRY_CONVERT(NVARCHAR(255)	,AltEmail				)							AS AltEmail
		,TRY_CONVERT(DATE			,BirthDT				)							AS BirthDate
		,TRY_CONVERT(INT			,isPrivacy				)							AS IsPrivacy
		,TRY_CONVERT(INT			,isMailToAlt			)							AS IsMailToAlt
		,TRY_CONVERT(INT			,1 - IsMailPlayer		)							AS IsNoMail
		,TRY_CONVERT(INT			,isReturnMail			)							AS IsReturnMail
		,TRY_CONVERT(INT			,isVIP					)							AS IsVIP
		,TRY_CONVERT(INT			,isLostAndFound			)							AS IsLostAndFound
		,TRY_CONVERT(INT			,isCreditAcct			)							AS IsCreditAccount
		,TRY_CONVERT(INT			,isBanned				)							AS IsBanned
		,TRY_CONVERT(INT			,isInactive				)							AS IsInactive
		,TRY_CONVERT(INT			,IsCallPlayer			)							AS IsCall
		,TRY_CONVERT(INT			,IsEmailPlayer			)							AS IsEmailSend
		,TRY_CONVERT(NVARCHAR(100)	,SetupCasinoID			)							AS SetupCasinoID
		,TRY_CONVERT(NVARCHAR(100)	,SetupEmpID				)							AS SetupEmployeeID
		,TRY_CONVERT(DATE			,SetupDTM				)							AS SetupDate
		,TRY_CONVERT(NVARCHAR(50)	,Gender					)							AS Gender
		,TRY_CONVERT(NVARCHAR(100)	,MailingAddr01			)							AS MailingAddress1
		,TRY_CONVERT(NVARCHAR(100)	,MailingAddr02			)							AS MailingAddress2
		,TRY_CONVERT(NVARCHAR(100)	,MailingCity			)							AS MailingCity
		,TRY_CONVERT(NVARCHAR(50)	,MailingStateCode		)							AS MailingStateCode
		,TRY_CONVERT(NVARCHAR(50)	,MailingCountryCode		)							AS MailingCountryCode
		,TRY_CONVERT(NVARCHAR(20)	,MailingPostalCode		)							AS MailingPostalCode
		,TRY_CONVERT(NVARCHAR(20)	,'GeoCode'				)							AS Address_MatchType
		,TRY_CONVERT(REAL			,MailingLatitude		)							AS Address_Latitude
		,TRY_CONVERT(REAL			,MailingLongitude		)							AS Address_Longitude
		,TRY_CONVERT(DATETIME		,StartGamingDt			)							AS StartDateTime
		,TRY_CONVERT(DATETIME		,EndGamingDt			)							AS EndDateTime
		,TRY_CONVERT(INT			,isCurrent				)							AS IsCurrent
		,GETUTCDATE()																	AS InsertDateTime
FROM	 [qcic].[HRC_Dim_MarketablePlayersAttribute] WITH (NOLOCK)
WHERE	 isBanned = 0
----------------------


CREATE TABLE [test].[Dim_PlayerDistanceToCasino_SCD](
	[SystemName] [nvarchar](100) NOT NULL,
	[SystemNameInt] [int] NOT NULL,
	[CasinoID] [nvarchar](100) NOT NULL,
	[CasinoIDInt] [int] NOT NULL,
	[PlayerID] [nvarchar](100) NOT NULL,
	[PlayerIDBigInt] [bigint] NOT NULL,
	[PlayerDistanceToCasino] [float] NULL,
	[StartDateTime] [date] NOT NULL,
	[EndDateTime] [datetime] NULL,
	[IsCurrent] [int] NOT NULL,
	[InsertDateTime] [datetime] NULL,
PRIMARY KEY NONCLUSTERED 
(
	[SystemNameInt] ASC,
	[PlayerIDBigInt] ASC,
	[CasinoIDInt] ASC,
	[IsCurrent] ASC,
	[StartDateTime] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY];

INSERT INTO [test].[Dim_PlayerDistanceToCasino_SCD]
select * from [qcids].[Dim_PlayerDistanceToCasino_SCD];


CREATE VIEW [test].[uvw_HRC_ETL03_Dim_PlayerDistanceToCasino_SCD]  AS (

SELECT	 	
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'					)							AS SystemName --param
		,TRY_CONVERT(INT			,3						)							AS SystemNameInt --param
		,TRY_CONVERT(NVARCHAR(100)	,b.CasinoID				)							AS CasinoID
		,TRY_CONVERT(INT			,b.CasinoIDInt			)							AS CasinoIDInt
		,TRY_CONVERT(NVARCHAR(100)	,a.PlayerID				)							AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](a.PlayerID,1))	AS PlayerIDBigInt
		,TRY_CONVERT(FLOAT			,DistanceToProperty		)							AS PlayerDistanceToCasino
		,TRY_CONVERT(DATETIME		,a.StartGamingDt		)							AS StartDateTime
		,TRY_CONVERT(DATETIME		,a.EndGamingDt			)							AS EndDateTime
		,TRY_CONVERT(INT			,a.IsCurrent			)							AS IsCurrent
		,GETUTCDATE()																	AS InsertDateTime
FROM	 [qcic].[HRC_Dim_MarketablePlayersAttribute] a WITH (NOLOCK)
CROSS JOIN (
		SELECT	 SystemName
				,SystemNameInt
				,CasinoID
				,CasinoIDInt
		FROM	 [qcids].[App_Casino] WITH (NOLOCK)
		WHERE	 SystemNameInt = 3
		GROUP BY SystemName
				,SystemNameInt
				,CasinoID
				,CasinoIDInt
) b
WHERE	 a.isBanned = 0 AND b.SystemName = 'HRC'); --param

CREATE TABLE [test].[uvw_HRC_ETL03_Dim_PlayerDistanceToCasino_SCD] ( 
SystemName NVARCHAR(100), SystemNameInt INT, CasinoID NVARCHAR(100), CasinoIDInt INT, PlayerID NVARCHAR(100), PlayerIDBigInt BIGINT, 
PlayerDistanceToCasino FLOAT, StartDateTime DATETIME , EndDateTime DATETIME, IsCurrent INT, InsertDateTime DATETIME) ; 

INSERT  INTO  [test].[uvw_HRC_ETL03_Dim_PlayerDistanceToCasino_SCD] 
SELECT	 	
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'					)							AS SystemName --param
		,TRY_CONVERT(INT			,3						)							AS SystemNameInt --param
		,TRY_CONVERT(NVARCHAR(100)	,b.CasinoID				)							AS CasinoID
		,TRY_CONVERT(INT			,b.CasinoIDInt			)							AS CasinoIDInt
		,TRY_CONVERT(NVARCHAR(100)	,a.PlayerID				)							AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](a.PlayerID,1))	AS PlayerIDBigInt
		,TRY_CONVERT(FLOAT			,DistanceToProperty		)							AS PlayerDistanceToCasino
		,TRY_CONVERT(DATETIME		,a.StartGamingDt		)							AS StartDateTime
		,TRY_CONVERT(DATETIME		,a.EndGamingDt			)							AS EndDateTime
		,TRY_CONVERT(INT			,a.IsCurrent			)							AS IsCurrent
		,GETUTCDATE()																	AS InsertDateTime
FROM	 [qcic].[HRC_Dim_MarketablePlayersAttribute] a WITH (NOLOCK)
CROSS JOIN (
		SELECT	 SystemName
				,SystemNameInt
				,CasinoID
				,CasinoIDInt
		FROM	 [test].[App_Casino] WITH (NOLOCK)
		WHERE	 SystemNameInt = 3
		GROUP BY SystemName
				,SystemNameInt
				,CasinoID
				,CasinoIDInt
) b
WHERE	 a.isBanned = 0 AND b.SystemName = 'HRC'; --param
-------------------------

--drop table [test].[Dim_PlayerLoyalty_SCD]
CREATE TABLE [test].[Dim_PlayerLoyalty_SCD](
	[SystemName] [nvarchar](100) NULL,
	[SystemNameInt] [int] NOT NULL,
	[PlayerID] [nvarchar](100) NULL,
	[PlayerIDBigInt] [bigint] NOT NULL,
	[LoyaltyCardLevel] [nvarchar](100) NULL,
	[StartDateTime] [datetime] NOT NULL,
	[EndDateTime] [datetime] NULL,
	[IsCurrent] [int] NOT NULL,
	[InsertDateTime] [datetime] NULL,
PRIMARY KEY NONCLUSTERED 
(
	[SystemNameInt] ASC,
	[PlayerIDBigInt] ASC,
	[IsCurrent] ASC,
	[StartDateTime] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY];

INSERT INTO [test].[Dim_PlayerLoyalty_SCD]
SELECT * from [qcids].[Dim_PlayerLoyalty_SCD];


----

CREATE VIEW [test].[uvw_HRC_ETL04_Dim_PlayerLoyalty_SCD]  AS

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
FROM	 [qcic].[HRC_Dim_MarketablePlayersAttribute] WITH (NOLOCK);

CREATE TABLE [test].[uvw_HRC_ETL04_Dim_PlayerLoyalty_SCD]  ( 
SystemName NVARCHAR(100), SystemNameInt INT , PlayerID NVARCHAR(100), PlayerIDBigInt BIGINT, LoyaltyCardLevel NVARCHAR(100), 
StartDateTime DATETIME, EndDateTime DATETIME, IsCurrent INT , InsertDateTime DATETIME
);

INSERT INTO [test].[uvw_HRC_ETL04_Dim_PlayerLoyalty_SCD] 
SELECT
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'					)							AS SystemName --param
		,TRY_CONVERT(INT			,3						)							AS SystemNameInt --param
		,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)							AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](PlayerID,1))	AS PlayerIDBigInt
		,TRY_CONVERT(NVARCHAR(100)	,P_ClubStatus			)							AS LoyaltyCardLevel
		,TRY_CONVERT(DATETIME		,StartGamingDt			)							AS StartDateTime
		,TRY_CONVERT(DATETIME		,EndGamingDt			)							AS EndDateTime
		,TRY_CONVERT(INT			,isCurrent				)							AS IsCurrent
		,GETUTCDATE()																	AS InsertDateTime
FROM	 [qcic].[HRC_Dim_MarketablePlayersAttribute] WITH (NOLOCK) ; 
-----



CREATE TABLE [test].[Dim_PlayerHost_SCD](
	[SystemName] [nvarchar](100) NULL,
	[SystemNameInt] [int] NULL,
	[CasinoID] [nvarchar](100) NULL,
	[CasinoIDInt] [int] NULL,
	[PlayerID] [nvarchar](100) NOT NULL,
	[PlayerIDBigInt] [bigint] NULL,
	[HostID] [nvarchar](100) NULL,
	[HostIDBigInt] [bigint] NULL,
	[PlayerHostType] [nvarchar](100) NULL,
	[StartDateTime] [datetime] NULL,
	[EndDateTime] [datetime] NULL,
	[IsCurrent] [int] NULL,
	[InsertDateTime] [datetime] NULL
) ON [PRIMARY]; 
insert into [test].[Dim_PlayerHost_SCD]
select * from [qcids].[Dim_PlayerHost_SCD];


CREATE VIEW [test].[uvw_HRC_ETL05_Dim_PlayerHost_SCD]  AS

SELECT	 	
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'					)		AS SystemName
		,TRY_CONVERT(INT			,3						)		AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,a.CasinoID				)		AS CasinoID
		,TRY_CONVERT(INT			,b.CasinoIDInt			)		AS CasinoIDInt
		,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)		AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](PlayerID,1)) AS PlayerIDBigInt
		,TRY_CONVERT(NVARCHAR(100)	,HostEmpID				)		AS HostID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](HostEmpID,2)) AS HostIDBigInt
		,TRY_CONVERT(NVARCHAR(100)	,'Primary'				)		AS PlayerHostType
		,TRY_CONVERT(DATETIME		,HostStartDTM			)		AS StartDateTime
		,TRY_CONVERT(DATETIME		,HostEndDTM				)		AS EndDateTime
		,TRY_CONVERT(INT			,isCurrent				)		AS IsCurrent
		,GETUTCDATE()												AS InsertDateTime
FROM	 [qcic].[HRC_Fact_PlayerHostHistory_Primary] a WITH (NOLOCK)
JOIN	 [qcids].[App_Casino] b WITH (NOLOCK)
ON		(a.CasinoID = b.CasinoID AND b.SystemNameInt = 3)
GROUP BY a.CasinoID
		,b.CasinoIDInt
		,PlayerID
		,HostEmpID
		,HostStartDTM
		,HostEndDTM
		,isCurrent
UNION ALL
SELECT	 	
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'					)		AS SystemName
		,TRY_CONVERT(INT			,3						)		AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,a.CasinoID				)		AS CasinoID
		,TRY_CONVERT(INT			,b.CasinoIDInt			)		AS CasinoIDInt
		,TRY_CONVERT(NVARCHAR(100)	,PlayerID				)		AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](PlayerID,1)) AS PlayerIDBigInt
		,TRY_CONVERT(NVARCHAR(100)	,HostEmpID				)		AS HostID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](HostEmpID,2)) AS HostIDBigInt
		,TRY_CONVERT(NVARCHAR(100)	,'Secondary'			)		AS PlayerHostType
		,TRY_CONVERT(DATETIME		,HostStartDTM			)		AS StartDateTime
		,TRY_CONVERT(DATETIME		,HostEndDTM				)		AS EndDateTime
		,TRY_CONVERT(INT			,isCurrent				)		AS IsCurrent
		,GETUTCDATE()												AS InsertDateTime
FROM	 [qcic].[HRC_Fact_PlayerHostHistory_Secondary] a WITH (NOLOCK)
JOIN	 [qcids].[App_Casino] b WITH (NOLOCK)
ON		(a.CasinoID = b.CasinoID AND b.SystemNameInt = 3)
GROUP BY a.CasinoID
		,b.CasinoIDInt
		,PlayerID
		,HostEmpID
		,HostStartDTM
		,HostEndDTM
		,isCurrent;
-------

CREATE TABLE [test].[Dim_PlayerCredit_SCD](
	[SystemName] [nvarchar](100) NULL,
	[SystemNameInt] [int] NULL,
	[CasinoID] [nvarchar](100) NULL,
	[CasinoIDInt] [int] NULL,
	[PlayerID] [nvarchar](100) NULL,
	[PlayerIDBigInt] [bigint] NULL,
	[CreditLimitInt100] [int] NULL,
	[IsActiveCredit] [int] NULL,
	[SetupCasinoID] [nvarchar](100) NULL,
	[Note] [nvarchar](4000) NULL,
	[StartDateTime] [datetime] NULL,
	[EndDateTime] [datetime] NULL,
	[IsCurrent] [int] NULL,
	[InsertDateTime] [datetime] NULL
) ON [PRIMARY];

insert into  [test].[Dim_PlayerCredit_SCD]
select * from  [qcids].[Dim_PlayerCredit_SCD];


---
CREATE VIEW [test].[uvw_HRC_ETL06_Dim_PlayerCredit_SCD]  AS

SELECT	 	
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'					)		AS SystemName
		,TRY_CONVERT(INT			,3						)		AS SystemNameInt
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
FROM	 [qcic].[HRC_Dim_PlayerCreditLimit_History] a WITH (NOLOCK)
JOIN	 [qcids].[App_Casino] b WITH (NOLOCK)
ON		(a.TranCasinoID = b.CasinoID AND b.SystemNameInt = 3); 

----------------------------------
CREATE TABLE [test].[Dim_PlayerCreditElectronic_SCD](
	[SystemName] [nvarchar](100) NULL,
	[SystemNameInt] [int] NULL,
	[CasinoID] [nvarchar](100) NULL,
	[CasinoIDInt] [int] NULL,
	[PlayerID] [nvarchar](100) NULL,
	[PlayerIDBigInt] [bigint] NULL,
	[ElectronicCreditType] [nvarchar](100) NULL,
	[IsEnrolled] [int] NULL,
	[ElectronicCreditAccountID] [nvarchar](100) NULL,
	[SignatureID] [nvarchar](100) NULL,
	[CreatedByID] [nvarchar](100) NULL,
	[Note] [nvarchar](4000) NULL,
	[StartDateTime] [datetime] NULL,
	[EndDateTime] [datetime] NULL,
	[IsCurrent] [int] NULL,
	[InsertDateTime] [datetime] NULL
) ON [PRIMARY]

insert into  [test].[Dim_PlayerCreditElectronic_SCD]
select * 
from  [qcids].[Dim_PlayerCreditElectronic_SCD]


CREATE VIEW [test].[uvw_HRC_ETL07_Dim_PlayerCreditElectronic_SCD]  AS
SELECT	 SystemName
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
		,EndDateTime = LEAD(StartDateTime, 1, {d '2999-12-31'}) OVER 
				(
				   PARTITION BY	 SystemName
								,CasinoID
								,PlayerID
								,ElectronicCreditType
				   ORDER BY StartDateTime
			    )
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
				FROM	 [qcic].[HRC_Dim_PlayerECreditEnrollment_History] WITH (NOLOCK)
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
				FROM	 [qcic].[HRC_Dim_PlayerECreditEnrollment_History] WITH (NOLOCK)
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
				FROM	 [qcic].[HRC_Dim_PlayerECreditEnrollment_History] WITH (NOLOCK)
				WHERE	 EnrollOption IN (0,1)
				) a
		JOIN	 [qcids].[App_Casino] b WITH (NOLOCK)
		ON		(a.CasinoID = b.CasinoID AND b.SystemNameInt = 3)
		) x ;




---
CREATE TABLE [test].[Fact_PlayerDeviceRatingDay]( 	[SystemName] [nvarchar](100) NULL, [SystemNameInt] [int] NULL, [CasinoID] [nvarchar](100) NULL, [CasinoIDInt] [int] NULL,
	[DeviceID] [nvarchar](100) NULL, [DeviceIDBigInt] [bigint] NULL, [PlayerID] [nvarchar](100) NULL, [PlayerIDBigInt] [bigint] NULL, [GamingDate] [date] NULL,
	[StartDateTime] [datetime] NULL, [EndDateTime] [datetime] NULL, [CoinInInt100] [bigint] NULL, [CoinOutInt100] [bigint] NULL, [ActualWinWithFreeplayInt100] [int] NULL,
	[ActualWinNetFreeplayInt100] [int] NULL, [NumberGamesPlayed] [int] NULL, [JackpotInt100] [int] NULL, [FreeplayInt100] [int] NULL, [TheoWinWithFreeplayInt100] [int] NULL,
	[TheoWinNetFreeplayInt100] [int] NULL, [TimePlayedInt100] [int] NULL, [CashInInt100] [bigint] NULL, [ChipsInInt100] [bigint] NULL,
	[CreditInInt100] [bigint] NULL, [WalkedWithInt100] [bigint] NULL, [PlayerZipCode] [varchar](100) NULL, [PlayerGender] [nvarchar](50) NULL,
	[PlayerEnrollmentDate] [date] NULL, [PlayerBirthDate] [date] NULL, [PlayerAddressDistance] [float] NULL, [GameTheme] [nvarchar](100) NULL,
	[GameThemeCount] [nvarchar](100) NULL, [GameTheoHold] [float] NULL, [DeviceTheoHoldPercent] [float] NULL, [DeviceManufacturer] [nvarchar](200) NULL,
	[Denomination] [float] NULL, [DeviceGameType] [nvarchar](200) NULL, [DeviceCabinetType] [nvarchar](100) NULL, [DeviceCabinetModel] [nvarchar](200) NULL,
	[DeviceLocation] [nvarchar](20) NULL, [DeviceLocationArea] [nvarchar](20) NULL, [DeviceLocationBank] [nvarchar](20) NULL, [DeviceLocationStand] [nvarchar](20) NULL,
	[DeviceStartDate] [date] NULL, [DeviceSerial] [nvarchar](50) NULL, [DeviceGameClass] [int] NULL, [DeviceLeaseFeeInt100] [int] NULL,
	[DeviceLeaseAttributes] [nvarchar](4000) NULL, [DeviceExtraAttributes] [nvarchar](4000) NULL, 	[IsSlotDevice] [int] NULL, [IsTableDevice] [int] NULL,
	[IsPokerDevice] [int] NULL, [IsOtherDevice] [int] NULL,	[IsActiveDevice] [int] NULL, [DeviceLocationBigInt] [bigint] NULL, 	[InsertDateTime] [datetime] NULL
) ON [PRIMARY];

CREATE NONCLUSTERED INDEX [Fact_PlayerDeviceRatingDay_ETL_Runner_1]
ON [test].[Fact_PlayerDeviceRatingDay] ([SystemNameInt])
INCLUDE ([GamingDate]);

CREATE NONCLUSTERED INDEX [Fact_PlayerDeviceRatingDay_ETL_Runner_2]
ON [test].[Fact_PlayerDeviceRatingDay] ([SystemNameInt],[GamingDate]);

CREATE NONCLUSTERED INDEX [Fact_PlayerDeviceRatingDay_ETL_Runner_3]
ON [test].[Fact_PlayerDeviceRatingDay] ([SystemNameInt])

insert into [test].[Fact_PlayerDeviceRatingDay]
select * 
from [qcids].[Fact_PlayerDeviceRatingDay];



--------------
CREATE VIEW [test].[uvw_HRC_ETL08_Fact_PlayerDeviceRatingDay]  AS
--Slot Ratings
SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'						)								AS SystemName
		,TRY_CONVERT(INT			,3							)								AS SystemNameInt

		--Primary Key (CasinoID, DeviceID, PlayerID, GamingDate)
		,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId					)								AS CasinoID
		,TRY_CONVERT(INT			,b2.CasinoIDInt				)								AS CasinoIDInt
		,TRY_CONVERT(NVARCHAR(100)	,b.Asset					)								AS DeviceID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](b.Asset,2))			AS DeviceIDBigInt
		,TRY_CONVERT(NVARCHAR(100)	,a.PlayerID					)								AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](a.PlayerID,1))			AS PlayerIDBigInt
		,TRY_CONVERT(DATE			,a.GamingDt					)								AS GamingDate

		--Player Related Metrics
		,TRY_CONVERT(DATETIME		,a.RatingStartDtm			)								AS StartDateTime
		,TRY_CONVERT(DATETIME		,a.RatingEndDtm				)								AS EndDateTime

		--Gaming TOTAL metrics
		,TRY_CONVERT(BIGINT			,ROUND(a.Bet*100,0)			)								AS CoinInInt100
		,TRY_CONVERT(BIGINT			,ROUND(a.PaidOut*100,0)		)								AS CoinOutInt100
		,TRY_CONVERT(INT			,ROUND(a.Actual*100,0)		)								AS ActualWinWithFreeplayInt100
		,TRY_CONVERT(INT			,ROUND(a.Actual*100,0)		)								AS ActualWinNetFreeplayInt100
		,TRY_CONVERT(INT			,a.GamesPlayed				)								AS NumberGamesPlayed
		,TRY_CONVERT(INT			,ROUND(Jackpot*100,0)		)								AS JackpotInt100
		,TRY_CONVERT(INT			,0							)								AS FreeplayInt100
		,TRY_CONVERT(INT			,ROUND(a.Theo*100,0)		)								AS TheoWinWithFreeplayInt100
		,TRY_CONVERT(INT			,ROUND(a.Theo*100,0)		)								AS TheoWinNetFreeplayInt100
		,TRY_CONVERT(INT			,ROUND(MinsPlayed*100,0)	)								AS TimePlayedInt100
		,TRY_CONVERT(BIGINT			,ROUND(CashBuyIn*100,0)		)								AS CashInInt100
		,TRY_CONVERT(BIGINT			,0							)								AS ChipsInInt100
		,TRY_CONVERT(BIGINT			,0							)								AS CreditInInt100
		,TRY_CONVERT(BIGINT			,0							)								AS WalkedWithInt100

		--Player dimension attributes
		,TRY_CONVERT(VARCHAR(100)	 ,p.HomePostalCode			)								AS PlayerZipCode
		,TRY_CONVERT(NVARCHAR(50)	 ,p.Gender					)								AS PlayerGender
		,TRY_CONVERT(DATE			 ,p.SetupDate				)								AS PlayerEnrollmentDate
		,TRY_CONVERT(DATE			 ,p.BirthDate				)								AS PlayerBirthDate
		,TRY_CONVERT(FLOAT			 ,d.PlayerDistanceToCasino	)								AS PlayerAddressDistance																								

		--Game dimension attributes (most of the time there is one game per device, this will only show the MOST COMMON game)
		,TRY_CONVERT(NVARCHAR(100)	,b.SDSGameTheme				)								AS GameTheme	
		,TRY_CONVERT(NVARCHAR(100)	,''							)								AS GameThemeCount	
		,TRY_CONVERT(FLOAT			,a.TheoHold					)								AS GameTheoHold

		--Device dimension attributes
		,TRY_CONVERT(FLOAT			,b.TheoHold					)								AS DeviceTheoHoldPercent
		,TRY_CONVERT(NVARCHAR(200)	,b.Mfg						)								AS DeviceManufacturer
		,TRY_CONVERT(FLOAT			,b.Denom					)								AS Denomination
		,TRY_CONVERT(NVARCHAR(200)	,b.SDSGameType				)								AS DeviceGameType
		,TRY_CONVERT(NVARCHAR(100)	,''							)								AS DeviceCabinetType 
		,TRY_CONVERT(NVARCHAR(200)	,''							)								AS DeviceCabinetModel	
		,TRY_CONVERT(NVARCHAR(20)	,b.Location					)								AS DeviceLocation
		,TRY_CONVERT(NVARCHAR(20)	,LEFT(CONCAT(b.Location,'00'),2))							AS DeviceLocationArea
		,TRY_CONVERT(NVARCHAR(20)	,RIGHT(LEFT(CONCAT(b.Location,'0000'),4),2))				AS DeviceLocationBank
		,TRY_CONVERT(NVARCHAR(20)	,RIGHT(CONCAT('00',b.Location),2))							AS DeviceLocationStand
		,TRY_CONVERT(DATE			,b.StartGamingDt			)								AS DeviceStartDate
		,TRY_CONVERT(NVARCHAR(50)	,b.Serial					)								AS DeviceSerial	
		,TRY_CONVERT(INT			,3							)								AS DeviceGameClass	
		,TRY_CONVERT(INT			,0							)								AS DeviceLeaseFeeInt100	
		,TRY_CONVERT(NVARCHAR(4000)	,''							)								AS DeviceLeaseAttributes	
		,TRY_CONVERT(NVARCHAR(4000)	,''							)								AS DeviceExtraAttributes	
		,TRY_CONVERT(INT			,1							)								AS IsSlotDevice
		,TRY_CONVERT(INT			,0							)								AS IsTableDevice
		,TRY_CONVERT(INT			,0							)								AS IsPokerDevice
		,TRY_CONVERT(INT			,0							)								AS IsOtherDevice
		,TRY_CONVERT(INT			,1							)								AS IsActiveDevice
		,DeviceLocationBigInt = TRY_CONVERT(BIGINT			,CHECKSUM(b.Location))
		,TRY_CONVERT(DATETIME		,GETUTCDATE()				)								AS InsertDateTime
FROM	 [qcic].[HRC_Fact_PlayerSlotRating_Tran] a WITH (NOLOCK)
JOIN	 [qcids].[App_Casino] b2 WITH (NOLOCK)
ON		(a.CasinoID = b2.CasinoID AND b2.SystemNameInt = 3)
LEFT JOIN [qcic].[HRC_Dim_SlotAttribute_History] b WITH (NOLOCK)
ON		(a.SlotHistoryId = b.SlotHistoryId AND a.CasinoId = b.CasinoId)
LEFT JOIN [qcids].[Dim_Player_SCD] p WITH (NOLOCK)
ON		(p.SystemName = 'HRC' AND a.PlayerID = p.PlayerID AND p.StartDateTime <= a.GamingDt AND a.GamingDt < p.EndDateTime)
LEFT JOIN [qcids].[Dim_PlayerDistanceToCasino_SCD] d WITH (NOLOCK)
ON		(d.SystemName = 'HRC' AND a.CasinoId = d.CasinoID AND a.PlayerID = d.PlayerID AND d.StartDateTime <= a.GamingDt AND a.GamingDt < d.EndDateTime)
UNION ALL
--Table Ratings
SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'						)								AS SystemName
		,TRY_CONVERT(INT			,3							)								AS SystemNameInt

		--Primary Key (CasinoID, DeviceID, PlayerID, GamingDate)
		,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId					)								AS CasinoID
		,TRY_CONVERT(INT			,b2.CasinoIDInt				)								AS CasinoIDInt
		,TRY_CONVERT(NVARCHAR(100)	,b.GameId					)								AS DeviceID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](b.GameId,2))			AS DeviceIDBigInt
		,TRY_CONVERT(NVARCHAR(100)	,a.PlayerID					)								AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](a.PlayerID,1))			AS PlayerIDBigInt
		,TRY_CONVERT(DATE			,a.GamingDt					)								AS GamingDate

		--Player Related Metrics
		,TRY_CONVERT(DATETIME		,a.RatingStartDtm			)								AS StartDateTime
		,TRY_CONVERT(DATETIME		,a.RatingEndDtm				)								AS EndDateTime

		--Gaming TOTAL metrics
		,TRY_CONVERT(INT			,ROUND(a.Bet*100,0)			)								AS CoinInInt100
		,TRY_CONVERT(INT			,ROUND(a.PaidOut*100,0)		)								AS CoinOutInt100
		,TRY_CONVERT(INT			,ROUND(a.Actual*100,0)		)								AS ActualWinWithFreeplayInt100
		,TRY_CONVERT(INT			,ROUND(a.Actual*100,0)		)								AS ActualWinNetFreeplayInt100
		,TRY_CONVERT(INT			,a.GamesPlayed				)								AS NumberGamesPlayed
		,TRY_CONVERT(INT			,0							)								AS JackpotInt100
		,TRY_CONVERT(INT			,0							)								AS FreeplayInt100
		,TRY_CONVERT(INT			,ROUND(a.Theo*100,0)		)								AS TheoWinWithFreeplayInt100
		,TRY_CONVERT(INT			,ROUND(a.Theo*100,0)		)								AS TheoWinNetFreeplayInt100
		,TRY_CONVERT(INT			,ROUND(MinsPlayed*100,0)	)								AS TimePlayedInt100
		,TRY_CONVERT(INT			,ROUND(CashBuyIn*100,0)		)								AS CashInInt100
		,TRY_CONVERT(INT			,ROUND(ChipBuyIn*100,0)		)								AS ChipsInInt100
		,TRY_CONVERT(INT			,ROUND(CreditBuyIn*100,0)	)								AS CreditInInt100
		,TRY_CONVERT(INT			,ROUND(WalkedWith*100,0)	)								AS WalkedWithInt100

		--Player dimension attributes
		,TRY_CONVERT(VARCHAR(100)	 ,p.HomePostalCode			)								AS PlayerZipCode
		,TRY_CONVERT(NVARCHAR(50)	 ,p.Gender					)								AS PlayerGender
		,TRY_CONVERT(DATE			 ,p.SetupDate				)								AS PlayerEnrollmentDate
		,TRY_CONVERT(DATE			 ,p.BirthDate				)								AS PlayerBirthDate
		,TRY_CONVERT(FLOAT			 ,d.PlayerDistanceToCasino	)								AS PlayerAddressDistance																								

		--Game dimension attributes (most of the time there is one game per device, this will only show the MOST COMMON game)
		,TRY_CONVERT(NVARCHAR(100)	,b.GameName					)								AS GameTheme	
		,TRY_CONVERT(NVARCHAR(100)	,NULL						)								AS GameThemeCount	
		,TRY_CONVERT(FLOAT			,a.TheoHold					)								AS GameTheoHold

		--Device dimension attributes
		,TRY_CONVERT(FLOAT			,b.TheoHold					)								AS DeviceTheoHoldPercent
		,TRY_CONVERT(NVARCHAR(200)	,''							)								AS DeviceManufacturer
		,TRY_CONVERT(FLOAT			,''							)								AS Denomination
		,TRY_CONVERT(NVARCHAR(200)	,b.GameCode					)								AS DeviceGameType	
		,TRY_CONVERT(NVARCHAR(100)	,''							)								AS DeviceCabinetType 
		,TRY_CONVERT(NVARCHAR(200)	,''							)								AS DeviceCabinetModel	
		,TRY_CONVERT(NVARCHAR(20)	,b.LocnCode					)								AS DeviceLocation		
		,TRY_CONVERT(NVARCHAR(20)	,LEFT(CONCAT(b.LocnCode,'00'),2))							AS DeviceLocationArea
		,TRY_CONVERT(NVARCHAR(20)	,RIGHT(LEFT(CONCAT(b.LocnCode,'0000'),4),2))				AS DeviceLocationBank
		,TRY_CONVERT(NVARCHAR(20)	,RIGHT(CONCAT('00',b.LocnCode),2))							AS DeviceLocationStand
		,TRY_CONVERT(DATE			,b.InsertDtm				)								AS DeviceStartDate
		,TRY_CONVERT(NVARCHAR(50)	,''							)								AS DeviceSerial	
		,TRY_CONVERT(INT			,0							)								AS DeviceGameClass	
		,TRY_CONVERT(INT			,0							)								AS DeviceLeaseFeeInt100
		,TRY_CONVERT(NVARCHAR(4000)	,''							)								AS DeviceLeaseAttributes	
		,TRY_CONVERT(NVARCHAR(4000)	,concat('MinBet:',b.MinBet,
											'MaxBet:',b.MaxBet,
											'AreaName:',b.AreaName)
																)								AS DeviceExtraAttributes	
		,TRY_CONVERT(INT			,0							)								AS IsSlotDevice
		,TRY_CONVERT(INT			,1							)								AS IsTableDevice
		,TRY_CONVERT(INT			,0							)								AS IsPokerDevice
		,TRY_CONVERT(INT			,0							)								AS IsOtherDevice
		,TRY_CONVERT(INT			,1							)								AS IsActiveDevice
		,DeviceLocationBigInt = TRY_CONVERT(BIGINT			,CHECKSUM(b.LocnCode))
		,TRY_CONVERT(DATETIME		,GETUTCDATE()				)								AS InsertDateTime
FROM	 [qcic].[HRC_Fact_PlayerTableRating_Tran] a WITH (NOLOCK)
JOIN	 [qcids].[App_Casino] b2 WITH (NOLOCK)
ON		(a.CasinoID = b2.CasinoID AND b2.SystemNameInt = 3)
LEFT JOIN [qcic].[HRC_Dim_TableAttribute_History] b WITH (NOLOCK)
ON		(a.TableHistoryId = b.TableHistoryId AND a.CasinoId = b.CasinoId)
LEFT JOIN [qcids].[Dim_Player_SCD] p WITH (NOLOCK)
ON		(p.SystemName = 'HRC' AND a.PlayerID = p.PlayerID AND p.StartDateTime <= a.GamingDt AND a.GamingDt < p.EndDateTime)
LEFT JOIN [qcids].[Dim_PlayerDistanceToCasino_SCD] d WITH (NOLOCK)
ON		(d.SystemName = 'HRC' AND a.CasinoId = d.CasinoID AND a.PlayerID = d.PlayerID AND d.StartDateTime <= a.GamingDt AND a.GamingDt < d.EndDateTime)
UNION ALL
--Free Play and Table Games Coupon
SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'						)								AS SystemName
		,TRY_CONVERT(INT			,3							)								AS SystemNameInt

		--Primary Key (CasinoID, DeviceID, PlayerID, GamingDate)
		,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId					)								AS CasinoID
		,TRY_CONVERT(INT			,b2.CasinoIDInt				)								AS CasinoIDInt
		,TRY_CONVERT(NVARCHAR(100)	,''							)								AS DeviceID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt]('',2))					AS DeviceIDBigInt
		,TRY_CONVERT(NVARCHAR(100)	,a.PlayerID					)								AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](a.PlayerID,1))			AS PlayerIDBigInt
		,TRY_CONVERT(DATE			,a.GamingDt					)								AS GamingDate

		--Player Related Metrics
		,TRY_CONVERT(DATETIME		,a.RedeemedDtm				)								AS StartDateTime
		,TRY_CONVERT(DATETIME		,a.RedeemedDtm				)								AS EndDateTime

		--Gaming TOTAL metrics
		,TRY_CONVERT(INT			,0							)								AS CoinInInt100
		,TRY_CONVERT(INT			,0							)								AS CoinOutInt100
		,TRY_CONVERT(INT			,0							)								AS ActualWinWithFreeplayInt100
		,TRY_CONVERT(INT			,ROUND(-1 * a.AmountUsed*100,0))								AS ActualWinNetFreeplayInt100
		,TRY_CONVERT(INT			,0							)								AS NumberGamesPlayed
		,TRY_CONVERT(INT			,0							)								AS JackpotInt100
		,TRY_CONVERT(INT			,ROUND(a.AmountUsed*100,0)	)								AS FreeplayInt100
		,TRY_CONVERT(INT			,0							)								AS TheoWinWithFreeplayInt100
		,TRY_CONVERT(INT			,ROUND(-1 * a.AmountUsed*100,0))								AS TheoWinNetFreeplayInt100
		,TRY_CONVERT(INT			,0							)								AS TimePlayedInt100
		,TRY_CONVERT(INT			,0							)								AS CashInInt100
		,TRY_CONVERT(INT			,0							)								AS ChipsInInt100
		,TRY_CONVERT(INT			,0							)								AS CreditInInt100
		,TRY_CONVERT(INT			,0							)								AS WalkedWithInt100

		--Player dimension attributes
		,TRY_CONVERT(VARCHAR(100)	 ,p.HomePostalCode			)								AS PlayerZipCode
		,TRY_CONVERT(NVARCHAR(50)	 ,p.Gender					)								AS PlayerGender
		,TRY_CONVERT(DATE			 ,p.SetupDate				)								AS PlayerEnrollmentDate
		,TRY_CONVERT(DATE			 ,p.BirthDate				)								AS PlayerBirthDate
		,TRY_CONVERT(FLOAT			 ,d.PlayerDistanceToCasino	)								AS PlayerAddressDistance																								

		--Game dimension attributes (most of the time there is one game per device, this will only show the MOST COMMON game)
		,TRY_CONVERT(NVARCHAR(100)	,''							)								AS GameTheme	
		,TRY_CONVERT(NVARCHAR(100)	,NULL						)								AS GameThemeCount	
		,TRY_CONVERT(FLOAT			,''							)								AS GameTheoHold

		--Device dimension attributes
		,TRY_CONVERT(FLOAT			,''							)								AS DeviceTheoHoldPercent
		,TRY_CONVERT(NVARCHAR(200)	,''							)								AS DeviceManufacturer
		,TRY_CONVERT(FLOAT			,''							)								AS Denomination
		,TRY_CONVERT(NVARCHAR(200)	,''							)								AS DeviceGameType	
		,TRY_CONVERT(NVARCHAR(100)	,''							)								AS DeviceCabinetType 
		,TRY_CONVERT(NVARCHAR(200)	,''							)								AS DeviceCabinetModel	
		,TRY_CONVERT(NVARCHAR(20)	,''							)								AS DeviceLocation		
		,TRY_CONVERT(NVARCHAR(20)	,''							)								AS DeviceLocationArea	
		,TRY_CONVERT(NVARCHAR(20)	,''							)								AS DeviceLocationBank	
		,TRY_CONVERT(NVARCHAR(20)	,''							)								AS DeviceLocationStand
		,TRY_CONVERT(DATE			,''							)								AS DeviceStartDate
		,TRY_CONVERT(NVARCHAR(50)	,''							)								AS DeviceSerial	
		,TRY_CONVERT(INT			,0							)								AS DeviceGameClass	
		,TRY_CONVERT(INT			,0							)								AS DeviceLeaseFeeInt100
		,TRY_CONVERT(NVARCHAR(4000)	,''							)								AS DeviceLeaseAttributes	
		,TRY_CONVERT(NVARCHAR(4000)	,''							)								AS DeviceExtraAttributes
		,TRY_CONVERT(INT			,case when a.TranType = 'Coupon' then 0 else 1 end)			AS IsSlotDevice
		,TRY_CONVERT(INT			,case when a.TranType = 'Coupon' then 1 else 0 end)			AS IsTableDevice
		,TRY_CONVERT(INT			,0							)								AS IsPokerDevice
		,TRY_CONVERT(INT			,0							)								AS IsOtherDevice
		,TRY_CONVERT(INT			,1							)								AS IsActiveDevice
		,DeviceLocationBigInt = TRY_CONVERT(BIGINT			,CHECKSUM(''))
		,TRY_CONVERT(DATETIME		,GETUTCDATE()				)								AS InsertDateTime
FROM	 [qcic].[HRC_Fact_PlayerRedemption_Tran] a WITH (NOLOCK)
JOIN	 [qcids].[App_Casino] b2 WITH (NOLOCK)
ON		(a.CasinoID = b2.CasinoID AND b2.SystemNameInt = 3)
LEFT JOIN [qcids].[Dim_Player_SCD] p WITH (NOLOCK)
ON		(p.SystemName = 'HRC' AND a.PlayerID = p.PlayerID AND p.StartDateTime <= a.GamingDt AND a.GamingDt < p.EndDateTime)
LEFT JOIN [qcids].[Dim_PlayerDistanceToCasino_SCD] d WITH (NOLOCK)
ON		(d.SystemName = 'HRC' AND a.CasinoId = d.CasinoID AND a.PlayerID = d.PlayerID AND d.StartDateTime <= a.GamingDt AND a.GamingDt < d.EndDateTime)
WHERE	 a.TranType IN ('Autoload','Coupon') AND a.AmountUsed > 0;

--- Paso 9 

CREATE TABLE [test].[App_DeviceManufacturerGroup](
	[SystemName] [nvarchar](100) NULL,
	[SystemNameInt] [int] NULL,
	[OriginalManufacturer] [nvarchar](100) NULL,
	[ManufacturerCleaned] [nvarchar](100) NULL,
	[ManufacturerGroup] [nvarchar](100) NULL,
	[InsertDateTime] [datetime] NULL
) ON [PRIMARY];

INSERT INTO  [test].[App_DeviceManufacturerGroup]
select * from [qcids].[App_DeviceManufacturerGroup];

CREATE VIEW [test].[uvw_HRC_ETL09_App_DeviceManufacturerGroup]  AS
--Slot Ratings
SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'						)								AS SystemName
		,TRY_CONVERT(INT			,3							)								AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,OrigMfg					)								AS OriginalManufacturer
		,TRY_CONVERT(NVARCHAR(100)	,Mfg						)								AS ManufacturerCleaned
		,TRY_CONVERT(NVARCHAR(100)	,Mfg						)								AS ManufacturerGroup
		,TRY_CONVERT(DATETIME		,GETUTCDATE()				)								AS InsertDateTime
FROM	 [qcic].[HRC_Dim_SlotMFGGroup] WITH (NOLOCK);



-- paso 10 
CREATE TABLE [test].[App_DeviceGameTypeGroup](
	[SystemName] [nvarchar](100) NULL,
	[SystemNameInt] [int] NULL,
	[OriginalGameType] [nvarchar](100) NULL,
	[GameTypeCleaned] [nvarchar](100) NULL,
	[GameTypeGroup] [nvarchar](100) NULL,
	[GameTypeShortName] [nvarchar](100) NULL,
	[GameTypeSummary] [nvarchar](100) NULL,
	[InsertDateTime] [datetime] NULL
) ON [PRIMARY];

INSERT INTO  [test].[App_DeviceGameTypeGroup]
select * from  [qcids].[App_DeviceGameTypeGroup];

CREATE VIEW [test].[uvw_HRC_ETL10_App_DeviceGameTypeGroup]  AS
--Slot Ratings
SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'						)								AS SystemName
		,TRY_CONVERT(INT			,3							)								AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,OrigGameType				)								AS OriginalGameType
		,TRY_CONVERT(NVARCHAR(100)	,GameTypeCleaned			)								AS GameTypeCleaned
		,TRY_CONVERT(NVARCHAR(100)	,GameType					)								AS GameTypeGroup
		,TRY_CONVERT(NVARCHAR(100)	,GameTypeShortName			)								AS GameTypeShortName
		,TRY_CONVERT(NVARCHAR(100)	,GameTypeSummary			)								AS GameTypeSummary
		,TRY_CONVERT(DATETIME		,GETUTCDATE()				)								AS InsertDateTime
FROM	 [qcic].[HRC_Dim_SlotGameTypeGroup] WITH (NOLOCK);



----Paso 11 

CREATE TABLE [test].[App_DeviceDenominationGroup](
	[SystemName] [nvarchar](100) NULL,
	[SystemNameInt] [int] NULL,
	[OriginalDenomination] [float] NULL,
	[DenominationCleaned] [nvarchar](100) NULL,
	[DenominationSort] [nvarchar](100) NULL,
	[DenominationGroup] [nvarchar](100) NULL,
	[DenominationGroupSort] [nvarchar](100) NULL,
	[InsertDateTime] [datetime] NULL
) ON [PRIMARY];
INSERT INTO  [test].[App_DeviceDenominationGroup]
select * from  [qcids].[App_DeviceDenominationGroup];




CREATE VIEW [test].[uvw_HRC_ETL11_App_DeviceDenominationGroup]  AS
--Slot Ratings
SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'					)								AS SystemName
		,TRY_CONVERT(INT			,3						)								AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,OrigDenom				)								AS OriginalDenomination
		,TRY_CONVERT(NVARCHAR(100)	,Denom					)								AS DenominationCleaned
		,TRY_CONVERT(NVARCHAR(100)	,DenomSort				)								AS DenominationSort
		,TRY_CONVERT(NVARCHAR(100)	,DenomGroup				)								AS DenominationGroup
		,TRY_CONVERT(NVARCHAR(100)	,DenomGroupSort			)								AS DenominationGroupSort
		,TRY_CONVERT(DATETIME		,GETUTCDATE()			)								AS InsertDateTime
FROM	 [qcic].[HRC_Dim_SlotDenomGroup] WITH (NOLOCK);


--Paso 12 
CREATE TABLE [test].[Fact_PlayerPrize](
	[SystemName] [nvarchar](100) NULL,
	[SystemNameInt] [int] NULL,
	[IssueCasinoID] [nvarchar](100) NULL,
	[IssueCasinoIDInt] [int] NULL,
	[RedeemCasinoID] [nvarchar](100) NULL,
	[RedeemCasinoIDInt] [int] NULL,
	[PlayerID] [nvarchar](100) NULL,
	[PlayerIDBigInt] [bigint] NULL,
	[RedeemDateTime] [datetime] NULL,
	[PrizeID] [nvarchar](100) NULL,
	[PrizeType] [nvarchar](100) NULL,
	[PrizeIssueAmountInt100] [int] NULL,
	[PrizeRedeemAmountInt100] [int] NULL,
	[PointForPrizeRedeemAmountInt100] [int] NULL,
	[PrizeStartDateTime] [datetime] NULL,
	[PrizeEndDateTime] [datetime] NULL,
	[PrizeCampaignID] [nvarchar](100) NULL,
	[PrizeCampaignSegmentID] [nvarchar](100) NULL,
	[InsertDateTime] [datetime] NULL
) ON [PRIMARY];
INSERT INTO  [test].[Fact_PlayerPrize]
select * 
from  [qcids].[Fact_PlayerPrize] ;

CREATE VIEW [test].[uvw_HRC_ETL12_Fact_PlayerPrize]  AS
--Slot Ratings
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
FROM	 [qcic].[HRC_Fact_PlayerRedemption_Tran] a WITH (NOLOCK)
JOIN	 [qcids].[App_Casino] b WITH (NOLOCK)
ON		(a.CasinoId = b.CasinoID AND b.SystemNameInt = 3);


-- paso 14

CREATE TABLE [test].[Fact_PlayerEvaluationCurrent](
	[SystemName] [nvarchar](100) NULL,
	[SystemNameInt] [int] NOT NULL,
	[PlayerID] [nvarchar](100) NULL,
	[PlayerIDBigInt] [bigint] NOT NULL,
	[CasinoGroupID] [nvarchar](100) NULL,
	[CasinoGroupIDInt] [int] NOT NULL,
	[EvaluationScoreInt100] [int] NULL,
	[InsertDateTime] [datetime] NULL,
PRIMARY KEY NONCLUSTERED 
(
	[SystemNameInt] ASC,
	[PlayerIDBigInt] ASC,
	[CasinoGroupIDInt] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]; 
INSERT INTO [test].[Fact_PlayerEvaluationCurrent]
select * from [qcids].[Fact_PlayerEvaluationCurrent];




CREATE VIEW [test].[uvw_HRC_ETL14_Fact_PlayerEvaluationCurrent]  AS
--Slot Ratings
SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'					)								AS SystemName
		,TRY_CONVERT(INT			,3						)								AS SystemNameInt
		,TRY_CONVERT(NVARCHAR(100)	,PlayerId				)								AS PlayerID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](PlayerID,1))		AS PlayerIDBigInt
		,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId				)								AS CasinoGroupID
		,TRY_CONVERT(INT			,b.CasinoGroupIDInt		)								AS CasinoGroupIDInt
		,TRY_CONVERT(INT			,ROUND(MAX(ValuationScore)*100,0))						AS EvaluationScoreInt100
		,TRY_CONVERT(DATETIME		,GETUTCDATE()			)								AS InsertDateTime
FROM	 [qcic].[HRC_Fact_PlayerValuationScore] a WITH (NOLOCK)
JOIN	 [qcids].[App_CasinoGroup] b WITH (NOLOCK)
ON		(a.CasinoID = b.CasinoGroupID AND b.SystemNameInt = 3)
GROUP BY PlayerID
		,a.CasinoId
		,b.CasinoGroupIDInt;

-- paso 15 
-- DROP TABLE [test].[Fact_DeviceDay]
CREATE TABLE [test].[Fact_DeviceDay](
	[SystemName] [nvarchar](100) NULL,
	[SystemNameInt] [int] NULL,
	[CasinoID] [nvarchar](100) NULL,
	[CasinoIDInt] [int] NULL,
	[DeviceID] [nvarchar](100) NULL,
	[DeviceIDBigInt] [bigint] NULL,
	[GamingDate] [date] NULL,
	[CoinInInt100] [bigint] NULL,
	[CoinOutInt100] [bigint] NULL,
	[ActualWinWithFreeplayInt100] [int] NULL,
	[ActualWinNetFreeplayInt100] [int] NULL,
	[NumberGamesPlayed] [int] NULL,
	[JackpotInt100] [int] NULL,
	[FreeplayInt100] [int] NULL,
	[TheoWinWithFreeplayInt100] [int] NULL,
	[TheoWinNetFreeplayInt100] [int] NULL,
	[TimePlayedInt100] [int] NULL,
	[CashInInt100] [bigint] NULL,
	[ChipsInInt100] [bigint] NULL,
	[CreditInInt100] [bigint] NULL,
	[WalkedWithInt100] [bigint] NULL,
	[GameTheme] [nvarchar](100) NULL,
	[GameThemeCount] [nvarchar](100) NULL,
	[GameTheoHold] [float] NULL,
	[DeviceTheoHoldPercent] [float] NULL,
	[DeviceManufacturer] [nvarchar](200) NULL,
	[Denomination] [float] NULL,
	[DeviceGameType] [nvarchar](200) NULL,
	[DeviceCabinetType] [nvarchar](100) NULL,
	[DeviceCabinetModel] [nvarchar](200) NULL,
	[DeviceLocation] [nvarchar](20) NULL,
	[DeviceLocationArea] [nvarchar](20) NULL,
	[DeviceLocationBank] [nvarchar](20) NULL,
	[DeviceLocationStand] [nvarchar](20) NULL,
	[DeviceStartDate] [date] NULL,
	[DeviceSerial] [nvarchar](50) NULL,
	[DeviceGameClass] [int] NULL,
	[DeviceLeaseFeeInt100] [int] NULL,
	[DeviceLeaseAttributes] [nvarchar](4000) NULL,
	[DeviceExtraAttributes] [nvarchar](4000) NULL,
	[IsSlotDevice] [int] NULL,
	[IsTableDevice] [int] NULL,
	[IsPokerDevice] [int] NULL,
	[IsOtherDevice] [int] NULL,
	[IsActiveDevice] [int] NULL,
	[DeviceLocationBigInt] [bigint] NULL,
	[InsertDateTime] [datetime] NULL
) ON [PRIMARY];
INSERT INTO [test].[Fact_DeviceDay]
select * from [qcids].[Fact_DeviceDay];

CREATE VIEW [test].[uvw_HRC_ETL15_Fact_DeviceDay]  AS
--Slot Ratings
SELECT 
		--The ETL_Runner connects qcic to qcids, so we use SystemName to track which qcic system starts the connection.
		 TRY_CONVERT(NVARCHAR(100)	,'HRC'						)								AS SystemName
		,TRY_CONVERT(INT			,3							)								AS SystemNameInt

		--Primary Key (CasinoID, DeviceID, GamingDate)
		,TRY_CONVERT(NVARCHAR(100)	,a.CasinoId					)								AS CasinoID
		,TRY_CONVERT(INT			,b.CasinoIDInt				)								AS CasinoIDInt
		,TRY_CONVERT(NVARCHAR(100)	,a.Asset					)								AS DeviceID
		,TRY_CONVERT(BIGINT			,[qcids].[udf_ConvertStringToBigInt](a.Asset,2))			AS DeviceIDBigInt
		,TRY_CONVERT(DATE			,a.GamingDt					)								AS GamingDate

		--Gaming TOTAL metrics
		,TRY_CONVERT(BIGINT			,ROUND(a.Bet*100,0)			)								AS CoinIn     
		,TRY_CONVERT(BIGINT			,ROUND(a.Paidout*100,0)		)								AS CoinOut
		,TRY_CONVERT(INT			,ROUND(a.Actual*100,0)		)								AS ActualWinWithFreeplay
		,TRY_CONVERT(INT			,ROUND(a.Actual*100 - a.CouponIn*100 - a.TransferIn*100,0))	AS ActualWinNetFreeplay
		,TRY_CONVERT(INT			,a.GamesPlayed				)								AS NumberGamesPlayed
		,TRY_CONVERT(INT			,ROUND(a.JackpotOut*100,0)	)								AS Jackpot
		,TRY_CONVERT(INT			,ROUND(a.CouponIn*100 + a.TransferIn*100,0))				AS Freeplay
		,TRY_CONVERT(INT			,ROUND(a.Theo*100,0)		)								AS TheoWinWithFreeplay  
		,TRY_CONVERT(INT			,ROUND(a.Theo*100 - a.CouponIn*100 - a.TransferIn*100,0))	AS TheoWinNetFreeplay  
		,TRY_CONVERT(INT			,0							)								AS TimePlayed
		,TRY_CONVERT(BIGINT			,ROUND(a.CashIn*100,0)		)								AS CashIn   
		,TRY_CONVERT(BIGINT			,0							)								AS ChipsIn
		,TRY_CONVERT(BIGINT			,0							)								AS CreditIn   
		,TRY_CONVERT(BIGINT			,0							)								AS WalkedWith

		--Game dimension attributes (most of the time there is one game per device, this will only show the MOST COMMON game)
		,TRY_CONVERT(NVARCHAR(100)	,a.gametheme				)								AS GameTheme	
		,TRY_CONVERT(NVARCHAR(100)	,''							)								AS GameThemeCount	
		,TRY_CONVERT(FLOAT			,a.TheoHoldPct				)								AS GameTheoHold

		--Device dimension attributes
		,TRY_CONVERT(FLOAT			,a.TheoHoldPct				)								AS DeviceTheoHoldPercent
		,TRY_CONVERT(NVARCHAR(200)	,a.StdMfg					)								AS DeviceManufacturer
		,TRY_CONVERT(FLOAT			,a.Denom					)								AS Denomination
		,TRY_CONVERT(NVARCHAR(200)	,a.GameType					)								AS DeviceGameType	
		,TRY_CONVERT(NVARCHAR(100)	,''							)								AS DeviceCabinetType 
		,TRY_CONVERT(NVARCHAR(200)	,a.Cabinet					)								AS DeviceCabinetModel	
		,TRY_CONVERT(NVARCHAR(20)	,a.Location					)								AS DeviceLocation		
		,TRY_CONVERT(NVARCHAR(20)	,LEFT(CONCAT(a.Location,'00'),2))							AS DeviceLocationArea
		,TRY_CONVERT(NVARCHAR(20)	,RIGHT(LEFT(CONCAT(a.Location,'0000'),4),2))				AS DeviceLocationBank
		,TRY_CONVERT(NVARCHAR(20)	,RIGHT(CONCAT('00',a.Location),2))							AS DeviceLocationStand
		,TRY_CONVERT(DATE			,DATEADD(DAY,-1 * a.DaysOnFloor,a.GamingDt))				AS DeviceStartDate
		,TRY_CONVERT(NVARCHAR(50)	,a.Serial					)								AS DeviceSerial	
		,TRY_CONVERT(INT			,3							)								AS DeviceGameClass	
		,TRY_CONVERT(MONEY			,0							)								AS DeviceLeaseFeeInt100
		,TRY_CONVERT(NVARCHAR(4000)	,''							)								AS DeviceLeaseAttributes	
		,TRY_CONVERT(NVARCHAR(4000)	,''							)								AS DeviceExtraAttributes	
		,TRY_CONVERT(INT			,1							)								AS IsSlotDevice
		,TRY_CONVERT(INT			,0							)								AS IsTableDevice
		,TRY_CONVERT(INT			,0							)								AS IsPokerDevice
		,TRY_CONVERT(INT			,0							)								AS IsOtherDevice
		,TRY_CONVERT(INT			,1							)								AS IsActiveDevice
		,DeviceLocationBigInt = TRY_CONVERT(BIGINT			,CHECKSUM(a.Location))
		,TRY_CONVERT(DATETIME		,GETUTCDATE()				)								AS InsertDateTime
FROM	 [qcic].[HRC_Fact_DayMeterHistory] a WITH (NOLOCK)
JOIN	 [qcids].[App_Casino] b WITH (NOLOCK)
ON		(a.CasinoId = b.CasinoID AND b.SystemNameInt = 3);
