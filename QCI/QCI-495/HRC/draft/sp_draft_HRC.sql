/****** Object:  StoredProcedure [qcic].[usp_HRC_ETL_Runner]    Script Date: 9/29/2020 9:59:40 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/***********************************************************************************************
Author:			Ralph Thomas

Date:			2020-04-19

Notes:			Runs the ETLs for HRC System to build the UDM Connection Layer as well as the base UDM Data Science Layer Tables

				Requires schemas qcic, qcids, and qciapi to be created first
				Requires qcic tables to be created second
				Requires qcic ETLXX views to be created third
				This gets run fourth				
				
Modifictions:
Date			Name			Notes
<CurrentDate>	<Name>			<Desc>
***********************************************************************************************/
ALTER PROCEDURE [qcic].[usp_HRC_ETL_Runner_foo]
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;



END

