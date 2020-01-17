
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*

This script will aid in creating a variety of DML statements, by referencing an existing table. 
Use with global temp tables or against exiting fixed tables. 
For use with global temp tables:

1) Create a global temp table (##<temptablename>) in another session 
2) Refer to that table in this session by chaning the @Source_Table variable reference
3) Set the output variables (@Output_Table, @tableSchema, @dropTableStatement, @insertStatement, @selectStatement)
4) Execute

*/

DECLARE 
		@Source_Table VARCHAR(128) =  N'##DirectFTE'-- Global temp table (##temptablename), or fixed table being referenced
,		@Output_Table VARCHAR (128) = N'#DirectFTE' -- The table name you wish to create in your script
,		@tAlias VARCHAR(3)  = 't' -- if you want the select statement to have a specific alias for the table
,		@tableSchema BIT = 1 -- Generate a DDL statement for the referenced table
,		@dropTableStatement BIT = 1 -- Generate a drop table statement
,		@insertStatement BIT = 1 -- Generate an insert statement
,		@selectStatement BIT = 1 -- generate a select statement w/ aliasing

/* DO NOT CHANGE THESE VARIABLES UNLESS WORKING WITH PERMANENT FIXED TABLES */
,		@Source_DB NVARCHAR(30)  = 'tempdb' -- 'tempdb' for ## tables
,		@Schema VARCHAR (20)  -- = 'stg' -- Schema for which you will be creating the table

SET NOCOUNT ON;

/* Common Variables */
DECLARE @strSQL NVARCHAR(MAX) = ''
DECLARE @Count INT = 0
DECLARE @CountMax INT
DECLARE @ServerName sysname = @@SERVERNAME
DECLARE @ProcStart DATETIME = CURRENT_TIMESTAMP

DECLARE @Source_Columns table
(
	[TableName] [sysname] NOT NULL,
	[ColumnName] [sysname] NOT NULL,
	[column_id] [int] NOT NULL,
	[Column_Text] [varchar](MAX) NOT NULL
	PRIMARY KEY ([TableName], [column_id])
)

DECLARE @tOrV BIT 

DECLARE @checkObjType NVARCHAR (3072) = N'USE ' + @Source_DB + N';
IF EXISTS (
SELECT TOP 1 1 
FROM sys.tables t (NOLOCK) 
JOIN sys.schemas s (NOLOCK) ON t.schema_id = s.schema_id 
WHERE s.name = COALESCE(@Schema,s.name) and t.name = @Source_Table
			)
SET @out = 1
ELSE SET @out = 0'
DECLARE @checkObjTypeParms NVARCHAR (512) = N'@Db NVARCHAR (128), @Schema NVARCHAR (128), @Source_Table NVARCHAR (128), @out BIT out'
EXEC sp_ExecuteSQL @checkObjType, @checkObjTypeParms, @Db = @Source_DB, @Schema = @Schema, @Source_Table = @Source_Table, @out = @tOrV out

SET @Schema = COALESCE(@Schema,'')

IF @tAlias IS NULL
BEGIN
SELECT @tAlias = 
CASE WHEN SUBSTRING(@Source_Table,2,1) = '#'
		THEN SUBSTRING(@Source_Table,3,1)
		WHEN (SUBSTRING(@Source_Table,2,1) != '#' AND LEFT(@Source_Table,1) = '#')
			THEN SUBSTRING(@Source_Table,2,1)
		ELSE LEFT(@Source_Table,1)
		END
END
DECLARE @ReversedData NVARCHAR(MAX)
DECLARE @LineBreakIndex INT
DECLARE @SearchLength INT = 4000 

SET @strSQL = '

; WITH TableColumns
AS
(
	SELECT TAB.[name] AS TableName
	, TAb.object_id
	, Col.[Name] AS ColumnName
	, Col.[column_id]
	, Col.[user_type_id]
	, Col.[max_length]
	, Col.[precision]
	, Col.[scale]
	, Col.[is_nullable]
	, Col.[is_identity]
	, Col.[is_computed]
	, Col.[default_object_id]
	, Col.[is_sparse]
	, TYP.[name] AS DataType
	, TAB.create_date
	, idColumns.seed_value
	, idColumns.increment_value
	, COM_COL.[definition]
	 FROM ' + @Source_DB + '.SYS.COLUMNS COL
	 JOIN ' + @Source_DB + '.SYS.TYPES TYP	ON COL.user_type_id = TYP.user_type_id
	 JOIN ' + @Source_DB 
	IF @tOrv = 1
	BEGIN
	SET @strSQL = @strSQL  + '.SYS.TABLES TAB ON TAB.object_id = COL.object_id'
	END
	IF @tOrv = 0
	BEGIN
	SET @strSQL = @strSQL  + '.SYS.VIEWS TAB ON TAB.object_id = COL.object_id'
	END
	SET @strSQL = @strSQL  + 
	' JOIN ' + @Source_DB + '.SYS.SCHEMAS SCH ON TAB.Schema_Id = SCH.Schema_Id
	 LEFT JOIN ' + @Source_DB + '.sys.identity_columns idColumns ON idColumns.object_id = Col.object_id AND Col.is_identity = 1
	 LEFT JOIN ' + @Source_DB + '.sys.computed_columns COM_COL	ON COL.object_id = COM_COL.object_id AND COl.is_computed = 1
	WHERE SCH.Name = ' 
	IF @Schema != ''
	SET @strSQL = @strSQL  + '''' + @Schema + ''''
	ELSE SET @strSQL = @strSQL  + 'SCH.Name'
	
	 SET @strSQL = @strSQL  + '
	AND TAB.name = ''' + @Source_Table + '''
)
SELECT  TableName,ColumnName
,column_id
, ''[''+ColumnName+''] ''+ 
CASE WHEN is_computed = 1 Then ''AS (''+[definition]+'')'' 
ELSE '' [''+ DataType +''] '' END +
CASE is_identity WHEN 1 THEN ''Identity (''+Convert(Varchar(10),increment_value)+'',''+Convert(Varchar(10),seed_value)+'')'' ELSE '''' END +
CASE 
	WHEN DataType in (''binary'',''char'') Then ''(''+Convert(Varchar(10),max_length)+'')'' 
	WHEN DataType in (''nchar'') Then ''(''+Convert(Varchar(10),(max_length/2))+'')''  
	WHEN DataType in (''nvarchar'') AND max_length != -1 Then ''(''+Convert(Varchar(10),(max_length/2))+'')''  
	WHEN DataType in (''nvarchar'') AND max_length = -1  Then ''(MAX)'' 
	WHEN DataType in (''varbinary'', ''varChar'') AND max_length = -1 Then ''(MAX)'' 
	WHEN DataType in (''varbinary'', ''varChar'') AND max_length != -1 Then ''(''+Convert(Varchar(10),max_length)+'')''
	WHEN DataType in (''decimal'',''numeric'') Then ''(''+Convert(Varchar(10),[precision])+'', ''++Convert(Varchar(10),(scale))+'')'' 
	ELSE ''''
 END +
CASE is_sparse WHEN 1 THEN '' SPARSE ''ELSE '''' END +
CASE WHEN IS_computed = 1 THEN '''' WHEN Is_Nullable = 1 THEN '' NULL '' ELSE '' NOT NULL '' END AS Column_Text
FROM TableColumns
ORDER BY create_date,column_id
'

DELETE FROM @Source_Columns
INSERT INTO @Source_Columns 
EXEC (@strSQL)

SET @strSQL = ''
IF @tableSchema = 1
BEGIN
IF @dropTableStatement = 1
BEGIN
SET @strSQL = + @strSQL + '

IF OBJECT_ID(N''' + @Source_DB +'.' + COALESCE(@Schema,N'.') + '.' + @Output_Table + ''',N''U'') IS NOT NULL
DROP TABLE '+ CASE WHEN LEFT(@Source_Table,1)=N'#'
					THEN N''
					ELSE @Source_DB +  '.' + COALESCE(@Schema,N'.') + '.'
					END   + @Output_Table +'
'
END  

SET @strSQL = + @strSQL + '
CREATE TABLE '+ CASE WHEN LEFT(@Source_Table,1)=N'#'
					THEN N''
					ELSE @Source_DB +  '.' + COALESCE(@Schema,N'.') + '.'
					END   + @Output_Table  + ' 
(	'
SELECT @strSQL = @strSQL + sc.[Column_Text] +'
,	'
FROM @Source_Columns sc 
SET @strSQL = LEFT(@strSQL,LEN(@strSQL)-3)+')'
END
IF @insertStatement = 1
BEGIN
SET @strSQL = + @strSQL + '
INSERT ' + CASE WHEN LEFT(@Source_Table,1)=N'#'
					THEN N''
					ELSE @Source_DB +  '.' + COALESCE(@Schema,N'.') + '.'
					END   + @Output_Table  + '
(	'
SELECT @strSQL = @strSQL + '[' + sc.ColumnName + ']
,	'
FROM @Source_Columns sc
SET @strSQL = LEFT (@strSQL,LEN(@strSQL)-3)+ ')'
END

IF @selectStatement = 1
BEGIN
SET @strSQL = + @strSQL + '
SELECT 
	' SELECT @strSQL = @strSQL + @tAlias + '.[' + sc.ColumnName + ']
,	'
FROM @Source_Columns sc
SET @strSQL = LEFT (@strSQL,LEN(@strSQL)-4)
SET @strSQL = @strSQL + '
FROM ' + CASE WHEN LEFT(@Source_Table,1)=N'#'
					THEN N''
					ELSE @Source_DB +  '.' + COALESCE(@Schema,N'.') + '.'
					END   + @Output_Table  + ' AS '+ @tAlias + ' (NOLOCK) '
END

/* Below here properly prints an entire nvarchar(max) variable if len > 8000 */

WHILE (LEN(@strSQL) > @SearchLength) 
 BEGIN
  SET @ReversedData = LEFT(@strSQL, @SearchLength);
  SET @ReversedData = REVERSE(@ReversedData);
  SET @LineBreakIndex = CHARINDEX(CHAR(10) + CHAR(13), @ReversedData);
  PRINT LEFT(@strSQL, @SearchLength - @LineBreakIndex + 1);
  SET @strSQL = RIGHT(@strSQL, LEN(@strSQL) - @SearchLength + @LineBreakIndex );
 END;
 IF (LEN(@strSQL) > 0) BEGIN
  PRINT @strSQL;
 END;
