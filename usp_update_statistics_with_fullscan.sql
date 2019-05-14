USE [¿]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF OBJECT_ID('[dbo].[usp_update_statistics_with_fullscan]') IS NULL
       EXEC('CREATE PROCEDURE [dbo].[usp_update_statistics_with_fullscan] AS SET NOCOUNT ON;')
GO
ALTER PROCEDURE [dbo].[usp_update_statistics_with_fullscan] (
       @debug BIT = 0
)
AS
BEGIN
       SET NOCOUNT ON;
       --     table for holding table related information (+ 'index_name' being = 'the statistic')
       CREATE TABLE #databases_tables_indexes (
              [database_name] SYSNAME NOT NULL,
              [schema_name] SYSNAME NOT NULL,
              [table_name] SYSNAME NOT NULL,
              [table_object_id] INT NOT NULL,
              [index_name] SYSNAME NOT NULL
       );
       --     table for holding statistics related information
       CREATE TABLE #table_index_statistics (
             [id] INT IDENTITY(1,1)  NOT NULL,
             [stats_id] INT NULL,
             [database_name] SYSNAME NULL,
             [schema_name] SYSNAME NULL,
             [table_name] SYSNAME NULL,
             [index_name] SYSNAME NULL,
             [updated] DATETIME2 NULL,
             [rows] BIGINT NULL,
             [rows_sampled] BIGINT NULL,
             [modification_counter] BIGINT NULL,
             [steps] INT NULL,
             [density] DECIMAL(8,2) NULL,
             [average_key_length] INT NULL,
             [string_index] VARCHAR(10) NULL,
             [filter_expression] VARCHAR(250) NULL,
             [unfiltered_rows] BIGINT NULL,
              [persisted_sample_percent] INT NULL
       )
       --     table for holding statistics which need to be UPDATED
       CREATE TABLE #update_these_stats (
              [database_name] SYSNAME NOT NULL,
              [schema_name] SYSNAME NOT NULL,
              [table_name] SYSNAME NOT NULL,
              [index_name] SYSNAME NOT NULL,
              [updated] DATETIME2 NULL,
              [rows] BIGINT NOT NULL,
              [sample_percent] INT NOT NULL,
              [reason] VARCHAR(1000) NOT NULL,
              [sql] VARCHAR(MAX)
       )
       --     table for logging
       IF (SELECT [name] FROM sys.tables WHERE [name] = 'update_statistics_log') IS NULL
       BEGIN
              CREATE TABLE ¿.dbo.update_statistics_log (
                    [id] INT IDENTITY(1,1) NOT NULL,
                    [date_created] DATETIME DEFAULT GETDATE() NOT NULL,
                    [stat_last_updated_date] DATETIME NULL,
                    [runtime_sec] INT NULL,
                    [stat_update_date_diff] INT NULL,
                    [database_name] SYSNAME NOT NULL,
                    [schema_name] SYSNAME NOT NULL,
                    [table_name] SYSNAME NOT NULL,
                    [index_name] SYSNAME NOT NULL,
                    [rows] BIGINT NOT NULL,
                    [sample_percent] INT NOT NULL,
                    [reason] VARCHAR(1000) NULL,
                    [sql] VARCHAR(MAX) NOT NULL
              )
       END
       IF (@debug = 1)
       BEGIN
              CREATE TABLE #update_statistics_debug (
                    [id] INT IDENTITY(1,1) NOT NULL,
                    [date_created] DATETIME DEFAULT GETDATE() NOT NULL,
                    [stat_last_updated_date] DATETIME NULL,
                    [runtime_sec] INT NULL,
                    [stat_update_date_diff] INT NULL,
                    [database_name] SYSNAME NOT NULL,
                    [schema_name] SYSNAME NOT NULL,
                    [table_name] SYSNAME NOT NULL,
                    [index_name] SYSNAME NOT NULL,
                    [rows] BIGINT NOT NULL,
                    [sample_percent] INT NOT NULL,
                    [reason] VARCHAR(1000) NULL,
                    [sql] VARCHAR(MAX) NOT NULL
              )
       END
       --     cursor for looping through each database on the server with inequalities
       DECLARE @db_name AS sysname, @schema_name AS sysname, @table_name AS sysname, @table_object_id AS INT, @index_name AS sysname, @dbcc_output VARCHAR(MAX), @sSQL AS NVARCHAR(MAX);
       DECLARE @stats_id AS INT, @mod_counter AS BIGINT;
       DECLARE cur_grab_statistics CURSOR FOR SELECT [name] FROM sys.databases WHERE [database_id] > 4 AND [state_desc] = 'ONLINE' AND [is_read_only] = 0 AND [name] [NOT IN...NOT LIKE...,]
       OPEN cur_grab_statistics
              FETCH cur_grab_statistics INTO @db_name
                    WHILE @@FETCH_STATUS <> - 1
                    BEGIN
                           --     insert objects which have a statistic
                           SET @sSQL = N'
                                  USE [' + @db_name + ']
                                  INSERT INTO  #databases_tables_indexes ([database_name], [schema_name], [table_name], [table_object_id], [index_name])
                                  SELECT ''' + @db_name + ''' as [database_name], s.[name], t.[name], t.[object_id], i.[name]
                                  FROM ' + @db_name + '.sys.tables t WITH (NOLOCK)
                                  INNER JOIN sys.schemas s WITH (NOLOCK) ON s.schema_id=t.schema_id
                                  INNER JOIN sys.indexes i WITH (NOLOCK) ON i.object_id=t.object_id AND i.type_desc <> ''HEAP''
                           '
                           EXEC(@sSQL)
                          
                           --     loop through each statistic to gather its metrics
                           DECLARE cur_fetch_tables_indexes CURSOR FOR SELECT [schema_name], [table_name], [table_object_id], [index_name] FROM #databases_tables_indexes WHERE [database_name] = @db_name
                           OPEN cur_fetch_tables_indexes
                                  FETCH cur_fetch_tables_indexes INTO @schema_name, @table_name, @table_object_id, @index_name
                                         WHILE @@FETCH_STATUS <> - 1
                                         BEGIN
                                                SET @dbcc_output = '
                                                INSERT INTO #table_index_statistics (
                                                       [index_name], [updated], [rows], [rows_sampled], [steps], [density], [average_key_length], [string_index], [filter_expression], [unfiltered_rows], [persisted_sample_percent]
                                                )
                                                EXEC (''DBCC SHOW_STATISTICS(''''' + @db_name + '.' + @schema_name + '.' + @table_name + ''''', ''''' + @index_name + ''''') WITH STAT_HEADER'')
                                         '
                                         EXEC(@dbcc_output)
                                        
                                         --       update to include database_name, etc for each statistic (index_name)
                                         --       NOTE: it's possible for duplicate statistic names in separate tables thus the TOP (1)
                                         SET @sSQL = N'
                                                UPDATE TOP (1) s
                                                SET
                                                       [database_name] = ''' + @db_name + ''',
                                                       [schema_name] = ''' + @schema_name + ''',
                                                       [table_name] = ''' + @table_name + '''
                                                FROM #table_index_statistics s WITH (NOLOCK)
                                                WHERE
                                                       s.index_name = ''' + @index_name + ''' AND s.table_name IS NULL      
                                         '
                                         EXEC(@sSQL)
                                         --       fetch the modification_counter per statistic
                                         SET @sSQL = N'
                                                USE [' + @db_name + ']
                                                SELECT
                                                       @stats_id = s.stats_id,
                                                       @mod_counter = modification_counter 
                                                FROM sys.stats s WITH (NOLOCK)
                                                CROSS APPLY sys.dm_db_stats_properties(' + CAST(@table_object_id AS NVARCHAR(100)) + ', s.stats_id)
                                                WHERE
                                                       name IN (SELECT index_name FROM #table_index_statistics WHERE table_name = ''' + @table_name + ''')
                                         '
                                         EXEC sp_executesql @sSQL, N'@stats_id INT OUTPUT, @mod_counter BIGINT OUTPUT', @stats_id = @stats_id OUTPUT, @mod_counter = @mod_counter OUTPUT
                                        
                                         --       update to include the stats_id and modification_counter to the 'main' table
                                         SET @sSQL = N'
                                                UPDATE s
                                                SET
                                                       stats_id = ' + CAST(@stats_id AS VARCHAR) + ',
                                                       modification_counter = ' + CAST(@mod_counter AS VARCHAR) + '
                                                FROM #table_index_statistics s WITH (NOLOCK)
                                                WHERE
                                                       s.index_name = ''' + @index_name + ''' AND s.table_name = ''' + @table_name + '''                              
                                         '
                                         EXEC(@sSQL)
                                  FETCH NEXT FROM cur_fetch_tables_indexes INTO @schema_name, @table_name, @table_object_id, @index_name
                           END
                    CLOSE cur_fetch_tables_indexes
                    DEALLOCATE cur_fetch_tables_indexes
              FETCH NEXT FROM cur_grab_statistics INTO @db_name
       END
       CLOSE cur_grab_statistics
       DEALLOCATE cur_grab_statistics
       --     logic to determine if a statistic needs to be updated
       ; WITH update_stats AS (
              SELECT
              [database_name],
              [schema_name],
              [table_name],
              [index_name],
              [updated],
              [rows],
              ([rows_sampled] * 100) / [rows] AS [sample_percent],
              [modification_counter],
              CASE
                    WHEN [rows] > 50000 AND ROUND(CONVERT(FLOAT, [modification_counter]) / CONVERT(FLOAT, [rows]),2) > .19 THEN '1'
                    WHEN [rows] > 100000 AND ROUND(CONVERT(FLOAT, [modification_counter]) / CONVERT(FLOAT, [rows]),2) > .14 THEN '2'
                    WHEN [rows] > 500000 AND ROUND(CONVERT(FLOAT, [modification_counter]) / CONVERT(FLOAT, [rows]),2) > .09 THEN '3'
                    WHEN [rows] > 1000000 AND ROUND(CONVERT(FLOAT, [modification_counter]) / CONVERT(FLOAT, [rows]),2) > .04 THEN '4'
                    WHEN [rows] > 5000000 AND ROUND(CONVERT(FLOAT, [modification_counter]) / CONVERT(FLOAT, [rows]),2) > .01 THEN '5'
              ELSE '0'
              END AS [update_my_stats],
              ROUND(CONVERT(FLOAT, [modification_counter]) / CONVERT(FLOAT, [rows]),2) AS [modification_difference],
              [persisted_sample_percent]
       FROM #table_index_statistics WITH (NOLOCK)
       WHERE
              [rows] >= 5000
       )
       --     insert statistics which pass the logic gate and build sql statement
       --     NOTE: the OR is a catch all for any statistics which make it pass the logic gate
       INSERT INTO #update_these_stats
       SELECT
              [database_name],
              [schema_name],
              [table_name],
              [index_name],
              [updated],
              [rows],
              [sample_percent],
              CASE [update_my_stats]
                    WHEN '1' THEN '50000 > rows and mod_counter > .19'
                    WHEN '2' THEN '100000 > rows and mod_counter > .14'
                    WHEN '3' THEN '500000 > rows and mod_counter > .09'
                    WHEN '4' THEN '1000000 > rows and mod_counter > .04'
                    WHEN '5' THEN '5000000 > rows and mod_counter > .01'
                    ELSE 'last_updated >= four weeks and mod_rows > 50000'
              END AS [reason],
              'UPDATE STATISTICS ' + [schema_name] + '.' + [table_name] + ' (' + [index_name] + ') WITH FULLSCAN; --total rows > ' + CAST([rows] AS VARCHAR(250)) + ' --mod difference > ' + CAST([modification_difference] AS VARCHAR(250)) + ' --mod rows > ' + CAST([modification_counter] AS VARCHAR(250)) + '' AS [sql]
       FROM update_stats WITH (NOLOCK)
       WHERE
       (
              [update_my_stats] <> 0
       ) OR
       (
              DATEADD(ww, -4, CONVERT(DATE, GETDATE(), 103)) >= CONVERT(DATE, [updated], 103) AND
              [modification_counter] > 50000
       )
       ;
       --     loop through all statistics to be updated and insert into the statistic log table
       DECLARE @updated DATETIME, @reason AS VARCHAR(1000), @rows BIGINT, @sample_percent INT, @sql_text AS VARCHAR(MAX);
       DECLARE cur_update_statistics CURSOR FOR SELECT [updated], [database_name], [schema_name], [table_name], [index_name], [rows], [sample_percent], [reason], [sql] FROM #update_these_stats
       OPEN cur_update_statistics
              FETCH cur_update_statistics INTO @updated, @db_name, @schema_name, @table_name, @index_name, @rows, @sample_percent, @reason, @sql_text
                    WHILE @@FETCH_STATUS <> - 1
                    BEGIN
                    IF (@debug = 0)
                    BEGIN
                           INSERT INTO DBA_ADMIN.dbo.update_statistics_log ([stat_last_updated_date], [database_name], [schema_name], [table_name], [index_name], [rows], [sample_percent], [reason], [sql])
                           SELECT @updated, @db_name, @schema_name, @table_name, @index_name, @rows, @sample_percent, @reason, @sql_text
                           SET @sSQL = '
                                  USE [' + @db_name + '];
                                  ' + @sql_text + '
                           '
                           EXEC(@sSQL)
                           UPDATE l
                           SET
                                  [runtime_sec] = DATEDIFF(ss, [date_created], GETDATE()),
                                  [stat_update_date_diff] = DATEDIFF(d, [stat_last_updated_date], [date_created])
                           FROM DBA_ADMIN.dbo.update_statistics_log l WITH (NOLOCK)
                           WHERE 
                                  [sql] = @sql_text
                           END
                           IF(@debug = 1)
                           BEGIN
                                  INSERT INTO #update_statistics_debug ([stat_last_updated_date], [database_name], [schema_name], [table_name], [index_name], [rows], [sample_percent], [reason], [sql])
                                  SELECT @updated, @db_name, @schema_name, @table_name, @index_name, @rows, @sample_percent, @reason, @sql_text
                           END
              FETCH cur_update_statistics INTO @updated, @db_name, @schema_name, @table_name, @index_name, @rows, @sample_percent, @reason, @sql_text
       END
       CLOSE cur_update_statistics
       DEALLOCATE cur_update_statistics
       IF (@debug = 1)
       BEGIN
              SELECT * FROM #update_statistics_debug
       END
END
GO
