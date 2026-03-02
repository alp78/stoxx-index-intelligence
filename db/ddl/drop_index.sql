/*
    Removes ALL data for a given index from every table.
    Set @key below and execute. Pure T-SQL, works in any client.
*/

DECLARE @key      NVARCHAR(100) = 'stoxx_asia_50';  -- << SET THIS
DECLARE @prefix   NVARCHAR(100) = REPLACE(@key, '_', '');
DECLARE @ohlcv    NVARCHAR(100) = @prefix + '_ohlcv';
DECLARE @sql      NVARCHAR(500);

PRINT '=== Dropping index: ' + @key + ' (prefix: ' + @prefix + ', ohlcv: ' + @ohlcv + ') ===';

-- 1. Silver layer (derived)
PRINT 'Deleting silver.signals_quarterly ...';
DELETE FROM silver.signals_quarterly WHERE _index = @key;

PRINT 'Deleting silver.signals_daily ...';
DELETE FROM silver.signals_daily WHERE _index = @key;

PRINT 'Deleting silver.index_dim ...';
DELETE FROM silver.index_dim WHERE _index = @key;

IF OBJECT_ID('silver.' + @ohlcv, 'U') IS NOT NULL
BEGIN
    PRINT 'Dropping silver.' + @ohlcv + ' ...';
    SET @sql = 'DROP TABLE silver.' + QUOTENAME(@ohlcv);
    EXEC sp_executesql @sql;
END

-- 2. Bronze layer (source)
PRINT 'Deleting bronze.pulse ...';
DELETE FROM bronze.pulse WHERE _index = @key;

PRINT 'Deleting bronze.pulse_tickers ...';
DELETE FROM bronze.pulse_tickers WHERE _index = @key;

PRINT 'Deleting bronze.signals_quarterly ...';
DELETE FROM bronze.signals_quarterly WHERE _index = @key;

PRINT 'Deleting bronze.signals_daily ...';
DELETE FROM bronze.signals_daily WHERE _index = @key;

PRINT 'Deleting bronze.index_dim ...';
DELETE FROM bronze.index_dim WHERE _index = @key;

IF OBJECT_ID('bronze.' + @ohlcv, 'U') IS NOT NULL
BEGIN
    PRINT 'Dropping bronze.' + @ohlcv + ' ...';
    SET @sql = 'DROP TABLE bronze.' + QUOTENAME(@ohlcv);
    EXEC sp_executesql @sql;
END

PRINT '=== Done: ' + @key + ' removed ===';
