
CREATE OR REPLACE PROCEDURE GET_RANDOM_SAMPLE_01()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
    CREATE OR REPLACE TABLE CTRF_PROD.DATA_QUALITY.SAMPLE_DATA_SAMPLE_01 AS
    SELECT *
    FROM CTRF_PROD.DATA_QUALITY.SAMPLE_DATA
    WHERE RANDOM() < 0.01;
$$;
CALL GET_RANDOM_SAMPLE_01();


CREATE OR REPLACE PROCEDURE AGGREGATE_RECODED_SAMPLE_01()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
    CREATE OR REPLACE TABLE CTRF_PROD.DATA_QUALITY.RECODED_SAMPLE_01 AS
    WITH Base AS (
        SELECT 
            USAGE_DATE, 
            SESSION_HOUR, 
            TOTAL_MB_CHARGED, 
            TOTAL_SESSIONS,
            CASE 
                WHEN COUNTRY IN (
                    SELECT COUNTRY 
                    FROM CTRF_PROD.DATA_QUALITY.SAMPLE_DATA_SAMPLE_01 
                    GROUP BY COUNTRY 
                    HAVING COUNT(*) > (SELECT COUNT(*) * 0.01 FROM CTRF_PROD.DATA_QUALITY.SAMPLE_DATA_SAMPLE_01)
                ) THEN COUNTRY ELSE 'Other' 
            END AS COUNTRY,
            CASE 
                WHEN OPERATOR IN (
                    SELECT OPERATOR 
                    FROM CTRF_PROD.DATA_QUALITY.SAMPLE_DATA_SAMPLE_01 
                    GROUP BY OPERATOR 
                    HAVING COUNT(*) > (SELECT COUNT(*) * 0.01 FROM CTRF_PROD.DATA_QUALITY.SAMPLE_DATA_SAMPLE_01)
                ) THEN OPERATOR ELSE 'Other' 
            END AS OPERATOR,
            SERVINGNETWORK, RAT, APN, REGISTRATION_COHORT, ECUTYPE
        FROM CTRF_PROD.DATA_QUALITY.SAMPLE_DATA_SAMPLE_01
        WHERE SESSION_HOUR != -1
    )
    SELECT * FROM Base;
$$;
CALL AGGREGATE_RECODED_SAMPLE_01();


CREATE OR REPLACE PROCEDURE AGGREGATE_NUMERIC_SAMPLE_01()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
    CREATE OR REPLACE TABLE CTRF_PROD.DATA_QUALITY.NUMERIC_AGG_SAMPLE_01 AS
    SELECT 
        USAGE_DATE, 
        SESSION_HOUR, 
        SUM(TOTAL_MB_CHARGED) AS SUM_MB, 
        SUM(TOTAL_SESSIONS) AS SUM_SESSIONS
    FROM CTRF_PROD.DATA_QUALITY.RECODED_SAMPLE_01
    GROUP BY USAGE_DATE, SESSION_HOUR;
$$;
CALL AGGREGATE_NUMERIC_SAMPLE_01();


CREATE OR REPLACE PROCEDURE AGGREGATE_PIVOTS_COUNTRY_SAMPLE_01()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
// Pivot for COUNTRY
var stmt = snowflake.createStatement({sqlText: `
    SELECT LISTAGG(DISTINCT COUNTRY, '|') WITHIN GROUP (ORDER BY COUNTRY)
    FROM CTRF_PROD.DATA_QUALITY.RECODED_SAMPLE_01
    WHERE COUNTRY NOT IN ('Japan', 'South Korea', 'Taiwan, Province of China')
      AND COUNTRY <> '-1'
`});
var result = stmt.execute();
var pivotColumns = "";
if (result.next()) {
    pivotColumns = result.getColumnValue(1);
}
var pivotColumnsArr = pivotColumns.split("|");
var pivotColumnsSQL = pivotColumnsArr.map(function(col) {
    return "'" + col + "'";
}).join(", ");

var pivotSQL = `
    CREATE OR REPLACE TABLE CTRF_PROD.DATA_QUALITY.PIVOT_COUNTRY_SAMPLE_01 AS
    WITH PIVOT_RESULT AS (
        SELECT *
        FROM (
            SELECT 
                USAGE_DATE, 
                SESSION_HOUR, 
                COUNTRY, 
                1 AS CNT 
            FROM CTRF_PROD.DATA_QUALITY.RECODED_SAMPLE_01
            WHERE COUNTRY NOT IN ('Japan', 'South Korea', 'Taiwan, Province of China')
              AND COUNTRY <> '-1'
        ) 
        PIVOT (
            SUM(CNT) FOR COUNTRY IN (${pivotColumnsSQL})
        )
    )
    SELECT * FROM PIVOT_RESULT;
`;
var pivotStmt = snowflake.createStatement({sqlText: pivotSQL});
pivotStmt.execute();
$$;
CALL AGGREGATE_PIVOTS_COUNTRY_SAMPLE_01();

CREATE OR REPLACE PROCEDURE AGGREGATE_PIVOTS_OPERATOR_SAMPLE_01()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
// Pivot for OPERATOR
var stmt = snowflake.createStatement({sqlText: `
    SELECT LISTAGG(DISTINCT OPERATOR, '|') WITHIN GROUP (ORDER BY OPERATOR)
    FROM CTRF_PROD.DATA_QUALITY.RECODED_SAMPLE_01
    WHERE OPERATOR <> '-1'
`});
var result = stmt.execute();
var pivotColumns = "";
if (result.next()) {
    pivotColumns = result.getColumnValue(1);
}
var pivotColumnsArr = pivotColumns.split("|");
var pivotColumnsSQL = pivotColumnsArr.map(function(col) {
    return "'" + col + "'";
}).join(", ");

var pivotSQL = `
    CREATE OR REPLACE TABLE CTRF_PROD.DATA_QUALITY.PIVOT_OPERATOR_SAMPLE_01 AS
    WITH PIVOT_RESULT AS (
        SELECT *
        FROM (
            SELECT 
                USAGE_DATE, 
                SESSION_HOUR, 
                OPERATOR, 
                1 AS CNT 
            FROM CTRF_PROD.DATA_QUALITY.RECODED_SAMPLE_01
            WHERE OPERATOR <> '-1'
        ) 
        PIVOT (
            SUM(CNT) FOR OPERATOR IN (${pivotColumnsSQL})
        )
    )
    SELECT * FROM PIVOT_RESULT;
`;
var pivotStmt = snowflake.createStatement({sqlText: pivotSQL});
pivotStmt.execute();
$$;
CALL AGGREGATE_PIVOTS_OPERATOR_SAMPLE_01();

CREATE OR REPLACE PROCEDURE AGGREGATE_PIVOTS_SERVINGNETWORK_SAMPLE_01()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
// Pivot for SERVINGNETWORK
var stmt = snowflake.createStatement({sqlText: `
    SELECT LISTAGG(DISTINCT SERVINGNETWORK, '|') WITHIN GROUP (ORDER BY SERVINGNETWORK)
    FROM CTRF_PROD.DATA_QUALITY.RECODED_SAMPLE_01
    WHERE SERVINGNETWORK <> '-1'
`});
var result = stmt.execute();
var pivotColumns = "";
if (result.next()) {
    pivotColumns = result.getColumnValue(1);
}
var pivotColumnsArr = pivotColumns.split("|");
var pivotColumnsSQL = pivotColumnsArr.map(function(col) {
    return "'" + col + "'";
}).join(", ");

var pivotSQL = `
    CREATE OR REPLACE TABLE CTRF_PROD.DATA_QUALITY.PIVOT_SERVINGNETWORK_SAMPLE_01 AS
    WITH PIVOT_RESULT AS (
        SELECT *
        FROM (
            SELECT 
                USAGE_DATE, 
                SESSION_HOUR, 
                SERVINGNETWORK, 
                1 AS CNT 
            FROM CTRF_PROD.DATA_QUALITY.RECODED_SAMPLE_01
            WHERE SERVINGNETWORK <> '-1'
        ) 
        PIVOT (
            SUM(CNT) FOR SERVINGNETWORK IN (${pivotColumnsSQL})
        )
    )
    SELECT * FROM PIVOT_RESULT;
`;
var pivotStmt = snowflake.createStatement({sqlText: pivotSQL});
pivotStmt.execute();
$$;
CALL AGGREGATE_PIVOTS_SERVINGNETWORK_SAMPLE_01();


CREATE OR REPLACE PROCEDURE AGGREGATE_PIVOTS_RAT_SAMPLE_01()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
// Pivot for RAT
var stmt = snowflake.createStatement({sqlText: `
    SELECT LISTAGG(DISTINCT RAT, '|') WITHIN GROUP (ORDER BY RAT)
    FROM CTRF_PROD.DATA_QUALITY.RECODED_SAMPLE_01
    WHERE RAT <> '-1'
`});
var result = stmt.execute();
var pivotColumns = "";
if (result.next()) {
    pivotColumns = result.getColumnValue(1);
}
var pivotColumnsArr = pivotColumns.split("|");
var pivotColumnsSQL = pivotColumnsArr.map(function(col) {
    return "'" + col + "'";
}).join(", ");

var pivotSQL = `
    CREATE OR REPLACE TABLE CTRF_PROD.DATA_QUALITY.PIVOT_RAT_SAMPLE_01 AS
    WITH PIVOT_RESULT AS (
        SELECT *
        FROM (
            SELECT 
                USAGE_DATE, 
                SESSION_HOUR, 
                RAT, 
                1 AS CNT 
            FROM CTRF_PROD.DATA_QUALITY.RECODED_SAMPLE_01
            WHERE RAT <> '-1'
        ) 
        PIVOT (
            SUM(CNT) FOR RAT IN (${pivotColumnsSQL})
        )
    )
    SELECT * FROM PIVOT_RESULT;
`;
var pivotStmt = snowflake.createStatement({sqlText: pivotSQL});
pivotStmt.execute();
$$;
CALL AGGREGATE_PIVOTS_RAT_SAMPLE_01();


CREATE OR REPLACE PROCEDURE AGGREGATE_PIVOTS_APN_SAMPLE_01()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
// Pivot for APN
var stmt = snowflake.createStatement({sqlText: `
    SELECT LISTAGG(DISTINCT APN, '|') WITHIN GROUP (ORDER BY APN)
    FROM CTRF_PROD.DATA_QUALITY.RECODED_SAMPLE_01
    WHERE APN <> '-1'
`});
var result = stmt.execute();
var pivotColumns = "";
if (result.next()) {
    pivotColumns = result.getColumnValue(1);
}
var pivotColumnsArr = pivotColumns.split("|");
var pivotColumnsSQL = pivotColumnsArr.map(function(col) {
    return "'" + col + "'";
}).join(", ");

var pivotSQL = `
    CREATE OR REPLACE TABLE CTRF_PROD.DATA_QUALITY.PIVOT_APN_SAMPLE_01 AS
    WITH PIVOT_RESULT AS (
        SELECT *
        FROM (
            SELECT 
                USAGE_DATE, 
                SESSION_HOUR, 
                APN, 
                1 AS CNT 
            FROM CTRF_PROD.DATA_QUALITY.RECODED_SAMPLE_01
            WHERE APN <> '-1'
        ) 
        PIVOT (
            SUM(CNT) FOR APN IN (${pivotColumnsSQL})
        )
    )
    SELECT * FROM PIVOT_RESULT;
`;
var pivotStmt = snowflake.createStatement({sqlText: pivotSQL});
pivotStmt.execute();
$$;
CALL AGGREGATE_PIVOTS_APN_SAMPLE_01();


CREATE OR REPLACE PROCEDURE AGGREGATE_FINAL_SAMPLE_01()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
    CREATE OR REPLACE TABLE CTRF_PROD.DATA_QUALITY.AGGREGATED_FOR_IF_DYNAMIC_ALL_SAMPLE_01 AS
    SELECT 
        *
    FROM CTRF_PROD.DATA_QUALITY.NUMERIC_AGG_SAMPLE_01 N
    LEFT JOIN CTRF_PROD.DATA_QUALITY.PIVOT_COUNTRY_SAMPLE_01 C USING (USAGE_DATE, SESSION_HOUR)
    LEFT JOIN CTRF_PROD.DATA_QUALITY.PIVOT_OPERATOR_SAMPLE_01 O USING (USAGE_DATE, SESSION_HOUR)
    LEFT JOIN CTRF_PROD.DATA_QUALITY.PIVOT_SERVINGNETWORK_SAMPLE_01 S USING (USAGE_DATE, SESSION_HOUR)
    LEFT JOIN CTRF_PROD.DATA_QUALITY.PIVOT_RAT_SAMPLE_01 R USING (USAGE_DATE, SESSION_HOUR)
    LEFT JOIN CTRF_PROD.DATA_QUALITY.PIVOT_APN_SAMPLE_01 A USING (USAGE_DATE, SESSION_HOUR)
    ORDER BY N.USAGE_DATE, N.SESSION_HOUR;
$$;
CALL AGGREGATE_FINAL_SAMPLE_01();


DECLARE
  sql_cmd STRING;
BEGIN
  SELECT 
    'UPDATE CTRF_PROD.DATA_QUALITY.AGGREGATED_FOR_IF_DYNAMIC_ALL_SAMPLE_01 SET ' ||
    LISTAGG('"' || COLUMN_NAME || '" = COALESCE("' || COLUMN_NAME || '", 0)', ', ') || ';'
  INTO sql_cmd
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_CATALOG = 'CTRF_PROD'
    AND TABLE_SCHEMA = 'DATA_QUALITY'
    AND TABLE_NAME = 'AGGREGATED_FOR_IF_DYNAMIC_ALL_SAMPLE_01'
    AND DATA_TYPE IN ('NUMBER', 'FLOAT', 'DECIMAL', 'INTEGER', 'INT', 'DOUBLE');

  EXECUTE IMMEDIATE sql_cmd;
END;
