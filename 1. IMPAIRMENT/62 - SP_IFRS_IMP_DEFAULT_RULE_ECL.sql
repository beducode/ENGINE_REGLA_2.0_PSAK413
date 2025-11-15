---- DROP PROCEDURE SP_IFRS_IMP_DEFAULT_RULE;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMP_DEFAULT_RULE(
    IN P_RUNID VARCHAR(20) DEFAULT 'S_00000_0000', 
    IN P_DOWNLOAD_DATE DATE DEFAULT NULL,
    IN P_PRC VARCHAR(1) DEFAULT 'S',
    IN P_MODEL_ID BIGINT DEFAULT 0,
    IN P_MODEL_TYPE VARCHAR(4) DEFAULT '')
LANGUAGE PLPGSQL AS $$
DECLARE
    ---- DATE
    V_PREVDATE DATE;
    V_PREVMONTH DATE;
    V_CURRDATE DATE;
    V_LASTYEAR DATE;
    V_LASTYEARNEXTMONTH DATE;
      
    V_STR_QUERY TEXT;
    V_STR_SQL_RULE TEXT;        
    V_TABLENAME VARCHAR(100); 
    V_TABLENAME_MON VARCHAR(100);
    V_TABLEINSERT1 VARCHAR(100);
    V_TABLEINSERT2 VARCHAR(100);
    V_TABLEINSERT3 VARCHAR(100);
    V_CODITION TEXT;
    V_RETURNROWS INT;
    V_RETURNROWS2 INT;
    V_TABLEDEST VARCHAR(100);
    V_COLUMNDEST VARCHAR(100);
    V_SPNAME VARCHAR(100);
    V_OPERATION VARCHAR(100);

    ---- RESULT
    V_QUERYS TEXT;
    V_CODITION2 TEXT;
    V_RULE_ID BIGINT;
    RULE_CODE1 VARCHAR(250);
    V_RULE_TYPE VARCHAR(25);
    DEFAULT_FLAG1 VARCHAR(5);
    PD_SEGMENT2 VARCHAR(250);
    DEFAULT_FLAG2 VARCHAR(5);
    V_PKID INT;
    AOC VARCHAR(3);
    MAX_PKID INT;
    MIN_PKID INT;
    QG INT;
    PREV_QG INT;
    NEXT_QG INT;
    V_JML INT;
    V_RN INT;
    PD_SEGMENT_PKID INT;
    V_COLUMN_NAME VARCHAR(250);
    V_DATA_TYPE VARCHAR(250);
    V_OPERATOR VARCHAR(50);
    V_VALUE1 VARCHAR(250);
    V_VALUE2 VARCHAR(250);
    V_DEFAULT_FLAG   VARCHAR(5);
    INCREMENTS INT;
    STATEMENT TEXT;
    HISTORICAL_DATA VARCHAR(30);
    V_TABLE_NAME VARCHAR(30);
    V_UPDATED_TABLE VARCHAR(30);
    V_UPDATED_COLUMN VARCHAR(30);

    ---
    V_LOG_SEQ INTEGER;
    V_DIFF_LOG_SEQ INTEGER;
    V_SP_NAME VARCHAR(100);
    V_PRC_NAME VARCHAR(100);
    V_SEQ INTEGER;
    V_SP_NAME_PREV VARCHAR(100);
    STACK TEXT; 
    FCESIG TEXT;
BEGIN 
    -------- ====== VARIABLE ======
	GET DIAGNOSTICS STACK = PG_CONTEXT;
	FCESIG := substring(STACK from 'function (.*?) line');
	V_SP_NAME := UPPER(LEFT(fcesig::regprocedure::text, POSITION('(' in fcesig::regprocedure::text)-1));

    IF COALESCE(P_PRC, NULL) IS NULL THEN
        P_PRC := 'S';
    END IF;

    IF COALESCE(P_RUNID, NULL) IS NULL THEN
        P_RUNID := 'S_00000_0000';
    END IF;

    IF COALESCE(P_MODEL_ID, NULL) IS NULL THEN
        P_MODEL_ID := 0;
    END IF;

    IF P_PRC = 'S' THEN 
        V_TABLENAME := 'TMP_IMA_' || P_RUNID || '';
        V_TABLENAME_MON := 'TMP_IMAM_' || P_RUNID || '';
        V_TABLEINSERT1 := 'TMP_IFRS_ECL_IMA_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_IMA_IMP_CURR_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_IMA_IMP_PREV_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLENAME_MON := 'IFRS_MASTER_ACCOUNT_MONTHLY';
        V_TABLEINSERT1 := 'TMP_IFRS_ECL_IMA';
        V_TABLEINSERT2 := 'IFRS_IMA_IMP_CURR';
        V_TABLEINSERT3 := 'IFRS_IMA_IMP_PREV';
    END IF;

    IF P_DOWNLOAD_DATE IS NULL 
    THEN
        SELECT
            CURRDATE INTO V_CURRDATE
        FROM
            IFRS_PRC_DATE;
    ELSE        
        V_CURRDATE := P_DOWNLOAD_DATE;
    END IF;

    V_PREVMONTH := F_EOMONTH(V_CURRDATE, 1, 'M', 'PREV');
    V_LASTYEAR := F_EOMONTH(V_CURRDATE, 1, 'Y', 'PREV');
    V_LASTYEARNEXTMONTH := F_EOMONTH(V_LASTYEAR, 1, 'M', 'NEXT');

    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    -------- RECORD RUN_ID --------
    CALL SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, PG_BACKEND_PID(), CURRENT_DATE);
    -------- RECORD RUN_ID --------

    -------- ====== BODY ======
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TEMP_DEFAULT_' || P_RUNID || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE TEMP_DEFAULT_' || P_RUNID || ' AS TABLE IFRS_DEFAULT WITH NO DATA';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TMPRULE_' || P_RUNID || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE TMPRULE_' || P_RUNID || ' (DEFAULT_RULE_ID BIGINT)';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO TMPRULE_' || P_RUNID || ' (
            DEFAULT_RULE_ID
        )
        SELECT
            DISTINCT B.PKID
        FROM
            IFRS_SCENARIO_RULES_HEADER B
        WHERE
            B.RULE_TYPE = ''DEFAULT_RULE_ECL''';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM IFRS_SCENARIO_GENERATE_QUERY 
    WHERE RULE_TYPE = ''DEFAULT_RULE_ECL''';
    EXECUTE (V_STR_QUERY);

    FOR V_UPDATED_TABLE, V_UPDATED_COLUMN, V_RULE_TYPE, V_TABLE_NAME, RULE_CODE1, V_RULE_ID IN

    EXECUTE 'SELECT DISTINCT
                CASE UPDATED_TABLE WHEN ''IFRS_MASTER_ACCOUNT'' THEN ''' || V_TABLEINSERT2 || ''' ELSE UPDATED_TABLE END,
                UPDATED_COLUMN,
                RULE_TYPE,
                CASE TABLE_NAME WHEN ''IFRS_MASTER_ACCOUNT'' THEN ''' || V_TABLEINSERT2 || ''' ELSE TABLE_NAME END,
                A.RULE_NAME,
                A.PKID,
                A.IS_DELETE
            FROM
                IFRS_SCENARIO_RULES_HEADER A
                INNER JOIN IFRS_SCENARIO_RULES_DETAIL B
                ON A.PKID = B.RULE_ID
                INNER JOIN TMPRULE_' || P_RUNID || ' C
                ON A.PKID = C.DEFAULT_RULE_ID
            WHERE
                A.IS_DELETE = 0
                AND B.IS_DELETE = 0'
    LOOP
        V_STR_SQL_RULE := '';
        V_STR_QUERY := '';
        FOR V_COLUMN_NAME, V_DATA_TYPE, V_OPERATOR, V_VALUE1, V_VALUE2, QG, AOC, PREV_QG, NEXT_QG, V_JML, V_RN, V_PKID IN
            SELECT
            'A.' || COLUMN_NAME,
            DATA_TYPE,
            OPERATOR,
            VALUE1,
            VALUE2,
            QUERY_GROUPING,
            AND_OR_CONDITION,
            LAG(QUERY_GROUPING,
            1,
            MIN_QG) OVER (PARTITION BY RULE_ID ORDER BY QUERY_GROUPING,
            SEQUENCE) PREQG,
            LEAD(QUERY_GROUPING,
            1,
            MAX_QG) OVER (PARTITION BY RULE_ID ORDER BY QUERY_GROUPING,
            SEQUENCE) NEXT_QG,
            JML,
            RN,
            PKID
            FROM
            (
            SELECT
            MIN(QUERY_GROUPING) OVER (PARTITION BY RULE_ID) MIN_QG,
            MAX(QUERY_GROUPING) OVER (PARTITION BY RULE_ID) MAX_QG,
            ROW_NUMBER() OVER (PARTITION BY RULE_ID ORDER BY QUERY_GROUPING,
            SEQUENCE) RN,
            COUNT(0) OVER (PARTITION BY RULE_ID) JML,
            COLUMN_NAME,
            DATA_TYPE,
            OPERATOR,
            VALUE1,
            VALUE2,
            QUERY_GROUPING,
            RULE_ID,
            AND_OR_CONDITION,
            PKID,
            SEQUENCE
            FROM
            IFRS_SCENARIO_RULES_DETAIL
            WHERE
            RULE_ID = V_RULE_ID
            AND IS_DELETE = 0
            ) A
        LOOP
            V_STR_SQL_RULE := V_STR_SQL_RULE
                || ' '
                || AOC
                || ' '
                || CASE
                WHEN QG <> PREV_QG THEN
                '('
                ELSE
                ' '
                END
                || COALESCE(
                CASE
                WHEN TRIM(V_DATA_TYPE) IN ('NUMBER', 'DECIMAL', 'NUMERIC', 'DOUBLE PRECISION', 'INT') THEN
                CASE
                WHEN V_OPERATOR IN ('=', '<>', '>', '<', '>=', '<=') THEN
                V_COLUMN_NAME
                || ' '
                || V_OPERATOR
                || ' '
                || V_VALUE1
                WHEN UPPER(V_OPERATOR) = 'BETWEEN' THEN
                V_COLUMN_NAME
                || ' '
                || V_OPERATOR
                || ' '
                || V_VALUE1
                || ' AND '
                || V_VALUE2
                WHEN UPPER(V_OPERATOR) = 'IN' THEN
                V_COLUMN_NAME
                || ' '
                || V_OPERATOR
                || ' ('
                || V_VALUE1
                || ')'
                ELSE
                'XXX'
                END
                WHEN TRIM(V_DATA_TYPE) IN ('DATE', 'DATETIME') THEN
                CASE
                WHEN V_OPERATOR IN ('=', '<>', '>', '<', '>=', '<=') THEN
                V_COLUMN_NAME
                || ' '
                || V_OPERATOR
                || ' TO_DATE('''
                || V_VALUE1
                || ''',''MM/DD/YYYY'')'
                WHEN UPPER(V_OPERATOR) = 'BETWEEN' THEN
                V_COLUMN_NAME
                || ' '
                || V_OPERATOR
                || ' '
                || ' CONVERT(DATE,'''
                || V_VALUE1
                || ''',110)'
                || ' AND '
                || ' CONVERT(DATE,'''
                || V_VALUE2
                || ''',110)'
                WHEN UPPER(V_OPERATOR) IN ('=', '<>', '>', '<', '>=', '<=') THEN
                V_COLUMN_NAME
                || ' '
                || V_OPERATOR
                || ' ('
                || ' TO_DATE('''
                || V_VALUE1
                || ''',''MM/DD/YYYY'')'
                || ')'
                ELSE
                'XXX'
                END
                WHEN UPPER(TRIM(V_DATA_TYPE)) IN ('CHAR', 'CHARACTER', 'VARCHAR', 'VARCHAR2', 'BIT') THEN
                CASE
                WHEN TRIM(V_OPERATOR) = '=' THEN
                V_COLUMN_NAME
                || ' '
                || V_OPERATOR
                || ''''
                || V_VALUE1
                || ''''
                WHEN UPPER(V_OPERATOR) = 'BETWEEN' THEN
                V_COLUMN_NAME
                || ' '
                || V_OPERATOR
                || ' '
                || V_VALUE1
                || ' AND '
                || V_VALUE2
                WHEN UPPER(V_OPERATOR) = 'IN' THEN
                V_COLUMN_NAME
                || ' '
                || V_OPERATOR
                || ' ('''
                || REPLACE(V_VALUE1, ',', ''',''')
                || ''')'
                ELSE
                'XXX'
                END
                ELSE
                'XXX'
                END,
                ' ')
                || CASE
                WHEN QG <> NEXT_QG OR V_RN = V_JML THEN
                ')'
                ELSE
                ' '
            END;
        END LOOP;

         V_STR_SQL_RULE := '(' || TRIM(SUBSTRING(V_STR_SQL_RULE, 6, LENGTH(V_STR_SQL_RULE)));
         V_STR_QUERY := V_STR_QUERY
                || 'SELECT DOWNLOAD_DATE, '
                || V_RULE_ID
                || ', MASTERID, ACCOUNT_NUMBER, CUSTOMER_NUMBER, OUTSTANDING, OUTSTANDING * EXCHANGE_RATE, PLAFOND, PLAFOND * EXCHANGE_RATE, COALESCE(EIR, INTEREST_RATE), CURRENT_DATE FROM '
                || V_UPDATED_TABLE || ' A WHERE A.DOWNLOAD_DATE = ''' || TO_CHAR(V_CURRDATE, 'YYYYMMDD') || ''' AND ' || V_STR_SQL_RULE || ' ';

        EXECUTE FORMAT('INSERT INTO TEMP_DEFAULT_' || P_RUNID || ' (DOWNLOAD_DATE, RULE_ID, MASTERID, ACCOUNT_NUMBER, CUSTOMER_NUMBER, OS_AT_DEFAULT, EQV_AT_DEFAULT, PLAFOND_AT_DEFAULT, EQV_PLAFOND_AT_DEFAULT, EIR_AT_DEFAULT, CREATED_DATE) %s', V_STR_QUERY);
        
        GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
        V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
        V_RETURNROWS := 0;

        INSERT INTO IFRS_SCENARIO_GENERATE_QUERY (
            RULE_ID,
            RULE_NAME,
            RULE_TYPE,
            TABLE_NAME,
            PD_RULES_QRY_RESULT,
            CREATEDBY,
            CREATEDDATE
        ) VALUES (
            CAST(V_RULE_ID AS INT),
            RULE_CODE1,
            V_RULE_TYPE,
            V_TABLE_NAME,
            V_STR_SQL_RULE,
            'SP_IFRS_IMP_DEFAULT_RULE',
            CURRENT_DATE
        );

        GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
        V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
        V_RETURNROWS := 0;

    END LOOP;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS UPDATE_TEMP_DEFAULT_' || P_RUNID || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE UPDATE_TEMP_DEFAULT_' || P_RUNID || ' AS
    SELECT CASE WHEN COALESCE(B.MASTERID,'''') = '''' THEN 0 ELSE 1 END DF_FLAG, A.MASTERID AS MS_ID
    FROM ' || V_TABLEINSERT1 || ' A                 
    LEFT JOIN TEMP_DEFAULT_' || P_RUNID || ' B                  
    ON A.DEFAULT_RULE_ID = B.RULE_ID 
    AND A.MASTERID = B.MASTERID 
    AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE                 
    WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || ''' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || '
    SET DEFAULT_FLAG = B.DF_FLAG
    FROM UPDATE_TEMP_DEFAULT_' || P_RUNID || ' B                 
    WHERE MASTERID = B.MS_ID';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'WITH CTE_BUCKET AS
    (
    SELECT X.MASTERID, X.BUCKET_GROUP, 
    CASE WHEN DEFAULT_FLAG = 1 THEN Y.BUCKET_ID ELSE X.BUCKET_ID END AS BUCKET_ID
    FROM ' || V_TABLEINSERT1 || ' X          
    JOIN          
    (          
    SELECT A.BUCKET_GROUP, MAX(BUCKET_ID) AS BUCKET_ID           
    FROM IFRS_BUCKET_HEADER A JOIN IFRS_BUCKET_DETAIL B ON A.BUCKET_GROUP = B.BUCKET_GROUP           
    GROUP BY A.BUCKET_GROUP          
    ) Y ON X.BUCKET_GROUP = Y.BUCKET_GROUP           
    WHERE X.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
    )
    UPDATE ' || V_TABLEINSERT1 || ' A
    SET BUCKET_ID = B.BUCKET_ID
    FROM CTE_BUCKET B 
    WHERE A.MASTERID = B.MASTERID';
    EXECUTE (V_STR_QUERY);
    

    RAISE NOTICE 'SP_IFRS_IMP_DEFAULT_RULE | AFFECTED RECORD : %', V_RETURNROWS2;
    -------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT2;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_IMP_DEFAULT_RULE';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT2 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;