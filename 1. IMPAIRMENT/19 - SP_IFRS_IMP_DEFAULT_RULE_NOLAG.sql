---- DROP PROCEDURE SP_IFRS_IMP_DEFAULT_RULE_NOLAG;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMP_DEFAULT_RULE_NOLAG(
    IN P_RUNID VARCHAR(20) DEFAULT 'S_00000_0000',
    IN P_DOWNLOAD_DATE DATE DEFAULT NULL,
    IN P_PRC VARCHAR(1) DEFAULT 'S')
LANGUAGE PLPGSQL AS $$
DECLARE
    ---- DATE
    V_PREVDATE DATE;
    V_CURRDATE DATE;

    ---- QUERY   
    V_STR_QUERY TEXT;

    ---- TABLE LIST       
    V_TABLENAME VARCHAR(100);
    V_TABLEINSERT1 VARCHAR(100);
    V_TABLEINSERT2 VARCHAR(100);
    
    ---- VARIABLE PROCESS
    V_SEG RECORD;
    V_SEG_RULE RECORD;
    V_CONDITION TEXT;
    
    ---- CONDITION
    V_RETURNROWS INT;
    V_RETURNROWS2 INT;
    V_TABLEDEST VARCHAR(100);
    V_COLUMNDEST VARCHAR(100);
    V_SPNAME VARCHAR(100);
    V_OPERATION VARCHAR(100);

    ---- RESULT
    V_QUERYS TEXT;

    --- VARIABLE
    V_SP_NAME VARCHAR(100);
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

    IF P_PRC = 'S' THEN 
        V_TABLENAME := 'TMP_IMA_' || P_RUNID || '';
        V_TABLEINSERT1 := 'IFRS_DEFAULT_NOLAG_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_SCENARIO_GENERATE_QUERY_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'IFRS_DEFAULT_NOLAG';
        V_TABLEINSERT2 := 'IFRS_SCENARIO_GENERATE_QUERY';
    END IF;
    
    IF P_DOWNLOAD_DATE IS NULL 
    THEN
        SELECT
            CURRDATE, PREVDATE INTO V_CURRDATE, V_PREVDATE
        FROM
            IFRS_PRC_DATE;
    ELSE        
        V_CURRDATE := P_DOWNLOAD_DATE;
        V_PREVDATE := V_CURRDATE - INTERVAL '1 DAY';
    END IF;

    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    -------- RECORD RUN_ID --------
    CALL SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, PG_BACKEND_PID(), CURRENT_DATE);
    -------- RECORD RUN_ID --------

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT1 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT1 || ' AS SELECT * FROM IFRS_DEFAULT_NOLAG WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT2 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT2 || ' AS SELECT * FROM IFRS_SCENARIO_GENERATE_QUERY WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TMP_RULE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE TMP_RULE (DEFAULT_RULE_ID BIGINT) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO TMP_RULE (DEFAULT_RULE_ID) 
        SELECT DISTINCT B.PKID FROM IFRS_SCENARIO_RULES_HEADER B 
        WHERE B.RULE_TYPE = ''DEFAULT_RULE_NOLAG'' AND IS_DELETE = 0 ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT2 || ' 
        WHERE RULE_TYPE = ''DEFAULT_RULE_NOLAG'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' A
        USING TMP_RULE B 
        WHERE A.RULE_ID = B.DEFAULT_RULE_ID 
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    FOR V_SEG IN 
        EXECUTE 'SELECT DISTINCT 
            UPDATED_TABLE 
            ,UPDATED_COLUMN 
            ,RULE_TYPE 
            ,TABLE_NAME 
            ,A.RULE_NAME AS RULE_CODE1 
            ,A.PKID AS RULE_ID 
        FROM IFRS_SCENARIO_RULES_HEADER A 
        JOIN IFRS_SCENARIO_RULES_DETAIL B 
        ON A.PKID = B.RULE_ID 
        JOIN TMP_RULE C 
        ON A.PKID = C.DEFAULT_RULE_ID 
        WHERE A.IS_DELETE = 0 
        AND B.IS_DELETE = 0 '
    LOOP 
        V_CONDITION := ' ';
        
        FOR V_SEG_RULE IN 
            EXECUTE 'SELECT 
                ''A.'' || COLUMN_NAME AS COLUMN_NAME 
                ,DATA_TYPE 
                ,OPERATOR 
                ,VALUE1 
                ,VALUE2 
                ,QUERY_GROUPING AS QG
                ,AND_OR_CONDITION AS AOC
                ,LAG(QUERY_GROUPING, 1, MIN_QG) OVER (PARTITION BY RULE_ID ORDER BY QUERY_GROUPING, SEQUENCE) AS PREV_QG 
                ,LEAD(QUERY_GROUPING, 1, MAX_QG) OVER (PARTITION BY RULE_ID ORDER BY QUERY_GROUPING, SEQUENCE) AS NEXT_QG 
                ,JML 
                ,RN 
                ,PKID 
            FROM (
                SELECT 
                    MIN(QUERY_GROUPING) OVER (PARTITION BY RULE_ID) AS MIN_QG 
                    ,MAX(QUERY_GROUPING) OVER (PARTITION BY RULE_ID) AS MAX_QG 
                    ,ROW_NUMBER() OVER (PARTITION BY RULE_ID ORDER BY QUERY_GROUPING, SEQUENCE) RN 
                    ,COUNT(0) OVER (PARTITION BY RULE_ID) JML 
                    ,COLUMN_NAME 
                    ,DATA_TYPE 
                    ,OPERATOR 
                    ,VALUE1 
                    ,VALUE2 
                    ,QUERY_GROUPING 
                    ,RULE_ID 
                    ,AND_OR_CONDITION 
                    ,PKID 
                    ,SEQUENCE 
                FROM IFRS_SCENARIO_RULES_DETAIL 
                WHERE RULE_ID = ' || V_SEG.RULE_ID || ' 
                AND IS_DELETE = 0
            ) A'
        LOOP 
            V_CONDITION = COALESCE(V_CONDITION, ' ') || ' ' || V_SEG_RULE.AOC || ' ' || 
                CASE 
                    WHEN V_SEG_RULE.QG <> V_SEG_RULE.PREV_QG THEN '(' 
                    ELSE ' '
                END || COALESCE(
                CASE 
                    WHEN TRIM(V_SEG_RULE.DATA_TYPE) IN ('NUMBER', 'DECIMAL', 'NUMERIC', 'DOUBLE PRECISION', 'INT') THEN 
                    CASE 
                        WHEN V_SEG_RULE.OPERATOR IN ('=', '<>', '>', '<', '>=', '<=') THEN COALESCE(V_SEG_RULE.COLUMN_NAME, '')
                         || ' '
                         || COALESCE(V_SEG_RULE.OPERATOR, '')
                         || ' '
                         || COALESCE(V_SEG_RULE.VALUE1, '')
                        WHEN UPPER(V_SEG_RULE.OPERATOR) = 'BETWEEN' THEN COALESCE(V_SEG_RULE.COLUMN_NAME, '') 
                         || ' '
                         || COALESCE(V_SEG_RULE.OPERATOR, '')
                         || ' '
                         || COALESCE(V_SEG_RULE.VALUE1, '')
                         || ' AND '
                         || COALESCE(V_SEG_RULE.VALUE2, '')
                        WHEN UPPER(V_SEG_RULE.OPERATOR) = 'IN' THEN COALESCE(V_SEG_RULE.COLUMN_NAME, '') 
                         || ' '
                         || COALESCE(V_SEG_RULE.OPERATOR, '')
                         || ' ('
                         || COALESCE(V_SEG_RULE.VALUE1, '')
                         || ')'
                        ELSE 'xxx' 
                    END
                    WHEN TRIM(V_SEG_RULE.DATA_TYPE) IN ('DATE', 'DATETIME') THEN 
                    CASE 
                        WHEN V_SEG_RULE.OPERATOR IN ('=', '<>', '>', '<', '>=', '<=') THEN COALESCE(V_SEG_RULE.COLUMN_NAME, '')
                         || ' '
                         || COALESCE(V_SEG_RULE.OPERATOR, '')
                         || ' TO_DATE('''
                         || COALESCE(V_SEG_RULE.VALUE1, '') 
                         || ''', ''MM/DD/YYYY'')'
                        WHEN UPPER(V_SEG_RULE.OPERATOR) = 'BETWEEN' THEN COALESCE(V_SEG_RULE.COLUMN_NAME, '')
                         || ' '
                         || COALESCE(V_SEG_RULE.OPERATOR, '')
                         || ' CONVERT(DATE, '''
                         || COALESCE(V_SEG_RULE.VALUE1, '') 
                         || ''', 101) '
                         || ' AND CONVERT(DATE, '''
                         || COALESCE(V_SEG_RULE.VALUE2, '')
                         || ''', 101) '
                        WHEN UPPER(V_SEG_RULE.OPERATOR) IN ('=', '<>', '>', '<', '>=', '<=') THEN COALESCE(V_SEG_RULE.COLUMN_NAME, '')
                         || ' '
                         || COALESCE(V_SEG_RULE.OPERATOR, '')
                         || ' ('
                         || ' TO_DATE('''
                         || COALESCE(V_SEG_RULE.VALUE1, '') 
                         || ''', ''MM/DD/YYYY'')'
                         || ')'
                        ELSE 'xXx'
                    END
                    WHEN TRIM(V_SEG_RULE.DATA_TYPE) IN ('CHAR', 'CHARACTER', 'VARCHAR', 'VARCHAR2', 'BIT') THEN 
                    CASE 
                        WHEN TRIM(V_SEG_RULE.OPERATOR) = '=' THEN COALESCE(V_SEG_RULE.COLUMN_NAME, '')
                         || ' '
                         || COALESCE(V_SEG_RULE.OPERATOR, '')
                         || ' '''
                         || COALESCE(V_SEG_RULE.VALUE1, '') 
                         || ''''
                        WHEN UPPER(TRIM(V_SEG_RULE.OPERATOR)) = 'BETWEEN' THEN COALESCE(V_SEG_RULE.COLUMN_NAME, '') 
                         || ' '
                         || COALESCE(V_SEG_RULE.OPERATOR, '')
                         || ' '
                         || COALESCE(V_SEG_RULE.VALUE1, '') 
                         || ' AND '
                         || COALESCE(V_SEG_RULE.VALUE2, '') 
                        WHEN UPPER(TRIM(V_SEG_RULE.OPERATOR)) = 'IN' THEN COALESCE(V_SEG_RULE.COLUMN_NAME, '') 
                         || ' '
                         || COALESCE(V_SEG_RULE.OPERATOR, '')
                         || ' ('''
                         || COALESCE(REPLACE(V_SEG_RULE.VALUE1, ',', ''','''), '') 
                         || ''')'
                        ELSE 'XXX'
                    END
                    ELSE 'XxX'
                END, ' ') || CASE 
                    WHEN V_SEG_RULE.QG <> V_SEG_RULE.NEXT_QG OR V_SEG_RULE.RN = V_SEG_RULE.JML THEN ')'
                END;
        END LOOP;

        V_CONDITION := '(' || TRIM(SUBSTRING(V_CONDITION, 6, LENGTH(V_CONDITION)));

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
            (
                DOWNLOAD_DATE 
                ,RULE_ID, 
                MASTERID 
                ,ACCOUNT_NUMBER 
                ,CUSTOMER_NUMBER 
                ,OS_AT_DEFAULT 
                ,EQV_AT_DEFAULT 
                ,PLAFOND_AT_DEFAULT 
                ,EQV_PLAFOND_AT_DEFAULT 
                ,EIR_AT_DEFAULT 
                ,CREATED_DATE 
                ,FACILITY_NUMBER 
            ) SELECT 
                DOWNLOAD_DATE 
                ,' || V_SEG.RULE_ID || ' 
                ,MASTERID 
                ,ACCOUNT_NUMBER 
                ,CUSTOMER_NUMBER 
                ,OUTSTANDING 
                ,OUTSTANDING * EXCHANGE_RATE 
                ,PLAFOND 
                ,PLAFOND * EXCHANGE_RATE 
                ,COALESCE(EIR, INTEREST_RATE) 
                ,CURRENT_TIMESTAMP 
                ,FACILITY_NUMBER 
            FROM ' || V_SEG.UPDATED_TABLE || ' A 
            WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND (' || REPLACE(V_CONDITION, '"', '') || ') ';
        EXECUTE (V_STR_QUERY);

        GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
        V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
        V_RETURNROWS := 0;

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || '
            (
                RULE_ID 
                ,RULE_NAME 
                ,RULE_TYPE 
                ,TABLE_NAME 
                ,PD_RULES_QRY_RESULT 
                ,CREATEDBY 
                ,CREATEDDATE
            ) SELECT 
                   ' || V_SEG.RULE_ID                      || '
                ,''' || V_SEG.RULE_CODE1                   || ''' 
                ,''' || V_SEG.RULE_TYPE                    || '''
                ,''' || V_SEG.TABLE_NAME                   || '''
                ,''' || REPLACE(V_CONDITION, '''', '''''') || '''
                ,''SP_IFRS_DFAULT_RULE'' 
                ,CURRENT_TIMESTAMP ';
        EXECUTE (V_STR_QUERY);
    END LOOP;

    RAISE NOTICE 'SP_IFRS_IMP_DEFAULT_RULE_NOLAG | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT1;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_IMP_DEFAULT_RULE_NOLAG';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT1 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;