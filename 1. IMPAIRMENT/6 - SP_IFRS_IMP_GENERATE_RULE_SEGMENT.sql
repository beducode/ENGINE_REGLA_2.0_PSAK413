---- DROP PROCEDURE SP_IFRS_IMP_GENERATE_RULE_SEGMENT;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMP_GENERATE_RULE_SEGMENT(
    IN P_RUNID VARCHAR(20) DEFAULT 'S_00000_0000',
    IN P_DOWNLOAD_DATE DATE DEFAULT NULL,
    IN P_PRC VARCHAR(1) DEFAULT 'S')
LANGUAGE PLPGSQL AS $$
DECLARE
    ---- DATE
    V_CURRMONTH DATE;
    V_PREVDATE DATE;
    V_PREVMONTH DATE;
    V_CURRDATE DATE;
    V_LASTYEAR DATE;
    V_LASTYEARNEXTMONTH DATE;
    V_STARTDATEOFYEAR DATE;

    ---- QUERY   
    V_STR_QUERY TEXT;

    ---- TABLE LIST       
    V_TABLENAME VARCHAR(100); 
    V_TABLEINSERT VARCHAR(100);

    ---- VARIABLE PROCESS
    V_SEGMENT RECORD;
    V_SEGMENT_RULE RECORD;
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
        V_TABLEINSERT := 'IFRS_SCENARIO_SEGMENT_GENERATE_QUERY_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT := 'IFRS_SCENARIO_SEGMENT_GENERATE_QUERY';
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
    
    V_PREVDATE := V_CURRDATE - INTERVAL '1 DAY';
    V_CURRMONTH := F_EOMONTH(V_CURRDATE, 0, 'M', 'NEXT');
    V_PREVMONTH := F_EOMONTH(V_CURRDATE, 1, 'M', 'PREV');
    V_LASTYEAR := F_EOMONTH(V_CURRDATE, 1, 'Y', 'PREV');
    V_LASTYEARNEXTMONTH := F_EOMONTH(V_LASTYEAR, 1, 'M', 'NEXT');
    V_STARTDATEOFYEAR := (DATE_PART('YEAR', V_CURRDATE) || '-01-01')::DATE;
    
    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    -------- RECORD RUN_ID --------
    CALL SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, PG_BACKEND_PID(), CURRENT_DATE);
    -------- RECORD RUN_ID --------

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT || ' AS SELECT * FROM IFRS_SCENARIO_SEGMENT_GENERATE_QUERY WHERE 0=1';
        EXECUTE (V_STR_QUERY);
    ELSE
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT || ' ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======
    
    -------- ====== BODY ======
    FOR V_SEGMENT IN
        EXECUTE 'SELECT DISTINCT SEGMENT_TYPE, GROUP_SEGMENT, SEGMENT, SUB_SEGMENT, RULE_ID, TABLE_NAME, SEQUENCE
        FROM (SELECT DISTINCT A.SEGMENT_TYPE, A.GROUP_SEGMENT, A.SEGMENT, A.SUB_SEGMENT, B.RULE_ID, B.TABLE_NAME, A.SEQUENCE
        FROM IFRS_MSTR_SEGMENT_RULES_HEADER A
        INNER JOIN IFRS_MSTR_SEGMENT_RULES_DETAIL B ON A.PKID = B.RULE_ID
        WHERE COALESCE(A.IS_DELETE, 0) = 0) SEGMENT
        ORDER BY RULE_ID'
    LOOP
        V_CONDITION := NULL;

        FOR V_SEGMENT_RULE IN
            EXECUTE 'SELECT DISTINCT
                TRIM(''A."'' || COALESCE(COLUMN_NAME, '''') || ''"'') AS COLUMN_NAME
                ,TRIM(DATA_TYPE) AS DATA_TYPE
                ,TRIM(OPERATOR) AS OPERATOR
                ,TRIM(VALUE1) AS VALUE1
                ,TRIM(VALUE2) AS VALUE2
                ,QUERY_GROUPING AS QG
                ,TRIM(AND_OR_CONDITION) AS AOC
                ,LAG(QUERY_GROUPING, 1, MIN_QG) OVER (PARTITION BY RULE_ID ORDER BY QUERY_GROUPING, SEQUENCE) AS PREV_QG
                ,LEAD(QUERY_GROUPING, 1, MAX_QG) OVER (PARTITION BY RULE_ID ORDER BY QUERY_GROUPING, SEQUENCE) AS NEXT_QG
                ,JML
                ,RN
                ,PKID
            FROM (SELECT
                MIN(QUERY_GROUPING) OVER (PARTITION BY RULE_ID) AS MIN_QG
                ,MAX(QUERY_GROUPING) OVER (PARTITION BY RULE_ID) AS MAX_QG
                ,ROW_NUMBER() OVER (PARTITION BY RULE_ID ORDER BY QUERY_GROUPING, SEQUENCE) AS RN
                ,COUNT(0) OVER (PARTITION BY RULE_ID) AS JML
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
            FROM IFRS_MSTR_SEGMENT_RULES_DETAIL
            WHERE RULE_ID = ' || V_SEGMENT.RULE_ID || ') A'
        LOOP
            V_CONDITION := COALESCE(V_CONDITION, '');
            
            -- ADD LOGICAL OPERATOR
            V_CONDITION := V_CONDITION || ' ' || COALESCE(
                CASE
                    WHEN V_SEGMENT_RULE.QG > V_SEGMENT_RULE.PREV_QG THEN 'OR'
                    WHEN V_SEGMENT_RULE.QG = V_SEGMENT_RULE.PREV_QG THEN V_SEGMENT_RULE.AOC
                END, '');

            -- ADD OPENING PARANTHESIS
            V_CONDITION := V_CONDITION || ' ' || COALESCE(
                CASE
                    WHEN V_SEGMENT_RULE.QG <> V_SEGMENT_RULE.PREV_QG THEN '('
                    ELSE ' '
                END, '');

            -- ADD CONDITION
            V_CONDITION := V_CONDITION || COALESCE(
                CASE
                    WHEN UPPER(V_SEGMENT_RULE.DATA_TYPE) IN ('NUMBER', 'DECIMAL', 'NUMERIC', 'DOUBLE PRECISION', 'INT') THEN
                    CASE
                        WHEN V_SEGMENT_RULE.OPERATOR IN ('=', '<>', '>', '<', '>=', '<=') THEN 
                            COALESCE(V_SEGMENT_RULE.COLUMN_NAME, '') || ' ' ||
                            COALESCE(V_SEGMENT_RULE.OPERATOR, '') || ' ' ||
                            COALESCE(V_SEGMENT_RULE.VALUE1, '')
                        WHEN UPPER(V_SEGMENT_RULE.OPERATOR) = 'BETWEEN' THEN
                            COALESCE(V_SEGMENT_RULE.COLUMN_NAME, '') || ' ' ||
                            COALESCE(V_SEGMENT_RULE.OPERATOR, '') || ' ' ||
                            COALESCE(V_SEGMENT_RULE.VALUE1, '') || ' ' ||
                            'AND ' || COALESCE(V_SEGMENT_RULE.VALUE2, '')
                        WHEN UPPER(V_SEGMENT_RULE.OPERATOR) IN ('IN', 'NOT IN') THEN
                            COALESCE(V_SEGMENT_RULE.COLUMN_NAME, '') || ' ' ||
                            COALESCE(V_SEGMENT_RULE.OPERATOR, '') || ' ' ||
                            '(' || COALESCE(V_SEGMENT_RULE.VALUE1, '') || ')'
                        ELSE 'xxx'
                    END
                    WHEN UPPER(V_SEGMENT_RULE.DATA_TYPE) IN ('DATE', 'DATETIME') THEN
                    CASE
                        WHEN V_SEGMENT_RULE.OPERATOR IN ('=', '<>', '>', '<', '>=', '<=') THEN 
                            COALESCE(V_SEGMENT_RULE.COLUMN_NAME, '') || ' ' ||
                            COALESCE(V_SEGMENT_RULE.OPERATOR, '') || ' ' ||
                            'DATE(''' || COALESCE(REPLACE(V_SEGMENT_RULE.VALUE1, ' ', '/'), '') || ''')::DATE'
                        WHEN UPPER(V_SEGMENT_RULE.OPERATOR) = 'BETWEEN' THEN
                            COALESCE(V_SEGMENT_RULE.COLUMN_NAME, '') || ' ' ||
                            COALESCE(V_SEGMENT_RULE.OPERATOR, '') || ' ' ||
                            'DATE(''' || COALESCE(REPLACE(V_SEGMENT_RULE.VALUE1, ' ', '/'), '') || ''')::DATE' ||
                            ' AND ' ||
                            'DATE(''' || COALESCE(REPLACE(V_SEGMENT_RULE.VALUE2, ' ', '/'), '') || ''')::DATE'
                        ELSE 'xXx'
                    END
                    WHEN UPPER(V_SEGMENT_RULE.DATA_TYPE) IN ('CHAR', 'CHARACTER', 'VARCHAR', 'VARCHAR2', 'BIT') THEN
                    CASE
                        WHEN V_SEGMENT_RULE.OPERATOR = '=' THEN
                            COALESCE(V_SEGMENT_RULE.COLUMN_NAME, '') || ' ' ||
                            COALESCE(V_SEGMENT_RULE.OPERATOR, '') || ' ' ||
                            '''' || COALESCE(V_SEGMENT_RULE.VALUE1, '') || ''''
                        WHEN UPPER(V_SEGMENT_RULE.OPERATOR) = 'BETWEEN' THEN
                            COALESCE(V_SEGMENT_RULE.COLUMN_NAME, '') || ' ' ||
                            COALESCE(V_SEGMENT_RULE.OPERATOR, '') || ' ' ||
                            '''' || COALESCE(V_SEGMENT_RULE.VALUE1, '') || '''' ||
                            ' AND ' ||
                            '''' || COALESCE(V_SEGMENT_RULE.VALUE2, '') || ''''
                        WHEN UPPER(V_SEGMENT_RULE.OPERATOR) IN ('IN', 'NOT IN') THEN
                            COALESCE(V_SEGMENT_RULE.COLUMN_NAME, '') || ' ' ||
                            COALESCE(V_SEGMENT_RULE.OPERATOR, '') || ' ' ||
                            '(''' || COALESCE(REPLACE(V_SEGMENT_RULE.VALUE1, ',', ''','''), '') || ''')'
                        ELSE 'XXX'
                    END
                    ELSE 'XxX'
                END, '');

            -- ADD CLOSING PARANTHESIS
            V_CONDITION := V_CONDITION || COALESCE(
                CASE
                    WHEN V_SEGMENT_RULE.QG <> V_SEGMENT_RULE.NEXT_QG
                    OR V_SEGMENT_RULE.RN = V_SEGMENT_RULE.JML THEN ')'
                    ELSE ' '
                END, '');
            
        END LOOP;

        V_CONDITION := '(' || COALESCE(LTRIM(SUBSTRING(V_CONDITION FROM 6 FOR LENGTH(TRIM(TRAILING FROM V_CONDITION)))), '');

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT || '
        (
            RULE_ID,
            SEGMENT_TYPE,
            GROUP_SEGMENT,
            SUB_SEGMENT,
            SEGMENT,
            TABLE_NAME,
            CONDITION,
            SEQUENCE
        ) VALUES (
            ''' || V_SEGMENT.RULE_ID || ''',
            ''' || V_SEGMENT.SEGMENT_TYPE || ''',
            ''' || V_SEGMENT.GROUP_SEGMENT || ''',
            ''' || V_SEGMENT.SUB_SEGMENT || ''',
            ''' || V_SEGMENT.SEGMENT || ''',
            ''' || V_SEGMENT.TABLE_NAME || ''',
            ''' || REPLACE(V_CONDITION, '''', '''''') || ''',
        ';

        IF V_SEGMENT.SEQUENCE IS NOT NULL THEN
            V_STR_QUERY := V_STR_QUERY || V_SEGMENT.SEQUENCE || ');';
        ELSE
            V_STR_QUERY := V_STR_QUERY || 'NULL);';
        END IF;

        EXECUTE (V_STR_QUERY);

        GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
        V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
        V_RETURNROWS := 0;

    END LOOP;

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    RAISE NOTICE 'SP_IFRS_IMP_GENERATE_RULE_SEGMENT | AFFECTED RECORD : %', V_RETURNROWS2;
    -------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_IMP_GENERATE_RULE_SEGMENT';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;