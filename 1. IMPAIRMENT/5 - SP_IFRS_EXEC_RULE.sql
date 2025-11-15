---- DROP PROCEDURE SP_IFRS_EXEC_RULE;

CREATE OR REPLACE PROCEDURE SP_IFRS_EXEC_RULE(
    IN P_RUNID VARCHAR(20) DEFAULT 'S_00000_0000',
    IN P_DOWNLOAD_DATE DATE DEFAULT NULL,
    IN P_PRC VARCHAR(1) DEFAULT 'S',
    IN P_CONSTNAME VARCHAR(50) DEFAULT 'GL')
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

    IF COALESCE(P_CONSTNAME, NULL) IS NULL THEN
        P_CONSTNAME := 'GL';
    END IF;

    IF P_PRC = 'S' THEN 
        V_TABLENAME := 'TMP_IMA_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
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
        FOR V_SEGMENT IN 
            EXECUTE 'SELECT DISTINCT UPDATED_TABLE 
            FROM IFRS_SCENARIO_RULES_HEADER WHERE RULE_TYPE = ''' || P_CONSTNAME || ''' '
        LOOP
            IF V_SEGMENT.UPDATED_TABLE <> 'IFRS_MASTER_ACCOUNT' THEN
                V_STR_QUERY := 'DROP TABLE IF EXISTS ' || V_SEGMENT.UPDATED_TABLE || '_' || P_RUNID || ' ';
                EXECUTE (V_STR_QUERY);

                V_STR_QUERY := 'CREATE TABLE ' || V_SEGMENT.UPDATED_TABLE || '_' || P_RUNID || ' AS SELECT * FROM ' || V_SEGMENT.UPDATED_TABLE || ' ';
                EXECUTE (V_STR_QUERY);
            END IF;
        END LOOP;
    END IF;
    -------- ====== PRE SIMULATION TABLE ======
    
    -------- ====== BODY ======
    FOR V_SEGMENT IN 
        EXECUTE 'SELECT DISTINCT ''' || V_TABLENAME || ''', UPDATED_COLUMN, RULE_TYPE, ''' || V_TABLENAME || ''', A.RULE_NAME, A.PKID AS RULE_ID
        FROM IFRS_SCENARIO_RULES_HEADER A 
        INNER JOIN IFRS_SCENARIO_RULES_DETAIL B 
        ON A.PKID = B.RULE_ID 
        AND B.IS_DELETE = 0 
        AND A.IS_DELETE = 0 
        WHERE A.RULE_TYPE = ''' || P_CONSTNAME || ''' '
    LOOP 
        V_CONDITION := NULL;

        FOR V_SEGMENT_RULE IN 
            EXECUTE 'SELECT 
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
                ,SEQUENCE 
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
                ,SEQUENCE 
            FROM IFRS_SCENARIO_RULES_DETAIL 
            WHERE RULE_ID = ' || V_SEGMENT.RULE_ID || ' 
            AND IS_DELETE = 0 ) A '
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
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLENAME  || ' A
            SET ' || V_SEGMENT.UPDATED_COLUMN || ' = ''' || V_SEGMENT.RULE_NAME || ''' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND (' || REPLACE(V_CONDITION, '"', '') || ')';
        EXECUTE (V_STR_QUERY);

        -- RAISE NOTICE '---> %', V_STR_QUERY; 

        GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
        V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
        V_RETURNROWS := 0;
    END LOOP;

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    RAISE NOTICE 'SP_IFRS_EXEC_RULE | AFFECTED RECORD : %', V_RETURNROWS2;
    -------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLENAME;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_EXEC_RULE';
    V_OPERATION = 'UPDATE';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLENAME || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;