---- DROP PROCEDURE SP_IFRS_IMP_EXEC_RULE_STAGE;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMP_EXEC_RULE_STAGE(
    IN P_RUNID VARCHAR(20) DEFAULT 'S_00000_0000', 
    IN P_DOWNLOAD_DATE DATE DEFAULT NULL,
    IN P_PRC VARCHAR(1) DEFAULT 'S',
    IN P_MODEL_ID BIGINT DEFAULT 0)
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
    V_STR_SQL_SICR TEXT;      
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

    ---
    V_LOG_SEQ INTEGER;
    V_DIFF_LOG_SEQ INTEGER;
    V_SP_NAME VARCHAR(100);
    V_PRC_NAME VARCHAR(100);
    V_SEQ INTEGER;
    V_SP_NAME_PREV VARCHAR(100);
    STACK TEXT; 
    FCESIG TEXT;

    ---
    V_RULE_CODE1 BIGINT;
    V_VALUE INTEGER;
    V_RULE_TYPE VARCHAR(50);
    V_PKID INT;
    V_AOC VARCHAR(3);
    V_MAX_PKID INT;
    V_MIN_PKID INT;
    V_QG INT;
    V_PREV_QG INT;
    V_NEXT_QG INT;
    V_JML INT;
    V_RN INT;
    V_COLUMN_NAME VARCHAR(250);
    V_DATA_TYPE VARCHAR(250);
    V_OPERATOR VARCHAR(50);
    V_VALUE1 VARCHAR(250);
    V_VALUE2 VARCHAR(250);
    V_UPDATED_COLUMN VARCHAR(30);
    V_SEGMENTATION_ID INTEGER;
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
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TMP_ECL_SICR_RUN_' || P_RUNID || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE TMP_ECL_SICR_RUN_' || P_RUNID || ' AS
    SELECT DISTINCT B.SICR_RULE_ID, B.SEGMENTATION_ID          
    FROM IFRS_ECL_MODEL_HEADER A          
    JOIN IFRS_ECL_MODEL_DETAIL_PF B ON A.PKID = B.ECL_MODEL_ID          
    WHERE A.IS_DELETE = 0          
    AND B.IS_DELETE = 0          
    AND A.PKID = ' || P_MODEL_ID || ' OR (' || P_MODEL_ID || ' = 0 AND (A.ACTIVE_STATUS = ''1''))';
    EXECUTE (V_STR_QUERY);

    FOR V_UPDATED_COLUMN,V_RULE_TYPE,V_RULE_CODE1,V_VALUE,V_SEGMENTATION_ID  IN
    EXECUTE 'SELECT DISTINCT      
		UPDATED_COLUMN          
		,RULE_TYPE
		,A.PKID           
		,B.DETAIL_TYPE         
		,C.SEGMENTATION_ID          
		FROM IFRS_SCENARIO_RULES_HEADER A          
		INNER JOIN IFRS_SCENARIO_RULES_DETAIL B ON A.PKID = B.RULE_ID          
		INNER JOIN TMP_ECL_SICR_RUN_' || P_RUNID || ' C ON A.PKID = C.SICR_RULE_ID          
		WHERE A.RULE_TYPE = ''STAGE'' AND B.DETAIL_TYPE <> ''SICR''          
		ORDER BY A.PKID, B.DETAIL_TYPE DESC'
    LOOP
        V_STR_QUERY := '';
        V_STR_SQL_RULE := '';
        V_STR_SQL_SICR := '';

        FOR V_COLUMN_NAME,V_DATA_TYPE,V_OPERATOR,V_VALUE1,V_VALUE2,V_QG,V_AOC,V_PREV_QG,V_NEXT_QG,V_JML,V_RN,V_PKID IN
        EXECUTE 'SELECT ''A.'' || COLUMN_NAME          
		,DATA_TYPE          
		,OPERATOR          
		,VALUE1          
		,VALUE2          
		,QUERY_GROUPING          
		,AND_OR_CONDITION          
		,LAG(QUERY_GROUPING, 1, MIN_QG) OVER (PARTITION BY RULE_ID ORDER BY QUERY_GROUPING,PKID) PREV_QG          
		,LEAD(QUERY_GROUPING, 1, MAX_QG) OVER (PARTITION BY RULE_ID ORDER BY QUERY_GROUPING,PKID) NEXT_QG          
		,JML          
		,RN          
		,PKID          
		FROM (          
		SELECT MIN(QUERY_GROUPING) OVER (PARTITION BY RULE_ID) MIN_QG          
			,MAX(QUERY_GROUPING) OVER (PARTITION BY RULE_ID) MAX_QG          
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
		FROM IFRS_SCENARIO_RULES_DETAIL          
		WHERE RULE_ID = ' || V_RULE_CODE1 || '          
			AND DETAIL_TYPE = CAST(' || V_VALUE || ' AS VARCHAR(1))       
		) A'
        LOOP
            V_STR_SQL_RULE := COALESCE(V_STR_SQL_RULE, ' ') || ' ' || V_AOC || ' ' || CASE           
            WHEN V_QG <> V_PREV_QG          
            THEN '('          
            ELSE ' '          
            END || COALESCE(CASE           
            WHEN RTRIM(LTRIM(V_DATA_TYPE)) IN ('NUMBER','DECIMAL','NUMERIC','DOUBLE PRECISION','INT')          
            THEN CASE           
            WHEN V_OPERATOR IN ('=','<>','>','<','>=','<=')          
                THEN COALESCE(V_COLUMN_NAME, '') || ' ' || COALESCE(V_OPERATOR, '') || ' ' || COALESCE(V_VALUE1, '')          
                WHEN UPPER(V_OPERATOR) = 'BETWEEN'          
                THEN COALESCE(V_COLUMN_NAME, '') || ' ' || COALESCE(V_OPERATOR, '') || ' ' || COALESCE(V_VALUE1, '') || ' AND ' || COALESCE(V_VALUE2, '')          
                WHEN UPPER(V_OPERATOR) IN ('IN','NOT IN')          
                THEN COALESCE(V_COLUMN_NAME, '') || ' ' || COALESCE(V_OPERATOR, '') || ' ' || '(' || COALESCE(V_VALUE1, '') || ')'          
                ELSE 'XXX'          
                END          
                WHEN RTRIM(LTRIM(V_DATA_TYPE)) = 'DATE'          
                THEN CASE WHEN V_OPERATOR IN ('=','<>','>','<','>=','<=')          
                THEN COALESCE(V_COLUMN_NAME, '') || ' ' || COALESCE(V_OPERATOR, '') || '  TO_DATE(''' || COALESCE(V_VALUE1, '') || ''',''MM/DD/YYYY'')'              
                    WHEN UPPER(V_OPERATOR) = 'BETWEEN'          
                THEN COALESCE(V_COLUMN_NAME, '') || ' ' || COALESCE(V_OPERATOR, '') || ' ' || '   CONVERT(DATE,''' || COALESCE(V_VALUE1, '') || ''',110)' || ' AND ' || '  CONVERT(DATE,''' || COALESCE(V_VALUE2, '') || ''',110)'          
                    WHEN UPPER(V_OPERATOR) IN ('=','<>','>','<','>=','<=')          
                THEN COALESCE(V_COLUMN_NAME, '') || ' ' || COALESCE(V_OPERATOR, '') || ' ' || '(' || '  TO_DATE(''' || COALESCE(V_VALUE1, '') || ''',''MM/DD/YYYY'')' || ')'          
                    ELSE 'XXX'          
                END          
                WHEN UPPER(RTRIM(LTRIM(V_DATA_TYPE))) IN ('CHAR','CHARACTER','VARCHAR','VARCHAR2','BIT')          
                    THEN CASE WHEN RTRIM(LTRIM(V_OPERATOR)) = '='          
                    THEN COALESCE(V_COLUMN_NAME, ' ') || ' ' || COALESCE(V_OPERATOR, ' ') || '''' || COALESCE(V_VALUE1, ' ') || ''''          
                WHEN RTRIM(LTRIM(UPPER(V_OPERATOR))) = 'BETWEEN'          
                    THEN COALESCE(V_COLUMN_NAME, '') || ' ' || COALESCE(V_OPERATOR, '') || '  ' || COALESCE(V_VALUE1, '') || ' AND ' || COALESCE(V_VALUE2, '')          
                WHEN RTRIM(LTRIM(UPPER(V_OPERATOR))) IN ('IN','NOT IN')          
                    THEN COALESCE(V_COLUMN_NAME, '') || ' ' || COALESCE(V_OPERATOR, '') || '  ' || '(''' || COALESCE(REPLACE(V_VALUE1, ',', ''','''), '') || ''')'          
                    ELSE 'XXX'          
                END          
                ELSE 'XXX'          
                END, ' ') || CASE           
                WHEN V_QG <> V_NEXT_QG OR V_RN = V_JML          
                THEN ')'          
                ELSE ' '          
                END;
        END LOOP;

        V_STR_SQL_RULE := '(' || LTRIM(SUBSTRING(V_STR_SQL_RULE, 6, CHAR_LENGTH(V_STR_SQL_RULE)));

        SELECT F_GET_RULES_SICR(V_VALUE, '''' || V_RULE_CODE1 || '''') INTO V_STR_SQL_SICR;
        
        V_STR_SQL_SICR := 'CASE ' || V_STR_SQL_SICR;          
        V_STR_SQL_SICR := V_STR_SQL_SICR || ' WHEN ' || V_VALUE || ' = ' || V_VALUE || '';          
        V_STR_SQL_SICR := V_STR_SQL_SICR || ' THEN ' || V_VALUE || '';          
        V_STR_SQL_SICR := V_STR_SQL_SICR || ' END';          
        ---- ==== SICR CONDITION             
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A SET ' || V_UPDATED_COLUMN || ' = ' || V_STR_SQL_SICR;          
        V_STR_QUERY := V_STR_QUERY || ',          
        SICR_RULE_ID = ''' || LTRIM(RTRIM(CAST(V_RULE_CODE1 AS VARCHAR))) || ''',          
        SICR_FLAG = CASE WHEN ' || V_VALUE || ' = ' || V_STR_SQL_SICR || ' THEN 0 ELSE 1 END';          
        V_STR_QUERY := V_STR_QUERY || ' WHERE A.DOWNLOAD_DATE = ''' || TO_CHAR(V_CURRDATE, 'YYYYMMDD') || ''' ';          
        V_STR_QUERY := V_STR_QUERY || ' AND (' || V_STR_SQL_RULE || ')' || ' AND A.SEGMENTATION_ID = ' || V_SEGMENTATION_ID || '';                  
        EXECUTE (V_STR_QUERY);

        GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
        V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
        V_RETURNROWS := 0;

    END LOOP;

    RAISE NOTICE 'SP_IFRS_IMP_EXEC_RULE_STAGE | AFFECTED RECORD : %', V_RETURNROWS2;
    -------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT1;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_IMP_EXEC_RULE_STAGE';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT1 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

    CALL SP_IFRS_IMP_OVERRIDE_RESTRU_COVID(P_RUNID, V_CURRDATE, P_PRC);

END;

$$;