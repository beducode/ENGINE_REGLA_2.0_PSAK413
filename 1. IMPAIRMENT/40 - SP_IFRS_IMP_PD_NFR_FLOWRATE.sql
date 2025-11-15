---- DROP PROCEDURE SP_IFRS_IMP_PD_NFR_FLOWRATE;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMP_PD_NFR_FLOWRATE(
    IN P_RUNID VARCHAR(20) DEFAULT 'S_00000_0000',
    IN P_DOWNLOAD_DATE DATE DEFAULT NULL,
    IN P_PRC VARCHAR(1) DEFAULT 'S')
LANGUAGE PLPGSQL AS $$
DECLARE
    ---- DATE
    V_PREVDATE DATE;
    V_CURRDATE DATE;
    V_LASTYEAR DATE;
    V_CURRMONTH DATE;
    V_LASTYEARNEXTMONTH DATE;
    
    V_PREVDATE_NOLAG DATE;
    V_CURRDATE_NOLAG DATE;

    ---- QUERY   
    V_STR_QUERY TEXT;

    ---- TABLE LIST       
    V_TABLENAME VARCHAR(100);
    V_TABLEINSERT1 VARCHAR(100);
    
    ---- VARIABLE PROCESS
    V_SEGMENT RECORD;
    V_MIN_SEQ INT;
    V_MAX_SEQ INT;
    
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
        V_TABLEINSERT1 := 'IFRS_PD_NFR_FLOWRATE_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'IFRS_PD_NFR_FLOWRATE';
    END IF;
    
    IF P_DOWNLOAD_DATE IS NULL 
    THEN
        SELECT F_EOMONTH(CURRDATE, 1, 'M', 'PREV') INTO V_CURRDATE
        FROM IFRS_PRC_DATE;
        
        SELECT F_EOMONTH(CURRDATE, 0, 'M', 'PREV') INTO V_CURRDATE_NOLAG
        FROM IFRS_PRC_DATE;
    ELSE        
        V_CURRDATE := F_EOMONTH(P_DOWNLOAD_DATE, 1, 'M', 'PREV');
        V_CURRDATE_NOLAG := F_EOMONTH(P_DOWNLOAD_DATE, 0, 'M', 'PREV');
        V_PREVDATE := V_CURRDATE - INTERVAL '1 DAY';
        V_PREVDATE_NOLAG := V_CURRDATE_NOLAG - INTERVAL '1 DAY';
    END IF;

    V_CURRMONTH := F_EOMONTH(V_CURRDATE, 0, 'M', 'NEXT');
    V_LASTYEAR := F_EOMONTH(V_CURRDATE, 1, 'Y', 'PREV');
    V_LASTYEARNEXTMONTH := F_EOMONTH(V_LASTYEAR, 1, 'M', 'NEXT');

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
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT1 || ' AS SELECT * FROM IFRS_PD_NFR_FLOWRATE WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (
            DOWNLOAD_DATE
            ,PD_RULE_ID
            ,PD_RULE_NAME
            ,BUCKET_GROUP
            ,BUCKET_ID
            ,CALC_METHOD
            ,FLOW_RATE
            ,CREATEDBY
            ,CREATEDDATE
        ) SELECT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE
            ,A.PD_RULE_ID
            ,A.PD_RULE_NAME
            ,A.BUCKET_GROUP
            ,A.BUCKET_ID
            ,A.CALC_METHOD
            ,CASE WHEN B.CALC_AMOUNT = 0 THEN 0 ELSE CAST (A.CALC_AMOUNT AS DOUBLE PRECISION)/CAST(B.CALC_AMOUNT AS DOUBLE PRECISION) END AS   FLOW_RATE
            ,''SP_IFRS_IMP_PD_NFR_FLOWRATE'' AS CREATEDBY
            ,CURRENT_TIMESTAMP AS CREATEDDATE
        FROM (
            SELECT 
                DOWNLOAD_DATE
                ,PD_RULE_ID
                ,PD_RULE_NAME
                ,BUCKET_GROUP
                ,BUCKET_ID-1 AS BUCKET_ID
                ,CALC_AMOUNT
                ,CALC_METHOD
                ,CREATEDBY
                ,CREATEDDATE 
            FROM IFRS_PD_NFR_ENR 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND BUCKET_ID > 1 
        ) A 
        JOIN (
            SELECT 
                BUCKET_ID
                ,PD_RULE_ID
                ,CALC_AMOUNT  
            FROM IFRS_PD_NFR_ENR 
            WHERE DOWNLOAD_DATE = F_EOMONTH(CAST(''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE - INTERVAL ''1 MONTH'' AS DATE), 0, ''M'', ''NEXT'')  
        ) B 
            ON A.PD_RULE_ID = B.PD_RULE_ID 
            AND A.BUCKET_ID = B.BUCKET_ID
        JOIN ' || 'IFRS_PD_RULES_CONFIG' || ' C 
            ON A.PD_RULE_ID = C.PKID 
            AND IS_DELETE = 0  
        WHERE C.CUT_OFF_DATE < ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET FLOW_RATE = 1 
        WHERE DOWNLOAD_DATE  = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AND FLOW_RATE >= 1 ';
    EXECUTE (V_STR_QUERY);

    FOR V_SEGMENT IN 
        EXECUTE 'SELECT DISTINCT PD_RULE_ID, BUCKET_GROUP 
            FROM ' || V_TABLEINSERT1 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE '
    LOOP 
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
            (
                DOWNLOAD_DATE
                ,PD_RULE_ID
                ,PD_RULE_NAME
                ,BUCKET_GROUP
                ,BUCKET_ID
                ,CALC_METHOD
                ,FLOW_RATE
                ,CREATEDBY
                ,CREATEDDATE
            ) SELECT 
                ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE
                ,A.PKID AS PD_RULE_ID
                ,A.TM_RULE_NAME AS PD_RULE_NAME
                ,A.BUCKET_GROUP
                ,B.MAX_BUCKET_ID AS  BUCKET_ID
                ,A.CALC_METHOD
                ,1 AS FLOW_RATE
                ,''SP_IFRS_IMP_PD_NFR_FLOWRATE'' AS CREATEDBY
                ,CURRENT_TIMESTAMP AS CREATEDDATE 
            FROM ' || 'IFRS_PD_RULES_CONFIG' || ' A 
            JOIN ' || 'VW_IFRS_MAX_BUCKET' || ' B 
            ON A.BUCKET_GROUP = B.BUCKET_GROUP
            WHERE A.PKID = ' || V_SEGMENT.PD_RULE_ID || ' ';
        EXECUTE (V_STR_QUERY);

        GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
        V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
        V_RETURNROWS := 0;
    END LOOP;

    RAISE NOTICE 'SP_IFRS_IMP_PD_NFR_FLOWRATE | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT1;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_IMP_PD_NFR_FLOWRATE';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT1 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;