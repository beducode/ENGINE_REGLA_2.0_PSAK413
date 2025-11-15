---- DROP PROCEDURE SP_IFRS_IMP_PD_CHR_GAMMA_DIST;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMP_PD_CHR_GAMMA_DIST(
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

    ---- QUERY   
    V_STR_QUERY TEXT;

    ---- TABLE LIST       
    V_TABLENAME VARCHAR(100);
    V_TABLEINSERT1 VARCHAR(100);
    V_TABLEINSERT2 VARCHAR(100);
    
    ---- VARIABLE PROCESS
    V_SEGMENT RECORD;
    
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
        V_TABLEINSERT1 := 'IFRS_PD_CHR_RESULT_YEARLY_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_MPD_CHR_RESULT_YEARLY_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'IFRS_PD_CHR_RESULT_YEARLY';
        V_TABLEINSERT2 := 'IFRS_MPD_CHR_RESULT_YEARLY';
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
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT1 || ' AS SELECT * FROM IFRS_PD_CHR_RESULT_YEARLY WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    FOR V_SEGMENT IN 
        EXECUTE 'SELECT DISTINCT 
                PD_RULE_ID 
                ,INCREMENT_PERIOD 
            FROM ' || V_TABLEINSERT1 || ' A 
            JOIN IFRS_PD_RULES_CONFIG B 
            ON A.PD_RULE_ID = B.PKID 
            WHERE DOWNLOAD_DATE = F_EOMONTH(CAST(''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE - (B.INCREMENT_PERIOD * INTERVAL ''1 MONTH'') AS DATE), 0, ''M'', ''NEXT'') 
            AND B.ACTIVE_FLAG = 1 
            AND B.IS_DELETE = 0 '
    LOOP 
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
            (
                DOWNLOAD_DATE 
                ,PROJECTION_DATE 
                ,PD_RULE_ID 
                ,PD_RULE_NAME 
                ,SEGMENT 
                ,CALC_METHOD 
                ,BUCKET_GROUP 
                ,BUCKET_ID 
                ,BUCKET_NAME 
                ,SEQ_YEAR 
                ,CUMULATIVE_PD_RATE 
                ,MARGINAL_PD_RATE 
            ) SELECT 
                DOWNLOAD_DATE  
                ,PROJECTION_DATE  
                ,PD_RULE_ID  
                ,PD_RULE_NAME  
                ,SEGMENT  
                ,CALC_METHOD  
                ,A.BUCKET_GROUP  
                ,MAX_BUCKET_ID  
                ,''NPL'' AS BUCKET_NAME  
                ,SEQ_YEAR  
                ,1  AS CUMULATIVE_PD_RATE  
                ,CASE WHEN SEQ_YEAR = 1 THEN 1 ELSE 0 END AS MARGINAL_PD_RATE   
            FROM ' || V_TABLEINSERT2 || ' A  
            JOIN ' || 'VW_IFRS_MAX_BUCKET' || ' B 
            ON A.BUCKET_GROUP = B.BUCKET_GROUP  
            WHERE DOWNLOAD_DATE = F_EOMONTH(CAST(''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE - (' || V_SEGMENT.INCREMENT_PERIOD || ' * INTERVAL ''1 MONTH'') AS DATE), 0, ''M'', ''NEXT'') 
            AND PD_RULE_ID = ' || V_SEGMENT.PD_RULE_ID || ' 
            AND BUCKET_ID = 1 ';
        EXECUTE (V_STR_QUERY);

        GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
        V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
        V_RETURNROWS := 0;
    END LOOP;

    RAISE NOTICE 'SP_IFRS_IMP_PD_CHR_GAMMA_DIST | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT2;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_IMP_PD_CHR_GAMMA_DIST';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT2 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;