---- DROP PROCEDURE SP_IFRS_IMP_PD_MAA_CORP_DETAIL;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMP_PD_MAA_CORP_DETAIL(
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
    V_TABLEINSERT3 VARCHAR(100);
    
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
        V_TABLEINSERT1 := 'IFRS_PD_MAA_CORP_DETAIL_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_PD_SCENARIO_DATA_NOLAG_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'IFRS_PD_MAA_CORP_DETAIL';
        V_TABLEINSERT2 := 'IFRS_PD_SCENARIO_DATA_NOLAG';
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
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT1 || ' AS SELECT * FROM IFRS_PD_MAA_CORP_DETAIL WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE TMP_IFRS_PD_MAA_CORP_DETAIL';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO TMP_IFRS_PD_MAA_DETAIL 
        (
            DOWNLOAD_DATE 
            ,BASE_DATE 
            ,TO_DATE 
            ,PD_RULE_ID 
            ,PD_RULE_NAME 
            ,SEGMENT 
            ,BUCKET_GROUP 
            ,PD_UNIQUE_ID 
            ,BUCKET_FROM 
            ,BUCKET_TO 
            ,CALC_METHOD 
            ,CALC_AMOUNT 
            ,OUTSTANDING 
            ,BUCKET_TO_ORIG 
            ,BUCKET_FROM_NAME 
            ,BUCKET_TO_NAME 
            ,NEXT_DEFAULT_FLAG 
            ,CREATEDBY 
            ,CREATEDDATE 
        ) SELECT 
            A.DOWNLOAD_DATE AS DOWNLOAD_DATE 
            ,A.DOWNLOAD_DATE AS BASE_DATE 
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS TO_DATE 
            ,A.PD_RULE_ID 
            ,A.PD_RULE_NAME 
            ,A.SEGMENT 
            ,A.BUCKET_GROUP 
            ,A.PD_UNIQUE_ID 
            ,A.BUCKET_ID AS BUCKET_FROM 
            ,CASE 
                WHEN COALESCE(A.BUCKET_ID, 0) = C.MAX_BUCKET_ID THEN C.MAX_BUCKET_ID 
	            WHEN A.NEXT_DEFAULT_FLAG = 1 THEN C.MAX_BUCKET_ID 
	            WHEN COALESCE(B.BUCKET_ID, 0) = 0 THEN 0 
	            ELSE B.BUCKET_ID 
             END AS BUCKET_TO 
            ,A.CALC_METHOD 
            ,A.CALC_AMOUNT  
            ,A.OUTSTANDING 
            ,B.BUCKET_ID AS BUCKET_TO_ORIG 
            ,NULL AS BUCKET_FROM_NAME 
            ,NULL AS BUCKET_TO_NAME 
            ,A.NEXT_DEFAULT_FLAG 
            ,''SYSTEM'' AS CREATEDBY 
            ,CURRENT_TIMESTAMP AS CREATEDDATE 
        FROM (
            SELECT 
                A.DOWNLOAD_DATE 
                ,A.PD_RULE_ID 
                ,MAX(A.PD_RULE_NAME) AS PD_RULE_NAME 
                ,MAX(A.BUCKET_GROUP) AS BUCKET_GROUP 
                ,MAX(A.SEGMENT) AS SEGMENT 
                ,MAX(A.PD_METHOD) AS PD_METHOD 
                ,MAX(B.CALC_METHOD) AS CALC_METHOD 
                ,CASE WHEN MAX(B.CALC_METHOD) IN (''AOS'',''COS'') THEN SUM(A.OUTSTANDING) ELSE 1 END AS CALC_AMOUNT 
                ,MAX(A.BUCKET_ID) AS BUCKET_ID 
                ,SUM(A.OUTSTANDING) AS OUTSTANDING 
                ,A.PD_UNIQUE_ID 
                ,MAX(CASE WHEN A.NEXT_12M_DEFAULT_FLAG = 1 THEN 1 ELSE 0 END ) AS NEXT_DEFAULT_FLAG 
            FROM ' || V_TABLEINSERT2 || ' A
            JOIN (
                SELECT * 
                FROM IFRS_PD_RULES_CONFIG 
                WHERE PD_METHOD = ''MAA_CORP'' 
                AND ACTIVE_FLAG = 1 
                AND IS_DELETE = 0 
                AND CUT_OFF_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ) B 
            ON A.PD_RULE_ID = B.PKID 
            WHERE DOWNLOAD_DATE = DATE_TRUNC(''MONTH'', ''' || CAST(V_CURRMONTH AS VARCHAR(10)) || '''::DATE + INTERVAL ''1 MONTH'' * (B.INCREMENT_PERIOD * -1)) + INTERVAL ''1 MONTH'' - INTERVAL ''1 DAY'' 
            AND A.BUCKET_ID <> 0 
            GROUP BY PD_RULE_ID, PD_UNIQUE_ID, DOWNLOAD_DATE 
        ) A 
        LEFT JOIN (
            SELECT 
                A.DOWNLOAD_DATE 
                ,A.PD_RULE_ID 
                ,MAX(A.PD_RULE_NAME) AS PD_RULE_NAME 
                ,MAX(A.BUCKET_GROUP) AS BUCKET_GROUP 
                ,MAX(A.SEGMENT) AS SEGMENT 
                ,MAX(A.PD_METHOD) AS PD_METHOD 
                ,MAX(B.CALC_METHOD) AS CALC_METHOD 
                ,CASE WHEN MAX(B.CALC_METHOD) IN (''AOS'',''COS'') THEN SUM(A.OUTSTANDING) ELSE 1 END  AS CALC_AMOUNT 
                ,MAX(A.BUCKET_ID) AS BUCKET_ID 
                ,SUM(A.OUTSTANDING) AS OUTSTANDING 
                ,A.PD_UNIQUE_ID 
                ,MAX(CASE WHEN A.NEXT_12M_DEFAULT_FLAG = 1 THEN 1 ELSE 0 END) AS NEXT_DEFAULT_FLAG 
            FROM ' || V_TABLEINSERT2 || ' A 
            JOIN (
                SELECT * 
                FROM IFRS_PD_RULES_CONFIG 
                WHERE PD_METHOD = ''MAA_CORP'' 
                AND ACTIVE_FLAG = 1 
                AND IS_DELETE = 0 
                AND CUT_OFF_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ) B 
            ON A.PD_RULE_ID = B.PKID 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRMONTH AS VARCHAR(10)) || '''::DATE 
            GROUP BY PD_RULE_ID, PD_UNIQUE_ID, DOWNLOAD_DATE 
        ) B
        ON A.PD_RULE_ID = B.PD_RULE_ID 
        AND A.PD_UNIQUE_ID = B.PD_UNIQUE_ID 
        LEFT JOIN VW_IFRS_MAX_BUCKET C 
        ON A.BUCKET_GROUP = C.BUCKET_GROUP ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' A 
        USING IFRS_PD_RULES_CONFIG B 
        WHERE A.PD_RULE_ID = B.PKID 
        AND TO_DATE = ''' || CAST(V_CURRMONTH AS VARCHAR(10)) || '''::DATE 
        AND B.ACTIVE_FLAG = 1 
        AND B.IS_DELETE = 0 ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        SELECT * FROM TMP_IFRS_PD_MAA_DETAIL ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    RAISE NOTICE 'SP_IFRS_IMP_PD_MAA_CORP_DETAIL | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT1;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_IMP_PD_MAA_CORP_DETAIL';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT1 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;