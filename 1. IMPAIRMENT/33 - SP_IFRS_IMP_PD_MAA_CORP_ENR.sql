---- DROP PROCEDURE SP_IFRS_IMP_PD_MAA_CORP_ENR;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMP_PD_MAA_CORP_ENR(
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
        V_TABLEINSERT1 := 'IFRS_PD_MAA_CORP_ENR_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'IFRS_PD_MAA_CORP_ENR';
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
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT1 || ' AS SELECT * FROM IFRS_PD_MAA_CORP_ENR WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' A 
        USING ' || 'IFRS_PD_RULES_CONFIG' || ' B 
        WHERE A.PD_RULE_ID = B.PKID 
        AND TO_DATE = ''' || CAST(V_CURRDATE_NOLAG AS VARCHAR(10)) || '''::DATE 
        AND B.ACTIVE_FLAG = 1 
        AND B.IS_DELETE = 0 ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (
            DOWNLOAD_DATE
            ,BASE_DATE
            ,TO_DATE
            ,PD_RULE_ID
            ,PD_RULE_NAME
            ,BUCKET_GROUP
            ,BUCKET_FROM
            ,BUCKET_TO
            ,CALC_AMOUNT
            ,CALC_METHOD
            ,CREATEDBY
            ,CREATEDDATE
        ) SELECT 
            DOWNLOAD_DATE
            ,BASE_DATE
            ,''' || CAST(V_CURRDATE_NOLAG AS VARCHAR(10)) || '''::DATE AS TO_DATE
            ,PD_RULE_ID
            ,MAX(A.PD_RULE_NAME)
            ,MAX(A.BUCKET_GROUP)
            ,BUCKET_FROM
            ,BUCKET_TO
            ,SUM(A.CALC_AMOUNT) AS CALC_AMOUNT
            ,MAX(A.CALC_METHOD)
            ,''SP_IFRS_IMP_PD_MAA_ENR'' AS  CREATEDBY
            ,CURRENT_TIMESTAMP AS CREATEDDATE 
        FROM ' || 'IFRS_PD_MAA_CORP_DETAIL' || ' A
        JOIN ' || 'IFRS_PD_RULES_CONFIG' || ' B 
        ON A.PD_RULE_ID = B.PKID
        WHERE TO_DATE = ''' || CAST(V_CURRDATE_NOLAG AS VARCHAR(10)) || '''::DATE 
            AND BUCKET_FROM <> 0 
            AND BUCKET_TO <> 0 
            AND B.ACTIVE_FLAG = 1 
            AND B.IS_DELETE = 0
        GROUP BY DOWNLOAD_DATE,BASE_DATE,TO_DATE,PD_RULE_ID,BUCKET_FROM,BUCKET_TO
        ORDER BY DOWNLOAD_DATE, PD_RULE_ID, BUCKET_FROM, BUCKET_TO ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' 
        SET 
            PERCENTAGE = CASE 
                WHEN B.TOTAL_ACCOUNT = 0 
                THEN NULL 
                ELSE CAST(A.CALC_AMOUNT AS DOUBLE PRECISION)/CAST(B.TOTAL_ACCOUNT AS DOUBLE PRECISION) 
            END
            ,TOTAL_ACCOUNT = B.TOTAL_ACCOUNT
        FROM ' || V_TABLEINSERT1 || ' A
        JOIN (
            SELECT DOWNLOAD_DATE, PD_RULE_ID,BUCKET_FROM, SUM(CALC_AMOUNT) AS TOTAL_ACCOUNT 
            FROM ' || V_TABLEINSERT1 || ' 
            WHERE TO_DATE = ''' || CAST(V_CURRDATE_NOLAG AS VARCHAR(10)) || '''::DATE 
            GROUP BY DOWNLOAD_DATE, PD_RULE_ID,BUCKET_FROM
        ) B 
            ON A.DOWNLOAD_DATE = B.DOWNLOAD_DATE 
            AND A.PD_RULE_ID = B.PD_RULE_ID 
            AND A.BUCKET_FROM = B.BUCKET_FROM 
        JOIN ' || 'IFRS_PD_RULES_CONFIG' || ' C 
            ON A.PD_RULE_ID = C.PKID
        WHERE A.TO_DATE = ''' || CAST(V_CURRDATE_NOLAG AS VARCHAR(10)) || '''::DATE 
            AND C.ACTIVE_FLAG = 1 
            AND C.IS_DELETE = 0 ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TMP_ENR_CURR ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE TMP_ENR_CURR AS 
        SELECT * FROM ' || V_TABLEINSERT1 || ' 
        WHERE TO_DATE = ''' || CAST(V_CURRDATE_NOLAG AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    FOR V_SEGMENT IN 
        EXECUTE 'SELECT DISTINCT PD_RULE_ID, BUCKET_GROUP 
            FROM TMP_ENR_CURR '
    LOOP 
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TMP_ENR_MAA_BASE ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE TMP_ENR_MAA_BASE AS 
            SELECT DOWNLOAD_DATE
                ,BASE_DATE
                ,TO_DATE
                ,PD_RULE_ID
                ,PD_RULE_NAME
                ,BUCKET_GROUP
                ,BUCKET_FROM
                ,0 AS CALC_AMOUNT
                ,TOTAL_ACCOUNT
                ,CALC_METHOD
                ,0 AS PERCENTAGE
                ,MAX(CREATEDBY) AS CREATEDBY
                ,MAX(CREATEDDATE) AS CREATEDDATE
            FROM TMP_ENR_CURR 
            WHERE PD_RULE_ID = ' || V_SEGMENT.PD_RULE_ID || ' 
            GROUP BY DOWNLOAD_DATE
                ,BASE_DATE
                ,TO_DATE
                ,PD_RULE_ID
                ,PD_RULE_NAME
                ,BUCKET_GROUP
                ,BUCKET_FROM
                ,TOTAL_ACCOUNT
                ,CALC_METHOD ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
            (
                DOWNLOAD_DATE
                ,BASE_DATE
                ,TO_DATE
                ,PD_RULE_ID
                ,PD_RULE_NAME
                ,BUCKET_GROUP
                ,BUCKET_FROM
                ,BUCKET_TO
                ,CALC_AMOUNT
                ,TOTAL_ACCOUNT
                ,CALC_METHOD
                ,PERCENTAGE
                ,CREATEDBY
                ,CREATEDDATE
            ) SELECT 
                F_EOMONTH(CAST(''' || CAST(V_CURRDATE_NOLAG AS VARCHAR(10)) || '''::DATE - (C.INCREMENT_PERIOD * INTERVAL ''1 MONTH'') AS DATE), 0, ''M'', ''NEXT'') AS DOWNLOAD_DATE 
                ,F_EOMONTH(CAST(''' || CAST(V_CURRDATE_NOLAG AS VARCHAR(10)) || '''::DATE - (C.INCREMENT_PERIOD * INTERVAL ''1 MONTH'') AS DATE), 0, ''M'', ''NEXT'') AS BASE_DATE 
                ,''' || CAST(V_CURRDATE_NOLAG AS VARCHAR(10)) || '''::DATE AS TO_DATE 
                ,A.PD_RULE_ID::INT
                ,C.TM_RULE_NAME
                ,A.BUCKET_GROUP
                ,A.BUCKET_FROM
                ,A.BUCKET_TO
                ,COALESCE(B.CALC_AMOUNT,0)
                ,COALESCE(B.TOTAL_ACCOUNT,0)
                ,C.CALC_METHOD
                ,CASE WHEN COALESCE(B.TOTAL_ACCOUNT,0) = 0 THEN NULL ELSE 0 END AS PERCENTAGE
                ,''SP_IFRS_IMP_PD_MAA_ENR'' AS CREATEDBY
                ,CURRENT_TIMESTAMP AS CREATEDDATE
                FROM (
                    SELECT 
                        ''' || V_SEGMENT.PD_RULE_ID || ''' AS PD_RULE_ID 
                        ,A.BUCKET_GROUP 
                        ,A.BUCKET_FROM 
                        ,A.BUCKET_TO 
                    FROM ' || 'VW_MAA_FULL_BUCKET' || ' A 
                    LEFT JOIN TMP_ENR_CURR B 
                        ON A.BUCKET_GROUP = B.BUCKET_GROUP 
                        AND A.BUCKET_FROM= B.BUCKET_FROM 
                        AND A.BUCKET_TO = B.BUCKET_TO 
                        AND B.PD_RULE_ID::INT = ' || V_SEGMENT.PD_RULE_ID || ' 
                    WHERE A.BUCKET_GROUP = ''' || V_SEGMENT.BUCKET_GROUP || ''' 
                        AND B.BUCKET_FROM IS NULL 
                    ORDER BY A.BUCKET_FROM, A.BUCKET_TO
                ) A
                JOIN ' || 'IFRS_PD_RULES_CONFIG' || ' C 
                    ON A.PD_RULE_ID::INT = C.PKID
                LEFT JOIN TMP_ENR_MAA_BASE B 
                    ON A.BUCKET_FROM = B.BUCKET_FROM 
                    AND A.PD_RULE_ID::INT = B.PD_RULE_ID
                WHERE C.ACTIVE_FLAG = 1 AND C.IS_DELETE = 0 ';
        EXECUTE (V_STR_QUERY);
        -- RAISE NOTICE '---> %', V_STR_QUERY;

        GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
        V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
        V_RETURNROWS := 0;
    END LOOP;

    RAISE NOTICE 'SP_IFRS_IMP_PD_MAA_CORP_ENR | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT1;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_IMP_PD_MAA_CORP_ENR';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT1 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;