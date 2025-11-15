---- DROP PROCEDURE SP_IFRS_IMP_PD_MAA_CORP_MMULT;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMP_PD_MAA_CORP_MMULT(
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
        V_TABLEINSERT1 := 'IFRS_PD_MAA_CORP_AVERAGE_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_PD_MAA_CORP_MMULT_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'IFRS_PD_MAA_CORP_AVERAGE';
        V_TABLEINSERT2 := 'IFRS_PD_MAA_CORP_MMULT';
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
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT2 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT2 || ' AS SELECT * FROM IFRS_PD_MAA_CORP_MMULT WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT2 || ' A 
        USING ' || 'IFRS_PD_RULES_CONFIG' || ' B 
        WHERE A.PD_RULE_ID = B.PKID 
        AND A.TO_DATE = ''' || CAST(V_CURRDATE_NOLAG AS VARCHAR(10)) || '''::DATE 
        AND B.ACTIVE_FLAG = 1 
        AND B.IS_DELETE = 0 ';
    EXECUTE (V_STR_QUERY);

    FOR V_SEGMENT IN 
        EXECUTE 'SELECT DISTINCT PKID 
            FROM IFRS_PD_RULES_CONFIG 
            WHERE PD_METHOD = ''MAA_CORP'' 
                AND ACTIVE_FLAG = 1 
                AND IS_DELETE  = 0'
    LOOP 
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TMP_BASE_MAA_MMULT';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE TMP_BASE_MAA_MMULT AS 
            SELECT * FROM ' || V_TABLEINSERT1 || ' 
            WHERE TO_DATE = ''' || CAST(V_CURRDATE_NOLAG AS VARCHAR(10)) || '''::DATE 
                AND BUCKET_TO <> 0 
                AND PD_RULE_ID = ' || V_SEGMENT.PKID || ' ';
        EXECUTE (V_STR_QUERY);

        V_MIN_SEQ := 1;

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT EXPECTED_LIFE/INCREMENT_PERIOD 
            FROM ' || 'IFRS_PD_RULES_CONFIG' || ' 
            WHERE PD_METHOD = ''MAA_CORP'' 
            AND ACTIVE_FLAG = 1 
            AND IS_DELETE  = 0 
            AND PKID = ' || V_SEGMENT.PKID || ' ';
        EXECUTE (V_STR_QUERY) INTO V_MAX_SEQ;

        IF V_MIN_SEQ = 1 THEN 
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
                (
                    DOWNLOAD_DATE
                    ,TO_DATE
                    ,PD_RULE_ID
                    ,PD_RULE_NAME
                    ,FL_SEQ
                    ,BUCKET_FROM
                    ,BUCKET_TO
                    ,MMULT
                    ,CREATEDBY
                    ,CREATEDDATE
                ) SELECT 
                    DOWNLOAD_DATE
                    ,''' || CAST(V_CURRDATE_NOLAG AS VARCHAR(10)) || '''::DATE AS TO_DATE
                    ,PD_RULE_ID
                    ,PD_RULE_NAME
                    ,1 AS FL_SEQ
                    ,BUCKET_FROM
                    ,BUCKET_TO
                    , AVERAGE_RATE_FINAL AS  MMULT
                    ,''SP_IFRS_IMP_PD_MAA_CORP_MMULT'' AS CREATEDBY
                    ,CURRENT_TIMESTAMP AS CREATEDDATE
                FROM TMP_BASE_MAA_MMULT  
                WHERE PD_RULE_ID = ' || V_SEGMENT.PKID || ' ';
            EXECUTE (V_STR_QUERY);

            GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
            V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
            V_RETURNROWS := 0;
        END IF;

        V_MIN_SEQ := 2;
        WHILE V_MIN_SEQ <= V_MAX_SEQ 
        LOOP
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
                (
                    DOWNLOAD_DATE
                    ,TO_DATE
                    ,PD_RULE_ID
                    ,PD_RULE_NAME
                    ,FL_SEQ
                    ,BUCKET_FROM
                    ,BUCKET_TO
                    ,MMULT
                    ,CREATEDBY
                    ,CREATEDDATE
                ) SELECT 
                    A.DOWNLOAD_DATE
                    ,A.TO_DATE
                    ,A.PD_RULE_ID
                    ,A.PD_RULE_NAME
                    ,' || V_MIN_SEQ || ' AS FL_SEQ
                    ,B.BUCKET_FROM AS BUCKET_FROM
                    ,A.BUCKET_TO
                    ,SUM(A.AVERAGE_RATE_FINAL*B.MMULT) AS MMULT
                    ,''SP_IFRS_IMP_PD_MAA_AVERAGE''  AS CREATEDBY
                    ,CURRENT_TIMESTAMP AS CREATEDDATE
                FROM TMP_BASE_MAA_MMULT A 
                JOIN (
                    SELECT * FROM ' || V_TABLEINSERT2 || ' 
                    WHERE TO_DATE = ''' || CAST(V_CURRDATE_NOLAG AS VARCHAR(10)) || '''::DATE 
                    AND FL_SEQ = ' || V_MIN_SEQ || '-1 
                    AND PD_RULE_ID = ' || V_SEGMENT.PKID || '
                ) B 
                    ON A.PD_RULE_ID = B.PD_RULE_ID 
                    AND A.BUCKET_FROM = B.BUCKET_TO
                WHERE A.PD_RULE_ID = ' || V_SEGMENT.PKID || '
                GROUP BY 
                    A.DOWNLOAD_DATE
                    ,A.TO_DATE
                    ,A.PD_RULE_ID
                    ,A.PD_RULE_NAME
                    ,B.BUCKET_FROM 
                    ,A.BUCKET_TO ';
            EXECUTE (V_STR_QUERY);

            GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
            V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
            V_RETURNROWS := 0;

            V_MIN_SEQ := V_MIN_SEQ + 1;
        END LOOP;
    END LOOP;

    RAISE NOTICE 'SP_IFRS_IMP_PD_MAA_CORP_MMULT | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT2;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_IMP_PD_MAA_CORP_MMULT';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT2 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;