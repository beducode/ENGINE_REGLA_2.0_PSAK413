---- DROP PROCEDURE SP_IFRS_IMP_PD_MAA_CORP_FITTED;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMP_PD_MAA_CORP_FITTED(
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
        V_TABLEINSERT1 := 'IFRS_PD_MAA_CORP_AVERAGE_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_PD_MAA_CORP_FITTED_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'IFRS_PD_MAA_CORP_AVERAGE';
        V_TABLEINSERT2 := 'IFRS_PD_MAA_CORP_FITTED';
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
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT2 || ' AS SELECT * FROM IFRS_PD_MAA_CORP_FITTED WHERE 1=0 ';
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

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
        (
            DOWNLOAD_DATE  
            ,BASE_DATE  
            ,TO_DATE  
            ,PD_RULE_ID  
            ,PD_RULE_NAME  
            ,BUCKET_GROUP  
            ,BUCKET_ID  
            ,BUCKET_NAME  
            ,DEFAULT_RATE  
            ,ADJUSTED_DEFAULT_RATE  
            ,FITTED_DEFAULT_RATE  
            ,RESIDUAL  
            ,SQUARED_RESIDUAL  
            ,LCL  
            ,UCL  
            ,CREATEDBY  
            ,CREATEDDATE
        ) SELECT 
            DOWNLOAD_DATE  AS DOWNLOAD_DATE  
            ,BASE_DATE AS BASE_DATE  
            ,A.TO_DATE AS TO_DATE  
            ,PD_RULE_ID  
            ,PD_RULE_NAME  
            ,A.BUCKET_GROUP  
            ,BUCKET_FROM AS BUCKET_ID  
            ,NULL AS BUCKET_NAME  
            ,AVERAGE_RATE AS DEFAULT_RATE  
            ,NULL AS ADJUSTED_DEFAULT_RATE  
            ,NULL AS FITTED_DEFAULT_RATE  
            ,NULL AS RESIDUAL  
            ,NULL AS SQUARED_RESIDUAL  
            ,NULL AS LCL  
            ,NULL AS UCL  
            ,''SP_IFRS_IMP_PD_MAA_CORP_FITTED'' AS CREATEDBY  
            ,CURRENT_TIMESTAMP AS CREATEDDATE 
        FROM ' || V_TABLEINSERT1 || ' A  
        JOIN ' || 'VW_IFRS_MAX_BUCKET' || ' B 
            ON A.BUCKET_GROUP = B.BUCKET_GROUP 
            AND A.BUCKET_TO = B.MAX_BUCKET_ID  
        JOIN ' || 'IFRS_PD_RULES_CONFIG' || ' C 
            ON A.PD_RULE_ID = C.PKID  
        WHERE A.TO_DATE = ''' || CAST(V_CURRDATE_NOLAG AS VARCHAR(10)) || '''::DATE   
            AND C.ACTIVE_FLAG = 1 
            AND C.IS_DELETE = 0 ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' A 
        SET ADJUSTED_DEFAULT_RATE = CASE 
            WHEN DEFAULT_RATE = 0 
            THEN 0.0003 
            ELSE DEFAULT_RATE 
        END   
        WHERE A.TO_DATE  = ''' || CAST(V_CURRDATE_NOLAG AS VARCHAR(10)) || '''::DATE  ';
    EXECUTE (V_STR_QUERY);

    FOR V_SEGMENT IN 
        EXECUTE 'SELECT PKID 
            FROM IFRS_PD_RULES_CONFIG 
            WHERE PD_METHOD = ''MAA_CORP'' 
            AND ACTIVE_FLAG = 1 
            AND IS_DELETE = 0   
            ORDER BY PKID '
    LOOP 
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'CF' || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'CF' || ' 
            (X, Y)
            SELECT BUCKET_ID, ADJUSTED_DEFAULT_RATE 
            FROM ' || V_TABLEINSERT2 || ' A 
            WHERE A.TO_DATE = ''' || CAST(V_CURRDATE_NOLAG AS VARCHAR(10)) || '''::DATE 
            AND PD_RULE_ID = ' || V_SEGMENT.PKID || ' 
            ORDER BY BUCKET_ID ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' X 
            SET 
                A = Y.A  
                ,B = Y.B  
                ,R2 =  Y.R2  
                ,FITTED_DEFAULT_RATE = CASE 
                WHEN Z.MAX_BUCKET_ID IS NOT NULL
                THEN 1   
                WHEN Y.A * EXP(Y.b*CAST(X.BUCKET_ID AS DOUBLE PRECISION)) <= 0.0003 
                THEN 0.0003   
                ELSE  Y.A * EXP(Y.b*CAST(X.BUCKET_ID AS DOUBLE PRECISION)) 
                END   
            FROM (
                SELECT 
                    ' || V_SEGMENT.PKID || ' AS PD_RULE_ID 
                    ,* 
                FROM F_CURVE_FITTING(2)
                ) Y, VW_IFRS_MAX_BUCKET Z 
            WHERE X.PD_RULE_ID = Y.PD_RULE_ID 
                AND X.BUCKET_GROUP = Z.BUCKET_GROUP 
                AND X.BUCKET_ID  = Z.MAX_BUCKET_ID  
                AND X.TO_DATE = ''' || CAST(V_CURRDATE_NOLAG AS VARCHAR(10)) || '''::DATE 
                AND X.PD_RULE_ID = ' || V_SEGMENT.PKID || ' ';
        EXECUTE (V_STR_QUERY);
    END LOOP;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET AVERAGE_RATE_FINAL = AVERAGE_RATE
        WHERE A.TO_DATE = ''' || CAST(V_CURRDATE_NOLAG AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' 
        SET AVERAGE_RATE_FINAL = COALESCE(A.AVERAGE_RATE,0) + (
            (CASE 
                WHEN COALESCE(B.TOTAL_AVG,0) = 0 
                THEN 0 
                ELSE COALESCE(A.AVERAGE_RATE,0)/COALESCE(B.TOTAL_AVG,0) 
            END) * (COALESCE(c.DEFAULT_RATE,0) - COALESCE(C.FITTED_DEFAULT_RATE,0))
        )  
        FROM ' || V_TABLEINSERT1 || ' A   
        JOIN (
            SELECT A.TO_DATE, PD_RULE_ID, BUCKET_FROM, SUM (AVERAGE_RATE) AS TOTAL_AVG 
            FROM ' || V_TABLEINSERT1 || ' A   
            JOIN ' || 'VW_IFRS_MAX_BUCKET' || ' B 
                ON A.BUCKET_GROUP = B.BUCKET_GROUP 
                AND A.BUCKET_FROM <> B.MAX_BUCKET_ID 
                AND A.BUCKET_TO <> B.MAX_BUCKET_ID 
            JOIN ' || 'IFRS_PD_RULES_CONFIG' || ' D 
                ON A.PD_RULE_ID = D.PKID 
            WHERE A.TO_DATE = ''' || CAST(V_CURRDATE_NOLAG AS VARCHAR(10)) || '''::DATE  
                AND D.ACTIVE_FLAG = 1 
                AND D.IS_DELETE = 0
            GROUP BY A.TO_DATE, PD_RULE_ID, BUCKET_FROM  
        ) B 
            ON A.TO_DATE = B.TO_DATE  
            AND A.PD_RULE_ID = B.PD_RULE_ID 
            AND A.BUCKET_FROM = B.BUCKET_FROM  
        JOIN ' || V_TABLEINSERT2 || '  C 
            ON A.TO_DATE = C.TO_DATE 
            AND A.BUCKET_FROM = C.BUCKET_ID 
            AND A.PD_RULE_ID = C.PD_RULE_ID  
        JOIN ' || 'IFRS_PD_RULES_CONFIG' || ' D 
            ON A.PD_RULE_ID = D.PKID 
        WHERE A.TO_DATE = ''' || CAST(V_CURRDATE_NOLAG AS VARCHAR(10)) || '''::DATE   
            AND D.ACTIVE_FLAG = 1 
            AND D.IS_DELETE = 0 ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' 
        SET AVERAGE_RATE_FINAL = C.FITTED_DEFAULT_RATE  
        FROM ' || V_TABLEINSERT1 || ' A   
        JOIN ' || 'VW_IFRS_MAX_BUCKET' || ' B 
            ON A.BUCKET_GROUP  = B.BUCKET_GROUP 
            AND A.BUCKET_TO = B.MAX_BUCKET_ID  
        JOIN ' || V_TABLEINSERT2 || '  C 
            ON A.TO_DATE = C.TO_DATE 
            AND A.BUCKET_FROM = C.BUCKET_ID 
            AND A.PD_RULE_ID = C.PD_RULE_ID  
        JOIN ' || 'IFRS_PD_RULES_CONFIG' || ' D 
            ON A.PD_RULE_ID = D.PKID 
        WHERE A.TO_DATE = ''' || CAST(V_CURRDATE_NOLAG AS VARCHAR(10)) || '''::DATE    
            AND D.ACTIVE_FLAG = 1 
            AND D.IS_DELETE = 0 ';
    EXECUTE (V_STR_QUERY);

    RAISE NOTICE 'SP_IFRS_IMP_PD_MAA_CORP_FITTED | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT2;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_IMP_PD_MAA_CORP_FITTED';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT2 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;