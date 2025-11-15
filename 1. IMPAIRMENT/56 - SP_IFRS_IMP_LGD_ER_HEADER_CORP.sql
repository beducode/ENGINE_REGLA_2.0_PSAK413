---- DROP PROCEDURE SP_IFRS_IMP_LGD_ER_HEADER_CORP;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMP_LGD_ER_HEADER_CORP(
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
        V_TABLEINSERT1 := 'IFRS_LGD_ER_HEADER_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_LGD_ER_DETAIL_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_LGD_TERM_STRUCTURE_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'IFRS_LGD_ER_HEADER';
        V_TABLEINSERT2 := 'IFRS_LGD_ER_DETAIL';
        V_TABLEINSERT3 := 'IFRS_LGD_TERM_STRUCTURE';
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
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT1 || ' AS SELECT * FROM IFRS_LGD_ER_HEADER WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT3 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT3 || ' AS SELECT * FROM IFRS_LGD_TERM_STRUCTURE WHERE 1=0 ';
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
            ,CALC_METHOD  
            ,LGD_METHOD  
            ,LGD_RULE_ID  
            ,LGD_RULE_NAME  
            ,PRODUCT_GROUP  
            ,SEGMENT  
            ,SUB_SEGMENT  
            ,GROUP_SEGMENT  
            ,RECOVERY_RATE  
            ,LGD  
        ) SELECT   
            DOWNLOAD_DATE  
            ,A.CALC_METHOD  
            ,A.LGD_METHOD  
            ,A.LGD_RULE_ID  
            ,A.LGD_RULE_NAME  
            ,A.PRODUCT_GROUP  
            ,A.SEGMENT  
            ,A.SUB_SEGMENT  
            ,A.GROUP_SEGMENT  
            ,1 - (SUM(LGD_X_OS) / SUM(EQV_AT_DEFAULT)) AS RECOVERY_RATE  
            ,SUM(LGD_X_OS) / SUM(EQV_AT_DEFAULT) AS LGD  
        FROM ' || V_TABLEINSERT2 || '  A 
        JOIN ' || 'IFRS_LGD_RULES_CONFIG' || ' B 
        ON A.LGD_RULE_ID = B.PKID 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
        GROUP BY  
            DOWNLOAD_DATE  
            ,A.CALC_METHOD  
            ,A.LGD_METHOD  
            ,A.LGD_RULE_ID  
            ,A.LGD_RULE_NAME  
            ,A.PRODUCT_GROUP  
            ,A.SEGMENT  
            ,A.SUB_SEGMENT  
            ,A.GROUP_SEGMENT ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT3 || ' A 
        USING ' || 'IFRS_LGD_RULES_CONFIG' || ' B 
        WHERE A.LGD_RULE_ID = B.PKID          
        AND A.DEFAULT_RULE_ID = B.DEFAULT_RULE_ID          
        AND A.DOWNLOAD_DATE = CASE 
            WHEN B.LAG_1MONTH_FLAG = 1 
            THEN F_EOMONTH(CAST(''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE - INTERVAL ''1 MONTH'' AS DATE), 0, ''M'', ''NEXT'') 
            ELSE ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        END      
        AND B.LGD_METHOD IN (''EXPECTED RECOVERY'',''EXTERNAL'')  
        AND B.ACTIVE_FLAG = 1 
        AND B.IS_DELETE = 0 ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT3 || ' 
        (                              
            DOWNLOAD_DATE                              
            ,LGD_RULE_ID                              
            ,LGD_RULE_NAME                              
            ,DEFAULT_RULE_ID                              
            ,LGD                              
        ) SELECT                              
            DOWNLOAD_DATE                              
            ,A.LGD_RULE_ID                              
            ,A.LGD_RULE_NAME                              
            ,B.DEFAULT_RULE_ID                              
            ,LGD                              
        FROM ' || V_TABLEINSERT1 || ' A 
        JOIN ' || 'IFRS_LGD_RULES_CONFIG' || ' B 
            ON A.LGD_RULE_ID = B.PKID          
            AND B.DEFAULT_RULE_ID = B.DEFAULT_RULE_ID          
        WHERE A.DOWNLOAD_DATE = CASE 
            WHEN B.LAG_1MONTH_FLAG = 1 
            THEN F_EOMONTH(CAST(''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE - INTERVAL ''1 MONTH'' AS DATE), 0, ''M'', ''NEXT'') 
            ELSE ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        END
        AND B.ACTIVE_FLAG = 1  
        AND B.IS_DELETE = 0
        -- COMBINE WITH UPLOADED TREASURY LGD  
        UNION ALL  
        SELECT   
            A.DOWNLOAD_DATE
            ,C.PKID AS LGD_RULE_ID
            ,C.LGD_RULE_NAME
            ,C.DEFAULT_RULE_ID
            ,(1 - RECOVERY_RATE) AS LGD  
        FROM ' || 'IFRS_RECOVERY_RATE_TREASURY' || ' A  
        JOIN ' || 'IFRS_MSTR_SEGMENT_RULES_HEADER' || ' B 
            ON A.SEGMENT = B.SEGMENT 
            AND B.SEGMENT_TYPE = ''LGD_SEGMENT''  
        JOIN ' || 'IFRS_LGD_RULES_CONFIG' || ' C 
            ON B.PKID = C.SEGMENTATION_ID  
        WHERE C.LGD_METHOD = ''EXTERNAL'' 
            AND A.DOWNLOAD_DATE = CASE 
                WHEN C.LAG_1MONTH_FLAG = 1 
                THEN F_EOMONTH(CAST(''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE - INTERVAL ''1 MONTH'' AS DATE), 0, ''M'', ''NEXT'') 
                ELSE ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            END
            AND B.ACTIVE_FLAG = 1  AND B.IS_DELETE = 0 ';
    EXECUTE (V_STR_QUERY);

    RAISE NOTICE 'SP_IFRS_IMP_LGD_ER_HEADER_CORP | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT1;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_IMP_LGD_ER_HEADER_CORP';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT1 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;