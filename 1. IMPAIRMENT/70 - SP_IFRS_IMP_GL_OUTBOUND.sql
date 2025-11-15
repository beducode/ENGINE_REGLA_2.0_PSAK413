---- DROP PROCEDURE SP_IFRS_IMP_GL_OUTBOUND;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMP_GL_OUTBOUND(
    IN P_RUNID VARCHAR(20) DEFAULT 'S_00000_0000',
    IN P_DOWNLOAD_DATE DATE DEFAULT NULL,
    IN P_PRC VARCHAR(1) DEFAULT 'S')
LANGUAGE PLPGSQL AS $$
DECLARE
    ---- DATE
    V_PREVDATE DATE;
    V_CURRDATE DATE;
    V_LASTYEAR DATE;
    V_PREVMONTH DATE;
    V_CURRMONTH DATE;
    V_SANDI_DATE DATE;
    V_LASTYEARNEXTMONTH DATE;

    ---- QUERY   
    V_STR_QUERY TEXT;

    ---- TABLE LIST       
    V_TABLENAME VARCHAR(100);
    V_TABLEINSERT1 VARCHAR(100);
    V_TABLEINSERT2 VARCHAR(100);
    V_TABLEINSERT3 VARCHAR(100);
    V_TABLEINSERT4 VARCHAR(100);

    ---- VARIABLE PROCESS
    V_ROUND INT;
    V_FUNCROUND INT;
    
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
        V_TABLEINSERT1 := 'IFRS_GL_OUTBOUND_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_IMP_JOURNAL_DATA_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_IMA_IMP_CURR_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_IMA_IMP_PREV_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'IFRS_GL_OUTBOUND';
        V_TABLEINSERT2 := 'IFRS_IMP_JOURNAL_DATA';
        V_TABLEINSERT3 := 'IFRS_IMA_IMP_CURR';
        V_TABLEINSERT4 := 'IFRS_IMA_IMP_PREV';
    END IF;
    
    IF P_DOWNLOAD_DATE IS NULL 
    THEN
        SELECT
            MAX(CURRDATE), MAX(PREVDATE) INTO V_CURRDATE, V_PREVDATE
        FROM
            IFRS_PRC_DATE;
    ELSE        
        V_CURRDATE := P_DOWNLOAD_DATE;
        V_PREVDATE := V_CURRDATE - INTERVAL '1 DAY';
    END IF;

    V_CURRMONTH := F_EOMONTH(V_CURRDATE, 0, 'M', 'NEXT');
    V_PREVMONTH := F_EOMONTH(V_CURRDATE, 1, 'M', 'PREV');
    V_LASTYEAR := F_EOMONTH(V_CURRDATE, 1, 'Y', 'PREV');
    V_LASTYEARNEXTMONTH := F_EOMONTH(V_LASTYEAR, 1, 'M', 'NEXT');

    SELECT MAX(BUSS_DATE) INTO V_SANDI_DATE 
    FROM IFRS_BTPN_MAPPING_SANDI;

    V_SANDI_DATE := COALESCE(V_SANDI_DATE, V_CURRDATE);
    
    V_RETURNROWS2 := 0;
    V_ROUND := 2;
    V_FUNCROUND := 0;
    -------- ====== VARIABLE ======

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT1 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT1 || ' AS SELECT * FROM IFRS_GL_OUTBOUND WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT4 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT4 || ' AS SELECT * FROM IFRS_IMA_IMP_PREV WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (  
            BUSS_DATE
            ,BRANCH
            ,ACCOUNT_NO
            ,DESCRIPTION
            ,CCY
            ,ACCOUNT_TYPE
            ,VALUE_DATE
            ,SIGN
            ,AMOUNT
            ,NARRATIVE1
            ,GROUP_OR_USER_ID
            ,TIME_STAMP
            ,PRODUCT_CODE
            ,CUSTOMER_TYPE
            ,TRANSACTION_RATE
            ,AMOUNT_LEV
            ,JURNAL_NUMBER
            ,SOURCE_DATA
            ,EVENT_TYPE
            ,CLASS
        ) SELECT  
            BUSS_DATE
            ,BRANCH
            ,ACCOUNT_NO
            ,MAX(LEFT(DESCRIPTION,50))
            ,CCY
            ,ACCOUNT_TYPE
            ,VALUE_DATE
            ,SIGN
            ,SUM(AMOUNT) AS AMOUNT
            ,NARRATIVE1
            ,GROUP_OR_USER_ID
            ,MAX(TIME_STAMP) TIME_STAMP        
            ,PRODUCT_CODE
            ,CUSTOMER_TYPE
            ,TRANSACTION_RATE
            ,SUM(AMOUNT_LEV) AS AMOUNT_LEV
            ,JURNAL_NUMBER
            ,SOURCE_DATA
            ,EVENT_TYPE
            ,''I'' AS CLASS   
        FROM (
            SELECT 
                A.DOWNLOAD_DATE AS BUSS_DATE
                ,A.BRANCH_CODE AS BRANCH
                ,A.GL_ACCOUNT AS ACCOUNT_NO
                ,LEFT(B.DESCRIPTION,50) AS DESCRIPTION
                ,A.CURRENCY AS CCY
                ,COALESCE(C.ACCOUNT_TYPE, F.ACCOUNT_TYPE) AS ACCOUNT_TYPE
                ,A.DOWNLOAD_DATE AS VALUE_DATE
                ,LEFT(A.TXN_TYPE, 1) AS SIGN
                ,ROUND(CAST(COALESCE(A.AMOUNT, 0) AS NUMERIC(38, 2)), ' || V_ROUND || ') AS AMOUNT
                ,A.REVERSAL_FLAG AS NARRATIVE1
                ,''REGLA'' AS GROUP_OR_USER_ID
                ,TO_CHAR(CURRENT_TIMESTAMP, ''YYYY-MM-DD HH24:MI:SS.MS'') AS TIME_STAMP
                ,LEFT(PRD_CODE, 6) AS PRODUCT_CODE
                ,COALESCE(C.CUSTOMER_TYPE, F.CUSTOMER_TYPE) AS CUSTOMER_TYPE
                ,CAST(COALESCE(D.RATE_AMOUNT, 1) AS NUMERIC(38, 2)) AS TRANSACTION_RATE
                ,(ROUND(CAST(COALESCE(A.AMOUNT, 0) AS NUMERIC(38, 2)), ' || V_ROUND || ') * CAST(COALESCE(D.RATE_AMOUNT, 1) AS NUMERIC(38, 2))) AS AMOUNT_LEV           
                ,CONCAT(TO_CHAR(A.DOWNLOAD_DATE, ''YYYYMMDD''), A.JOURNAL_TYPE, A.BRANCH_CODE, A.CURRENCY, LEFT(PRD_CODE, 6)) AS JURNAL_NUMBER          
                ,''PSAK71'' AS SOURCE_DATA
                ,A.JOURNAL_TYPE AS EVENT_TYPE 
            FROM ' || V_TABLEINSERT2 || ' A 
            LEFT JOIN (
                SELECT ACCOUNT,  MAX(DESCRIPTION) AS DESCRIPTION 
                FROM (
                    SELECT ACCOUNT, SANDI_LBU, LEFT(DESCRIPTION,50) AS DESCRIPTION
                    FROM IFRS_BTPN_MAPPING_SANDI
                    WHERE SANDI_LBU = ''175'' and LEFT(ACCOUNT, 2) = ''10''              
                    AND BUSS_DATE = ''' || CAST(V_SANDI_DATE AS VARCHAR(10)) || '''::DATE                       
                    UNION ALL                      
                    SELECT ACCOUNT, SANDI_LBU, LEFT(DESCRIPTION,50) AS DESCRIPTION
                    FROM IFRS_BTPN_MAPPING_SANDI
                    WHERE SANDI_LBU <> ''175''              
                    AND BUSS_DATE = ''' || CAST(V_SANDI_DATE AS VARCHAR(10)) || '''::DATE                       
                ) A 
                GROUP BY ACCOUNT
            ) B ON A.GL_ACCOUNT = B.ACCOUNT
            LEFT JOIN ' || V_TABLEINSERT3 || ' C 
                ON A.MASTERID = C.MASTERID 
            LEFT JOIN ' || 'IFRS_MASTER_EXCHANGE_RATE' || ' D 
                ON A.DOWNLOAD_DATE = D.DOWNLOAD_DATE 
                AND A.CURRENCY = D.CURRENCY     
            LEFT JOIN ' || 'IFRS_MASTER_EXCHANGE_RATE' || ' E 
                ON E.DOWNLOAD_DATE = CAST(''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE - INTERVAL ''1 DAY'' AS DATE) 
                AND A.CURRENCY = E.CURRENCY
            LEFT JOIN ' || V_TABLEINSERT4 || ' F 
                ON A.MASTERID = F.MASTERID 
            WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE   
        ) A
        GROUP BY 
            BUSS_DATE
            ,BRANCH
            ,ACCOUNT_NO
            ,CCY
            ,ACCOUNT_TYPE
            ,VALUE_DATE
            ,SIGN
            ,NARRATIVE1
            ,GROUP_OR_USER_ID  
            ,PRODUCT_CODE
            ,CUSTOMER_TYPE
            ,TRANSACTION_RATE                     
            ,JURNAL_NUMBER
            ,SOURCE_DATA
            ,EVENT_TYPE   
        ORDER BY
            BUSS_DATE ASC
            ,BRANCH ASC
            ,CCY ASC
            ,AMOUNT ASC
            ,SIGN DESC
            ,ACCOUNT_NO ASC ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    RAISE NOTICE 'SP_IFRS_IMP_GL_OUTBOUND | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT2;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_IMP_GL_OUTBOUND';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT2 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

    CALL SP_IFRS_IMP_GL_OUTBOUND_SUMMARIZE(P_RUNID, V_CURRDATE, P_PRC);
    CALL SP_IFRS_GL_OUTBOUND_SUMMARIZE(P_RUNID, V_CURRDATE, P_PRC);

END;

$$;