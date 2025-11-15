---- DROP PROCEDURE SP_IFRS_IMP_GL_OUTBOUND_SUMMARIZE;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMP_GL_OUTBOUND_SUMMARIZE(
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
    V_EXISTS BOOLEAN;

    
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
        V_TABLEINSERT2 := 'STG_TRX_PSAK71_REV_' || P_RUNID || '';
        V_TABLEINSERT3 := 'STG_TRX_PSAK71_' || P_RUNID || '';
        V_TABLEINSERT4 := 'STG_TRX_PSAK71_HISTORY_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'IFRS_GL_OUTBOUND';
        V_TABLEINSERT2 := 'STG_TRX_PSAK71_REV';
        V_TABLEINSERT3 := 'STG_TRX_PSAK71';
        V_TABLEINSERT4 := 'STG_TRX_PSAK71_HISTORY';
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
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT2 || ' AS SELECT * FROM STG_TRX_PSAK71_REV WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT3 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT3 || ' AS SELECT * FROM STG_TRX_PSAK71 WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT4 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT4 || ' AS SELECT * FROM STG_TRX_PSAK71_HISTORY WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
        WHERE EXTRACT(YEAR FROM BUSS_DATE) = EXTRACT(YEAR FROM ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE) 
        AND EXTRACT(MONTH FROM BUSS_DATE) = EXTRACT(MONTH FROM ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE) 
        AND CLASS = ''I'' 
        AND AMOUNT = 0 ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT2 || ' 
        WHERE BUSS_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND TRANSACTION_TYPE = ''IMPAIRMENT'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT3 || ' 
        WHERE TRANSACTION_TYPE = ''IMPAIRMENT'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT EXISTS (
        SELECT 1 
        FROM ' || 'STG_TRX_PSAK71_LOG' || ' 
        WHERE PROCESS_ID = ''GL_TRX'' 
        AND LAST_BUSS_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
    ) ';
    EXECUTE (V_STR_QUERY) INTO V_EXISTS;

    IF V_EXISTS THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
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
                ,TRANSACTION_TYPE                   
            ) SELECT                  
                F_EOMONTH(BUSS_DATE, 0, ''M'', ''NEXT'') AS BUSS_DATE                  
                ,BRANCH                
                ,ACCOUNT_NO                
                ,LEFT(DESCRIPTION,50)                
                ,CCY                
                ,ACCOUNT_TYPE                
                ,F_EOMONTH(VALUE_DATE, 0, ''M'', ''NEXT'') AS VALUE_DATE                  
                ,CASE WHEN SIGN = ''D'' THEN ''C'' ELSE ''D'' END AS SIGN                
                ,SUM(AMOUNT) AS AMOUNT                
                ,NARRATIVE1                
                ,GROUP_OR_USER_ID                
                ,TIME_STAMP                
                ,PRODUCT_CODE                
                ,CUSTOMER_TYPE                
                ,CASE 
                    WHEN SUM(AMOUNT) = 0 
                    THEN 0 
                    ELSE SUM(AMOUNT_LEV) / SUM(AMOUNT) 
                END AS TRANSACTION_RATE                
                ,SUM(AMOUNT_LEV) AS AMOUNT_LEV                
                ,JURNAL_NUMBER || ''_REV''                
                ,SOURCE_DATA                
                ,EVENT_TYPE            
                ,TRANSACTION_TYPE                  
            FROM ' || V_TABLEINSERT4 || ' 
            WHERE EXTRACT(YEAR FROM BUSS_DATE) = EXTRACT(YEAR FROM ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE) 
                AND EXTRACT(MONTH FROM BUSS_DATE) = EXTRACT(MONTH FROM ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE) 
                AND TRANSACTION_TYPE = ''IMPAIRMENT''                 
            GROUP BY                 
                BUSS_DATE                
                ,BRANCH                
                ,ACCOUNT_NO                
                ,LEFT(DESCRIPTION,50)                
                ,CCY                
                ,ACCOUNT_TYPE                
                ,VALUE_DATE                
                ,SIGN                
                ,NARRATIVE1                
                ,GROUP_OR_USER_ID                
                ,TIME_STAMP                
                ,PRODUCT_CODE                
                ,CUSTOMER_TYPE                
                ,JURNAL_NUMBER                
                ,SOURCE_DATA                
                ,EVENT_TYPE          
                ,TRANSACTION_TYPE                  
            ORDER BY                
                BUSS_DATE ASC                
                ,BRANCH ASC                
                ,CCY ASC                
                ,AMOUNT ASC                
                ,SIGN DESC                
                ,ACCOUNT_NO ASC ';
        EXECUTE (V_STR_QUERY);
    END IF;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT4 || ' 
        WHERE BUSS_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND TRANSACTION_TYPE = ''IMPAIRMENT'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT3 || ' 
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
            ,TRANSACTION_TYPE                
        ) SELECT                  
            F_EOMONTH(BUSS_DATE, 0, ''M'', ''NEXT'') AS BUSS_DATE                  
            ,BRANCH                
            ,ACCOUNT_NO                
            ,LEFT(DESCRIPTION,50)                
            ,CCY                
            ,ACCOUNT_TYPE                
            ,F_EOMONTH(VALUE_DATE, 0, ''M'', ''NEXT'') AS VALUE_DATE                  
            ,SIGN                
            ,SUM(AMOUNT) AS AMOUNT                
            ,NARRATIVE1                
            ,GROUP_OR_USER_ID                
            ,TIME_STAMP                
            ,REPLACE(REPLACE(REPLACE(PRODUCT_CODE,''CL_'',''''),''_LC'',''''),''_DR'','''') AS PRODUCT_CODE                
            ,CUSTOMER_TYPE                
            ,CASE 
                WHEN SUM(AMOUNT) = 0 
                THEN 0 
                ELSE SUM(AMOUNT_LEV) / SUM(AMOUNT) 
            END AS TRANSACTION_RATE                
            ,SUM(AMOUNT_LEV) AS AMOUNT_LEV                
            ,JURNAL_NUMBER                
            ,SOURCE_DATA                
            ,EVENT_TYPE            
            ,''IMPAIRMENT'' AS TRANSACTION_TYPE                  
        FROM ' || V_TABLEINSERT1 || ' 
        WHERE EXTRACT(YEAR FROM BUSS_DATE) = EXTRACT(YEAR FROM ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE) 
            AND EXTRACT(MONTH FROM BUSS_DATE) = EXTRACT(MONTH FROM ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE) 
            AND CLASS = ''I'' 
        GROUP BY                 
            BUSS_DATE                
            ,BRANCH                
            ,ACCOUNT_NO                
            ,LEFT(DESCRIPTION,50)                
            ,CCY                
            ,ACCOUNT_TYPE                
            ,VALUE_DATE                
            ,SIGN                
            ,NARRATIVE1                
            ,GROUP_OR_USER_ID                
            ,TIME_STAMP                
            ,PRODUCT_CODE                
            ,CUSTOMER_TYPE                
            ,JURNAL_NUMBER                
            ,SOURCE_DATA                
            ,EVENT_TYPE                  
        UNION ALL                  
        SELECT                  
            BUSS_DATE                  
            ,BRANCH                
            ,ACCOUNT_NO                
            ,LEFT(DESCRIPTION,50)                
            ,CCY                
            ,ACCOUNT_TYPE                
            ,VALUE_DATE                  
            ,SIGN                
            ,AMOUNT                
            ,NARRATIVE1                
            ,GROUP_OR_USER_ID                
            ,TIME_STAMP                
            ,REPLACE(REPLACE(REPLACE(PRODUCT_CODE,''CL_'',''''),''_LC'',''''),''_DR'','''') AS PRODUCT_CODE
            ,CUSTOMER_TYPE                
            ,TRANSACTION_RATE                
            ,AMOUNT_LEV                
            ,JURNAL_NUMBER                
            ,SOURCE_DATA                
            ,EVENT_TYPE            
            ,TRANSACTION_TYPE                  
        FROM ' || V_TABLEINSERT2 || ' 
        WHERE BUSS_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                  
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

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT4 || ' 
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
            ,TRANSACTION_TYPE                
        ) SELECT                  
            F_EOMONTH(BUSS_DATE, 0, ''M'', ''NEXT'') AS BUSS_DATE                  
            ,BRANCH                
            ,ACCOUNT_NO                
            ,LEFT(DESCRIPTION,50)             
            ,CCY                
            ,ACCOUNT_TYPE                
            ,F_EOMONTH(VALUE_DATE, 0, ''M'', ''NEXT'') AS VALUE_DATE                  
            ,SIGN                
            ,SUM(AMOUNT) AS AMOUNT                
            ,NARRATIVE1                
            ,GROUP_OR_USER_ID                
            ,TIME_STAMP                
            ,PRODUCT_CODE                
            ,CUSTOMER_TYPE                
            ,CASE 
                WHEN SUM(AMOUNT) = 0 
                THEN 0 
                ELSE SUM(AMOUNT_LEV) / SUM(AMOUNT) 
            END AS TRANSACTION_RATE 
            ,SUM(AMOUNT_LEV) AS AMOUNT_LEV                
            ,JURNAL_NUMBER                
            ,SOURCE_DATA                
            ,EVENT_TYPE            
            ,''IMPAIRMENT'' AS TRANSACTION_TYPE                  
        FROM ' || V_TABLEINSERT1 || ' 
        WHERE EXTRACT(YEAR FROM BUSS_DATE) = EXTRACT(YEAR FROM ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE) 
            AND EXTRACT(MONTH FROM BUSS_DATE) = EXTRACT(MONTH FROM ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE) 
            AND CLASS = ''I''                
        GROUP BY                 
            BUSS_DATE                
            ,BRANCH                
            ,ACCOUNT_NO                
            ,LEFT(DESCRIPTION,50)                
            ,CCY                
            ,ACCOUNT_TYPE                
            ,VALUE_DATE                
            ,SIGN                
            ,NARRATIVE1                
            ,GROUP_OR_USER_ID                
            ,TIME_STAMP                
            ,PRODUCT_CODE                
            ,CUSTOMER_TYPE                
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

    RAISE NOTICE 'SP_IFRS_IMP_GL_OUTBOUND_SUMMARIZE | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT3;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_IMP_GL_OUTBOUND_SUMMARIZE';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT3 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;