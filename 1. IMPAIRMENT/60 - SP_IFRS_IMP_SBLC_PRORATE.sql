---- DROP PROCEDURE SP_IFRS_IMP_SBLC_PRORATE;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMP_SBLC_PRORATE(
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
        V_TABLEINSERT1 := 'IFRS_SBLC_PRORATE_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_IMA_IMP_CURR_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'IFRS_SBLC_PRORATE';
        V_TABLEINSERT2 := 'IFRS_IMA_IMP_CURR';
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
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT1 || ' AS SELECT * FROM IFRS_SBLC_PRORATE WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT2 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT2 || ' AS SELECT * FROM IFRS_IMA_IMP_CURR WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || 'TMP_IMA_IMP_COLL' || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || 'TMP_IMA_IMP_COLL' || ' AS 
        SELECT *       
        FROM ' || V_TABLEINSERT2 || ' 
        WHERE FACILITY_NUMBER IN (      
            SELECT DISTINCT FACILITY_NUMBER       
            FROM ' || 'IFRS_MASTER_COLLATERAL_CORP' || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND COLL_TP = ''251'' 
            AND COLL_NAT = ''1''      
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || 'TMP_SUM_COLL_AMOUNT' || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || 'TMP_SUM_COLL_AMOUNT' || ' AS 
        SELECT 
            LEFT(ACCOUNT_NUMBER, (CASE 
                WHEN POSITION(''.'' IN SUBSTRING(ACCOUNT_NUMBER FROM 8)) > 0 
                THEN POSITION(''.'' IN SUBSTRING(ACCOUNT_NUMBER FROM 8)) + 7 
                ELSE 0 
            END)-1) AS COLL_ID
            ,SUM(AMOUNT) AS SUM_COLLATERAL      
        FROM ' || 'IFRS_MASTER_COLLATERAL_CORP' || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND COLL_TP = ''251'' 
            AND COLL_NAT = ''1''      
        GROUP BY LEFT(ACCOUNT_NUMBER, (CASE 
            WHEN POSITION(''.'' IN SUBSTRING(ACCOUNT_NUMBER FROM 8)) > 0 
            THEN POSITION(''.'' IN SUBSTRING(ACCOUNT_NUMBER FROM 8)) + 7 
            ELSE 0 
        END)-1) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || 'TMP_REMAINING_AMOUNT' || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || 'TMP_REMAINING_AMOUNT' || ' AS 
        SELECT 
            LEFT(ACCOUNT_NUMBER, (CASE 
                WHEN POSITION(''.'' IN SUBSTRING(ACCOUNT_NUMBER FROM 8)) > 0 
                THEN POSITION(''.'' IN SUBSTRING(ACCOUNT_NUMBER FROM 8)) + 7 
                ELSE 0 
            END)-1) AS COLL_ID
            ,SUM(AMOUNT) AS REMAINING      
        FROM ' || 'IFRS_MASTER_COLLATERAL_CORP' || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
            AND COLL_TP = ''251'' 
            AND COLL_NAT = ''1''      
        GROUP BY LEFT(ACCOUNT_NUMBER, (CASE 
            WHEN POSITION(''.'' IN SUBSTRING(ACCOUNT_NUMBER FROM 8)) > 0 
            THEN POSITION(''.'' IN SUBSTRING(ACCOUNT_NUMBER FROM 8)) + 7 
            ELSE 0 
        END)-1) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || 'TMP_COLL_FACILITY' || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || 'TMP_COLL_FACILITY' || ' AS 
        SELECT 
            LEFT(ACCOUNT_NUMBER, (CASE 
                WHEN POSITION(''.'' IN SUBSTRING(ACCOUNT_NUMBER FROM 8)) > 0 
                THEN POSITION(''.'' IN SUBSTRING(ACCOUNT_NUMBER FROM 8)) + 7 
                ELSE 0 
            END)-1) AS COLL_ID
            ,SUM(AMOUNT) AS REMAINING      
        FROM ' || 'IFRS_MASTER_COLLATERAL_CORP' || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND COLL_TP = ''251'' 
            AND COLL_NAT = ''1''      
        GROUP BY LEFT(ACCOUNT_NUMBER, (CASE 
            WHEN POSITION(''.'' IN SUBSTRING(ACCOUNT_NUMBER FROM 8)) > 0 
            THEN POSITION(''.'' IN SUBSTRING(ACCOUNT_NUMBER FROM 8)) + 7 
            ELSE 0 
        END)-1) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || 'TMP_COLL_FACILITY' || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || 'TMP_COLL_FACILITY' || ' AS 
        SELECT DISTINCT 
            LEFT(ACCOUNT_NUMBER, (CASE 
                WHEN POSITION(''.'' IN SUBSTRING(ACCOUNT_NUMBER FROM 8)) > 0 
                THEN POSITION(''.'' IN SUBSTRING(ACCOUNT_NUMBER FROM 8)) + 7 
                ELSE 0 
            END)-1) AS COLL_ID
            ,FACILITY_NUMBER      
        FROM ' || 'IFRS_MASTER_COLLATERAL_CORP' || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND COLL_TP = ''251'' 
            AND COLL_NAT = ''1'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || 'TMP_IMA_SUM_COLL' || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || 'TMP_IMA_SUM_COLL' || ' AS 
        SELECT 
            B.COLL_ID
            ,C.SUM_COLLATERAL
            ,CAST(A.OUTSTANDING AS NUMERIC(32,6)) * CAST(A.EXCHANGE_RATE AS NUMERIC(32,6)) AS OS_IDR
            ,CAST(A.UNUSED_AMOUNT AS NUMERIC(32,6)) * CAST(A.EXCHANGE_RATE AS NUMERIC(32,6)) AS UNUSED_IDR
            ,A.* 
        FROM ' || 'TMP_IMA_IMP_COLL' || ' A      
        JOIN ' || 'TMP_COLL_FACILITY' || ' B 
            ON A.FACILITY_NUMBER = B.FACILITY_NUMBER      
        JOIN ' || 'TMP_SUM_COLL_AMOUNT' || ' C 
            ON B.COLL_ID = C.COLL_ID      
        ORDER BY B.COLL_ID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || 'TMP_IFRS_SBLC_PRORATE' || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || 'TMP_IFRS_SBLC_PRORATE' || ' AS 
        SELECT * FROM ' || V_TABLEINSERT1 || ' WHERE 1 = 2 ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    FOR V_SEGMENT IN 
        EXECUTE 'SELECT 
                VALUE1  AS SEQUENCE
                ,VALUE2 AS FIELD_NAME
                ,VALUE3 AS FIELD_VALUE      
            FROM TBLM_COMMONCODEDETAIL      
            WHERE COMMONCODE = ''SBLC_RULES''
            AND COALESCE(IS_DELETE, 0) = 0 
            ORDER BY VALUE1 '
    LOOP 
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || 'TMP_SUM_OS_IDR' || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || 'TMP_SUM_OS_IDR' || ' AS 
            SELECT 
                COLL_ID
                ,SUM(CASE 
                    WHEN DATA_SOURCE = ''LIMIT_T24'' 
                    THEN UNUSED_IDR 
                    ELSE OS_IDR 
                END) AS SUM_IDR      
            FROM ' || 'TMP_IMA_SUM_COLL' || '
            WHERE ' || V_SEGMENT.FIELD_NAME || ' IN (''' || REPLACE(V_SEGMENT.FIELD_VALUE, ',', ''',''') || ''')      
            GROUP BY COLL_ID ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_IFRS_SBLC_PRORATE' || ' 
            (      
                DOWNLOAD_DATE      
                ,COLL_ID      
                ,CUSTOMER_NUMBER      
                ,CUSTOMER_NAME      
                ,FACILITY_NUMBER      
                ,ACCOUNT_NUMBER      
                ,MASTERID      
                ,IAS_CLASS      
                ,DATA_SOURCE      
                ,PRODUCT_CODE      
                ,OUTSTANDING_LCY      
                ,TOTAL_SBLC_AMOUNT_LCY      
                ,PRORATE_COLLATERAL_LCY      
                ,UNUSED_AMOUNT_LCY      
                ,PRIORITY      
                ,REMAINING_SBLC_AMOUNT_LCY      
                ,CREATEDBY      
                ,CREATEDDATE      
                ,CREATEDHOST      
                ,CURRENCY_MASTER      
                ,EXCHANGE_RATE_MASTER      
                ,SUM_OS_LCY_PRIORITY      
            )      
            SELECT       
                DOWNLOAD_DATE      
                ,A.COLL_ID      
                ,A.CUSTOMER_NUMBER      
                ,A.CUSTOMER_NAME      
                ,A.FACILITY_NUMBER      
                ,A.ACCOUNT_NUMBER      
                ,A.MASTERID      
                ,A.IAS_CLASS      
                ,A.DATA_SOURCE      
                ,A.PRODUCT_CODE      
                ,A.OS_IDR AS OUTSTANDING_LCY      
                ,A.SUM_COLLATERAL AS TOTAL_SBLC_AMOUNT_LCY      
                ,CASE 
                    WHEN COALESCE(B.SUM_IDR,0) <= 0 
                    THEN 0       
                    ELSE (CAST(CASE 
                        WHEN A.DATA_SOURCE = ''LIMIT_T24'' 
                        THEN A.UNUSED_IDR 
                        ELSE A.OS_IDR 
                    END AS DOUBLE PRECISION) / CAST(B.SUM_IDR AS DOUBLE PRECISION)) * CAST(C.REMAINING AS DOUBLE PRECISION) 
                END AS PRORATE_COLLATERAL_LCY      
                ,A.UNUSED_IDR AS UNUSED_AMOUNT_LCY      
                ,' || V_SEGMENT.SEQUENCE || ' AS PRIORITY      
                ,NULL AS REMAINING_SBLC_AMOUNT_LCY      
                ,''SP_IFRS_SBLC_PRORATE'' AS CREATEDBY      
                ,CURRENT_TIMESTAMP AS CREATEDDATE      
                ,''LOCALHOST'' AS CREATEDHOST      
                ,A.CURRENCY AS CURRENCY_MASTER      
                ,A.EXCHANGE_RATE AS EXCHANGE_RATE_MASTER      
                ,B.SUM_IDR AS SUM_OS_LCY_PRIORITY      
            FROM ' || 'TMP_IMA_SUM_COLL' || ' A      
            LEFT JOIN ' || 'TMP_SUM_OS_IDR' || ' B 
                ON A.COLL_ID = B.COLL_ID      
            LEFT JOIN ' || 'TMP_REMAINING_AMOUNT' || ' C 
                ON A.COLL_ID = C.COLL_ID      
            WHERE ' || V_SEGMENT.FIELD_NAME || ' IN (''' || REPLACE(V_SEGMENT.FIELD_VALUE, ',', ''',''') || ''') ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || 'TMP_IFRS_SBLC_PRORATE' || ' A 
            SET PRORATE_COLLATERAL_LCY = CASE 
                WHEN DATA_SOURCE = ''LIMIT_T24'' 
                THEN CASE 
                    WHEN PRORATE_COLLATERAL_LCY >=  UNUSED_AMOUNT_LCY 
                    THEN UNUSED_AMOUNT_LCY 
                    ELSE PRORATE_COLLATERAL_LCY 
                END       
                ELSE CASE 
                    WHEN PRORATE_COLLATERAL_LCY >=  OUTSTANDING_LCY 
                    THEN OUTSTANDING_LCY 
                    ELSE PRORATE_COLLATERAL_LCY 
                END       
            END      
            WHERE PRIORITY = ' || V_SEGMENT.SEQUENCE || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || 'TMP_REMAINING_AMOUNT' || ' A 
            SET REMAINING = ROUND(CASE WHEN A.REMAINING - B.PRORATE <= 0 THEN 0 ELSE A.REMAINING - B.PRORATE END,3)       
            FROM (      
                SELECT COLL_ID, SUM (PRORATE_COLLATERAL_LCY) AS PRORATE      
                FROM ' || 'TMP_IFRS_SBLC_PRORATE' || ' 
                WHERE PRIORITY = ' || V_SEGMENT.SEQUENCE || '      
                GROUP BY COLL_ID      
            ) B 
            WHERE A.COLL_ID = B.COLL_ID ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || 'TMP_IFRS_SBLC_PRORATE' || ' A 
            SET REMAINING_SBLC_AMOUNT_LCY = B.REMAINING      
            FROM ' || 'TMP_REMAINING_AMOUNT' || ' B 
            WHERE A.COLL_ID = B.COLL_ID      
            AND A.PRIORITY = ' || V_SEGMENT.SEQUENCE || ' ';
        EXECUTE (V_STR_QUERY);
    END LOOP;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        SELECT * FROM ' || 'TMP_IFRS_SBLC_PRORATE' || ' ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' 
    --     SET SBLC_AMOUNT = COALESCE(C.AGUNAN_AMOUNT,0) + CAST(COALESCE(B.PRORATE_COLLATERAL_LCY, 0) AS DOUBLE PRECISION) / CAST(A.EXCHANGE_RATE AS DOUBLE PRECISION)           
    --     FROM ' || V_TABLEINSERT2 || ' A                  
    --     JOIN ' || V_TABLEINSERT1 || ' B                   
    --         ON A.DOWNLOAD_DATE = B.DOWNLOAD_DATE                  
    --         AND A.MASTERID = B.MASTERID      
    --     JOIN (
    --         SELECT DOWNLOAD_DATE, AGUNAN_AMOUNT, MASTER_ID 
    --         FROM DBLINK(''ifrs9_stg'', ''SELECT * FROM DOWNLOAD_DATE, AGUNAN_AMOUNT, MASTER_ID STG_IFRS_MASTER_ACCOUNT_TF'') 
    --         AS STG_IFRS_MASTER_ACCOUNT_TF (
    --             DOWNLOAD_DATE   DATE
    --             ,AGUNAN_AMOUNT  NUMERIC(18,3)
    --             ,MASTER_ID      VARCHAR(50)
    --         ) WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
    --         AND AGUNAN_AMOUNT > 1     
    --     ) C
    --     ON A.DOWNLOAD_DATE = C.DOWNLOAD_DATE                  
    --     AND A.MASTERID = C.MASTER_ID                           
    --     WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    -- EXECUTE (V_STR_QUERY);

    RAISE NOTICE 'SP_IFRS_IMP_SBLC_PRORATE | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT1;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_IMP_SBLC_PRORATE';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT1 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;