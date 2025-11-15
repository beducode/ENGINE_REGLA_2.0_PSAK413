---- DROP PROCEDURE SP_IFRS_IMP_NOMINATIVE_OUTPUT;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMP_NOMINATIVE_OUTPUT(
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
    V_TABLENAME_MON VARCHAR(100);
    V_TABLEINSERT1 VARCHAR(100);
    V_TABLEINSERT2 VARCHAR(100);
    
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
        V_TABLENAME_MON := 'TMP_IMAM_' || P_RUNID || '';
        V_TABLEINSERT1 := 'STG_PSAK71_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_OUTBOUND_FLAG_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLENAME_MON := 'IFRS_MASTER_ACCOUNT_MONTHLY';
        V_TABLEINSERT1 := 'STG_PSAK71';
        V_TABLEINSERT2 := 'IFRS_OUTBOUND_FLAG';
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
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT1 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT1 || ' AS SELECT * FROM STG_PSAK71 WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT2 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT2 || ' AS SELECT * FROM IFRS_OUTBOUND_FLAG WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT1 || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (          
            BUSS_DATE          
            ,CIF          
            ,COMMITMENT_REFERENCE          
            ,DEAL_REF          
            ,CUSTOMER_FULL_NAME          
            ,BRANCH          
            ,DEAL_TYPE          
            ,CCY          
            ,OUTSTANDING_ORI          
            ,OUTSTANDING_LEV          
            ,INITIAL_FEE          
            ,INITIAL_COST          
            ,UNAMORT_FEE_ORI          
            ,UNAMORT_FEE_LEV          
            ,UNAMORT_COST_ORI          
            ,UNAMORT_COST_LEV          
            ,FAIR_VALUE_ORI          
            ,FAIR_VALUE_LEV          
            ,INT_RATE          
            ,EIR_RATE          
            ,EXCHANGE_RATE          
            ,COLLECT          
            ,CKPN_TYPE          
            ,CKPN_ORI          
            ,CKPN_LEV          
            ,UNWINDING_ORI          
            ,UNWINDING_LEV          
            ,STAFF_LOAN_FLAG          
            ,IFRS9_CLASS          
            ,SEGMENTATION          
            ,IFRS_STAGE          
            ,DEFAULT_FLAG          
            ,SOURCE_SYSTEM       
            ,PRODUCT_TYPE       
            ,PLAFOND        
            ,UNUSED_AMOUNT    
            ,BUCKET_NAME    
            ,DPD_FINAL  
            ,CKPN_ORI_NET  
            ,CKPN_LEV_NET        
        ) SELECT          
            DOWNLOAD_DATE AS BUSS_DATE          
            ,CUSTOMER_NUMBER AS CIF          
            ,FACILITY_NUMBER AS COMMITMENT_REFERENCE          
            ,ACCOUNT_NUMBER AS DEAL_REF          
            ,CUSTOMER_NAME AS CUSTOMER_FULL_NAME          
            ,BRANCH_CODE AS BRANCH          
            ,PRODUCT_CODE AS DEAL_TYPE          
            ,CURRENCY AS CCY          
            ,COALESCE(OUTSTANDING,0) AS OUTSTANDING_ORI          
            ,COALESCE(OUTSTANDING,0) * COALESCE(EXCHANGE_RATE, 1) AS OUTSTANDING_LEV          
            ,COALESCE(INITIAL_UNAMORT_ORG_FEE,0) AS  INITIAL_FEE          
            ,COALESCE(INITIAL_UNAMORT_TXN_COST,0) AS INITIAL_COST          
            ,COALESCE(UNAMORT_FEE_AMT,0) AS UNAMORT_FEE_ORI          
            ,COALESCE(UNAMORT_FEE_AMT,0) * COALESCE(EXCHANGE_RATE, 1) AS UNAMORT_FEE_LEV          
            ,COALESCE(UNAMORT_COST_AMT,0) AS UNAMORT_COST_ORI          
            ,COALESCE(UNAMORT_COST_AMT,0) * COALESCE(EXCHANGE_RATE, 1) AS UNAMORT_COST_LEV          
            ,COALESCE(FAIR_VALUE_AMOUNT,0) AS FAIR_VALUE_ORI          
            ,COALESCE(FAIR_VALUE_AMOUNT,0) * COALESCE(EXCHANGE_RATE, 1) AS FAIR_VALUE_LEV          
            ,INTEREST_RATE AS INT_RATE          
            ,EIR AS EIR_RATE          
            ,COALESCE(EXCHANGE_RATE, 1) AS EXCHANGE_RATE          
            ,BI_COLLECTABILITY AS COLLECT          
            ,IMPAIRED_FLAG AS CKPN_TYPE          
            ,COALESCE(ECL_AMOUNT,0) AS CKPN_ORI          
            ,COALESCE(ECL_AMOUNT,0) * COALESCE(EXCHANGE_RATE, 1) AS CKPN_LEV          
            ,COALESCE(IA_UNWINDING_AMOUNT,0)  AS UNWINDING_ORI          
            ,COALESCE(IA_UNWINDING_AMOUNT,0)  * COALESCE(EXCHANGE_RATE, 1) AS UNWINDING_LEV          
            ,COALESCE(STAFF_LOAN_FLAG,0)          
            ,IFRS9_CLASS          
            ,SUB_SEGMENT AS SEGMENTATION          
            ,STAGE AS IFRS_STAGE          
            ,DEFAULT_FLAG          
            ,SOURCE_SYSTEM        
            ,PRODUCT_TYPE_1 AS PRODUCT_TYPE       
            ,COALESCE(PLAFOND,0)        
            ,COALESCE(UNUSED_AMOUNT,0)    
            ,BUCKET_NAME    
            ,DPD_FINAL   
            ,COALESCE(ECL_AMOUNT,0)  - COALESCE(IA_UNWINDING_AMOUNT,0)  AS CKPN_ORI_NET  
            ,(COALESCE(ECL_AMOUNT,0)*COALESCE(EXCHANGE_RATE, 1) ) - (COALESCE(IA_UNWINDING_AMOUNT,0)*COALESCE(EXCHANGE_RATE, 1)) AS  CKPN_LEV_NET               
        FROM ' || V_TABLENAME_MON || ' A 
        LEFT JOIN (
            SELECT BUCKET_GROUP,BUCKET_ID, MAX(BUCKET_NAME) AS  BUCKET_NAME 
            FROM IFRS_BUCKET_DETAIL 
            WHERE IS_DELETE = 0 
            GROUP BY BUCKET_GROUP,BUCKET_ID 
        ) B     
            ON A.BUCKET_GROUP = B.BUCKET_GROUP 
            AND A.BUCKET_ID = B.BUCKET_ID    
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT2 || ' 
        WHERE TABLE_NAME = ''STG_PSAK71'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
        (
            DOWNLOAD_DATE    
            ,TABLE_NAME    
            ,FINISHED_FLAG    
            ,CREATED_DATE
        ) VALUES (
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            ,''STG_PSAK71''
            ,1
            ,CURRENT_TIMESTAMP
        ) ';
    EXECUTE (V_STR_QUERY);

    RAISE NOTICE 'SP_IFRS_IMP_NOMINATIVE_OUTPUT | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT1;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_IMP_NOMINATIVE_OUTPUT';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT1 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;