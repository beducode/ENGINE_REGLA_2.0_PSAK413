---- DROP PROCEDURE SP_IFRS_IMP_OVERRIDE_RESTRU_COVID;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMP_OVERRIDE_RESTRU_COVID(
    IN P_RUNID VARCHAR(20) DEFAULT 'S_00000_0000',
    IN P_DOWNLOAD_DATE DATE DEFAULT NULL,
    IN P_PRC VARCHAR(1) DEFAULT 'S')
LANGUAGE PLPGSQL AS $$
DECLARE
    ---- DATE
    V_PREVDATE DATE;
    V_PREVMONTH DATE;
    V_CURRDATE DATE;

    ---- QUERY   
    V_STR_QUERY TEXT;

    ---- TABLE LIST       
    V_TABLENAME VARCHAR(100);
    V_TABLEINSERT1 VARCHAR(100);
    V_TABLEINSERT2 VARCHAR(100);
    V_TABLEINSERT3 VARCHAR(100);
    
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
        V_TABLEINSERT1 := 'TMP_IFRS_ECL_IMA_COVID_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_MASTER_RESTRU_COVID_' || P_RUNID || '';
        V_TABLEINSERT3 := 'TMP_IFRS_ECL_IMA_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'TMP_IFRS_ECL_IMA_COVID';
        V_TABLEINSERT2 := 'IFRS_MASTER_RESTRU_COVID';
        V_TABLEINSERT3 := 'TMP_IFRS_ECL_IMA';
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

    V_PREVMONTH := F_EOMONTH(V_CURRDATE, 1, 'M', 'PREV');
    
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
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT1 || ' AS SELECT * FROM TMP_IFRS_ECL_IMA_COVID WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT3 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT3 || ' AS SELECT * FROM TMP_IFRS_ECL_IMA WHERE 1=0 ';
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
            ,MASTERID  
            ,LIFETIME  
            ,REVOLVING_FLAG  
            ,EIR  
            ,OUTSTANDING  
            ,PLAFOND  
            ,ECL_MODEL_ID  
            ,EAD_MODEL_ID  
            ,CCF_FLAG  
            ,LGD_MODEL_ID  
            ,PD_MODEL_ID  
            ,GROUP_SEGMENT  
            ,SEGMENT  
            ,SUB_SEGMENT  
            ,SEGMENTATION_ID  
            ,CUSTOMER_NUMBER  
            ,PD_ME_MODEL_ID  
            ,BUCKET_GROUP  
            ,BUCKET_ID  
            ,ACCOUNT_NUMBER  
            ,UNAMORT_COST_AMT  
            ,UNAMORT_FEE_AMT  
            ,INTEREST_ACCRUED  
            ,UNUSED_AMOUNT  
            ,FAIR_VALUE_AMOUNT  
            ,EAD_BALANCE  
            ,SICR_RULE_ID  
            ,DPD_CIF  
            ,PRODUCT_ENTITY  
            ,DATA_SOURCE  
            ,PRODUCT_CODE  
            ,PRODUCT_TYPE  
            ,PRODUCT_GROUP  
            ,STAFF_LOAN_FLAG  
            ,IS_IMPAIRED  
            ,PD_SEGMENT  
            ,LGD_SEGMENT  
            ,EAD_SEGMENT  
            ,PREV_ECL_AMOUNT  
            ,SICR_FLAG  
            ,DEFAULT_FLAG  
            ,DEFAULT_RULE_ID  
            ,CCF_RULES_ID  
            ,DPD_FINAL  
            ,BI_COLLECTABILITY  
            ,DPD_FINAL_CIF  
            ,BI_COLLECT_CIF  
            ,STAGE  
            ,RESTRUCTURE_COLLECT_FLAG  
            ,PRODUCT_TYPE_1  
            ,CCF  
            ,CCF_EFF_DATE  
            ,RESTRUCTURE_COLLECT_FLAG_CIF  
            ,IMPAIRED_FLAG  
        ) SELECT   
            DOWNLOAD_DATE  
            ,MASTERID  
            ,LIFETIME  
            ,REVOLVING_FLAG  
            ,EIR  
            ,OUTSTANDING  
            ,PLAFOND  
            ,ECL_MODEL_ID  
            ,EAD_MODEL_ID  
            ,CCF_FLAG  
            ,LGD_MODEL_ID  
            ,PD_MODEL_ID  
            ,GROUP_SEGMENT  
            ,SEGMENT  
            ,SUB_SEGMENT  
            ,SEGMENTATION_ID  
            ,CUSTOMER_NUMBER  
            ,PD_ME_MODEL_ID  
            ,BUCKET_GROUP  
            ,BUCKET_ID  
            ,ACCOUNT_NUMBER  
            ,UNAMORT_COST_AMT  
            ,UNAMORT_FEE_AMT  
            ,INTEREST_ACCRUED  
            ,UNUSED_AMOUNT  
            ,FAIR_VALUE_AMOUNT  
            ,EAD_BALANCE  
            ,SICR_RULE_ID  
            ,DPD_CIF  
            ,PRODUCT_ENTITY  
            ,DATA_SOURCE  
            ,PRODUCT_CODE  
            ,PRODUCT_TYPE  
            ,PRODUCT_GROUP  
            ,STAFF_LOAN_FLAG  
            ,IS_IMPAIRED  
            ,PD_SEGMENT  
            ,LGD_SEGMENT  
            ,EAD_SEGMENT  
            ,PREV_ECL_AMOUNT  
            ,SICR_FLAG  
            ,DEFAULT_FLAG  
            ,DEFAULT_RULE_ID  
            ,CCF_RULES_ID  
            ,DPD_FINAL  
            ,BI_COLLECTABILITY  
            ,DPD_FINAL_CIF  
            ,BI_COLLECT_CIF  
            ,STAGE  
            ,RESTRUCTURE_COLLECT_FLAG  
            ,PRODUCT_TYPE_1  
            ,CCF  
            ,CCF_EFF_DATE  
            ,RESTRUCTURE_COLLECT_FLAG_CIF  
            ,IMPAIRED_FLAG  
        FROM ' || V_TABLEINSERT3 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
        AND MASTERID IN (
            SELECT MASTERID 
            FROM ' || V_TABLEINSERT2 || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        ) ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' 
        SET 
            STAGE = B.STAGE
            ,BUCKET_ID = C.BUCKET_ID  
        FROM ' || V_TABLEINSERT3 || ' A  
        JOIN ' || V_TABLEINSERT2 || ' B  
            ON A.DOWNLOAD_DATE = B.DOWNLOAD_DATE  
            AND A.MASTERID = B.MASTERID 
        JOIN ' || 'IFRS_BUCKET_DETAIL' || ' C 
            ON A.BUCKET_GROUP = C.BUCKET_GROUP 
            AND B.BUCKET_NAME = C.BUCKET_NAME 
            AND C.IS_DELETE = 0
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    RAISE NOTICE 'SP_IFRS_IMP_OVERRIDE_RESTRU_COVID | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT2;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_IMP_OVERRIDE_RESTRU_COVID';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT2 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;