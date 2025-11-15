---- DROP PROCEDURE SP_IFRS_IMPI_JOURNAL_DATA;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMPI_JOURNAL_DATA(
    IN P_RUNID VARCHAR(20) DEFAULT 'S_00000_0000', 
    IN P_DOWNLOAD_DATE DATE DEFAULT NULL,
    IN P_PRC VARCHAR(1) DEFAULT 'S')
LANGUAGE PLPGSQL AS $$
DECLARE
    ---- DATE
    V_PREVDATE DATE;
    V_PREVMONTH DATE;
    V_CURRDATE DATE;
    V_LASTYEAR DATE;
    V_LASTYEARNEXTMONTH DATE;

    ---- QUERY   
    V_STR_QUERY TEXT;

    ---- TABLE LIST       
    V_TABLENAME VARCHAR(100); 
    V_TABLENAME_MON VARCHAR(100);
    V_TABLEINSERT1 VARCHAR(100);
    V_TABLEINSERT2 VARCHAR(100);
    V_TABLEINSERT3 VARCHAR(100);
    V_TABLEINSERT4 VARCHAR(100);
    V_TABLEINSERT5 VARCHAR(100);
    V_TABLEINSERT6 VARCHAR(100);
    V_TABLEINSERT7 VARCHAR(100);

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
        V_TABLEINSERT1 := 'TMP_IFRS_ECL_IMA_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_IMA_IMP_CURR_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_IMA_IMP_PREV_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_IMP_JOURNAL_DATA_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLENAME_MON := 'IFRS_MASTER_ACCOUNT_MONTHLY';
        V_TABLEINSERT1 := 'TMP_IFRS_ECL_IMA';
        V_TABLEINSERT2 := 'IFRS_IMA_IMP_CURR';
        V_TABLEINSERT3 := 'IFRS_IMA_IMP_PREV';
        V_TABLEINSERT4 := 'IFRS_IMP_JOURNAL_DATA';
    END IF;

    IF P_DOWNLOAD_DATE IS NULL 
    THEN
        SELECT
            CURRDATE INTO V_CURRDATE
        FROM
            IFRS_PRC_DATE;
    ELSE        
        V_CURRDATE := P_DOWNLOAD_DATE;
    END IF;
    
    V_PREVMONTH := F_EOMONTH(V_CURRDATE, 1, 'M', 'PREV');
    V_LASTYEAR := F_EOMONTH(V_CURRDATE, 1, 'Y', 'PREV');
    V_LASTYEARNEXTMONTH := F_EOMONTH(V_LASTYEAR, 1, 'M', 'NEXT');
    
    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    -------- RECORD RUN_ID --------
    CALL SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, PG_BACKEND_PID(), CURRENT_DATE);
    -------- RECORD RUN_ID --------

    ------ ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT4 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT4 || ' AS SELECT * FROM IFRS_IMP_JOURNAL_DATA WHERE 0=1';
        EXECUTE (V_STR_QUERY);
    END IF;
    ------ ====== PRE SIMULATION TABLE ======
    
    -------- ====== BODY ======

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS EXCLUSION_' || P_RUNID || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE EXCLUSION_' || P_RUNID || ' AS 
    SELECT DISTINCT MASTERID FROM IFRS_ECL_EXCLUSION WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT4 || ' 
    WHERE JOURNAL_TYPE IN (''BKIP'', ''BIUW'', ''BKIP_OCI'')                  
    AND DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT4 || '                
    (                  
    DOWNLOAD_DATE,                  
    MASTERID,                  
    ACCOUNT_NUMBER,                  
    FACILITY_NUMBER,                  
    JOURNAL_REF_NUM,                  
    JOURNAL_TYPE,                  
    DATA_SOURCE,                  
    PRD_TYPE,                  
    PRD_CODE,                  
    PRD_GROUP,                  
    BRANCH_CODE,                  
    CURRENCY,                  
    TXN_TYPE,                  
    AMOUNT,                  
    AMOUNT_IDR,                  
    GL_ACCOUNT,                  
    GL_CORE,                  
    JOURNAL_DESC,                  
    REVERSAL_FLAG,                  
    SEGMENT,                  
    CUSTOMER_NUMBER,                  
    RESTRUCTURE_FLAG,    
    CREATEDBY,    
    CREATEDDATE                   
    )                  
    SELECT *                  
    FROM                 
    (                
    SELECT                  
        F_EOMONTH(''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE, 0, ''M'', ''PREV'') AS DOWNLOAD_DATE,                
        PMA.MASTERID,                  
        PMA.ACCOUNT_NUMBER,                  
        PMA.FACILITY_NUMBER,                  
        CASE                 
            WHEN GL.IFRS_ACCT_TYPE = ''BKIP'' THEN ''IMPAIRMENT - IA''                  
            WHEN GL.IFRS_ACCT_TYPE = ''BIUW'' THEN ''UNWINDING - IA''                  
        ELSE NULL                  
        END AS JOURNAL_REF_NUM,                  
        GL.IFRS_ACCT_TYPE AS JOURNAL_TYPE,                  
        PMA.DATA_SOURCE,                  
        PMA.PRODUCT_TYPE,                  
        PMA.PRODUCT_CODE,                  
        PMA.PRODUCT_GROUP,                  
        PMA.BRANCH_CODE,                  
        PMA.CURRENCY,                  
        GL.TXN_TYPE,                  
        SUM(                  
            CASE                  
                WHEN GL.IFRS_ACCT_TYPE = ''BKIP'' THEN COALESCE(PMA.ECL_AMOUNT, 0)                  
                WHEN GL.IFRS_ACCT_TYPE = ''BIUW'' THEN COALESCE(PMA.IA_UNWINDING_AMOUNT, 0)                  
            ELSE NULL                  
            END    
        ) AS AMOUNT,                  
        SUM(                  
            CASE            
                WHEN GL.IFRS_ACCT_TYPE = ''BKIP'' THEN COALESCE(PMA.ECL_AMOUNT, 0)  * COALESCE(PMA.EXCHANGE_RATE, 1)                 
                WHEN GL.IFRS_ACCT_TYPE = ''BIUW'' THEN COALESCE(PMA.IA_UNWINDING_AMOUNT, 0) * COALESCE(PMA.EXCHANGE_RATE, 1)             
            ELSE NULL                  
            END                
        ) AS AMOUNT_IDR,                  
        GL.GL_CODE,                  
        GL.GL_INTERNAL_CODE,                  
        GL.REMARKS,                  
        ''N'' AS REVERSAL_FLAG,                  
        PMA.SEGMENT,                  
        PMA.CUSTOMER_NUMBER,                  
        COALESCE(PMA.RESTRUCTURE_FLAG,0) RESTRUCTURE_FLAG, ---NEW      
        ''' || V_SP_NAME || ''' AS CREATEDBY,    
        CURRENT_DATE AS CREATEDDATE                
    FROM ' || V_TABLENAME || ' PMA                  
    INNER JOIN IFRS_MASTER_JOURNAL_PARAM GL               
    ON                  
    UPPER(RTRIM(LTRIM(PMA.GL_CONSTNAME)))= UPPER(RTRIM(LTRIM(GL.GL_CONSTNAME)))                  
    AND (UPPER(RTRIM(LTRIM(PMA.CURRENCY))) = UPPER(RTRIM(LTRIM(GL.CCY))) or UPPER(RTRIM(LTRIM(GL.CCY)))=''ALL'')  
    WHERE PMA.DOWNLOAD_DATE = F_EOMONTH(''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE, 0, ''M'', ''PREV'')                  
    AND COALESCE(PMA.IMPAIRED_FLAG, ''C'') = ''I''           
    AND GL.IFRS_ACCT_TYPE IN (''BKIP'', ''BIUW'')                
    AND GL.IS_DELETE = 0
    AND PMA.MASTERID NOT IN (SELECT DISTINCT MASTERID FROM EXCLUSION_' || P_RUNID || ')
    GROUP BY PMA.DOWNLOAD_DATE,                  
    PMA.MASTERID,                  
    PMA.ACCOUNT_NUMBER,                  
    PMA.FACILITY_NUMBER,                  
    GL.IFRS_ACCT_TYPE,                  
    PMA.DATA_SOURCE,                  
    PMA.PRODUCT_TYPE,                  
    PMA.PRODUCT_CODE,                  
    PMA.PRODUCT_GROUP,                  
    PMA.BRANCH_CODE,                  
    PMA.CURRENCY,                  
    GL.TXN_TYPE,                  
    GL.GL_CODE,                  
    GL.GL_INTERNAL_CODE,                  
    GL.REMARKS,                  
    PMA.SEGMENT,                  
    PMA.CUSTOMER_NUMBER,                  
    COALESCE(PMA.RESTRUCTURE_FLAG,0)                
    ) S                   
    WHERE AMOUNT > 0
    
    UNION ALL
    
    SELECT *                  
    FROM                 
    (                
    SELECT                  
        F_EOMONTH(''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE, 0, ''M'', ''PREV'') AS DOWNLOAD_DATE,                
        PMA.MASTERID,                  
        PMA.ACCOUNT_NUMBER,                  
        PMA.FACILITY_NUMBER,                  
        ''IMPAIRMENT FVOCI - IA'' AS JOURNAL_REF_NUM,                  
        GL.IFRS_ACCT_TYPE AS JOURNAL_TYPE,                  
        PMA.DATA_SOURCE,                  
        PMA.PRODUCT_TYPE,                  
        PMA.PRODUCT_CODE,                  
        PMA.PRODUCT_GROUP,                  
        PMA.BRANCH_CODE,                  
        PMA.CURRENCY,                  
        GL.TXN_TYPE,                  
        SUM(COALESCE(PMA.ECL_AMOUNT, 0) ) AS AMOUNT,                  
        SUM(COALESCE(PMA.ECL_AMOUNT, 0)  * COALESCE(PMA.EXCHANGE_RATE, 1))AS AMOUNT_IDR,                  
        GL.GL_CODE,                  
        GL.GL_INTERNAL_CODE,                  
        GL.REMARKS,                  
        ''N'' AS REVERSAL_FLAG,                  
        PMA.SEGMENT,                  
        PMA.CUSTOMER_NUMBER,                  
        COALESCE(PMA.RESTRUCTURE_FLAG,0) RESTRUCTURE_FLAG, ---NEW      
        ''' || V_SP_NAME || ''' AS CREATEDBY,    
        CURRENT_DATE AS CREATEDDATE                     
    FROM ' || V_TABLENAME || ' PMA
    INNER JOIN IFRS_EIR_ADJUSTMENT ADJ ON PMA.MASTERID = ADJ.MASTERID AND ADJ.DOWNLOAD_DATE = F_EOMONTH(''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE, 0, ''M'', ''PREV'')  
    INNER JOIN IFRS_MASTER_JOURNAL_PARAM GL             
    ON                    
    UPPER(RTRIM(LTRIM(PMA.GL_CONSTNAME)))= UPPER(RTRIM(LTRIM(GL.GL_CONSTNAME)))                  
    AND (UPPER(RTRIM(LTRIM(PMA.CURRENCY))) = UPPER(RTRIM(LTRIM(GL.CCY))) or UPPER(RTRIM(LTRIM(GL.CCY))) = ''ALL'')  
    WHERE PMA.DOWNLOAD_DATE = F_EOMONTH(''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE, 0, ''M'', ''PREV'')                  
    AND COALESCE(PMA.IMPAIRED_FLAG,''C'') = ''I''                   
    AND GL.IFRS_ACCT_TYPE IN (''BKIP_OCI'' )               
    AND ADJ.IFRS9_CLASS = ''FVOCI'' 
    and GL.IS_DELETE = 0   
    AND PMA.MASTERID NOT IN (SELECT DISTINCT MASTERID FROM EXCLUSION_' || P_RUNID || ')        
    GROUP BY PMA.DOWNLOAD_DATE,                  
    PMA.MASTERID,                  
    PMA.ACCOUNT_NUMBER,                  
    PMA.FACILITY_NUMBER,                  
    GL.IFRS_ACCT_TYPE,                  
    PMA.DATA_SOURCE,        
    PMA.PRODUCT_TYPE,                  
    PMA.PRODUCT_CODE,                  
    PMA.PRODUCT_GROUP,                  
    PMA.BRANCH_CODE,                  
    PMA.CURRENCY,                  
    GL.TXN_TYPE,                  
    GL.GL_CODE,                  
    GL.GL_INTERNAL_CODE,                  
    GL.REMARKS,                  
    PMA.SEGMENT,                  
    PMA.CUSTOMER_NUMBER,                  
    COALESCE(PMA.RESTRUCTURE_FLAG,0)                
    ) A                   
    WHERE AMOUNT > 0';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT4 || '                  
    (                  
        DOWNLOAD_DATE,                  
        MASTERID,                  
        ACCOUNT_NUMBER,                  
        FACILITY_NUMBER,                  
        JOURNAL_REF_NUM,                  
        JOURNAL_TYPE,                  
        DATA_SOURCE,                  
        PRD_TYPE,                  
        PRD_CODE,                  
        PRD_GROUP,                  
        BRANCH_CODE,                  
        CURRENCY,                  
        TXN_TYPE,                  
        AMOUNT,                  
        AMOUNT_IDR,                  
        GL_ACCOUNT,                  
        GL_CORE,                  
        JOURNAL_DESC,                  
        REVERSAL_FLAG,                  
        SEGMENT,                  
        CUSTOMER_NUMBER,                  
        RESTRUCTURE_FLAG,      
        CREATEDBY,      
        CREATEDDATE                   
    )                  
    SELECT                  
        F_EOMONTH(''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE, 0, ''M'', ''PREV'') AS DOWNLOAD_DATE,                  
        GL.MASTERID,                  
        GL.ACCOUNT_NUMBER,                  
        GL.FACILITY_NUMBER,                  
        GL.JOURNAL_REF_NUM,                  
        GL.JOURNAL_TYPE,                  
        GL.DATA_SOURCE,                  
        GL.PRD_TYPE,                  
        GL.PRD_CODE,                  
        GL.PRD_GROUP,                  
        GL.BRANCH_CODE,                  
        GL.CURRENCY,                  
        CASE                  
          WHEN GL.TXN_TYPE = ''DB'' THEN ''CR''                  
          ELSE ''DB''                  
        END AS TXN_TYPE,                  
        GL.AMOUNT,                  
        GL.AMOUNT_IDR,                  
        GL.GL_ACCOUNT,                  
        GL.GL_CORE,                  
        GL.JOURNAL_DESC,                   
        ''Y'' AS REVERSAL_FLAG,                  
        GL.SEGMENT,                  
        GL.CUSTOMER_NUMBER,                  
        GL.RESTRUCTURE_FLAG,      
        ''' || V_SP_NAME || ''' AS CREATEDBY,    
        CURRENT_DATE AS CREATEDDATE                     
    FROM ' || V_TABLEINSERT4 || ' GL
    WHERE GL.DOWNLOAD_DATE = ''' || CAST(V_PREVMONTH AS VARCHAR(10)) || '''::DATE                  
    AND GL.JOURNAL_TYPE IN (''BKIP'', ''BIUW'')                  
    AND GL.REVERSAL_FLAG = ''N''';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    RAISE NOTICE 'SP_IFRS_IMPI_JOURNAL_DATA | AFFECTED RECORD : %', V_RETURNROWS2;
    -------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT4;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_IMPI_JOURNAL_DATA';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT4 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;