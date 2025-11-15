---- DROP PROCEDURE SP_IFRS_IMP_EAD_TERM_MONTH;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMP_EAD_TERM_MONTH(
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

    V_STR_QUERY TEXT;
    V_STR_SQL_RULE TEXT;        
    V_TABLENAME VARCHAR(100); 
    V_TABLENAME_MON VARCHAR(100);
    V_TABLEINSERT1 VARCHAR(100);
    V_TABLEINSERT2 VARCHAR(100);
    V_TABLEINSERT3 VARCHAR(100);
    V_TABLEMSTPARAM VARCHAR(100);

    V_CODITION TEXT;
    V_RETURNROWS INT;
    V_RETURNROWS2 INT;
    V_TABLEDEST VARCHAR(100);
    V_COLUMNDEST VARCHAR(100);
    V_SPNAME VARCHAR(100);
    V_OPERATION VARCHAR(100);

    V_TABLECCFCONFIG VARCHAR(100);

    ---- RESULT
    V_QUERYS TEXT;
    V_CODITION2 TEXT;

    ---
    V_LOG_SEQ INTEGER;
    V_DIFF_LOG_SEQ INTEGER;
    V_SP_NAME VARCHAR(100);
    V_PRC_NAME VARCHAR(100);
    V_SEQ INTEGER;
    V_SP_NAME_PREV VARCHAR(100);
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
        V_TABLEINSERT1 := 'TMP_IFRS_ECL_IMA';
        V_TABLEINSERT2 := 'IFRS_IMA_IMP_CURR_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_EAD_TERM_MONTHLY_' || P_RUNID || '';
        V_TABLECCFCONFIG  := 'IFRS_CCF_RULES_CONFIG_' || P_RUNID || '';
        V_TABLEMSTPARAM  := 'IFRS_MASTER_PRODUCT_PARAM_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLENAME_MON := 'IFRS_MASTER_ACCOUNT_MONTHLY';
        V_TABLEINSERT1 := 'TMP_IFRS_ECL_IMA';
        V_TABLEINSERT2 := 'IFRS_IMA_IMP_CURR';
        V_TABLEINSERT3 := 'IFRS_EAD_TERM_MONTHLY'; ---> CHANGE FROM IFRS_EAD_RESULT_PRK
        V_TABLECCFCONFIG  := 'IFRS_CCF_RULES_CONFIG';
        V_TABLEMSTPARAM  := 'IFRS_MASTER_PRODUCT_PARAM';
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

    -------- ====== BODY ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE IF NOT EXISTS ' || V_TABLEMSTPARAM || ' AS SELECT * FROM IFRS_MASTER_PRODUCT_PARAM';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE IF NOT EXISTS IFRS_EAD_TERM_MONTHLY_' || P_RUNID || ' AS SELECT * FROM IFRS_EAD_TERM_MONTHLY WHERE 0=1';
        EXECUTE (V_STR_QUERY);
    END IF;


    -------- ====== INSERT EAD MONTHLY ======

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT3 || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TMP_TABLE_' || P_RUNID || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE TMP_TABLE_' || P_RUNID || ' 
        AS SELECT ROW_NUMBER() OVER(ORDER BY MASTERID) AS R_NUMBER, A.* 
        FROM ' || V_TABLEINSERT1 || ' A 
        WHERE ((A.DATA_SOURCE=''LOAN'' AND A.PRODUCT_TYPE_1 = ''PRK'')
        OR (A.DATA_SOURCE = ''LOAN_T24'' AND COALESCE(A.REVOLVING_FLAG,1)=1)
        OR (A.DATA_SOURCE IN (''LIMIT'',''LIMIT_T24''))) AND A.IMPAIRED_FLAG = ''C'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP INDEX IF EXISTS NCI_TMP';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE INDEX NCI_TMP ON TMP_TABLE_' || P_RUNID || ' (CCF_RULES_ID ASC, EAD_MODEL_ID ASC, R_NUMBER ASC) ';
    EXECUTE (V_STR_QUERY);

    -- WITH N(N) AS         
    -- (        
    -- SELECT (GENERATE_SERIES.N) + 1
    -- FROM GENERATE_SERIES(0, 255) AS GENERATE_SERIES(N)
    -- ORDER BY GENERATE_SERIES.N
    -- LIMIT 256     
    -- )

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || ';WITH N(N) AS         
    (        
    SELECT (GENERATE_SERIES.N) + 1
    FROM GENERATE_SERIES(0, 255) AS GENERATE_SERIES(N)
    ORDER BY GENERATE_SERIES.N
    LIMIT 256     
    )
    
    INSERT INTO ' || V_TABLEINSERT3 || '              
    (              
    DOWNLOAD_DATE,              
    MASTERID,              
    GROUP_SEGMENT,              
    SEGMENT,              
    SUB_SEGMENT,              
    SEGMENTATION_ID,              
    ACCOUNT_NUMBER,              
    CUSTOMER_NUMBER,               
    SICR_RULE_ID,              
    BUCKET_GROUP,              
    BUCKET_ID,              
    LIFETIME,              
    STAGE,              
    REVOLVING_FLAG,              
    PD_SEGMENT,              
    LGD_SEGMENT,              
    EAD_SEGMENT,              
    PREV_ECL_AMOUNT,              
    ECL_MODEL_ID,              
    EAD_MODEL_ID,              
    CCF_RULES_ID,               
    LGD_MODEL_ID,               
    PD_MODEL_ID,              
    SEQ,              
    FL_YEAR,              
    FL_MONTH,              
    EIR,              
    OUTSTANDING,              
    UNAMORT_COST_AMT,              
    UNAMORT_FEE_AMT,              
    INTEREST_ACCRUED,              
    UNUSED_AMOUNT,              
    FAIR_VALUE_AMOUNT,              
    EAD_BALANCE,              
    PLAFOND,              
    EAD,              
    CCF,       
    BI_COLLECTABILITY,                
    COLL_AMOUNT,    
    SEGMENT_FLAG              
    )        
    SELECT               
    A.DOWNLOAD_DATE,              
    A.MASTERID,           
    A.GROUP_SEGMENT,              
    A.SEGMENT,              
    A.SUB_SEGMENT,              
    A.SEGMENTATION_ID,              
    A.ACCOUNT_NUMBER,              
    A.CUSTOMER_NUMBER,               
    A.SICR_RULE_ID,              
    A.BUCKET_GROUP,              
    A.BUCKET_ID,              
    A.LIFETIME,              
    A.STAGE::INT,         
    A.REVOLVING_FLAG,              
    A.PD_SEGMENT,              
    A.LGD_SEGMENT,              
    A.EAD_SEGMENT,              
    A.PREV_ECL_AMOUNT,              
    A.ECL_MODEL_ID,              
    A.EAD_MODEL_ID,              
    A.CCF_RULES_ID,               
    A.LGD_MODEL_ID,              
    A.PD_MODEL_ID,           
    N.N AS SEQ,              
    CASE WHEN CAST(N.N AS DECIMAL(10,2))/12 <= 1 THEN 1 ELSE CEILING(CAST(N.N AS DECIMAL(10,2))/12) END FL_YEAR,                       
    CASE WHEN (N.N % 12) = 0 THEN         
    (CASE WHEN CAST(N.N AS DECIMAL(10,2))/12 = 1 THEN N.N ELSE CAST(N.N/CEILING(CAST(N.N AS DECIMAL(10,2))/12) AS INT) END)        
    ELSE         
    (N.N % 12) END AS FL_MONTH,          
    A.EIR,              
    A.OUTSTANDING,              
    A.UNAMORT_COST_AMT,              
    A.UNAMORT_FEE_AMT,              
    CASE WHEN N.N = 1 THEN A.INTEREST_ACCRUED ELSE 0 END AS INTEREST_ACCRUED,              
    A.UNUSED_AMOUNT,              
    A.FAIR_VALUE_AMOUNT,              
    A.EAD_BALANCE AS EAD_BALANCE,          
    A.PLAFOND,              
    CASE WHEN N.N <> 1 AND E.EAD_BALANCE LIKE  ''%INTEREST_ACCRUED%'' THEN               
    CASE WHEN COALESCE(A.EAD_BALANCE,0) - COALESCE(A.INTEREST_ACCRUED,0) < 0 THEN 0 ELSE COALESCE(A.EAD_BALANCE,0) - COALESCE(A.INTEREST_ACCRUED,0) END               
    ELSE               
    CASE WHEN A.EAD_BALANCE < 0 THEN 0 ELSE A.EAD_BALANCE END               
    END   AS EAD, 
    CASE D.AVERAGE_METHOD WHEN ''WEIGHTED'' THEN C.WEIGHTED_AVG_CCF WHEN ''SIMPLE'' THEN C.SIMPLE_AVG_CCF END AS CCF,                
    BI_COLLECTABILITY,                
    A.COLL_AMOUNT,    
    A.SEGMENT_FLAG               
    FROM TMP_TABLE_' || P_RUNID || ' A         
    LEFT JOIN ' || V_TABLEMSTPARAM || ' PP ON         
    A.PRODUCT_CODE = PP.PRD_CODE         
    AND PP.PRD_TYPE = ''CREDITCARD''             
    INNER JOIN N ON CASE WHEN COALESCE(PP.PRD_CODE,'''') = '''' THEN 12 ELSE A.LIFETIME END >= N.N        
    JOIN IFRS_ECL_MODEL_DETAIL_EAD B              
    ON A.CCF_RULES_ID = B.CCF_MODEL_ID AND A.ECL_MODEL_ID = B.ECL_MODEL_ID AND A.SEGMENTATION_ID = B.SEGMENTATION_ID              
    LEFT JOIN IFRS_EAD_CCF_HEADER C ON (CASE B.CCF_EFF_DATE_OPTION WHEN ''SELECT_DATE'' THEN B.CCF_EFF_DATE WHEN ''LAST_MONTH'' THEN (A.DOWNLOAD_DATE + INTERVAL ''1 DAY'')::DATE END = C.DOWNLOAD_DATE) AND A.CCF_RULES_ID = C.CCF_RULE_ID                
    LEFT JOIN IFRS_CCF_RULES_CONFIG D ON C.CCF_RULE_ID = D.PKID              
    LEFT JOIN IFRS_EAD_RULES_CONFIG E ON A.EAD_MODEL_ID = E.PKID           
    WHERE N.N <= A.LIFETIME';
    EXECUTE (V_STR_QUERY);

    -------- ====== INSERT EAD MONTHLY ======

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    RAISE NOTICE 'SP_IFRS_IMP_EAD_TERM_MONTH | AFFECTED RECORD : %', V_RETURNROWS2;
    -------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT3;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_IMP_EAD_TERM_MONTH';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT3 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;