---- DROP PROCEDURE SP_IFRS_IMP_ECL_GENERATE_IMA;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMP_ECL_GENERATE_IMA(
    IN P_RUNID VARCHAR(20) DEFAULT 'S_00000_0000', 
    IN P_DOWNLOAD_DATE DATE DEFAULT NULL,
    IN P_PRC VARCHAR(1) DEFAULT 'S',
    IN P_MODEL_ID BIGINT DEFAULT 0)
LANGUAGE PLPGSQL AS $$
DECLARE
    QRY_RN INT;
    V_PREVDATE DATE;
    V_PREVMONTH DATE;
    V_CURRDATE DATE;
    V_LASTYEAR DATE;
    V_LASTYEARNEXTMONTH DATE;  
    V_STR_QUERY TEXT;        
    V_TABLENAME VARCHAR(100); 
    V_TABLENAME_MON VARCHAR(100);
    V_TABLEINSERT1 VARCHAR(100);
    V_TABLEINSERT2 VARCHAR(100);
    V_TABLEINSERT3 VARCHAR(100);
    V_TABLEINSERT4 VARCHAR(100);
    V_CODITION TEXT;
    V_RETURNROWS INT;
    V_RETURNROWS2 INT;
    V_TABLEDEST VARCHAR(100);
    V_COLUMNDEST VARCHAR(100);
    V_SPNAME VARCHAR(100);
    V_OPERATION VARCHAR(100);
    V_TABLEPDCONFIG VARCHAR(100);
    V_TABLECCFCONFIG VARCHAR(100);
    V_TABLELGDCONFIG VARCHAR(100);
    V_TABLEEADCONFIG VARCHAR(100);
    V_TABLEVIEWLIFETIME VARCHAR(100);

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

    IF COALESCE(P_MODEL_ID, NULL) IS NULL THEN
        P_MODEL_ID := 0;
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
        V_TABLEINSERT4 := 'IFRS_LIFETIME_OVERRIDE';
        V_TABLEVIEWLIFETIME := 'VW_LIFETIME_OVERRIDE';
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG_' || P_RUNID || '';
        V_TABLECCFCONFIG  := 'IFRS_CCF_RULES_CONFIG_' || P_RUNID || '';
        V_TABLEEADCONFIG := 'IFRS_EAD_RULES_CONFIG_' || P_RUNID || '';
        V_TABLELGDCONFIG := 'IFRS_LGD_RULES_CONFIG_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLENAME_MON := 'IFRS_MASTER_ACCOUNT_MONTHLY';
        V_TABLEINSERT1 := 'TMP_IFRS_ECL_IMA';
        V_TABLEINSERT2 := 'IFRS_IMA_IMP_CURR';
        V_TABLEINSERT3 := 'IFRS_IMA_IMP_PREV';
        V_TABLEINSERT4 := 'IFRS_LIFETIME_OVERRIDE';
        V_TABLEVIEWLIFETIME := 'VW_LIFETIME_OVERRIDE';
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG';
        V_TABLECCFCONFIG  := 'IFRS_CCF_RULES_CONFIG';
        V_TABLEEADCONFIG := 'IFRS_EAD_RULES_CONFIG';
        V_TABLELGDCONFIG := 'IFRS_LGD_RULES_CONFIG';
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
            V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEPDCONFIG || ' ';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEPDCONFIG || ' AS SELECT * FROM IFRS_PD_RULES_CONFIG';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLECCFCONFIG || ' ';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLECCFCONFIG || ' AS SELECT * FROM IFRS_CCF_RULES_CONFIG';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEEADCONFIG || ' ';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEEADCONFIG || ' AS SELECT * FROM IFRS_EAD_RULES_CONFIG';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLELGDCONFIG || ' ';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLELGDCONFIG || ' AS SELECT * FROM IFRS_LGD_RULES_CONFIG';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLELGDCONFIG || ' ';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLELGDCONFIG || ' AS SELECT * FROM IFRS_LGD_RULES_CONFIG';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT1 || ' ';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT1 || ' AS SELECT * FROM TMP_IFRS_ECL_IMA';
            EXECUTE (V_STR_QUERY);
        ELSE
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT1 || '';
            EXECUTE (V_STR_QUERY);
    END IF;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TMP_IFRS_ECL_MODEL_' || P_RUNID || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE TMP_IFRS_ECL_MODEL_' || P_RUNID || ' AS
    SELECT
    DISTINCT ''' || CAST(V_CURRDATE AS VARCHAR(10)) || ''' AS DOWNLOAD_DATE,
    A.PKID AS ECL_MODEL_ID,
    B.EAD_MODEL_ID,
    A.ECL_MODEL_NAME,
    I.SUB_SEGMENT AS SUB_SEGMENT_EAD,
    B.SEGMENTATION_ID,
    N.SUB_SEGMENT AS SEGMENTATION_NAME,
    C.CCF_FLAG,
    B.CCF_MODEL_ID AS CCF_RULES_ID,
    D.LGD_MODEL_ID,
    D.EFF_DATE AS LGD_EFF_DATE,
    D.ME_MODEL_ID AS LGD_ME_MODEL_ID,
    J.SUB_SEGMENT AS SUB_SEGMENT_LGD,
    E.PD_MODEL_ID,
    E.ME_MODEL_ID AS PD_ME_MODEL_ID,
    E.EFF_DATE AS PD_EFF_DATE,
    K.SUB_SEGMENT AS SUB_SEGMENT_PD,
    F.BUCKET_GROUP,
    C.EAD_BALANCE,
    F.LT_RULE_ID,
    F.SICR_RULE_ID,
    H.EXPECTED_LIFE,
    F.DEFAULT_RULE_ID,
    B.CCF_EFF_DATE_OPTION,
    L.AVERAGE_METHOD,
    CASE B.CCF_EFF_DATE_OPTION
        WHEN ''SELECT_DATE'' THEN
            B.CCF_EFF_DATE
        WHEN ''LAST_MONTH'' THEN
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE - INTERVAL ''1 day''
    END AS CCF_EFF_DATE,
    CASE L.AVERAGE_METHOD
        WHEN ''WEIGHTED'' THEN
            M.WEIGHTED_AVG_CCF
        WHEN ''SIMPLE'' THEN
            M.SIMPLE_AVG_CCF
    END AS CCF,
    I.SEGMENT,
    I.SUB_SEGMENT,
    I.GROUP_SEGMENT
FROM
    IFRS_ECL_MODEL_HEADER A
    JOIN IFRS_ECL_MODEL_DETAIL_EAD B
    ON A.PKID = B.ECL_MODEL_ID
    JOIN ' || V_TABLEEADCONFIG || ' C
    ON B.EAD_MODEL_ID = C.PKID
    JOIN IFRS_ECL_MODEL_DETAIL_LGD D
    ON A.PKID = D.ECL_MODEL_ID
    AND B.SEGMENTATION_ID = D.SEGMENTATION_ID
    JOIN IFRS_ECL_MODEL_DETAIL_PD E
    ON A.PKID = E.ECL_MODEL_ID
    AND B.SEGMENTATION_ID = E.SEGMENTATION_ID
    JOIN IFRS_ECL_MODEL_DETAIL_PF F
    ON A.PKID = F.ECL_MODEL_ID
    AND B.SEGMENTATION_ID = F.SEGMENTATION_ID
    LEFT JOIN ' || V_TABLELGDCONFIG || ' G
    ON D.LGD_MODEL_ID = G.PKID
    LEFT JOIN ' || V_TABLEPDCONFIG || ' H
    ON E.PD_MODEL_ID = H.PKID
    LEFT JOIN IFRS_MSTR_SEGMENT_RULES_HEADER I
    ON C.SEGMENTATION_ID = I.PKID
    LEFT JOIN IFRS_MSTR_SEGMENT_RULES_HEADER J
    ON G.SEGMENTATION_ID = J.PKID
    LEFT JOIN IFRS_MSTR_SEGMENT_RULES_HEADER K
    ON H.SEGMENTATION_ID = K.PKID
    LEFT JOIN ' || V_TABLECCFCONFIG || ' L
    ON B.CCF_MODEL_ID = L.PKID
    LEFT JOIN IFRS_EAD_CCF_HEADER M
    ON (
        CASE B.CCF_EFF_DATE_OPTION
            WHEN ''SELECT_DATE'' THEN
                B.CCF_EFF_DATE
            WHEN ''LAST_MONTH'' THEN
                ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE - INTERVAL ''1 day''
        END = M.DOWNLOAD_DATE )
    AND L.PKID = M.CCF_RULE_ID
    LEFT JOIN IFRS_MSTR_SEGMENT_RULES_HEADER N
    ON B.SEGMENTATION_ID = N.PKID
    WHERE A.IS_DELETE = 0          
    AND B.IS_DELETE = 0          
    AND C.IS_DELETE = 0          
    AND C.ACTIVE_FLAG = 1          
    AND D.IS_DELETE = 0          
    AND E.IS_DELETE = 0          
    AND F.IS_DELETE = 0  
    AND ((' || P_MODEL_ID || ' = 0
    AND A.ACTIVE_STATUS = ''1'')
    OR (A.PKID = ' || P_MODEL_ID || '))';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE TMP_IFRS_ECL_MODEL_' || P_RUNID || ' SET EAD_BALANCE = REPLACE(EAD_BALANCE,''UNUSED_AMOUNT'',''A.UNUSED_AMOUNT'')';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE TMP_IFRS_ECL_MODEL_' || P_RUNID || ' SET EAD_BALANCE = REPLACE(EAD_BALANCE,''OUTSTANDING'',''A.OUTSTANDING'')';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE TMP_IFRS_ECL_MODEL_' || P_RUNID || ' SET EAD_BALANCE = REPLACE(EAD_BALANCE,''INTEREST_ACCRUED'',''A.INTEREST_ACCRUED'')';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE TMP_IFRS_ECL_MODEL_' || P_RUNID || ' SET EAD_BALANCE = REPLACE(EAD_BALANCE,''COLL_AMOUNT'',''A.COLL_AMOUNT'')';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE TMP_IFRS_ECL_MODEL_' || P_RUNID || ' SET EAD_BALANCE = REPLACE(EAD_BALANCE,''CCF'',''COALESCE(CAST(A.CCF AS NUMERIC), 0)'')';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE TMP_IFRS_ECL_MODEL_' || P_RUNID || ' SET EAD_BALANCE = REPLACE(EAD_BALANCE,''+'',''+'')';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS UPDATE_IFRS_IMA_IMP_CURR_' || P_RUNID || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE UPDATE_IFRS_IMA_IMP_CURR_' || P_RUNID || ' AS
    SELECT Z.CCF_EFF_DATE, Z.CCF_RULES_ID, Z.CCF, Y.SEGMENT, Y.SUB_SEGMENT, Y.GROUP_SEGMENT 
    FROM IFRS_MSTR_SEGMENT_RULES_HEADER Y 
    LEFT JOIN TMP_IFRS_ECL_MODEL_' || P_RUNID || ' Z ON Z.SEGMENTATION_ID = Y.PKID
    WHERE Y.SEGMENT_TYPE = ''PORTFOLIO_SEGMENT'' AND Z.CCF_EFF_DATE IS NOT NULL';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP INDEX IF EXISTS NCI_UPDATE_IFRS_IMA_IMP_CURR_' || P_RUNID || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE INDEX IF NOT EXISTS NCI_UPDATE_IFRS_IMA_IMP_CURR_' || P_RUNID || '
    ON UPDATE_IFRS_IMA_IMP_CURR_' || P_RUNID || ' USING BTREE
    (SEGMENT ASC NULLS LAST, SUB_SEGMENT ASC NULLS LAST, GROUP_SEGMENT ASC NULLS LAST)
    TABLESPACE PG_DEFAULT;';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' X
    SET CCF_EFF_DATE = Y.CCF_EFF_DATE,
    CCF_RULE_ID = Y.CCF_RULES_ID,
    CCF = Y.CCF
    FROM UPDATE_IFRS_IMA_IMP_CURR_' || P_RUNID || ' Y
    WHERE X.SEGMENT = Y.SEGMENT
    AND X.SUB_SEGMENT = Y.SUB_SEGMENT
    AND X.GROUP_SEGMENT = Y.GROUP_SEGMENT;';
    EXECUTE (V_STR_QUERY);
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' X
    SET UNUSED_AMOUNT = 0 
    FROM IFRS_CREDITLINE_JENIUS Y
    WHERE X.CUSTOMER_NUMBER = Y.CUSTOMER_NUMBER
    AND X.PRODUCT_CODE = Y.DEAL_TYPE
    AND Y.DEAL_TYPE IS NOT NULL
    AND Y.ELIGIBILITY_STATUS IN (''NOT_ELIGIBLE'', ''NOT ELIGIBLE'')';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TMP_QRY';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE TMP_QRY AS
    SELECT ROW_NUMBER() OVER (ORDER BY A.ECL_MODEL_ID) AS RN, 
    ' || '''
    SELECT                                        
    A.DOWNLOAD_DATE,                                                          
    A.MASTERID,                                                          
    A.GROUP_SEGMENT,                                                          
    A.SEGMENT,
    A.SUB_SEGMENT, 
    '' || COALESCE(CAST(A.SEGMENTATION_ID AS VARCHAR(10)),' || '''''''''''''' || ') || '' AS SEGMENTATION_ID, 
    A.ACCOUNT_NUMBER,                                                     
    A.CUSTOMER_NUMBER, 
    '' || COALESCE(CAST(A.SICR_RULE_ID AS VARCHAR(10)),' || '''''''0''''''' || ') || '' AS SICR_RULE_ID,                                                          
    ''''0'''' AS SICR_FLAG,                                                                  
    A.DPD_CIF,
    A.PRODUCT_ENTITY,                                                    
    A.DATA_SOURCE,                                                   
    A.PRODUCT_CODE,                                                   
    A.PRODUCT_TYPE,                                                      
    A.PRODUCT_GROUP,                                                   
    A.STAFF_LOAN_FLAG,
    A.IS_IMPAIRED,
    '' || COALESCE('''''''' || A.SUB_SEGMENT_PD || '''''''',' || '''''''''''''' || ') || '' AS SUB_SEGMENT_PD,
    '' || COALESCE('''''''' || A.SUB_SEGMENT_LGD || '''''''',' || '''''''''''''' || ') || '' AS SUB_SEGMENT_LGD,
    '' || COALESCE('''''''' || A.SUB_SEGMENT_EAD || '''''''',' || '''''''''''''' || ') || '' AS SUB_SEGMENT_EAD,
    COALESCE(E.ECL_AMOUNT,0) AS PREV_ECL_AMOUNT,
    '' || COALESCE('''''''' || A.BUCKET_GROUP || '''''''',' || '''''''''''''' || ') || '' AS BUCKET_GROUP,
    D.BUCKET_ID,                                                      
    A.REVOLVING_FLAG,                                                          
    CASE WHEN COALESCE(A.EIR, 0) <> 0 THEN A.EIR WHEN COALESCE(A.INTEREST_RATE, 0) <> 0 THEN A.INTEREST_RATE ELSE F.AVG_EIR * 100.00 END AS EIR ,                                                      
    A.OUTSTANDING,                                                          
    COALESCE(A.UNAMORT_COST_AMT,0) AS UNAMORT_COST_AMT,                                                          
    COALESCE(A.UNAMORT_FEE_AMT,0) AS UNAMORT_FEE_AMT,           
    COALESCE(CASE WHEN A.INTEREST_ACCRUED < 0 THEN 0 ELSE A.INTEREST_ACCRUED END, 0) AS INTEREST_ACCRUED,                                            
    COALESCE(A.UNUSED_AMOUNT,0) AS UNUSED_AMOUNT,                                                          
    COALESCE(A.FAIR_VALUE_AMOUNT,0) AS FAIR_VALUE_AMOUNT,
    CASE WHEN A.PRODUCT_TYPE_1 <> ''''PRK'''' AND A.DATA_SOURCE NOT IN  (''''LIMIT'''',''''LIMIT_T24'''') THEN
    COALESCE(CASE WHEN A.BI_COLLECTABILITY >= 3 THEN '' || CASE
    WHEN A.EAD_BALANCE LIKE ''%A.INTEREST_ACCRUED%'' THEN
        REPLACE(REPLACE(A.EAD_BALANCE, ''A.UNUSED_AMOUNT'', ''0''), ''A.INTEREST_ACCRUED'', ''0'')
    ELSE
        REPLACE(A.EAD_BALANCE, ''A.UNUSED_AMOUNT'', ''0'')
    END || '' ELSE '' || REPLACE(
    CASE
        WHEN A.EAD_BALANCE LIKE ''%UNUSED_AMOUNT%'' THEN
            A.EAD_BALANCE
        ELSE
            REPLACE(A.EAD_BALANCE, ''A.UNUSED_AMOUNT'', ''0'')
    END,
    ''A.INTEREST_ACCRUED'',
    ''CASE WHEN A.INTEREST_ACCRUED < 0 THEN   0 ELSE A.INTEREST_ACCRUED END'') || '' END, 0)                                       
    ELSE                                                      
    COALESCE(CASE WHEN A.BI_COLLECTABILITY >= 3 THEN '' || CASE
    WHEN A.EAD_BALANCE LIKE ''%A.INTEREST_ACCRUED%'' THEN
        REPLACE(REPLACE(A.EAD_BALANCE, ''A.INTEREST_ACCRUED'', ''0''), ''A.UNUSED_AMOUNT'', ''0'')
    ELSE
        REPLACE(A.EAD_BALANCE, ''A.UNUSED_AMOUNT'', ''0'')
    END || '' WHEN A.DPD_FINAL > 30 THEN '' || REPLACE(REPLACE(A.EAD_BALANCE,
    ''A.UNUSED_AMOUNT'',
    ''0''),
    ''A.INTEREST_ACCRUED'',
    ''CASE WHEN A.INTEREST_ACCRUED < 0 THEN 0 ELSE A.INTEREST_ACCRUED END'') ||
    '' ELSE '' || REPLACE(A.EAD_BALANCE,
    ''A.INTEREST_ACCRUED'',
    ''CASE WHEN A.INTEREST_ACCRUED < 0 THEN 0 ELSE A.INTEREST_ACCRUED END'') || '' END, 0)                                                          
    END AS EAD_BALANCE,
    COALESCE(A.PLAFOND,0) PLAFOND, 
    '' || COALESCE(CAST(A.ECL_MODEL_ID AS VARCHAR(10)),' || '''''''''''''' || ') || '' AS ECL_MODEL_ID, 
    '' || COALESCE(CAST(A.EAD_MODEL_ID AS VARCHAR(10)),' || '''''''''''''' || ') || '' AS EAD_MODEL_ID, 
    '' || COALESCE(CAST(A.CCF_FLAG AS VARCHAR(10)),' || '''''''1''''''' || ') || '' AS CCF_FLAG, 
    '' || COALESCE(CAST(A.CCF_RULES_ID AS VARCHAR(10)),''NULL'') || '' AS CCF_RULE_ID, 
    '' || COALESCE(CAST(A.LGD_MODEL_ID AS VARCHAR(10)),' || '''''''''''''' || ') || '' AS LGD_MODEL_ID, 
    '' || COALESCE(CAST(A.PD_MODEL_ID AS VARCHAR(10)),' || '''''''''''''' || ') || '' AS PD_MODEL_ID, 
    '' || COALESCE(CAST(A.PD_ME_MODEL_ID AS VARCHAR(10)),' || '''''''0''''''' || ') || '' AS PD_ME_MODEL_ID,                                                           
    CASE WHEN G.LIFETIME_OVERRIDE IS NULL OR G.LIFETIME_OVERRIDE = 0 THEN CASE             
    WHEN A.PRODUCT_TYPE_1 = ''''PRK'''' AND A.REMAINING_TENOR <= 0 THEN 12               
    WHEN A.DATA_SOURCE = ''''LOAN_T24'''' AND COALESCE(A.REVOLVING_FLAG,''''1'''') = ''''1'''' AND A.REMAINING_TENOR <= 0 THEN 12              
    ELSE A.REMAINING_TENOR             
    END
	ELSE G.LIFETIME_OVERRIDE END AS LIFETIME,
    '' || COALESCE(CAST(A.DEFAULT_RULE_ID AS VARCHAR(10)),''NULL'') || '' AS DEFAULT_RULE_ID,
    A.DPD_FINAL,                                                           
    A.BI_COLLECTABILITY,                                                           
    A.DPD_FINAL_CIF,                                              
    A.BI_COLLECT_CIF,                                        
    A.RESTRUCTURE_COLLECT_FLAG,                                                
    A.PRODUCT_TYPE_1,                                                
    NULL AS CCF,                                               
    '''''' || CAST(COALESCE(A.CCF_EFF_DATE, DATE(CURRENT_TIMESTAMP)) AS VARCHAR(20)) || '''''' AS CCF_EFF_DATE,
    A.RESTRUCTURE_COLLECT_FLAG_CIF,                                    
    A.IMPAIRED_FLAG,
    A.INITIAL_RATING_CODE                                
    ,A.RATING_CODE                                
    ,A.RATING_DOWNGRADE                                
    ,A.PD_INITIAL_RATE                                
    ,A.WATCHLIST_FLAG                                
    ,A.PD_CURRENT_RATE                                
    ,A.PD_CHANGE             
    ,A.COLL_AMOUNT                    
    ,A.EXT_RATING_AGENCY                    
    ,A.EXT_RATING_CODE                    
    ,A.EXT_INIT_RATING_CODE                    
    ,A.EXT_RATING_DOWNGRADE
    ,A.SEGMENT_FLAG
    FROM ' || V_TABLEINSERT2 || ' A                                                           
    JOIN IFRS_MSTR_SEGMENT_RULES_HEADER B ON A.GROUP_SEGMENT = B.GROUP_SEGMENT                                                        
    AND A.SEGMENT = B.SEGMENT                              
    AND A.SUB_SEGMENT = B.SUB_SEGMENT
    JOIN IFRS_BUCKET_HEADER C ON '''''' || COALESCE(A.BUCKET_GROUP,' || '''''''1''''''' || ') || '''''' = C.BUCKET_GROUP                                                   
    JOIN IFRS_BUCKET_DETAIL D ON C.BUCKET_GROUP = D.BUCKET_GROUP
    AND ((CASE WHEN C.OPTION_GROUPING = ''''DPD''''                       
    THEN A.DAY_PAST_DUE                                                                 
    WHEN C.OPTION_GROUPING = ''''DPD_CIF''''                                                     
    THEN A.DPD_CIF                                                       
    WHEN C.OPTION_GROUPING = ''''DPD_FINAL''''                                   
    THEN A.DPD_FINAL                                                                   
    WHEN C.OPTION_GROUPING = ''''DPD_FINAL_CIF''''                                        
    THEN A.DPD_FINAL_CIF                                                     
    WHEN C.OPTION_GROUPING = ''''BIC''''                                                     
    THEN A.BI_COLLECTABILITY                                            
    END) BETWEEN D.RANGE_START AND D.RANGE_END OR C.OPTION_GROUPING  IN (''''IR'''',''''ER'''') AND  D.SUB_BUCKET_GROUP = CASE WHEN C.OPTION_GROUPING = ''''ER'''' THEN A.EXT_RATING_CODE WHEN C.OPTION_GROUPING = ''''IR'''' THEN A.RATING_CODE END)                                     
    LEFT JOIN ' || V_TABLEINSERT3 || ' E ON A.MASTERID = E.MASTERID
    LEFT JOIN IFRS_IMP_AVG_EIR F ON A.DOWNLOAD_DATE = F.DOWNLOAD_DATE AND A.EIR_SEGMENT = F.EIR_SEGMENT                                            
    LEFT JOIN ' || V_TABLEINSERT4 || ' G ON A.SEGMENTATION_ID = G.SEGMENTATION_ID
    WHERE B.PKID = '' || COALESCE(CAST(A.SEGMENTATION_ID AS INT), 1) || ''                                                      
        AND  A.DOWNLOAD_DATE = ''''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''''                                
        AND B.SEGMENT_TYPE = ''''PORTFOLIO_SEGMENT''''                                                           
        AND A.ACCOUNT_STATUS = ''''A''''                                           
        AND COALESCE(A.IFRS9_CLASS,' || '''''''''' || ') <> ''''FVTPL''''
    ''' || ' AS QRY 
    FROM TMP_IFRS_ECL_MODEL_' || P_RUNID || ' A;';
    EXECUTE (V_STR_QUERY);

    ----====== LOOP INSERT
    FOR QRY_RN IN 1..(
        SELECT
            COUNT(*)
        FROM
            TMP_QRY
    ) LOOP
    
    EXECUTE 'INSERT INTO ' || V_TABLEINSERT1 || ' (DOWNLOAD_DATE        
        ,MASTERID        
        ,GROUP_SEGMENT        
        ,SEGMENT        
        ,SUB_SEGMENT        
        ,SEGMENTATION_ID        
        ,ACCOUNT_NUMBER        
        ,CUSTOMER_NUMBER        
        ,SICR_RULE_ID        
        ,SICR_FLAG        
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
        ,BUCKET_GROUP        
        ,BUCKET_ID        
        ,REVOLVING_FLAG        
        ,EIR        
        ,OUTSTANDING        
        ,UNAMORT_COST_AMT        
        ,UNAMORT_FEE_AMT        
        ,INTEREST_ACCRUED        
        ,UNUSED_AMOUNT        
        ,FAIR_VALUE_AMOUNT        
        ,EAD_BALANCE        
        ,PLAFOND        
        ,ECL_MODEL_ID        
        ,EAD_MODEL_ID        
        ,CCF_FLAG        
        ,CCF_RULES_ID        
        ,LGD_MODEL_ID        
        ,PD_MODEL_ID        
        ,PD_ME_MODEL_ID        
        ,LIFETIME        
        ,DEFAULT_RULE_ID        
        ,DPD_FINAL        
        ,BI_COLLECTABILITY        
        ,DPD_FINAL_CIF        
        ,BI_COLLECT_CIF        
        ,RESTRUCTURE_COLLECT_FLAG        
        ,PRODUCT_TYPE_1        
        ,CCF        
        ,CCF_EFF_DATE        
        ,RESTRUCTURE_COLLECT_FLAG_CIF        
        ,IMPAIRED_FLAG        
        ,INITIAL_RATING_CODE        
        ,RATING_CODE        
        ,RATING_DOWNGRADE        
        ,PD_INITIAL_RATE        
        ,WATCHLIST_FLAG        
        ,PD_CURRENT_RATE        
        ,PD_CHANGE        
        ,COLL_AMOUNT        
        ,EXT_RATING_AGENCY        
        ,EXT_RATING_CODE        
        ,EXT_INIT_RATING_CODE        
        ,EXT_RATING_DOWNGRADE
        ,SEGMENT_FLAG) '
            || (
        SELECT
            QRY
        FROM
            TMP_QRY
        WHERE
            RN = QRY_RN
    );

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;
	
    END LOOP;
    ----====== END LOOP INSERT

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP INDEX IF EXISTS NCI_' || V_TABLEINSERT1 || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE INDEX IF NOT EXISTS NCI_' || V_TABLEINSERT1 || '
    ON ' || V_TABLEINSERT1 || ' USING BTREE
    (PRODUCT_CODE ASC NULLS LAST, MASTERID ASC NULLS LAST, CCF_RULES_ID ASC NULLS LAST)
    TABLESPACE PG_DEFAULT';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A
    SET LIFETIME = CASE WHEN CAST(VALUE2 AS SMALLINT) <= LIFETIME THEN CAST(VALUE2 AS SMALLINT) ELSE LIFETIME END 
    FROM TBLM_COMMONCODEDETAIL B
    WHERE A.PRODUCT_CODE = B.VALUE1
    AND B.COMMONCODE = ''RVW_PERIOD'';';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS UPDATE_TMP_IFRS_ECL_IMA';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE UPDATE_TMP_IFRS_ECL_IMA AS
    SELECT A.MASTERID, A.CCF_RULES_ID, A.CCF_EFF_DATE, COALESCE(B.CCF,0) AS CCF, CASE WHEN A.EAD_BALANCE < 0 THEN 0 ELSE COALESCE(A.EAD_BALANCE,0) END AS EAD_BALANCE
    FROM ' || V_TABLEINSERT1 || ' A
    LEFT JOIN TMP_IFRS_ECL_MODEL_' || P_RUNID || ' B USING (CCF_RULES_ID,CCF_EFF_DATE) 
    WHERE B.CCF_RULES_ID IS NULL
    AND B.CCF_EFF_DATE IS NULL';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP INDEX IF EXISTS IDX_UPDATE_TMP_IFRS_ECL_IMA';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE INDEX IF NOT EXISTS IDX_UPDATE_TMP_IFRS_ECL_IMA
    ON UPDATE_TMP_IFRS_ECL_IMA USING BTREE
    (MASTERID ASC NULLS LAST, CCF_RULES_ID ASC NULLS LAST)
    TABLESPACE PG_DEFAULT';
    EXECUTE (V_STR_QUERY);
    

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A
    SET CCF = B.CCF,
    EAD_BALANCE = B.EAD_BALANCE
    FROM UPDATE_TMP_IFRS_ECL_IMA B
    WHERE A.MASTERID = B.MASTERID
    AND A.CCF_RULES_ID = B.CCF_RULES_ID';
    EXECUTE (V_STR_QUERY);

    ----===== INSERT HISTORY ECL CONFIG
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM IFRS_ECL_MODEL_CONFIG_HIST 
    WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || ''' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO IFRS_ECL_MODEL_CONFIG_HIST (          
    DOWNLOAD_DATE          
    ,ECL_MODEL_ID           
    ,ECL_MODEL_NAME          
    ,SEGMENTATION_ID          
    ,SEGMENTATION_NAME          
    ,EAD_MODEL_ID          
    ,SUB_SEGMENT_EAD          
    ,PD_MODEL_ID          
    ,PD_ME_MODEL_ID          
    ,PD_EFF_DATE          
    ,SUB_SEGMENT_PD          
    ,BUCKET_GROUP          
    ,LGD_MODEL_ID          
    ,LGD_ME_MODEL_ID          
    ,SUB_SEGMENT_LGD          
    ,LGD_EFF_DATE          
    ,CCF_RULES_ID          
    ,CCF_FLAG          
    ,CCF_EFF_DATE_OPTION          
    ,CCF_EFF_DATE          
    ,CCF          
    ,AVERAGE_METHOD          
    ,SICR_RULE_ID         
    ,DEFAULT_RULE_ID          
    ,EAD_BALANCE          
    ,LT_RULE_ID          
    ,EXPECTED_LIFE          
    ,SEGMENT          
    ,SUB_SEGMENT          
    ,GROUP_SEGMENT          
    )   
    SELECT ''' || CAST(V_CURRDATE AS VARCHAR(10)) || ''' AS DOWNLOAD_DATE,
    ECL_MODEL_ID,
    ECL_MODEL_NAME,
    SEGMENTATION_ID,
    SEGMENTATION_NAME,
    EAD_MODEL_ID,
    SUB_SEGMENT_EAD,
    PD_MODEL_ID,
    PD_ME_MODEL_ID,
    PD_EFF_DATE,
    SUB_SEGMENT_PD,
    BUCKET_GROUP,
    LGD_MODEL_ID,
    LGD_ME_MODEL_ID,
    SUB_SEGMENT_LGD,
    LGD_EFF_DATE,
    CCF_RULES_ID,
    CCF_FLAG,
    CCF_EFF_DATE_OPTION,
    CCF_EFF_DATE,
    CCF,
    AVERAGE_METHOD,
    SICR_RULE_ID,
    DEFAULT_RULE_ID,
    EAD_BALANCE,
    LT_RULE_ID,
    EXPECTED_LIFE,
    SEGMENT,
    SUB_SEGMENT,
    GROUP_SEGMENT
    FROM TMP_IFRS_ECL_MODEL_' || P_RUNID || ' ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    -------- INSERT HISTORY ECL CONFIG

    -------- START UPDATE CCF TO TABLE IFRS_CCF_OVERRIDE --------
    IF P_PRC = 'P' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT4 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT4 || ' (LIFETIME_CONFIGURATION,
        SEGMENTATION_ID,
        SEGMENTATION,
        DOWNLOAD_DATE,
        LIFETIME_RATE,
        LIFETIME_OVERRIDE)
        SELECT * FROM ' || V_TABLEVIEWLIFETIME || '';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- END UPDATE CCF TO TABLE IFRS_CCF_OVERRIDE --------

    RAISE NOTICE 'SP_IFRS_IMP_ECL_GENERATE_IMA | AFFECTED RECORD : %', V_RETURNROWS2;

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT1;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_IMP_ECL_GENERATE_IMA';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT1 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;