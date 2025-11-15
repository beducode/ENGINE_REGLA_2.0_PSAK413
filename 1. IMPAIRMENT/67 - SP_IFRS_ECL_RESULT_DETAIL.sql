---- DROP PROCEDURE SP_IFRS_ECL_RESULT_DETAIL;

CREATE OR REPLACE PROCEDURE SP_IFRS_ECL_RESULT_DETAIL(
    IN P_RUNID VARCHAR(20) DEFAULT 'S_00000_0000', 
    IN P_DOWNLOAD_DATE DATE DEFAULT NULL,
    IN P_PRC VARCHAR(1) DEFAULT 'S',
    IN P_MODEL_ID BIGINT DEFAULT 0)
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
    V_TABLEINSERT4 VARCHAR(100);
    V_TABLEINSERT5 VARCHAR(100);
    V_TABLEINSERT6 VARCHAR(100);
    V_TABLEINSERT7 VARCHAR(100);
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

    IF COALESCE(P_MODEL_ID, NULL) IS NULL THEN
        P_MODEL_ID := 0;
    END IF;

    IF P_PRC = 'S' THEN 
        V_TABLENAME := 'TMP_IMA_' || P_RUNID || '';
        V_TABLENAME_MON := 'TMP_IMAM_' || P_RUNID || '';
        V_TABLEINSERT1 := 'TMP_IFRS_ECL_IMA_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_IMA_IMP_CURR_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_ECL_RESULT_DETAIL_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_ECL_RESULT_HEADER_' || P_RUNID || '';
        V_TABLEINSERT5 := 'IFRS_EAD_TERM_YEARLY_' || P_RUNID || '';
        V_TABLEINSERT6 := 'IFRS_EAD_TERM_YEARLY_CC_' || P_RUNID || '';
        V_TABLEINSERT7 := 'IFRS_EAD_TERM_MONTHLY_' || P_RUNID || '';
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG_' || P_RUNID || '';
        V_TABLECCFCONFIG  := 'IFRS_CCF_RULES_CONFIG_' || P_RUNID || '';
        V_TABLELGDCONFIG := 'IFRS_LGD_RULES_CONFIG_' || P_RUNID || '';
        V_TABLEEADCONFIG := 'IFRS_EAD_RULES_CONFIG_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLENAME_MON := 'IFRS_MASTER_ACCOUNT_MONTHLY';
        V_TABLEINSERT1 := 'TMP_IFRS_ECL_IMA';
        V_TABLEINSERT2 := 'IFRS_IMA_IMP_CURR';
        V_TABLEINSERT3 := 'IFRS_ECL_RESULT_DETAIL';
        V_TABLEINSERT4 := 'IFRS_ECL_RESULT_HEADER';
        V_TABLEINSERT5 := 'IFRS_EAD_TERM_YEARLY';
        V_TABLEINSERT6 := 'IFRS_EAD_TERM_YEARLY_CC';
        V_TABLEINSERT7 := 'IFRS_EAD_TERM_MONTHLY';
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG';
        V_TABLECCFCONFIG  := 'IFRS_CCF_RULES_CONFIG';
        V_TABLELGDCONFIG := 'IFRS_LGD_RULES_CONFIG';
        V_TABLEEADCONFIG := 'IFRS_EAD_RULES_CONFIG';
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
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE IF NOT EXISTS ' || V_TABLEPDCONFIG || ' AS SELECT * FROM IFRS_PD_RULES_CONFIG';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE IF NOT EXISTS ' || V_TABLECCFCONFIG || ' AS SELECT * FROM IFRS_CCF_RULES_CONFIG';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE IF NOT EXISTS ' || V_TABLELGDCONFIG || ' AS SELECT * FROM IFRS_LGD_RULES_CONFIG';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE IF NOT EXISTS ' || V_TABLEEADCONFIG || ' AS SELECT * FROM IFRS_EAD_RULES_CONFIG';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE IF NOT EXISTS ' || V_TABLEINSERT3 || ' AS SELECT * FROM IFRS_ECL_RESULT_DETAIL WHERE 0=1';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE IF NOT EXISTS ' || V_TABLEINSERT4 || ' AS SELECT * FROM IFRS_ECL_RESULT_HEADER WHERE 0=1';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE IF NOT EXISTS ' || V_TABLEINSERT5 || ' AS SELECT * FROM IFRS_EAD_TERM_YEARLY';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE IF NOT EXISTS ' || V_TABLEINSERT6 || ' AS SELECT * FROM IFRS_EAD_TERM_YEARLY_CC';
        EXECUTE (V_STR_QUERY);
    END IF;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT3 || ' WHERE DOWNLOAD_DATE = (DATE_TRUNC(''MONTH'', ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE) + INTERVAL ''1 MONTH - 1 DAY'')::DATE';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT4 || ' WHERE DOWNLOAD_DATE = (DATE_TRUNC(''MONTH'', ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE) + INTERVAL ''1 MONTH - 1 DAY'')::DATE';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TMP_IFRS_ECL_MODEL_ECL_' || P_RUNID || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE TMP_IFRS_ECL_MODEL_ECL_' || P_RUNID || ' AS
      SELECT DISTINCT      
      A.PKID AS ECL_MODEL_ID,      
      B.EAD_MODEL_ID,      
      B.SEGMENTATION_ID,      
      C.CCF_FLAG,      
      B.CCF_MODEL_ID AS CCF_RULES_ID,      
      D.LGD_MODEL_ID,      
      CASE D.EFF_DATE_OPTION      
      WHEN ''LAST_MONTH'' THEN (DATE_TRUNC(''MONTH'',(DATE_TRUNC(''MONTH'', ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE) + INTERVAL ''1 MONTH - 1 DAY'' - INTERVAL ''1 MONTH'')) + INTERVAL ''1 MONTH - 1 DAY'')::DATE      
      WHEN ''LAST_QUARTER'' THEN (DATE_TRUNC(''MONTH'',(DATE_TRUNC(''QUARTER'', ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE) + INTERVAL ''1 MONTH - 1 DAY'' -  INTERVAL ''3 MONTH'')) + INTERVAL ''1 MONTH - 1 DAY'')::DATE      
      WHEN ''SELECT_DATE'' THEN COALESCE(D.EFF_DATE ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE)      
      ELSE ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE       
      END AS LGD_DATE,      
      E.PD_MODEL_ID,      
      E.ME_MODEL_ID AS PD_ME_MODEL_ID,      
      CASE E.EFF_DATE_OPTION      
      WHEN ''LAST_MONTH'' THEN (DATE_TRUNC(''MONTH'',(DATE_TRUNC(''MONTH'', ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE) + INTERVAL ''1 MONTH - 1 DAY'' - INTERVAL ''1 MONTH'')) + INTERVAL ''1 MONTH - 1 DAY'')::DATE      
      WHEN ''LAST_QUARTER'' THEN (DATE_TRUNC(''MONTH'',(DATE_TRUNC(''QUARTER'', ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE) + INTERVAL ''1 MONTH - 1 DAY'' -  INTERVAL ''3 MONTH'')) + INTERVAL ''1 MONTH - 1 DAY'')::DATE      
      WHEN ''SELECT_DATE'' THEN COALESCE(E.EFF_DATE, ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE)      
      ELSE ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE      
      END AS PD_DATE    
    FROM IFRS_ECL_MODEL_HEADER A      
    JOIN IFRS_ECL_MODEL_DETAIL_EAD B ON A.PKID = B.ECL_MODEL_ID      
    JOIN ' || V_TABLEEADCONFIG || ' C ON B.EAD_MODEL_ID = C.PKID      
    JOIN IFRS_ECL_MODEL_DETAIL_LGD D ON A.PKID = D.ECL_MODEL_ID AND B.SEGMENTATION_ID = D.SEGMENTATION_ID      
    JOIN IFRS_ECL_MODEL_DETAIL_PD E ON A.PKID = E.ECL_MODEL_ID AND B.SEGMENTATION_ID = E.SEGMENTATION_ID      
    JOIN IFRS_ECL_MODEL_DETAIL_PF F ON A.PKID = F.ECL_MODEL_ID      
    WHERE A.IS_DELETE = 0     
    AND B.IS_DELETE = 0      
    AND C.IS_DELETE = 0      
    AND C.ACTIVE_FLAG = 1      
    AND ((' || P_MODEL_ID || ' = 0 AND A.ACTIVE_STATUS = ''1'') OR (A.PKID = ' || P_MODEL_ID || '))';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TMP_IFRS_ECL_PD_MONTHLY_ECL_' || P_RUNID || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE TMP_IFRS_ECL_PD_MONTHLY_ECL_' || P_RUNID || ' AS
      SELECT      
        A.DOWNLOAD_DATE,      
        A.PD_RULE_ID,      
        A.MODEL_ID AS PD_ME_MODEL_ID,      
        A.BUCKET_GROUP,      
        A.BUCKET_ID,      
        A.FL_SEQ,      
        A.FL_YEAR,      
        A.FL_MONTH,      
        A.SCENARIO_NO,      
        A.PD_RATE AS PD    
      FROM IFRS_PD_TERM_STRUCTURE_NOFL A      
      WHERE      
      A.PD_RATE >= 0      
      AND EXISTS      
      (      
        SELECT 1      
        FROM TMP_IFRS_ECL_MODEL_ECL_' || P_RUNID || ' X      
        WHERE X.PD_MODEL_ID = A.PD_RULE_ID      
        AND X.PD_DATE = A.DOWNLOAD_DATE
      )';
    EXECUTE (V_STR_QUERY);

     -------------- CREATE FOR PD FORWARD LOOKING MONTHLY ----------------------------

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TMP_IFRS_ECL_PD_FL_MONTHLY_ECL_' || P_RUNID || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE TMP_IFRS_ECL_PD_FL_MONTHLY_ECL_' || P_RUNID || '  AS 
    SELECT      
      A.DOWNLOAD_DATE,      
      A.PD_RULE_ID,      
      A.ME_MODEL_ID AS PD_ME_MODEL_ID,      
      A.BUCKET_GROUP,      
      A.BUCKET_ID,      
      A.FL_SEQ,       
      A.FL_YEAR,      
      A.FL_MONTH,       
      A.PD_FINAL AS PD_FL,      
      A.SEGMENTATION_ID        
    FROM IFRS_IMP_PD_FL_TERM_STRUCTURE A       
    WHERE      
    A.PD_FINAL >= 0 AND A.DOWNLOAD_DATE= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE      
    AND EXISTS      
    (      
      SELECT 1     
      FROM TMP_IFRS_ECL_MODEL_ECL_' || P_RUNID || ' X      
      WHERE X.PD_MODEL_ID = A.PD_RULE_ID      
      AND X.PD_DATE = A.PD_EFFECTIVE_DATE      
      AND X.SEGMENTATION_ID = A.SEGMENTATION_ID     
    )';
    EXECUTE (V_STR_QUERY);

    --------------END CREATE FOR PD FOWRWARD LOOKING MONTHLY -------------------------------- 

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TMP_IFRS_ECL_PD_YEARLY_' || P_RUNID || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE TMP_IFRS_ECL_PD_YEARLY_' || P_RUNID || ' AS     
    SELECT       
      A.DOWNLOAD_DATE,      
      A.CURR_DATE,      
      A.PD_RULE_ID,       
      A.BUCKET_GROUP,      
      A.BUCKET_ID,      
      A.FL_SEQ,      
      A.FL_YEAR,      
      A.PD_RATE AS PD,      
      A.CREATEDDATE    
    FROM IFRS_PD_TERM_STRUCTURE_NOFL_YEARLY A      
    WHERE      
    A.PD_RATE >= 0      
    AND EXISTS      
    (      
      SELECT 1      
      FROM TMP_IFRS_ECL_MODEL_ECL_' || P_RUNID || ' X      
      WHERE X.PD_MODEL_ID = A.PD_RULE_ID      
      AND X.PD_DATE = A.DOWNLOAD_DATE      
    ) AND A.FL_SEQ = 1      
    ORDER BY A.CURR_DATE, BUCKET_GROUP, BUCKET_ID, PD_RULE_ID';
    EXECUTE (V_STR_QUERY);

    -------------- CREATE FOR PD FL FORWARD LOOKING YEARLY ---------------- 

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TMP_IFRS_ECL_PD_FL_YEARLY_' || P_RUNID || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE TMP_IFRS_ECL_PD_FL_YEARLY_' || P_RUNID || ' AS     
    SELECT      
        A.DOWNLOAD_DATE,      
        A.PD_RULE_ID,      
        A.ME_MODEL_ID AS PD_ME_MODEL_ID,      
        A.BUCKET_GROUP,      
        A.BUCKET_ID,      
        A.FL_SEQ,      
        A.FL_YEAR,       
        A.PD_FINAL AS PD_FL,      
        A.SEGMENTATION_ID      
    FROM IFRS_IMP_PD_FL_TERM_YEARLY A       
    WHERE      
    A.PD_FINAL >= 0 AND A.FL_YEAR =1 AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE      
    AND EXISTS      
    (      
        SELECT 1      
        FROM TMP_IFRS_ECL_MODEL_ECL_' || P_RUNID || ' X      
        WHERE X.PD_MODEL_ID = A.PD_RULE_ID      
        AND X.SEGMENTATION_ID = A.SEGMENTATION_ID      
    )';
    EXECUTE (V_STR_QUERY);

    -------------- END CREATE FOR PD FOWRWARD LOOKING YEARLY---------------------------- 

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TMP_IFRS_ECL_LGD_' || P_RUNID || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE TMP_IFRS_ECL_LGD_' || P_RUNID || ' AS     
    SELECT A.DOWNLOAD_DATE,      
        A.LGD_RULE_ID,       
        A.FL_SEQ,      
        MAX(A.FL_SEQ) OVER(PARTITION BY A.LGD_RULE_ID, A.MODEL_ID) MAX_SEQ,      
        A.LGD      
    FROM IFRS_LGD_TERM_STRUCTURE A      
    WHERE      
    A.LGD >= 0      
    AND EXISTS      
    (      
        SELECT 1      
        FROM TMP_IFRS_ECL_MODEL_ECL_' || P_RUNID || ' X      
        WHERE      
        X.LGD_MODEL_ID = A.LGD_RULE_ID      
        AND A.DOWNLOAD_DATE = X.LGD_DATE      
    )';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO TMP_IFRS_ECL_LGD_' || P_RUNID || '  (DOWNLOAD_DATE, LGD_RULE_ID, LGD) SELECT ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE, 99999, 1';
    EXECUTE (V_STR_QUERY);

    ----------------------------------------- START INSERT DATA NONPRK -----------------------------------------

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT3 || '        
    (        
      DOWNLOAD_DATE,        
      MASTERID,        
      GROUP_SEGMENT,        
      SEGMENT,        
      SUB_SEGMENT,        
      SEGMENTATION_ID,        
      ACCOUNT_NUMBER,        
      CUSTOMER_NUMBER,        
      LT_SEGMENT,        
      SICR_RULE_ID,        
      SICR_FLAG,        
      LIFETIME,        
      STAGE,        
      REVOLVING_FLAG,        
      PD_SEGMENT,        
      LGD_SEGMENT,        
      EAD_SEGMENT,        
      PREV_ECL_AMOUNT,        
      BUCKET_GROUP,        
      BUCKET_ID,        
      ECL_MODEL_ID,        
      EAD_MODEL_ID,        
      CCF_RULES_ID,        
      PD_MODEL_ID,        
      LGD_MODEL_ID,         
      SEQ,        
      FL_YEAR,        
      FL_MONTH,        
      OUTSTANDING,        
      UNAMORT_COST_AMT,         
      UNAMORT_FEE_AMT,        
      INTEREST_ACCRUED,        
      UNUSED_AMOUNT,         
      FAIR_VALUE_AMOUNT,         
      EAD_BALANCE,        
      PLAFOND,        
      EIR,        
      CCF,        
      EAD,        
      PD_BFL,        
      LGD,        
      DISC_EAD,        
      ECL,        
      ECL_BFL,        
      PD,        
      COLL_AMOUNT         
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
      NULL AS LT_SEGMENT,      
      A.SICR_RULE_ID,      
      NULL AS SICR_FLAG,      
      A.LIFETIME,      
      A.STAGE,      
      A.REVOLVING_FLAG,      
      A.PD_SEGMENT,      
      A.LGD_SEGMENT,      
      A.EAD_SEGMENT,      
      A.PREV_ECL_AMOUNT,      
      A.BUCKET_GROUP,      
      A.BUCKET_ID,      
      A.ECL_MODEL_ID,      
      A.EAD_MODEL_ID,       
      A.CCF_RULES_ID,      
      A.PD_MODEL_ID,      
      A.LGD_MODEL_ID,      
      A.SEQ,      
      A.FL_YEAR,      
      A.FL_MONTH,      
      A.OUTSTANDING,      
      A.UNAMORT_COST_AMT,       
      A.UNAMORT_FEE_AMT,       
      A.INTEREST_ACCRUED,      
      A.UNUSED_AMOUNT,       
      A.FAIR_VALUE_AMOUNT,       
      A.EAD_BALANCE,       
      A.PLAFOND,      
      A.EIR,      
      A.CCF,      
      A.EAD,      
      C.PD AS PD_BFL,      
      D.LGD,      
      CASE WHEN A.STAGE = 3 THEN A.EAD ELSE       
      FUTIL_PV(COALESCE(A.EIR, 0)/100/12, A.SEQ, A.EAD) END AS DISC_EAD,      
      (CASE WHEN A.STAGE = 3 THEN A.EAD ELSE       
      FUTIL_PV(COALESCE(A.EIR, 0)/100/12, A.SEQ, A.EAD) END) * COALESCE(E.PD_FL,C.PD) * COALESCE(D.LGD,1) AS ECL,       
      (CASE WHEN A.STAGE = 3 THEN A.EAD ELSE       
      FUTIL_PV(COALESCE(A.EIR, 0)/100/12, A.SEQ, A.EAD) END) * C.PD * COALESCE(D.LGD,1) AS ECL_BFL,      
      E.PD_FL AS PD ,      
      A.COLL_AMOUNT      
      FROM ' || V_TABLEINSERT5 || ' A     
      JOIN TMP_IFRS_ECL_MODEL_ECL_' || P_RUNID || ' B ON A.ECL_MODEL_ID = B.ECL_MODEL_ID AND A.SEGMENTATION_ID = B.SEGMENTATION_ID      
      JOIN TMP_IFRS_ECL_PD_MONTHLY_ECL_' || P_RUNID || ' C ON B.PD_MODEL_ID = C.PD_RULE_ID  AND A.BUCKET_ID = C.BUCKET_ID  AND A.SEQ = C.FL_SEQ AND B.PD_DATE = C.DOWNLOAD_DATE       
      JOIN TMP_IFRS_ECL_LGD_' || P_RUNID || ' D ON B.LGD_MODEL_ID = D.LGD_RULE_ID AND COALESCE(B.LGD_DATE,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE) = D.DOWNLOAD_DATE       
      JOIN TMP_IFRS_ECL_PD_FL_MONTHLY_ECL_' || P_RUNID || ' E ON B.PD_MODEL_ID = E.PD_RULE_ID AND A.BUCKET_ID = E.BUCKET_ID AND A.SEQ = E.FL_SEQ AND A.SEGMENTATION_ID= E.SEGMENTATION_ID       
      WHERE A.EAD > 0';
      EXECUTE (V_STR_QUERY);

      GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
      V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
      V_RETURNROWS := 0;

      --------------------------------------- END INSERT DATA NONPRK --------------------------------------

      ----------------------------------------- START INSERT DATA PRK YEARLY --------------------------------------
      V_STR_QUERY := '';
      V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT3 || '      
      (      
      DOWNLOAD_DATE,      
      MASTERID,      
      GROUP_SEGMENT,      
      SEGMENT,      
      SUB_SEGMENT,      
      SEGMENTATION_ID,      
      ACCOUNT_NUMBER,      
      CUSTOMER_NUMBER,      
      LT_SEGMENT,      
      SICR_RULE_ID,      
      SICR_FLAG,      
      LIFETIME,      
      STAGE,      
      REVOLVING_FLAG,      
      PD_SEGMENT,      
      LGD_SEGMENT,      
      EAD_SEGMENT,      
      PREV_ECL_AMOUNT,      
      BUCKET_GROUP,      
      BUCKET_ID,      
      ECL_MODEL_ID,      
      EAD_MODEL_ID,      
      CCF_RULES_ID,      
      PD_MODEL_ID,       
      LGD_MODEL_ID,      
      SEQ,      
      FL_YEAR,      
      FL_MONTH,      
      OUTSTANDING,      
      UNAMORT_COST_AMT,       
      UNAMORT_FEE_AMT,       
      INTEREST_ACCRUED,      
      UNUSED_AMOUNT,       
      FAIR_VALUE_AMOUNT,       
      EAD_BALANCE,      
      PLAFOND,      
      EIR,      
      CCF,       
      EAD,      
      PD_BFL,      
      LGD,      
      DISC_EAD,      
      ECL,      
      ECL_BFL,       
      PD,      
      COLL_AMOUNT      
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
      NULL AS LT_SEGMENT,      
      A.SICR_RULE_ID,      
      NULL AS SICR_FLAG,      
      A.LIFETIME,      
      A.STAGE,      
      A.REVOLVING_FLAG,      
      A.PD_SEGMENT,      
      A.LGD_SEGMENT,      
      A.EAD_SEGMENT,      
      A.PREV_ECL_AMOUNT,      
      A.BUCKET_GROUP,      
      A.BUCKET_ID,      
      A.ECL_MODEL_ID,      
      A.EAD_MODEL_ID,      
      A.CCF_RULES_ID,      
      A.PD_MODEL_ID,      
      A.LGD_MODEL_ID,      
      A.SEQ,      
      C.FL_YEAR,      
      0 AS FL_MONTH,      
      A.OUTSTANDING,      
      A.UNAMORT_COST_AMT,       
      A.UNAMORT_FEE_AMT,       
      A.INTEREST_ACCRUED,      
      A.UNUSED_AMOUNT,       
      A.FAIR_VALUE_AMOUNT,      
      A.EAD_BALANCE,      
      A.PLAFOND,      
      A.EIR,      
      A.CCF,      
      A.EAD,      
      C.PD AS PD_BFL,      
      D.LGD,      
      FUTIL_PV(COALESCE(A.EIR, 0)/100, A.SEQ, A.EAD) AS DISC_EAD,      
      FUTIL_PV(COALESCE(A.EIR, 0)/100, A.SEQ, A.EAD) * COALESCE(E.PD_FL,C.PD) * COALESCE(D.LGD,1) AS ECL,      
      FUTIL_PV(COALESCE(A.EIR, 0)/100, A.SEQ, A.EAD) * C.PD * COALESCE(D.LGD,1) AS ECL_BFL,      
      E.PD_FL,      
      A.COLL_AMOUNT     
      FROM ' || V_TABLEINSERT6 || ' A      
      JOIN TMP_IFRS_ECL_MODEL_ECL_' || P_RUNID || ' B ON A.ECL_MODEL_ID = B.ECL_MODEL_ID       
      AND A.SEGMENTATION_ID = B.SEGMENTATION_ID       
      AND A.PD_MODEL_ID = B.PD_MODEL_ID       
      AND A.LGD_MODEL_ID = B.LGD_MODEL_ID      
      JOIN TMP_IFRS_ECL_PD_YEARLY_' || P_RUNID || ' C ON B.PD_MODEL_ID = C.PD_RULE_ID       
      AND A.BUCKET_ID = C.BUCKET_ID      
      AND B.PD_DATE = C.DOWNLOAD_DATE       
      JOIN TMP_IFRS_ECL_LGD_' || P_RUNID || ' D ON B.LGD_MODEL_ID = D.LGD_RULE_ID AND COALESCE(B.LGD_DATE,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE) = D.DOWNLOAD_DATE       
      JOIN TMP_IFRS_ECL_PD_FL_YEARLY_' || P_RUNID || ' E ON B.PD_MODEL_ID = E.PD_RULE_ID AND A.BUCKET_ID = E.BUCKET_ID AND A.SEGMENTATION_ID = E.SEGMENTATION_ID       
      WHERE A.EAD > 0';
      EXECUTE (V_STR_QUERY);

      ----------------------------------------- END INSERT DATA PRK YEARLY -----------------------------------------

      GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
      V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
      V_RETURNROWS := 0;

      ----------------------------------------- START INSERT PRK MONTHLY -----------------------------------------

      V_STR_QUERY := '';
      V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT3 || '      
      (      
        DOWNLOAD_DATE,      
        MASTERID,      
        GROUP_SEGMENT,      
        SEGMENT,      
        SUB_SEGMENT,      
        SEGMENTATION_ID,      
        ACCOUNT_NUMBER,      
        CUSTOMER_NUMBER,      
        LT_SEGMENT,      
        SICR_RULE_ID,      
        SICR_FLAG,      
        LIFETIME,      
        STAGE,      
        REVOLVING_FLAG,      
        PD_SEGMENT,      
        LGD_SEGMENT,      
        EAD_SEGMENT,      
        PREV_ECL_AMOUNT,      
        BUCKET_GROUP,      
        BUCKET_ID,      
        ECL_MODEL_ID,      
        EAD_MODEL_ID,      
        CCF_RULES_ID,      
        PD_MODEL_ID,       
        LGD_MODEL_ID,      
        SEQ,      
        FL_YEAR,      
        FL_MONTH,      
        OUTSTANDING,      
        UNAMORT_COST_AMT,       
        UNAMORT_FEE_AMT,       
        INTEREST_ACCRUED,      
        UNUSED_AMOUNT,       
        FAIR_VALUE_AMOUNT,       
        EAD_BALANCE,      
        PLAFOND,      
        EIR,      
        CCF,       
        EAD,      
        PD_BFL,      
        LGD,      
        DISC_EAD,      
        ECL,      
        ECL_BFL,       
        PD,      
        COLL_AMOUNT      
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
        NULL AS LT_SEGMENT,      
        A.SICR_RULE_ID,      
        NULL AS SICR_FLAG,      
        A.LIFETIME,      
        A.STAGE,      
        A.REVOLVING_FLAG,      
        A.PD_SEGMENT,      
        A.LGD_SEGMENT,      
        A.EAD_SEGMENT,      
        A.PREV_ECL_AMOUNT,      
        A.BUCKET_GROUP,      
        A.BUCKET_ID,      
        A.ECL_MODEL_ID,      
        A.EAD_MODEL_ID,       
        A.CCF_RULES_ID,      
        A.PD_MODEL_ID,      
        A.LGD_MODEL_ID,      
        A.SEQ,      
        A.FL_YEAR,      
        A.FL_MONTH,      
        A.OUTSTANDING,      
        A.UNAMORT_COST_AMT,       
        A.UNAMORT_FEE_AMT,       
        A.INTEREST_ACCRUED,      
        A.UNUSED_AMOUNT,       
        A.FAIR_VALUE_AMOUNT,       
        A.EAD_BALANCE,       
        A.PLAFOND,      
        A.EIR,      
        A.CCF,      
        A.EAD,      
        C.PD AS PD_BFL,      
        D.LGD,      
        CASE WHEN A.STAGE = 3 THEN A.EAD ELSE       
        FUTIL_PV(COALESCE(A.EIR, 0)/100/12, A.SEQ, A.EAD) END AS DISC_EAD,      
        (CASE WHEN A.STAGE = 3 THEN A.EAD ELSE       
        FUTIL_PV(COALESCE(A.EIR, 0)/100/12, A.SEQ, A.EAD) END) * COALESCE(E.PD_FL,C.PD) * COALESCE(D.LGD,1) AS ECL,       
        (CASE WHEN A.STAGE = 3 THEN A.EAD ELSE       
        FUTIL_PV(COALESCE(A.EIR, 0)/100/12, A.SEQ, A.EAD) END) * C.PD * COALESCE(D.LGD,1) AS ECL_BFL,      
        E.PD_FL ,      
        A.COLL_AMOUNT  
      FROM ' || V_TABLEINSERT7 || ' A      
      JOIN TMP_IFRS_ECL_MODEL_ECL_' || P_RUNID || ' B ON A.ECL_MODEL_ID = B.ECL_MODEL_ID AND A.SEGMENTATION_ID = B.SEGMENTATION_ID      
      JOIN TMP_IFRS_ECL_PD_MONTHLY_ECL_' || P_RUNID || ' C ON      
      B.PD_MODEL_ID = C.PD_RULE_ID      
      AND A.BUCKET_ID = C.BUCKET_ID       
      AND A.SEQ = C.FL_SEQ      
      AND B.PD_DATE = C.DOWNLOAD_DATE       
      JOIN TMP_IFRS_ECL_LGD_' || P_RUNID || ' D ON B.LGD_MODEL_ID = D.LGD_RULE_ID AND COALESCE(B.LGD_DATE,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE) = D.DOWNLOAD_DATE       
      JOIN TMP_IFRS_ECL_PD_FL_MONTHLY_ECL_' || P_RUNID || ' E ON B.PD_MODEL_ID = E.PD_RULE_ID AND A.BUCKET_ID = E.BUCKET_ID AND A.SEQ = E.FL_SEQ AND A.SEGMENTATION_ID= E.SEGMENTATION_ID       
      WHERE A.EAD > 0';
      EXECUTE (V_STR_QUERY);

      GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
      V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
      V_RETURNROWS := 0;

      ----------------------------------------- END INSERT PRK MONTHLY -----------------------------------------

      ----------------------------------------- START INSERT IFRS ECL RESULT HEADER -----------------------------------------

      V_STR_QUERY := '';
      V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT4 || '        
      (        
        DOWNLOAD_DATE,         
        MASTERID,         
        ECL_AMOUNT,         
        ECL_AMOUNT_BFL        
        )  
        SELECT       
        DOWNLOAD_DATE,       
        MASTERID,       
        SUM(ECL) AS ECL_AMOUNT,       
        SUM(ECL_BFL) AS ECL_AMOUNT_BFL      
        FROM ' || V_TABLEINSERT3 || '      
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE      
        GROUP BY DOWNLOAD_DATE, MASTERID';
      EXECUTE (V_STR_QUERY);

      GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
      V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
      V_RETURNROWS := 0;

      ----------------------------------------- END INSERT IFRS ECL RESULT HEADER -----------------------------------------

      RAISE NOTICE 'SP_IFRS_ECL_RESULT_DETAIL | AFFECTED RECORD : %', V_RETURNROWS2;
      -------- ====== BODY ======

      -------- ====== LOG ======
      V_TABLEDEST = V_TABLEINSERT3;
      V_COLUMNDEST = '-';
      V_SPNAME = 'SP_IFRS_ECL_RESULT_DETAIL';
      V_OPERATION = 'INSERT';
      
      CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
      -------- ====== LOG ======

      -------- ====== RESULT ======
      V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT3 || '';
      CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
      -------- ====== RESULT ======

END;

$$;