---- DROP PROCEDURE SP_IFRS_IMP_EAD_TERM_YEARLY;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMP_EAD_TERM_YEARLY(
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
    V_TABLEMSTPARAM VARCHAR(100);

    ---- RESULT
    V_QUERYS TEXT;
    V_CODITION2 TEXT;

    ----
    V_LOG_SEQ INTEGER;
    V_DIFF_LOG_SEQ INTEGER;
    V_SP_NAME VARCHAR(100);
    V_PRC_NAME VARCHAR(100);
    V_SEQ INTEGER;
    V_SP_NAME_PREV VARCHAR(100);
    STACK TEXT; 
    FCESIG TEXT;

    ----
    V_MAX_LIFETIME INT;
    V_START INT;
    V_END INT;
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
        V_TABLEINSERT3 := 'IFRS_EAD_TERM_YEARLY_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_EAD_TERM_YEARLY_CC_' || P_RUNID || '';
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG_' || P_RUNID || '';
        V_TABLECCFCONFIG  := 'IFRS_CCF_RULES_CONFIG_' || P_RUNID || '';
        V_TABLELGDCONFIG := 'IFRS_LGD_RULES_CONFIG_' || P_RUNID || '';
        V_TABLEEADCONFIG := 'IFRS_EAD_RULES_CONFIG_' || P_RUNID || '';
        V_TABLEMSTPARAM  := 'IFRS_MASTER_PRODUCT_PARAM_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLENAME_MON := 'IFRS_MASTER_ACCOUNT_MONTHLY';
        V_TABLEINSERT1 := 'TMP_IFRS_ECL_IMA';
        V_TABLEINSERT2 := 'IFRS_IMA_IMP_CURR';
        V_TABLEINSERT3 := 'IFRS_EAD_TERM_YEARLY'; ----> CHANGE FROM IFRS_EAD_RESULT_NONPRK
        V_TABLEINSERT4 := 'IFRS_EAD_TERM_YEARLY_CC';
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG';
        V_TABLECCFCONFIG  := 'IFRS_CCF_RULES_CONFIG';
        V_TABLELGDCONFIG := 'IFRS_LGD_RULES_CONFIG';
        V_TABLEEADCONFIG := 'IFRS_EAD_RULES_CONFIG';
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
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEMSTPARAM || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEMSTPARAM || ' AS SELECT * FROM IFRS_MASTER_PRODUCT_PARAM';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT4 || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT4 || ' AS SELECT * FROM IFRS_EAD_TERM_YEARLY_CC';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT4 || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT4 || ' AS SELECT * FROM IFRS_EAD_TERM_YEARLY_CC';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT3 || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT3 || ' AS SELECT * FROM IFRS_EAD_TERM_YEARLY';
        EXECUTE (V_STR_QUERY);
    END IF;

    EXECUTE 'SELECT MAX(LIFETIME - 1) AS LIFETIME FROM ' || V_TABLEINSERT1 || '' INTO V_MAX_LIFETIME;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TMP_LISTDATE_' || P_RUNID || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE TMP_LISTDATE_' || P_RUNID || ' AS WITH RECURSIVE CTE_DATE AS (
    SELECT DATE_TRUNC(''MONTH'',(''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE + INTERVAL ''1 MONTH'')) + INTERVAL ''1 MONTH - 1 DAY'' AS START_DATE,DATE_TRUNC(''MONTH'',(''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE + ' || V_MAX_LIFETIME || ' * INTERVAL ''1 MONTH'')) + INTERVAL ''1 MONTH - 1 DAY'' AS MAX_DATE 
    UNION ALL
    SELECT DATE_TRUNC(''MONTH'',(START_DATE + INTERVAL ''1 MONTH'')) + INTERVAL ''1 MONTH - 1 DAY'', MAX_DATE 
    FROM CTE_DATE
    WHERE DATE_TRUNC(''MONTH'', (START_DATE + INTERVAL ''1 MONTH'')) + INTERVAL ''1 MONTH - 1 DAY'' <= MAX_DATE
    ) SELECT START_DATE FROM CTE_DATE';
    -- EXECUTE (V_STR_QUERY);
    RAISE NOTICE 'SP_IFRS_IMP_EAD_TERM_YEARLY | QUERY CREATE TMP_LISTDATE : %', V_STR_QUERY;

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TMP_MAXDATE_' || P_RUNID || '';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE TMP_MAXDATE_' || P_RUNID || ' AS
    -- SELECT MASTERID, MAX(DATE_TRUNC(''MONTH'',PMTDATE) + INTERVAL ''1 MONTH - 1 DAY'') AS MAXDATE
    -- FROM IFRS_PAYM_SCHD_ALL A
    -- WHERE DATE_TRUNC(''MONTH'', PMTDATE) + INTERVAL ''1 MONTH - 1 DAY'' > DATE_TRUNC(''MONTH'', ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE) + INTERVAL ''1 MONTH - 1 DAY'' 
    -- AND (END_DATE IS NULL OR END_DATE > ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE)
    -- AND DOWNLOAD_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
    -- GROUP BY MASTERID';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TMP_IFRS_ECL_MODEL_' || P_RUNID || '';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE TMP_IFRS_ECL_MODEL_' || P_RUNID || ' AS
    -- SELECT DISTINCT B.EAD_BALANCE, EAD_MODEL_ID
    -- FROM ' || V_TABLEINSERT1 || ' A
    -- JOIN ' || V_TABLEEADCONFIG || ' B
    -- ON A.EAD_MODEL_ID = B.PKID
    -- WHERE B.EAD_BALANCE LIKE ''%INTEREST_ACCRUED%''';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'UPDATE TMP_IFRS_ECL_MODEL_' || P_RUNID || ' SET EAD_BALANCE = REPLACE(EAD_BALANCE,''UNUSED_AMOUNT'',''A.UNUSED_AMOUNT'')';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'UPDATE TMP_IFRS_ECL_MODEL_' || P_RUNID || ' SET EAD_BALANCE = REPLACE(EAD_BALANCE,''OUTSTANDING'',''A.OUTSTANDING'')';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'UPDATE TMP_IFRS_ECL_MODEL_' || P_RUNID || ' SET EAD_BALANCE = REPLACE(EAD_BALANCE,''INTEREST_ACCRUED'',''A.INTEREST_ACCRUED'')';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'UPDATE TMP_IFRS_ECL_MODEL_' || P_RUNID || ' SET EAD_BALANCE = REPLACE(EAD_BALANCE,''COLL_AMOUNT'',''A.COLL_AMOUNT'')';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'UPDATE TMP_IFRS_ECL_MODEL_' || P_RUNID || ' SET EAD_BALANCE = REPLACE(EAD_BALANCE,''CCF'',''A.CCF'')';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'UPDATE TMP_IFRS_ECL_MODEL_' || P_RUNID || ' SET EAD_BALANCE = REPLACE(EAD_BALANCE, ''+'',''+'')';
    -- EXECUTE (V_STR_QUERY);


    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS SCHD_' || P_RUNID || '';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE SCHD_' || P_RUNID || ' AS
    -- SELECT MAX(DOWNLOAD_DATE) AS DOWNLOAD_DATE, MASTERID
    -- FROM IFRS_PAYM_SCHD_ALL
    -- WHERE DOWNLOAD_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
    -- GROUP BY MASTERID';
    -- EXECUTE (V_STR_QUERY);
    
    
    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS IFRS_EAD_PAYM_NONPRK_' || P_RUNID || '';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE IFRS_EAD_PAYM_NONPRK_' || P_RUNID || ' AS WITH CTE_PAYM_SCHD ( DOWNLOAD_DATE, MASTERID, PMTDATE, OSPRN, PRINCIPAL, END_DATE, RN ) AS (
    -- SELECT
    -- DATE_TRUNC(''MONTH'', A.DOWNLOAD_DATE) + INTERVAL ''1 MONTH - 1 DAY'' AS DOWNLOAD_DATE,
    -- A.MASTERID,
    -- DATE_TRUNC(''MONTH'', A.PMTDATE) + INTERVAL ''1 MONTH - 1 DAY'' AS PMTDATE,
    -- MIN(A.OSPRN) AS OSPRN,
    -- SUM(A.PRINCIPAL) AS PRINCIPAL,
    -- DATE_TRUNC(''MONTH'', A.END_DATE) + INTERVAL ''1 MONTH - 1 DAY'' AS END_DATE,
    -- ROW_NUMBER() OVER (PARTITION BY A.MASTERID,DATE_TRUNC(''MONTH'', A.PMTDATE) + INTERVAL ''1 MONTH - 1 DAY'' 
    -- ORDER BY A.DOWNLOAD_DATE) RN
    -- FROM IFRS_PAYM_SCHD_ALL A
    -- JOIN SCHD_' || P_RUNID || ' B
    -- ON A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
    -- AND A.MASTERID = B.MASTERID
    -- WHERE (DATE_TRUNC(''MONTH'', A.DOWNLOAD_DATE) + INTERVAL ''1 MONTH - 1 DAY'' <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
    -- OR ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE <= PMTDATE)
    -- AND (A.END_DATE IS NULL
    -- OR DATE_TRUNC(''MONTH'', A.END_DATE) + INTERVAL ''1 MONTH - 1 DAY'' <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE)
    -- AND DATE_TRUNC(''MONTH'', A.PMTDATE) + INTERVAL ''1 MONTH - 1 DAY'' >= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
    -- GROUP BY A.DOWNLOAD_DATE, A.MASTERID,
    -- DATE_TRUNC(''MONTH'', A.PMTDATE) + INTERVAL ''1 MONTH - 1 DAY'',
    -- DATE_TRUNC(''MONTH'', A.END_DATE) + INTERVAL ''1 MONTH - 1 DAY''
    -- )
    -- SELECT
    -- DOWNLOAD_DATE,
    -- MASTERID,
    -- PMTDATE,
    -- OSPRN,
    -- PRINCIPAL,
    -- SEQ
    -- FROM
    -- (
    -- SELECT
    -- DATE_TRUNC(''MONTH'', DOWNLOAD_DATE) + INTERVAL ''1 MONTH - 1 DAY'' AS DOWNLOAD_DATE,
    -- B.MASTERID,
    -- C.START_DATE AS PMTDATE,
    -- COALESCE(D.TOTAL_PRIN,0) - SUM(COALESCE(PRINCIPAL,0)) OVER (PARTITION BY B.MASTERID ORDER BY C.START_DATE) AS OSPRN,
    -- SUM(PRINCIPAL) OVER (PARTITION BY B.MASTERID ORDER BY C.START_DATE) AS PRINCIPAL,
    -- ROW_NUMBER() OVER (PARTITION BY B.MASTERID ORDER BY C.START_DATE) AS SEQ
    -- FROM TMP_MAXDATE_' || P_RUNID || ' B
    -- JOIN TMP_LISTDATE_' || P_RUNID || ' C
    -- ON B.MAXDATE >= C.START_DATE
    -- LEFT JOIN CTE_PAYM_SCHD A
    -- ON A.MASTERID = B.MASTERID
    -- AND A.PMTDATE = C.START_DATE
    -- AND RN = 1
    -- LEFT JOIN (
    -- SELECT MASTERID, SUM(COALESCE(PRINCIPAL,0)) AS TOTAL_PRIN
    -- FROM CTE_PAYM_SCHD
    -- GROUP BY MASTERID
    -- ) D ON B.MASTERID = D.MASTERID) PAYM_SCHD';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TMP_CTE_EAD_' || P_RUNID || '';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE TMP_CTE_EAD_' || P_RUNID || ' AS WITH CTE_EAD AS (
    -- SELECT
    --     DOWNLOAD_DATE,
    --     MASTERID,
    --     GROUP_SEGMENT,
    --     SEGMENT,
    --     SUB_SEGMENT,
    --     SEGMENTATION_ID,
    --     ACCOUNT_NUMBER,
    --     CUSTOMER_NUMBER,
    --     SICR_RULE_ID,
    --     BUCKET_GROUP,
    --     BUCKET_ID,
    --     LIFETIME,
    --     (STAGE)::SMALLINT,
    --     REVOLVING_FLAG,
    --     PD_SEGMENT,
    --     LGD_SEGMENT,
    --     EAD_SEGMENT,
    --     PREV_ECL_AMOUNT,
    --     ECL_MODEL_ID,
    --     EAD_MODEL_ID,
    --     CCF_RULES_ID,
    --     LGD_MODEL_ID,
    --     PD_MODEL_ID,
    --     0 AS SEQ,
    --     1 AS FL_YEAR,
    --     1 AS FL_MONTH,
    --     EIR,
    --     OUTSTANDING,
    --     UNAMORT_COST_AMT,
    --     UNAMORT_FEE_AMT,
    --     INTEREST_ACCRUED,
    --     UNUSED_AMOUNT,
    --     FAIR_VALUE_AMOUNT,
    --     EAD_BALANCE,
    --     PLAFOND,
    --     EAD_BALANCE AS EAD,
    --     BI_COLLECTABILITY,
    --     COLL_AMOUNT,
    --     SEGMENT_FLAG
    -- FROM ' || V_TABLEINSERT1 || '
    -- WHERE ((DATA_SOURCE=''LOAN'' AND PRODUCT_TYPE_1 <> ''PRK'' AND DATA_SOURCE <> ''LIMIT'') OR ( DATA_SOURCE=''LOAN_T24'' AND REVOLVING_FLAG=''0'')) AND IMPAIRED_FLAG = ''C''
    -- UNION ALL
    -- SELECT
    --     A.DOWNLOAD_DATE,
    --     A.MASTERID,
    --     A.GROUP_SEGMENT,
    --     A.SEGMENT,
    --     A.SUB_SEGMENT,
    --     A.SEGMENTATION_ID,
    --     A.ACCOUNT_NUMBER,
    --     A.CUSTOMER_NUMBER,
    --     A.SICR_RULE_ID,
    --     A.BUCKET_GROUP,
    --     A.BUCKET_ID,
    --     A.LIFETIME,
    --     (A.STAGE)::SMALLINT,
    --     A.REVOLVING_FLAG,
    --     A.PD_SEGMENT,
    --     A.LGD_SEGMENT,
    --     A.EAD_SEGMENT,
    --     A.PREV_ECL_AMOUNT,
    --     A.ECL_MODEL_ID,
    --     A.EAD_MODEL_ID,
    --     A.CCF_RULES_ID,
    --     A.LGD_MODEL_ID,
    --     A.PD_MODEL_ID,
    --     B.SEQ,
    --     (CAST(B.SEQ AS INT) / 12) + 1 AS FL_YEAR,
    --     ((B.SEQ) % 12) + 1 AS FL_MONTH,
    --     A.EIR,
    --     CASE WHEN COALESCE(A.OUTSTANDING, 0) - COALESCE(B.PRINCIPAL, 0) < 0 THEN 0 ELSE COALESCE(A.OUTSTANDING, 0) - COALESCE(B.PRINCIPAL, 0) END AS OUTSTANDING,
    --     A.UNAMORT_COST_AMT,
    --     A.UNAMORT_FEE_AMT,
    --     A.INTEREST_ACCRUED,
    --     A.UNUSED_AMOUNT,
    --     A.FAIR_VALUE_AMOUNT,
    --     A.EAD_BALANCE,
    --     A.PLAFOND,
    --     CAST((A.EAD_BALANCE -
    --         CASE WHEN A.BI_COLLECTABILITY IN (1, 2) AND A.EAD_MODEL_ID IN (
    --         SELECT
    --             EAD_MODEL_ID
    --         FROM
    --             TMP_IFRS_ECL_MODEL_' || P_RUNID || ') 
    --         THEN
    --             COALESCE(A.INTEREST_ACCRUED, 0) --- CASE WHEN B.SEQ - 1 >= 1 THEN A.INTEREST_ACCRUED ELSE 0 END
    --         ELSE 0
    --         END - COALESCE(B.PRINCIPAL, 0)) AS DECIMAL(32,6)) AS EAD,
    --     A.BI_COLLECTABILITY,
    --     A.COLL_AMOUNT,
    --     A.SEGMENT_FLAG
    --     FROM ' || V_TABLEINSERT1 || ' A
    --     JOIN IFRS_EAD_PAYM_NONPRK_' || P_RUNID || ' B ON A.MASTERID = B.MASTERID
    --     WHERE A.LIFETIME > B.SEQ
    --     AND ((A.STAGE = ''1''
    --     AND B.SEQ < 12)
    --     OR (A.STAGE IN (''2'', ''3'')))
    --     AND IMPAIRED_FLAG = ''C''
    --     AND ((A.DATA_SOURCE=''LOAN''
    --     AND A.PRODUCT_TYPE_1 <> ''PRK''
    --     AND A.DATA_SOURCE <> ''LIMIT'')
    --     OR (A.DATA_SOURCE=''LOAN_T24''
    --     AND A.REVOLVING_FLAG= ''0'' ))
    -- )
    -- SELECT
    --     DOWNLOAD_DATE,
    --     MASTERID,
    --     GROUP_SEGMENT,
    --     SEGMENT,
    --     SUB_SEGMENT,
    --     SEGMENTATION_ID,
    --     ACCOUNT_NUMBER,
    --     CUSTOMER_NUMBER,
    --     SICR_RULE_ID,
    --     BUCKET_GROUP,
    --     BUCKET_ID,
    --     LIFETIME,
    --     (STAGE)::SMALLINT,
    --     REVOLVING_FLAG,
    --     PD_SEGMENT,
    --     LGD_SEGMENT,
    --     EAD_SEGMENT,
    --     PREV_ECL_AMOUNT,
    --     ECL_MODEL_ID,
    --     EAD_MODEL_ID,
    --     CCF_RULES_ID,
    --     LGD_MODEL_ID,
    --     PD_MODEL_ID,
    --     SEQ,
    --     FL_YEAR,
    --     FL_MONTH,
    --     EIR,
    --     OUTSTANDING,
    --     UNAMORT_COST_AMT,
    --     UNAMORT_FEE_AMT,
    --     INTEREST_ACCRUED,
    --     UNUSED_AMOUNT,
    --     FAIR_VALUE_AMOUNT,
    --     EAD_BALANCE,
    --     PLAFOND,
    --     EAD,
    --     BI_COLLECTABILITY,
    --     COLL_AMOUNT,
    --     SEGMENT_FLAG
    -- FROM CTE_EAD
    -- ORDER BY MASTERID, SEQ';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'UPDATE TMP_CTE_EAD_' || P_RUNID || '
    -- SET SEQ = SEQ + 1
    -- WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ZEROING_' || P_RUNID || '';
    -- EXECUTE (V_STR_QUERY);

    -- ----==== UPDATING & DELETING LESS THAN ZERO
    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ZEROING_' || P_RUNID || ' AS
    -- SELECT MASTERID, MIN(SEQ) SEQ, COUNT(1) COUNT 
    -- FROM TMP_CTE_EAD_' || P_RUNID || '
    -- WHERE EAD <= 0 AND DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
    -- GROUP BY MASTERID
    -- HAVING COUNT(1) > 1
    -- ORDER BY MASTERID';
    -- EXECUTE (V_STR_QUERY);
    -- ----==== UPDATING & DELETING LESS THAN ZERO

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS UPDATE_TMP_CTE_EAD_' || P_RUNID || '';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE UPDATE_TMP_CTE_EAD_' || P_RUNID || ' AS
    -- SELECT A.MASTERID, A.SEQ FROM TMP_CTE_EAD_' || P_RUNID || ' A
    -- JOIN ZEROING_' || P_RUNID || ' B USING (MASTERID,SEQ) 
    -- WHERE A.EAD <= 0';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'UPDATE TMP_CTE_EAD_' || P_RUNID || ' A
    -- SET EAD = 0 
    -- FROM UPDATE_TMP_CTE_EAD_' || P_RUNID || ' B 
    -- WHERE A.MASTERID = B.MASTERID AND A.SEQ = B.SEQ AND A.EAD <= 0';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'DELETE FROM TMP_CTE_EAD_' || P_RUNID || ' A
    -- USING ZEROING_' || P_RUNID || ' B 
    -- WHERE A.MASTERID = B.MASTERID
    -- AND A.SEQ > B.SEQ
    -- AND A.EAD <= 0';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT3 || '';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT3 || '
    -- (                          
    -- DOWNLOAD_DATE,                       
    -- MASTERID,                       
    -- GROUP_SEGMENT,                       
    -- SEGMENT,                       
    -- SUB_SEGMENT,                       
    -- SEGMENTATION_ID,                       
    -- ACCOUNT_NUMBER,                       
    -- CUSTOMER_NUMBER,                          
    -- SICR_RULE_ID,                       
    -- BUCKET_GROUP,                       
    -- BUCKET_ID,                       
    -- LIFETIME,                       
    -- STAGE,                       
    -- REVOLVING_FLAG,                         
    -- PD_SEGMENT,                         
    -- LGD_SEGMENT,                         
    -- EAD_SEGMENT,                         
    -- PREV_ECL_AMOUNT,                       
    -- ECL_MODEL_ID,                   
    -- EAD_MODEL_ID,                     
    -- CCF_RULES_ID,                         
    -- LGD_MODEL_ID,                         
    -- PD_MODEL_ID,                          
    -- SEQ,                       
    -- FL_YEAR,                       
    -- FL_MONTH,                       
    -- EIR,                       
    -- OUTSTANDING,                       
    -- UNAMORT_COST_AMT,                       
    -- UNAMORT_FEE_AMT,                       
    -- INTEREST_ACCRUED,                        
    -- UNUSED_AMOUNT,                       
    -- FAIR_VALUE_AMOUNT,                       
    -- EAD_BALANCE,                       
    -- PLAFOND,                       
    -- EAD,                       
    -- BI_COLLECTABILITY,          
    -- COLL_AMOUNT,  
    -- SEGMENT_FLAG                         
    -- ) 
    -- SELECT
    -- DOWNLOAD_DATE,
    -- MASTERID,
    -- GROUP_SEGMENT,
    -- SEGMENT,
    -- SUB_SEGMENT,
    -- SEGMENTATION_ID,
    -- ACCOUNT_NUMBER,
    -- CUSTOMER_NUMBER,
    -- SICR_RULE_ID,
    -- BUCKET_GROUP,
    -- BUCKET_ID,
    -- LIFETIME,
    -- (STAGE)::SMALLINT,
    -- REVOLVING_FLAG,
    -- PD_SEGMENT,
    -- LGD_SEGMENT,
    -- EAD_SEGMENT,
    -- PREV_ECL_AMOUNT,
    -- ECL_MODEL_ID,
    -- EAD_MODEL_ID,
    -- CCF_RULES_ID,
    -- LGD_MODEL_ID,
    -- PD_MODEL_ID,
    -- SEQ,
    -- FL_YEAR,
    -- FL_MONTH,
    -- EIR,
    -- OUTSTANDING,
    -- UNAMORT_COST_AMT,
    -- UNAMORT_FEE_AMT,
    -- INTEREST_ACCRUED,
    -- UNUSED_AMOUNT,
    -- FAIR_VALUE_AMOUNT,
    -- EAD_BALANCE,
    -- PLAFOND,
    -- CASE
    -- WHEN EAD < 0 THEN
    --     0
    -- ELSE
    --     EAD
    -- END AS EAD,
    -- BI_COLLECTABILITY,
    -- COLL_AMOUNT,
    -- SEGMENT_FLAG
    -- FROM TMP_CTE_EAD_' || P_RUNID || '
    -- ORDER BY DOWNLOAD_DATE, MASTERID';
    -- EXECUTE (V_STR_QUERY);

    -- GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    -- V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    -- V_RETURNROWS := 0;

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS MINUS_LIFETIME_' || P_RUNID || '';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE MINUS_LIFETIME_' || P_RUNID || ' AS
    -- SELECT MASTERID
    -- FROM ' || V_TABLEINSERT3 || '
    -- WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AND LIFETIME < 0';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TMP_IFRS_EAD_TERM_YEARLY_' || P_RUNID || '';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE TMP_IFRS_EAD_TERM_YEARLY_' || P_RUNID || ' AS
    -- SELECT * FROM ' || V_TABLEINSERT3 || ' WHERE 1 = 2';
    -- EXECUTE (V_STR_QUERY);

    -- IF((SELECT COUNT(1) FROM MINUS_LIFETIME) > 0) THEN
    --     V_START := 2;
    --     V_END := 12;

    --     WHILE V_START <= V_END LOOP
    --         EXECUTE 'INSERT INTO TMP_IFRS_EAD_TERM_YEARLY_' || P_RUNID || ' (
    --             DOWNLOAD_DATE,
    --             MASTERID,
    --             GROUP_SEGMENT,
    --             SEGMENT,
    --             SUB_SEGMENT,
    --             SEGMENTATION_ID,
    --             ACCOUNT_NUMBER,
    --             CUSTOMER_NUMBER,
    --             SICR_RULE_ID,
    --             BUCKET_GROUP,
    --             BUCKET_ID,
    --             LIFETIME,
    --             STAGE,
    --             REVOLVING_FLAG,
    --             PD_SEGMENT,
    --             LGD_SEGMENT,
    --             EAD_SEGMENT,
    --             PREV_ECL_AMOUNT,
    --             ECL_MODEL_ID,
    --             EAD_MODEL_ID,
    --             CCF_RULES_ID,
    --             LGD_MODEL_ID,
    --             PD_MODEL_ID,
    --             SEQ,
    --             FL_YEAR,
    --             FL_MONTH,
    --             EIR,
    --             OUTSTANDING,
    --             UNAMORT_COST_AMT,
    --             UNAMORT_FEE_AMT,
    --             INTEREST_ACCRUED,
    --             UNUSED_AMOUNT,
    --             FAIR_VALUE_AMOUNT,
    --             EAD_BALANCE,
    --             PLAFOND,
    --             EAD,
    --             CCF,
    --             BI_COLLECTABILITY,
    --             COLL_AMOUNT,
    --             SEGMENT_FLAG
    --         )
    --             SELECT
    --                 DOWNLOAD_DATE,
    --                 MASTERID,
    --                 GROUP_SEGMENT,
    --                 SEGMENT,
    --                 SUB_SEGMENT,
    --                 SEGMENTATION_ID,
    --                 ACCOUNT_NUMBER,
    --                 CUSTOMER_NUMBER,
    --                 SICR_RULE_ID,
    --                 BUCKET_GROUP,
    --                 BUCKET_ID,
    --                 LIFETIME,
    --                 (STAGE)::SMALLINT,
    --                 REVOLVING_FLAG,
    --                 PD_SEGMENT,
    --                 LGD_SEGMENT,
    --                 EAD_SEGMENT,
    --                 PREV_ECL_AMOUNT,
    --                 ECL_MODEL_ID,
    --                 EAD_MODEL_ID,
    --                 CCF_RULES_ID,
    --                 LGD_MODEL_ID,
    --                 PD_MODEL_ID,
    --                 ' || V_START || ' AS SEQ,
    --                 FL_YEAR,
    --                 ' || V_START || ' AS FL_MONTH,
    --                 EIR,
    --                 OUTSTANDING,
    --                 UNAMORT_COST_AMT,
    --                 UNAMORT_FEE_AMT,
    --                 INTEREST_ACCRUED,
    --                 UNUSED_AMOUNT,
    --                 FAIR_VALUE_AMOUNT,
    --                 EAD_BALANCE,
    --                 PLAFOND,
    --                 EAD,
    --                 CCF,
    --                 BI_COLLECTABILITY,
    --                 COLL_AMOUNT,
    --                 SEGMENT_FLAG
    --             FROM ' || V_TABLEINSERT3 || '
    --             WHERE
    --                 MASTERID IN (
    --                     SELECT
    --                         MASTERID
    --                     FROM
    --                         MINUS_LIFETIME
    --                 )';
    --         V_START := V_START + 1;

    --         GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    --         V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    --         V_RETURNROWS := 0;

    --     END LOOP;

    --     V_STR_QUERY := '';
    --     V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT3 || ' (
    --         DOWNLOAD_DATE,
    --         MASTERID,
    --         GROUP_SEGMENT,
    --         SEGMENT,
    --         SUB_SEGMENT,
    --         SEGMENTATION_ID,
    --         ACCOUNT_NUMBER,
    --         CUSTOMER_NUMBER,
    --         SICR_RULE_ID,
    --         BUCKET_GROUP,
    --         BUCKET_ID,
    --         LIFETIME,
    --         STAGE,
    --         REVOLVING_FLAG,
    --         PD_SEGMENT,
    --         LGD_SEGMENT,
    --         EAD_SEGMENT,
    --         PREV_ECL_AMOUNT,
    --         ECL_MODEL_ID,
    --         EAD_MODEL_ID,
    --         CCF_RULES_ID,
    --         LGD_MODEL_ID,
    --         PD_MODEL_ID,
    --         SEQ,
    --         FL_YEAR,
    --         FL_MONTH,
    --         EIR,
    --         OUTSTANDING,
    --         UNAMORT_COST_AMT,
    --         UNAMORT_FEE_AMT,
    --         INTEREST_ACCRUED,
    --         UNUSED_AMOUNT,
    --         FAIR_VALUE_AMOUNT,
    --         EAD_BALANCE,
    --         PLAFOND,
    --         EAD,
    --         CCF,
    --         BI_COLLECTABILITY,
    --         COLL_AMOUNT,
    --         SEGMENT_FLAG
    --     )
    --     SELECT
    --         DOWNLOAD_DATE,
    --         MASTERID,
    --         GROUP_SEGMENT,
    --         SEGMENT,
    --         SUB_SEGMENT,
    --         SEGMENTATION_ID,
    --         ACCOUNT_NUMBER,
    --         CUSTOMER_NUMBER,
    --         SICR_RULE_ID,
    --         BUCKET_GROUP,
    --         BUCKET_ID,
    --         LIFETIME,
    --         (STAGE)::SMALLINT,
    --         REVOLVING_FLAG,
    --         PD_SEGMENT,
    --         LGD_SEGMENT,
    --         EAD_SEGMENT,
    --         PREV_ECL_AMOUNT,
    --         ECL_MODEL_ID,
    --         EAD_MODEL_ID,
    --         CCF_RULES_ID,
    --         LGD_MODEL_ID,
    --         PD_MODEL_ID,
    --         SEQ,
    --         FL_YEAR,
    --         FL_MONTH,
    --         EIR,
    --         OUTSTANDING,
    --         UNAMORT_COST_AMT,
    --         UNAMORT_FEE_AMT,
    --         INTEREST_ACCRUED,
    --         UNUSED_AMOUNT,
    --         FAIR_VALUE_AMOUNT,
    --         EAD_BALANCE,
    --         PLAFOND,
    --         EAD,
    --         CCF,
    --         BI_COLLECTABILITY,
    --         COLL_AMOUNT,
    --         SEGMENT_FLAG
    --     FROM TMP_IFRS_EAD_TERM_YEARLY_' || P_RUNID || '';
    --     EXECUTE (V_STR_QUERY);

    --     GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    --     V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    --     V_RETURNROWS := 0;

    -- END IF;

    -- /* ENCHANGMENT CREDIT CARD 2021-03-31 - UPDATE LIFETIME - STAGE 1,2,3 */ 
    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS UPDATE_DATA_TMP_IFRS_ECL_IMA_' || P_RUNID || '';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE UPDATE_DATA_TMP_IFRS_ECL_IMA_' || P_RUNID || ' AS
    -- SELECT DISTINCT B.VALUE2, A.MASTERID, A.DOWNLOAD_DATE, A.PRODUCT_CODE, B.COMMONCODE, A.DATA_SOURCE
    -- FROM ' || V_TABLEINSERT1 || ' A
    -- INNER JOIN TBLM_COMMONCODEDETAIL AS B ON A.STAGE = B.VALUE1
    -- INNER JOIN (SELECT PRD_CODE FROM ' || V_TABLEMSTPARAM || ' WHERE PRD_TYPE = ''CREDITCARD'') C
    -- ON A.PRODUCT_CODE = C.PRD_CODE
    -- WHERE B.COMMONCODE = ''CC_PERIOD''          
    -- AND A.DATA_SOURCE <> ''LIMIT''';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || ' DROP INDEX IF EXISTS NCI_UPDATE_DATA_TMP_IFRS_ECL_IMA_' || P_RUNID || '';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'CREATE INDEX IF NOT EXISTS NCI_UPDATE_DATA_TMP_IFRS_ECL_IMA_' || P_RUNID || '
    -- ON UPDATE_DATA_TMP_IFRS_ECL_IMA_' || P_RUNID || ' USING BTREE
    -- (DOWNLOAD_DATE ASC NULLS LAST, MASTERID ASC NULLS LAST)
    -- TABLESPACE PG_DEFAULT';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A
    -- SET  LIFETIME = (B.VALUE2)::BIGINT
    -- FROM UPDATE_DATA_TMP_IFRS_ECL_IMA_' || P_RUNID || ' B 
    -- WHERE A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
    -- AND A.MASTERID = B.MASTERID';
    -- EXECUTE (V_STR_QUERY);


    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT4 || '';
    -- EXECUTE (V_STR_QUERY);

    -- V_STR_QUERY := '';
    -- V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT4 || '                
    -- (                
    -- DOWNLOAD_DATE,                
    -- MASTERID,                
    -- GROUP_SEGMENT,                
    -- SEGMENT,                
    -- SUB_SEGMENT,                
    -- SEGMENTATION_ID,                
    -- ACCOUNT_NUMBER,                
    -- CUSTOMER_NUMBER,                 
    -- SICR_RULE_ID,                
    -- BUCKET_GROUP,                
    -- BUCKET_ID,                
    -- LIFETIME,                
    -- STAGE,                
    -- REVOLVING_FLAG,                
    -- PD_SEGMENT,                
    -- LGD_SEGMENT,                
    -- EAD_SEGMENT,                
    -- PREV_ECL_AMOUNT,                
    -- ECL_MODEL_ID,                
    -- EAD_MODEL_ID,                
    -- CCF_RULES_ID,                 
    -- LGD_MODEL_ID,                 
    -- PD_MODEL_ID,                
    -- SEQ,                
    -- FL_YEAR,                
    -- FL_MONTH,                
    -- EIR,                
    -- OUTSTANDING,                
    -- UNAMORT_COST_AMT,                
    -- UNAMORT_FEE_AMT,                
    -- INTEREST_ACCRUED,                
    -- UNUSED_AMOUNT,                
    -- FAIR_VALUE_AMOUNT,                
    -- EAD_BALANCE,                
    -- PLAFOND,                
    -- EAD,                
    -- CCF,                
    -- BI_COLLECTABILITY,                  
    -- COLL_AMOUNT,      
    -- SEGMENT_FLAG                
    -- )                
    -- SELECT               
    -- A.DOWNLOAD_DATE,              
    -- A.MASTERID,              
    -- A.GROUP_SEGMENT,              
    -- A.SEGMENT,              
    -- A.SUB_SEGMENT,              
    -- A.SEGMENTATION_ID,              
    -- A.ACCOUNT_NUMBER,              
    -- A.CUSTOMER_NUMBER,               
    -- A.SICR_RULE_ID,              
    -- A.BUCKET_GROUP,              
    -- A.BUCKET_ID,              
    -- A.LIFETIME,              
    -- (A.STAGE)::SMALLINT,              
    -- A.REVOLVING_FLAG,              
    -- A.PD_SEGMENT,              
    -- A.LGD_SEGMENT,              
    -- A.EAD_SEGMENT,              
    -- A.PREV_ECL_AMOUNT,              
    -- A.ECL_MODEL_ID,              
    -- A.EAD_MODEL_ID,              
    -- A.CCF_RULES_ID,               
    -- A.LGD_MODEL_ID,              
    -- A.PD_MODEL_ID,              
    -- 1 AS SEQ,              
    -- 1 AS FL_YEAR,              
    -- 0 AS FL_MONTH,              
    -- A.EIR,              
    -- A.OUTSTANDING,              
    -- A.UNAMORT_COST_AMT,              
    -- A.UNAMORT_FEE_AMT,              
    -- A.INTEREST_ACCRUED,              
    -- A.UNUSED_AMOUNT,              
    -- A.FAIR_VALUE_AMOUNT,              
    -- A.EAD_BALANCE,              
    -- A.PLAFOND,              
    -- CASE WHEN EAD_BALANCE < 0 THEN 0 ELSE EAD_BALANCE END AS EAD,              
    -- CASE D.AVERAGE_METHOD WHEN ''WEIGHTED'' THEN C.WEIGHTED_AVG_CCF WHEN ''SIMPLE'' THEN C.SIMPLE_AVG_CCF END AS CCF,                
    -- BI_COLLECTABILITY,                
    -- A.COLL_AMOUNT,    
    -- A.SEGMENT_FLAG               
    -- FROM ' || V_TABLEINSERT1 || '  A              
    -- JOIN IFRS_ECL_MODEL_DETAIL_EAD B              
    -- ON A.CCF_RULES_ID = B.CCF_MODEL_ID AND A.ECL_MODEL_ID = B.ECL_MODEL_ID AND A.SEGMENTATION_ID = B.SEGMENTATION_ID              
    -- LEFT JOIN IFRS_EAD_CCF_HEADER C ON (CASE B.CCF_EFF_DATE_OPTION WHEN ''SELECT_DATE'' THEN B.CCF_EFF_DATE WHEN ''LAST_MONTH'' THEN A.DOWNLOAD_DATE - 1 END = C.DOWNLOAD_DATE) AND A.CCF_RULES_ID = C.CCF_RULE_ID                
    -- LEFT JOIN ' || V_TABLECCFCONFIG || ' D ON C.CCF_RULE_ID = D.PKID               
    -- WHERE (              
    -- A.DATA_SOURCE IN (''TRADE_T24'',''TRS'')               
    -- ) AND A.IMPAIRED_FLAG = ''C''';
    -- EXECUTE (V_STR_QUERY);
    -- 

    -- GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    -- V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    -- V_RETURNROWS := 0;

    -- RAISE NOTICE 'SP_IFRS_IMP_EAD_TERM_YEARLY | AFFECTED RECORD : %', V_RETURNROWS2;
    -- -------- ====== BODY ======

    -- -------- ====== LOG ======
    -- V_TABLEDEST = V_TABLEINSERT3;
    -- V_COLUMNDEST = '-';
    -- V_SPNAME = 'SP_IFRS_IMP_EAD_TERM_YEARLY';
    -- V_OPERATION = 'INSERT';
    
    -- CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -- -------- ====== LOG ======

    -- -------- ====== RESULT ======
    -- V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT3 || '';
    -- CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -- -------- ====== RESULT ======

END;

$$;