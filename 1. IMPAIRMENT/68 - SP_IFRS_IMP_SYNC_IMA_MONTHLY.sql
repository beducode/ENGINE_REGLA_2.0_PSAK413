---- DROP PROCEDURE SP_IFRS_IMP_SYNC_IMA_MONTHLY;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMP_SYNC_IMA_MONTHLY(
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
        V_TABLEINSERT1 := 'TMP_IFRS_ECL_IMA_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_IMA_IMP_CURR_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_IMA_IMP_PREV_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_ECL_RESULT_HEADER_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLENAME_MON := 'IFRS_MASTER_ACCOUNT_MONTHLY';
        V_TABLEINSERT1 := 'TMP_IFRS_ECL_IMA';
        V_TABLEINSERT2 := 'IFRS_IMA_IMP_CURR';
        V_TABLEINSERT3 := 'IFRS_IMA_IMP_PREV';
        V_TABLEINSERT4 := 'IFRS_ECL_RESULT_HEADER';
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
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' A
    SET IMPAIRED_FLAG = ''C''
    FROM IFRS_ECL_EXCLUSION B
    WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
    AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
    AND A.MASTERID = B.MASTERID';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'WITH UPDATE_CURR AS (
      SELECT A.MASTERID,
      A.DOWNLOAD_DATE,
      B.LIFETIME,
      B.ECL_MODEL_ID,
      B.EAD_MODEL_ID,
      B.CCF_RULES_ID,
      B.CCF_EFF_DATE,
      B.CCF,
      B.LGD_MODEL_ID,
      B.PD_MODEL_ID,
      B.SEGMENTATION_ID,
      B.PD_ME_MODEL_ID,
      B.BUCKET_GROUP,
      B.BUCKET_ID,
      B.EAD_BALANCE,
      B.PD_SEGMENT,
      B.LGD_SEGMENT,
      B.EAD_SEGMENT,
      B.DEFAULT_FLAG,
      B.DEFAULT_RULE_ID,
      B.STAGE,
      B.COLL_AMOUNT,
      CASE        
      WHEN D.MASTERID IS NOT NULL        
      THEN B.EAD_BALANCE * (CAST(D.EXCLUSION_PERCENTAGE AS DOUBLE PRECISION) / 100)            
      ELSE        
      CASE WHEN COALESCE(A.IMPAIRED_FLAG, ''C'') = ''C''        
      THEN COALESCE(C.ECL_AMOUNT    ,0)            
      ELSE A.ECL_AMOUNT        
      END        
      END AS ECL_AMOUNT,
      CASE        
      WHEN D.MASTERID IS NOT NULL        
      THEN B.EAD_BALANCE * (CAST(D.EXCLUSION_PERCENTAGE AS DOUBLE PRECISION) / 100)            
      ELSE        
      CASE        
      WHEN COALESCE(A.IMPAIRED_FLAG, ''C'') = ''C''        
      THEN COALESCE(C.ECL_AMOUNT_BFL,0)        
      ELSE A.ECL_AMOUNT_BFL       
      END        
      END AS ECL_AMOUNT_BFL,
      CASE WHEN D.MASTERID IS NOT NULL THEN 0 ELSE A.IA_UNWINDING_AMOUNT END AS IA_UNWINDING_AMOUNT
      FROM IFRS_IMA_IMP_CURR A           
      JOIN TMP_IFRS_ECL_IMA B          
      ON A.DOWNLOAD_DATE = B.DOWNLOAD_DATE           
      AND A.MASTERID = B.MASTERID           
      LEFT JOIN IFRS_ECL_RESULT_HEADER C             
      ON B.DOWNLOAD_DATE = C.DOWNLOAD_DATE AND A.MASTERID = C.MASTERID      
      LEFT JOIN IFRS_ECL_EXCLUSION D      
      ON A.MASTERID = D.MASTERID AND A.DOWNLOAD_DATE = D.DOWNLOAD_DATE
      WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
      )

      UPDATE IFRS_IMA_IMP_CURR AS A
      SET LIFETIME = B.LIFETIME           
      ,ECL_MODEL_ID = B.ECL_MODEL_ID           
      ,EAD_RULE_ID = B.EAD_MODEL_ID           
      ,CCF_RULE_ID = B.CCF_RULES_ID          
      ,CCF_EFF_DATE = B.CCF_EFF_DATE          
      ,CCF = B.CCF          
      ,LGD_RULE_ID = B.LGD_MODEL_ID           
      ,PD_RULE_ID = B.PD_MODEL_ID           
      ,SEGMENTATION_ID = B.SEGMENTATION_ID           
      ,PD_ME_MODEL_ID = B.PD_ME_MODEL_ID           
      ,BUCKET_GROUP = B.BUCKET_GROUP           
      ,BUCKET_ID = B.BUCKET_ID           
      ,EAD_AMOUNT = B.EAD_BALANCE           
      ,PD_SEGMENT = B.PD_SEGMENT           
      ,LGD_SEGMENT = B.LGD_SEGMENT           
      ,EAD_SEGMENT = B.EAD_SEGMENT           
      ,DEFAULT_FLAG = B.DEFAULT_FLAG           
      ,DEFAULT_RULE_ID = B.DEFAULT_RULE_ID           
      ,STAGE = B.STAGE
      ,COLL_AMOUNT = B.COLL_AMOUNT
      ,ECL_AMOUNT = B.ECL_AMOUNT
      ,ECL_AMOUNT_BFL = B.ECL_AMOUNT_BFL
      ,IA_UNWINDING_AMOUNT = B.IA_UNWINDING_AMOUNT
      FROM UPDATE_CURR B 
      WHERE A.MASTERID = B.MASTERID AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
      AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE';
      EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' A
    SET ECL_AMOUNT = 0, ECL_AMOUNT_BFL = 0
    FROM IFRS_CREDITLINE_JENIUS B
    WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
    AND A.FACILITY_NUMBER = B.CREDIT_LINE_REF
    AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
    AND A.DATA_SOURCE = ''LIMIT''
    AND B.ELIGIBILITY_STATUS = ''NOT_ELIGIBLE''';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' A
    SET BEGINNING_BALANCE = CASE WHEN B.ECL_AMOUNT < 0 THEN 0 ELSE B.ECL_AMOUNT END
    FROM ' || V_TABLEINSERT3 || ' B
    WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
    AND B.DOWNLOAD_DATE = ''' || CAST(V_PREVMONTH AS VARCHAR(10)) || '''::DATE
    AND A.MASTERID = B.MASTERID';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || '
    SET WRITEBACK_AMOUNT = CASE 
    WHEN COALESCE(BEGINNING_BALANCE, 0) > COALESCE(ECL_AMOUNT, 0) THEN ABS(COALESCE(ECL_AMOUNT, 0) - COALESCE(BEGINNING_BALANCE, 0))
        ELSE 0
    END,
    CHARGE_AMOUNT = CASE 
    WHEN COALESCE(BEGINNING_BALANCE, 0) < COALESCE(ECL_AMOUNT, 0) THEN ABS(COALESCE(ECL_AMOUNT, 0) - COALESCE(BEGINNING_BALANCE, 0))
        ELSE 0
    END,
    ENDING_BALANCE = CASE 
    WHEN COALESCE(ECL_AMOUNT, 0) < 0 THEN 0
        ELSE COALESCE(ECL_AMOUNT, 0)
    END
    WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' A
    SET ECL_AMOUNT = 0
    WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
    AND A.DATA_SOURCE = ''LIMIT''
    AND A.SEGMENT_FLAG = ''CROSS_SEGMENT_LIMIT''';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLENAME || ' WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLENAME_MON || ' WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLENAME ||'          
    (          
      DOWNLOAD_DATE            
      ,MASTERID            
      ,MASTER_ACCOUNT_CODE            
      ,DATA_SOURCE            
      ,GLOBAL_CUSTOMER_NUMBER            
      ,CUSTOMER_NUMBER            
      ,CUSTOMER_NAME            
      ,FACILITY_NUMBER            
      ,ACCOUNT_NUMBER            
      ,PREVIOUS_ACCOUNT_NUMBER            
      ,ACCOUNT_STATUS            
      ,INTEREST_RATE            
      ,MARKET_RATE            
      ,PRODUCT_GROUP            
      ,PRODUCT_TYPE            
      ,PRODUCT_CODE            
      ,PRODUCT_ENTITY            
      ,GL_CONSTNAME            
      ,BRANCH_CODE            
      ,BRANCH_CODE_OPEN            
      ,CURRENCY            
      ,EXCHANGE_RATE            
      ,INITIAL_OUTSTANDING            
      ,OUTSTANDING            
      ,OUTSTANDING_IDC            
      ,OUTSTANDING_JF            
      ,OUTSTANDING_BANK            
      ,OUTSTANDING_PASTDUE            
      ,OUTSTANDING_WO            
      ,PLAFOND            
      ,PLAFOND_CASH            
      ,INTEREST_ACCRUED            
      ,INSTALLMENT_AMOUNT            
      ,UNUSED_AMOUNT            
      ,DOWN_PAYMENT_AMOUNT            
      ,JF_FLAG            
      ,LOAN_START_DATE            
      ,LOAN_DUE_DATE            
      ,LOAN_START_AMORTIZATION            
      ,LOAN_END_AMORTIZATION            
      ,INSTALLMENT_GRACE_PERIOD            
      ,NEXT_PAYMENT_DATE            
      ,NEXT_INT_PAYMENT_DATE            
      ,LAST_PAYMENT_DATE            
      ,FIRST_INSTALLMENT_DATE            
      ,TENOR            
      ,REMAINING_TENOR            
      ,PAYMENT_CODE            
      ,PAYMENT_TERM            
      ,INTEREST_CALCULATION_CODE            
      ,INTEREST_PAYMENT_TERM            
      ,RESTRUCTURE_DATE            
      ,RESTRUCTURE_FLAG            
      ,POCI_FLAG            
      ,STAFF_LOAN_FLAG            
      ,BELOW_MARKET_FLAG            
      ,BTB_FLAG            
      ,COMMITTED_FLAG            
      ,REVOLVING_FLAG            
      ,IAS_CLASS            
      ,IFRS9_CLASS            
      ,AMORT_TYPE      
      ,EIR_STATUS            
      ,ECF_STATUS            
      ,EIR            
      ,EIR_AMOUNT            
      ,FAIR_VALUE_AMOUNT            
      ,INITIAL_UNAMORT_TXN_COST            
      ,INITIAL_UNAMORT_ORG_FEE            
      ,UNAMORT_COST_AMT            
      ,UNAMORT_FEE_AMT            
      ,DAILY_AMORT_AMT            
      ,UNAMORT_AMT_TOTAL_JF            
      ,UNAMORT_FEE_AMT_JF            
      ,UNAMORT_COST_AMT_JF            
      ,ORIGINAL_COLLECTABILITY            
      ,BI_COLLECTABILITY            
      ,DAY_PAST_DUE            
      ,DPD_START_DATE            
      ,DPD_ZERO_COUNTER            
      ,NPL_DATE            
      ,NPL_FLAG            
      ,DEFAULT_DATE            
      ,DEFAULT_FLAG            
      ,WRITEOFF_FLAG            
      ,WRITEOFF_DATE            
      ,IMPAIRED_FLAG            
      ,IS_IMPAIRED            
      ,GROUP_SEGMENT            
      ,SEGMENT            
      ,SUB_SEGMENT            
      ,STAGE            
      ,LIFETIME            
      ,EAD_RULE_ID            
      ,EAD_SEGMENT            
      ,EAD_AMOUNT            
      ,LGD_RULE_ID            
      ,LGD_SEGMENT            
      ,PD_RULE_ID            
      ,PD_SEGMENT            
      ,BUCKET_GROUP            
      ,BUCKET_ID            
      ,ECL_12_AMOUNT            
      ,ECL_LIFETIME_AMOUNT            
      ,ECL_AMOUNT            
      ,CA_UNWINDING_AMOUNT            
      ,IA_UNWINDING_AMOUNT            
      ,IA_UNWINDING_SUM_AMOUNT            
      ,BEGINNING_BALANCE            
      ,ENDING_BALANCE            
      ,WRITEBACK_AMOUNT            
      ,CHARGE_AMOUNT            
      ,CREATEDBY            
      ,CREATEDDATE            
      ,CREATEDHOST            
      ,UPDATEDBY            
      ,UPDATEDDATE            
      ,UPDATEDHOST            
      ,INITIAL_BENEFIT            
      ,UNAMORT_BENEFIT            
      ,SPPI_RESULT            
      ,BM_RESULT            
      ,ECONOMIC_SECTOR            
      ,AO_CODE            
      ,SUFFIX            
      ,ACCOUNT_TYPE            
      ,CUSTOMER_TYPE            
      ,OUTSTANDING_PROFIT_DUE            
      ,RESTRUCTURE_COLLECT_FLAG            
      ,DPD_FINAL            
      ,EIR_SEGMENT            
      ,DPD_CIF            
      ,DPD_FINAL_CIF            
      ,BI_COLLECT_CIF            
      ,PRODUCT_TYPE_1            
      ,RATING_CODE            
      ,CCF            
      ,CCF_RULE_ID            
      ,CCF_EFF_DATE            
      ,ECL_AMOUNT_BFL            
      ,AVG_EIR            
      ,ECL_MODEL_ID            
      ,SEGMENTATION_ID            
      ,PD_ME_MODEL_ID            
      ,DEFAULT_RULE_ID            
      ,PLAFOND_CIF            
      ,RESTRUCTURE_COLLECT_FLAG_CIF            
      ,SOURCE_SYSTEM            
      ,INITIAL_RATING_CODE            
      ,PD_INITIAL_RATE            
      ,PD_CURRENT_RATE            
      ,PD_CHANGE            
      ,LIMIT_CURRENCY            
      ,SUN_ID            
      ,RATING_DOWNGRADE            
      ,WATCHLIST_FLAG            
      ,COLL_AMOUNT            
      ,FACILITY_NUMBER_PARENT          
      ,EXT_RATING_AGENCY          
      ,EXT_RATING_CODE          
      ,EXT_INIT_RATING_CODE          
      ,INTEREST_TYPE          
      ,SOVEREIGN_FLAG          
      ,ISIN_CODE          
      ,INV_TYPE          
      ,UNAMORT_DISCOUNT_PREMIUM          
      ,DISCOUNT_PREMIUM_AMOUNT            
      ,PRODUCT_CODE_T24          
      ,EXT_RATING_DOWNGRADE          
      ,SANDI_BANK      
      ,LOB_CODE      
      ,COUNTER_GUARANTEE_FLAG   
      ,EARLY_PAYMENT      
      ,EARLY_PAYMENT_FLAG      
      ,EARLY_PAYMENT_DATE  
      ,SEGMENT_FLAG    
    )          
    SELECT             
      DOWNLOAD_DATE            
      ,MASTERID            
      ,MASTER_ACCOUNT_CODE            
      ,DATA_SOURCE            
      ,GLOBAL_CUSTOMER_NUMBER            
      ,CUSTOMER_NUMBER            
      ,CUSTOMER_NAME            
      ,FACILITY_NUMBER            
      ,ACCOUNT_NUMBER            
      ,PREVIOUS_ACCOUNT_NUMBER            
      ,ACCOUNT_STATUS            
      ,INTEREST_RATE            
      ,MARKET_RATE            
      ,PRODUCT_GROUP            
      ,PRODUCT_TYPE            
      ,PRODUCT_CODE            
      ,PRODUCT_ENTITY            
      ,GL_CONSTNAME            
      ,BRANCH_CODE            
      ,BRANCH_CODE_OPEN            
      ,CURRENCY            
      ,EXCHANGE_RATE            
      ,INITIAL_OUTSTANDING            
      ,OUTSTANDING            
      ,OUTSTANDING_IDC            
      ,OUTSTANDING_JF            
      ,OUTSTANDING_BANK            
      ,OUTSTANDING_PASTDUE            
      ,OUTSTANDING_WO            
      ,PLAFOND            
      ,PLAFOND_CASH            
      ,INTEREST_ACCRUED        
      ,INSTALLMENT_AMOUNT            
      ,UNUSED_AMOUNT            
      ,DOWN_PAYMENT_AMOUNT            
      ,JF_FLAG            
      ,LOAN_START_DATE            
      ,LOAN_DUE_DATE            
      ,LOAN_START_AMORTIZATION            
      ,LOAN_END_AMORTIZATION            
      ,INSTALLMENT_GRACE_PERIOD            
      ,NEXT_PAYMENT_DATE            
      ,NEXT_INT_PAYMENT_DATE            
      ,LAST_PAYMENT_DATE            
      ,FIRST_INSTALLMENT_DATE            
      ,TENOR            
      ,REMAINING_TENOR            
      ,PAYMENT_CODE            
      ,PAYMENT_TERM            
      ,INTEREST_CALCULATION_CODE            
      ,INTEREST_PAYMENT_TERM            
      ,RESTRUCTURE_DATE            
      ,RESTRUCTURE_FLAG            
      ,POCI_FLAG            
      ,STAFF_LOAN_FLAG            
      ,BELOW_MARKET_FLAG            
      ,BTB_FLAG            
      ,COMMITTED_FLAG            
      ,REVOLVING_FLAG            
      ,IAS_CLASS            
      ,IFRS9_CLASS            
      ,AMORT_TYPE            
      ,EIR_STATUS            
      ,ECF_STATUS            
      ,EIR            
      ,EIR_AMOUNT            
      ,FAIR_VALUE_AMOUNT            
      ,INITIAL_UNAMORT_TXN_COST            
      ,INITIAL_UNAMORT_ORG_FEE            
      ,UNAMORT_COST_AMT            
      ,UNAMORT_FEE_AMT            
      ,DAILY_AMORT_AMT            
      ,UNAMORT_AMT_TOTAL_JF            
      ,UNAMORT_FEE_AMT_JF            
      ,UNAMORT_COST_AMT_JF            
      ,ORIGINAL_COLLECTABILITY            
      ,BI_COLLECTABILITY            
      ,DAY_PAST_DUE            
      ,DPD_START_DATE            
      ,DPD_ZERO_COUNTER            
      ,NPL_DATE            
      ,NPL_FLAG            
      ,DEFAULT_DATE            
      ,DEFAULT_FLAG            
      ,WRITEOFF_FLAG            
      ,WRITEOFF_DATE            
      ,IMPAIRED_FLAG            
      ,IS_IMPAIRED            
      ,GROUP_SEGMENT            
      ,SEGMENT            
      ,SUB_SEGMENT            
      ,STAGE            
      ,LIFETIME            
      ,EAD_RULE_ID::BIGINT            
      ,EAD_SEGMENT            
      ,EAD_AMOUNT            
      ,LGD_RULE_ID::BIGINT           
      ,LGD_SEGMENT            
      ,PD_RULE_ID::BIGINT            
      ,PD_SEGMENT            
      ,BUCKET_GROUP            
      ,BUCKET_ID            
      ,ECL_12_AMOUNT            
      ,ECL_LIFETIME_AMOUNT            
      ,ECL_AMOUNT            
      ,CA_UNWINDING_AMOUNT            
      ,IA_UNWINDING_AMOUNT            
      ,IA_UNWINDING_SUM_AMOUNT            
      ,BEGINNING_BALANCE            
      ,ENDING_BALANCE            
      ,WRITEBACK_AMOUNT            
      ,CHARGE_AMOUNT            
      ,CREATEDBY            
      ,CREATEDDATE            
      ,CREATEDHOST            
      ,UPDATEDBY            
      ,UPDATEDDATE            
      ,UPDATEDHOST     
      ,INITIAL_BENEFIT            
      ,UNAMORT_BENEFIT            
      ,SPPI_RESULT            
      ,BM_RESULT            
      ,ECONOMIC_SECTOR            
      ,AO_CODE            
      ,SUFFIX            
      ,ACCOUNT_TYPE            
      ,CUSTOMER_TYPE            
      ,OUTSTANDING_PROFIT_DUE            
      ,RESTRUCTURE_COLLECT_FLAG            
      ,DPD_FINAL            
      ,EIR_SEGMENT            
      ,DPD_CIF            
      ,DPD_FINAL_CIF            
      ,BI_COLLECT_CIF            
      ,PRODUCT_TYPE_1            
      ,RATING_CODE            
      ,CAST(COALESCE(CCF,''0'') AS NUMERIC(36,2)) AS CCF
      ,CAST(COALESCE(CCF_RULE_ID,''0'') AS INTEGER) AS CCF_RULE_ID		            
      ,CCF_EFF_DATE::DATE           
      ,ECL_AMOUNT_BFL            
      ,AVG_EIR            
      ,ECL_MODEL_ID            
      ,SEGMENTATION_ID            
      ,PD_ME_MODEL_ID            
      ,DEFAULT_RULE_ID            
      ,PLAFOND_CIF            
      ,RESTRUCTURE_COLLECT_FLAG_CIF            
      ,SOURCE_SYSTEM            
      ,INITIAL_RATING_CODE            
      ,PD_INITIAL_RATE            
      ,PD_CURRENT_RATE            
      ,PD_CHANGE            
      ,LIMIT_CURRENCY            
      ,SUN_ID            
      ,RATING_DOWNGRADE            
      ,WATCHLIST_FLAG            
      ,COLL_AMOUNT            
      ,FACILITY_NUMBER_PARENT          
      ,EXT_RATING_AGENCY          
      ,EXT_RATING_CODE          
      ,EXT_INIT_RATING_CODE          
      ,INTEREST_TYPE          
      ,SOVEREIGN_FLAG          
      ,ISIN_CODE          
      ,INV_TYPE          
      ,UNAMORT_DISCOUNT_PREMIUM          
      ,DISCOUNT_PREMIUM_AMOUNT            
      ,PRODUCT_CODE_T24          
      ,EXT_RATING_DOWNGRADE          
      ,SANDI_BANK      
      ,LOB_CODE      
      ,COUNTER_GUARANTEE_FLAG     
      ,EARLY_PAYMENT      
      ,EARLY_PAYMENT_FLAG      
      ,EARLY_PAYMENT_DATE  
      ,SEGMENT_FLAG
    FROM ' || V_TABLEINSERT2 || '
    WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLENAME_MON || '          
    (          
      DOWNLOAD_DATE            
      ,MASTERID            
      ,MASTER_ACCOUNT_CODE            
      ,DATA_SOURCE            
      ,GLOBAL_CUSTOMER_NUMBER            
      ,CUSTOMER_NUMBER            
      ,CUSTOMER_NAME            
      ,FACILITY_NUMBER            
      ,ACCOUNT_NUMBER            
      ,PREVIOUS_ACCOUNT_NUMBER            
      ,ACCOUNT_STATUS            
      ,INTEREST_RATE            
      ,MARKET_RATE            
      ,PRODUCT_GROUP            
      ,PRODUCT_TYPE            
      ,PRODUCT_CODE            
      ,PRODUCT_ENTITY            
      ,GL_CONSTNAME            
      ,BRANCH_CODE            
      ,BRANCH_CODE_OPEN            
      ,CURRENCY            
      ,EXCHANGE_RATE            
      ,INITIAL_OUTSTANDING            
      ,OUTSTANDING            
      ,OUTSTANDING_IDC            
      ,OUTSTANDING_JF            
      ,OUTSTANDING_BANK            
      ,OUTSTANDING_PASTDUE            
      ,OUTSTANDING_WO            
      ,PLAFOND            
      ,PLAFOND_CASH            
      ,INTEREST_ACCRUED            
      ,INSTALLMENT_AMOUNT            
      ,UNUSED_AMOUNT            
      ,DOWN_PAYMENT_AMOUNT            
      ,JF_FLAG            
      ,LOAN_START_DATE            
      ,LOAN_DUE_DATE            
      ,LOAN_START_AMORTIZATION            
      ,LOAN_END_AMORTIZATION            
      ,INSTALLMENT_GRACE_PERIOD            
      ,NEXT_PAYMENT_DATE            
      ,NEXT_INT_PAYMENT_DATE            
      ,LAST_PAYMENT_DATE            
      ,FIRST_INSTALLMENT_DATE            
      ,TENOR            
      ,REMAINING_TENOR            
      ,PAYMENT_CODE            
      ,PAYMENT_TERM            
      ,INTEREST_CALCULATION_CODE            
      ,INTEREST_PAYMENT_TERM            
      ,RESTRUCTURE_DATE            
      ,RESTRUCTURE_FLAG            
      ,POCI_FLAG            
      ,STAFF_LOAN_FLAG            
      ,BELOW_MARKET_FLAG            
      ,BTB_FLAG            
      ,COMMITTED_FLAG            
      ,REVOLVING_FLAG            
      ,IAS_CLASS            
      ,IFRS9_CLASS            
      ,AMORT_TYPE            
      ,EIR_STATUS            
      ,ECF_STATUS            
      ,EIR            
      ,EIR_AMOUNT            
      ,FAIR_VALUE_AMOUNT            
      ,INITIAL_UNAMORT_TXN_COST            
      ,INITIAL_UNAMORT_ORG_FEE            
      ,UNAMORT_COST_AMT            
      ,UNAMORT_FEE_AMT            
      ,DAILY_AMORT_AMT            
      ,UNAMORT_AMT_TOTAL_JF            
      ,UNAMORT_FEE_AMT_JF            
      ,UNAMORT_COST_AMT_JF            
      ,ORIGINAL_COLLECTABILITY            
      ,BI_COLLECTABILITY            
      ,DAY_PAST_DUE            
      ,DPD_START_DATE            
      ,DPD_ZERO_COUNTER            
      ,NPL_DATE            
      ,NPL_FLAG            
      ,DEFAULT_DATE            
      ,DEFAULT_FLAG            
      ,WRITEOFF_FLAG            
      ,WRITEOFF_DATE            
      ,IMPAIRED_FLAG            
      ,IS_IMPAIRED            
      ,GROUP_SEGMENT            
      ,SEGMENT            
      ,SUB_SEGMENT            
      ,STAGE            
      ,LIFETIME            
      ,EAD_RULE_ID            
      ,EAD_SEGMENT            
      ,EAD_AMOUNT            
      ,LGD_RULE_ID           
      ,LGD_SEGMENT            
      ,PD_RULE_ID            
      ,PD_SEGMENT            
      ,BUCKET_GROUP            
      ,BUCKET_ID            
      ,ECL_12_AMOUNT            
      ,ECL_LIFETIME_AMOUNT            
      ,ECL_AMOUNT            
      ,CA_UNWINDING_AMOUNT            
      ,IA_UNWINDING_AMOUNT            
      ,IA_UNWINDING_SUM_AMOUNT            
      ,BEGINNING_BALANCE            
      ,ENDING_BALANCE            
      ,WRITEBACK_AMOUNT            
      ,CHARGE_AMOUNT            
      ,CREATEDBY            
      ,CREATEDDATE            
      ,CREATEDHOST            
      ,UPDATEDBY            
      ,UPDATEDDATE            
      ,UPDATEDHOST            
      ,INITIAL_BENEFIT            
      ,UNAMORT_BENEFIT            
      ,SPPI_RESULT            
      ,BM_RESULT            
      ,ECONOMIC_SECTOR            
      ,AO_CODE            
      ,SUFFIX            
      ,ACCOUNT_TYPE            
      ,CUSTOMER_TYPE            
      ,OUTSTANDING_PROFIT_DUE            
      ,RESTRUCTURE_COLLECT_FLAG            
      ,DPD_FINAL            
      ,EIR_SEGMENT            
      ,DPD_CIF            
      ,DPD_FINAL_CIF            
      ,BI_COLLECT_CIF            
      ,PRODUCT_TYPE_1            
      ,RATING_CODE            
      ,CCF            
      ,CCF_RULE_ID            
      ,CCF_EFF_DATE            
      ,ECL_AMOUNT_BFL            
      ,AVG_EIR            
      ,ECL_MODEL_ID            
      ,SEGMENTATION_ID            
      ,PD_ME_MODEL_ID            
      ,DEFAULT_RULE_ID            
      ,PLAFOND_CIF            
      ,RESTRUCTURE_COLLECT_FLAG_CIF            
      ,SOURCE_SYSTEM            
      ,INITIAL_RATING_CODE            
      ,PD_INITIAL_RATE            
      ,PD_CURRENT_RATE            
      ,PD_CHANGE            
      ,LIMIT_CURRENCY            
      ,SUN_ID            
      ,RATING_DOWNGRADE            
      ,WATCHLIST_FLAG            
      ,COLL_AMOUNT            
      ,FACILITY_NUMBER_PARENT          
      ,EXT_RATING_AGENCY          
      ,EXT_RATING_CODE          
      ,EXT_INIT_RATING_CODE          
      ,INTEREST_TYPE          
      ,SOVEREIGN_FLAG          
      ,ISIN_CODE          
      ,INV_TYPE          
      ,UNAMORT_DISCOUNT_PREMIUM          
      ,DISCOUNT_PREMIUM_AMOUNT            
      ,PRODUCT_CODE_T24          
      ,EXT_RATING_DOWNGRADE          
      ,SANDI_BANK      
      ,LOB_CODE       
      ,COUNTER_GUARANTEE_FLAG     
      ,EARLY_PAYMENT      
      ,EARLY_PAYMENT_FLAG      
      ,EARLY_PAYMENT_DATE  
      ,SEGMENT_FLAG  
    )          
    SELECT          
      DOWNLOAD_DATE            
      ,MASTERID            
      ,MASTER_ACCOUNT_CODE            
      ,DATA_SOURCE            
      ,GLOBAL_CUSTOMER_NUMBER            
      ,CUSTOMER_NUMBER            
      ,CUSTOMER_NAME            
      ,FACILITY_NUMBER            
      ,ACCOUNT_NUMBER            
      ,PREVIOUS_ACCOUNT_NUMBER            
      ,ACCOUNT_STATUS            
      ,INTEREST_RATE            
      ,MARKET_RATE            
      ,PRODUCT_GROUP            
      ,PRODUCT_TYPE            
      ,PRODUCT_CODE            
      ,PRODUCT_ENTITY            
      ,GL_CONSTNAME            
      ,BRANCH_CODE            
      ,BRANCH_CODE_OPEN            
      ,CURRENCY            
      ,EXCHANGE_RATE            
      ,INITIAL_OUTSTANDING            
      ,OUTSTANDING            
      ,OUTSTANDING_IDC            
      ,OUTSTANDING_JF            
      ,OUTSTANDING_BANK            
      ,OUTSTANDING_PASTDUE            
      ,OUTSTANDING_WO            
      ,PLAFOND            
      ,PLAFOND_CASH            
      ,INTEREST_ACCRUED            
      ,INSTALLMENT_AMOUNT            
      ,UNUSED_AMOUNT            
      ,DOWN_PAYMENT_AMOUNT            
      ,JF_FLAG            
      ,LOAN_START_DATE            
      ,LOAN_DUE_DATE            
      ,LOAN_START_AMORTIZATION            
      ,LOAN_END_AMORTIZATION            
      ,INSTALLMENT_GRACE_PERIOD            
      ,NEXT_PAYMENT_DATE            
      ,NEXT_INT_PAYMENT_DATE            
      ,LAST_PAYMENT_DATE            
      ,FIRST_INSTALLMENT_DATE            
      ,TENOR            
      ,REMAINING_TENOR            
      ,PAYMENT_CODE            
      ,PAYMENT_TERM            
      ,INTEREST_CALCULATION_CODE            
      ,INTEREST_PAYMENT_TERM            
      ,RESTRUCTURE_DATE            
      ,RESTRUCTURE_FLAG            
      ,POCI_FLAG            
      ,STAFF_LOAN_FLAG            
      ,BELOW_MARKET_FLAG            
      ,BTB_FLAG            
      ,COMMITTED_FLAG            
      ,REVOLVING_FLAG            
      ,IAS_CLASS            
      ,IFRS9_CLASS            
      ,AMORT_TYPE            
      ,EIR_STATUS            
      ,ECF_STATUS            
      ,EIR            
      ,EIR_AMOUNT            
      ,FAIR_VALUE_AMOUNT            
      ,INITIAL_UNAMORT_TXN_COST            
      ,INITIAL_UNAMORT_ORG_FEE            
      ,UNAMORT_COST_AMT            
      ,UNAMORT_FEE_AMT            
      ,DAILY_AMORT_AMT            
      ,UNAMORT_AMT_TOTAL_JF            
      ,UNAMORT_FEE_AMT_JF            
      ,UNAMORT_COST_AMT_JF            
      ,ORIGINAL_COLLECTABILITY            
      ,BI_COLLECTABILITY            
      ,DAY_PAST_DUE            
      ,DPD_START_DATE            
      ,DPD_ZERO_COUNTER            
      ,NPL_DATE            
      ,NPL_FLAG            
      ,DEFAULT_DATE            
      ,DEFAULT_FLAG            
      ,WRITEOFF_FLAG            
      ,WRITEOFF_DATE            
      ,IMPAIRED_FLAG            
      ,IS_IMPAIRED            
      ,GROUP_SEGMENT            
      ,SEGMENT            
      ,SUB_SEGMENT            
      ,STAGE            
      ,LIFETIME            
      ,EAD_RULE_ID::BIGINT            
      ,EAD_SEGMENT            
      ,EAD_AMOUNT            
      ,LGD_RULE_ID::BIGINT            
      ,LGD_SEGMENT            
      ,PD_RULE_ID::BIGINT            
      ,PD_SEGMENT            
      ,BUCKET_GROUP            
      ,BUCKET_ID            
      ,ECL_12_AMOUNT            
      ,ECL_LIFETIME_AMOUNT            
      ,ECL_AMOUNT            
      ,CA_UNWINDING_AMOUNT            
      ,IA_UNWINDING_AMOUNT            
      ,IA_UNWINDING_SUM_AMOUNT            
      ,BEGINNING_BALANCE            
      ,ENDING_BALANCE            
      ,WRITEBACK_AMOUNT            
      ,CHARGE_AMOUNT            
      ,CREATEDBY            
      ,CREATEDDATE            
      ,CREATEDHOST            
      ,UPDATEDBY            
      ,UPDATEDDATE            
      ,UPDATEDHOST            
      ,INITIAL_BENEFIT            
      ,UNAMORT_BENEFIT            
      ,SPPI_RESULT            
      ,BM_RESULT            
      ,ECONOMIC_SECTOR            
      ,AO_CODE            
      ,SUFFIX            
      ,ACCOUNT_TYPE            
      ,CUSTOMER_TYPE            
      ,OUTSTANDING_PROFIT_DUE            
      ,RESTRUCTURE_COLLECT_FLAG            
      ,DPD_FINAL            
      ,EIR_SEGMENT            
      ,DPD_CIF            
      ,DPD_FINAL_CIF            
      ,BI_COLLECT_CIF            
      ,PRODUCT_TYPE_1            
      ,RATING_CODE            
      ,CAST(COALESCE(CCF,''0'') AS NUMERIC(36,2)) AS CCF            
      ,CAST(COALESCE(CCF_RULE_ID,''0'') AS INTEGER) AS CCF_RULE_ID            
      ,CCF_EFF_DATE::DATE            
      ,ECL_AMOUNT_BFL            
      ,AVG_EIR            
      ,ECL_MODEL_ID            
      ,SEGMENTATION_ID            
      ,PD_ME_MODEL_ID            
      ,DEFAULT_RULE_ID            
      ,PLAFOND_CIF            
      ,RESTRUCTURE_COLLECT_FLAG_CIF            
      ,SOURCE_SYSTEM            
      ,INITIAL_RATING_CODE            
      ,PD_INITIAL_RATE            
      ,PD_CURRENT_RATE            
      ,PD_CHANGE            
      ,LIMIT_CURRENCY            
      ,SUN_ID            
      ,RATING_DOWNGRADE            
      ,WATCHLIST_FLAG            
      ,COLL_AMOUNT            
      ,FACILITY_NUMBER_PARENT          
      ,EXT_RATING_AGENCY          
      ,EXT_RATING_CODE          
      ,EXT_INIT_RATING_CODE          
      ,INTEREST_TYPE          
      ,SOVEREIGN_FLAG          
      ,ISIN_CODE          
      ,INV_TYPE          
      ,UNAMORT_DISCOUNT_PREMIUM          
      ,DISCOUNT_PREMIUM_AMOUNT              
      ,PRODUCT_CODE_T24          
      ,EXT_RATING_DOWNGRADE          
      ,SANDI_BANK      
      ,LOB_CODE      
      ,COUNTER_GUARANTEE_FLAG      
      ,EARLY_PAYMENT      
      ,EARLY_PAYMENT_FLAG      
      ,EARLY_PAYMENT_DATE  
      ,SEGMENT_FLAG
    FROM ' || V_TABLEINSERT2 || '
    WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    RAISE NOTICE 'SP_IFRS_IMP_SYNC_IMA_MONTHLY | AFFECTED RECORD : %', V_RETURNROWS2;
    -------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLENAME;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_IMP_SYNC_IMA_MONTHLY';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);

    V_TABLEDEST = V_TABLENAME_MON;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_IMP_SYNC_IMA_MONTHLY';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    ------ ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLENAME || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;