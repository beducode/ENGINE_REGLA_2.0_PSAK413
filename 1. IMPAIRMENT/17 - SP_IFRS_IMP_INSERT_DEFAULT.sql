---- DROP PROCEDURE SP_IFRS_IMP_INSERT_DEFAULT;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMP_INSERT_DEFAULT(
    IN P_RUNID VARCHAR(20) DEFAULT 'S_00000_0000', 
    IN P_DOWNLOAD_DATE DATE DEFAULT NULL,
    IN P_PRC VARCHAR(1) DEFAULT 'S')
LANGUAGE PLPGSQL AS $$
DECLARE
    V_FLAG_SURVIVE_DATE DATE;
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
        V_TABLEINSERT4 := 'IFRS_IMP_DEFAULT_STATUS_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLENAME_MON := 'IFRS_MASTER_ACCOUNT_MONTHLY';
        V_TABLEINSERT1 := 'TMP_IFRS_ECL_IMA';
        V_TABLEINSERT2 := 'IFRS_IMA_IMP_CURR';
        V_TABLEINSERT3 := 'IFRS_IMA_IMP_PREV';
        V_TABLEINSERT4 := 'IFRS_IMP_DEFAULT_STATUS';
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
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT4 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT4 || ' AS SELECT * FROM IFRS_IMP_DEFAULT_STATUS WHERE 0=1';
        EXECUTE (V_STR_QUERY);
    END IF;

    SELECT VALUE1 INTO V_FLAG_SURVIVE_DATE FROM TBLM_COMMONCODEDETAIL WHERE COMMONCODE = 'FLAG_SURVIVE';

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS CURR_RESTRU_SIFAT_PREVIOUS_ACCT_' || P_RUNID || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE CURR_RESTRU_SIFAT_PREVIOUS_ACCT_' || P_RUNID || ' AS
    SELECT DISTINCT A.PREVIOUS_ACCOUNT_NUMBER, A.CUSTOMER_NUMBER,COALESCE(B.FLAG_RESTRU_COVID19,''N'') AS FLAG_RESTRU_COVID19                      
    FROM IFRS_MASTER_RESTRU_SIFAT A              
    LEFT JOIN IFRS_MASTER_FLAG_COVID B ON               
    A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER               
    AND A.PREVIOUS_ACCOUNT_NUMBER = B.PREVIOUS_ACCOUNT_NUMBER              
    AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE';
    EXECUTE (V_STR_QUERY);


    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS CURR_RESTRU_SIFAT_EXISTING_ACCT_' || P_RUNID || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE CURR_RESTRU_SIFAT_EXISTING_ACCT_' || P_RUNID || ' AS
    SELECT  DISTINCT A.DOWNLOAD_DATE, A.CUSTOMER_NUMBER,A.PREVIOUS_ACCOUNT_NUMBER,A.ACCOUNT_NUMBER,COALESCE(C.FLAG_RESTRU_COVID19,              
    ''N'') AS FLAG_RESTRU_COVID19
    FROM IFRS_MASTER_RESTRU_SIFAT A JOIN (              
    SELECT              
    MAX(DOWNLOAD_DATE)as DOWNLOAD_DATE              
    ,CUSTOMER_NUMBER              
    ,ACCOUNT_NUMBER              
    FROM IFRS_MASTER_RESTRU_SIFAT              
    GROUP BY CUSTOMER_NUMBER,ACCOUNT_NUMBER) B ON A.DOWNLOAD_DATE=B.DOWNLOAD_DATE and A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER              
    LEFT JOIN IFRS_MASTER_FLAG_COVID C ON A.ACCOUNT_NUMBER = C.ACCOUNT_NUMBER AND A.CUSTOMER_NUMBER = C.CUSTOMER_NUMBER              
    AND C.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                       
    ORDER BY DOWNLOAD_DATE,ACCOUNT_NUMBER';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM CURR_RESTRU_SIFAT_PREVIOUS_ACCT_' || P_RUNID || ' A
    USING (SELECT * FROM CURR_RESTRU_SIFAT_EXISTING_ACCT_' || P_RUNID || ' X 
    INNER JOIN  (
    SELECT  CUSTOMER_NUMBER,PREVIOUS_ACCOUNT_NUMBER,COUNT(*) AS CT FROM CURR_RESTRU_SIFAT_PREVIOUS_ACCT_' || P_RUNID || '   
    GROUP BY CUSTOMER_NUMBER,PREVIOUS_ACCOUNT_NUMBER HAVING COUNT(*)>1
    ) Y  
    ON  X.CUSTOMER_NUMBER = Y.CUSTOMER_NUMBER AND X.PREVIOUS_ACCOUNT_NUMBER = Y.PREVIOUS_ACCOUNT_NUMBER) B
    WHERE A.PREVIOUS_ACCOUNT_NUMBER = B.ACCOUNT_NUMBER AND A.FLAG_RESTRU_COVID19 <> B.FLAG_RESTRU_COVID19';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS CURR_WO_' || P_RUNID || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE CURR_WO_' || P_RUNID || ' AS 
    SELECT DISTINCT MASTERID, CUSTOMER_NUMBER FROM IFRS_MASTER_WO WHERE DOWNLOAD_DATE  = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS PREV_SURVIVE_' || P_RUNID || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE PREV_SURVIVE_' || P_RUNID || ' AS 
    SELECT DISTINCT DOWNLOAD_DATE, CUSTOMER_NUMBER, PREVIOUS_ACCOUNT_NUMBER , ACCOUNT_NUMBER, SURVIVE_FLAG FROM IFRS_MASTER_SURVIVE WHERE DOWNLOAD_DATE  = ''' || CAST(V_PREVMONTH AS VARCHAR(10)) || '''::DATE';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS PREV_SURVIVE_RESTRU_' || P_RUNID || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE PREV_SURVIVE_RESTRU_' || P_RUNID || ' AS 
    SELECT DISTINCT                 
    A.CUSTOMER_NUMBER                
    ,A.PREVIOUS_ACCOUNT_NUMBER                
    ,A.FLAG_RESTRU_COVID19                  
    ,B.SURVIVE_FLAG
    FROM CURR_RESTRU_SIFAT_PREVIOUS_ACCT_' || P_RUNID || ' A                    
    LEFT JOIN PREV_SURVIVE_' || P_RUNID || ' B ON  A.PREVIOUS_ACCOUNT_NUMBER = B.PREVIOUS_ACCOUNT_NUMBER AND A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS CURR_SURVIVE_RESTRU_' || P_RUNID || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE CURR_SURVIVE_RESTRU_' || P_RUNID || ' AS 
    SELECT DISTINCT                 
    A.CUSTOMER_NUMBER                
    ,A.ACCOUNT_NUMBER                
    ,A.FLAG_RESTRU_COVID19                  
    ,B.SURVIVE_FLAG
    FROM CURR_RESTRU_SIFAT_EXISTING_ACCT_' || P_RUNID || ' A                    
    LEFT JOIN PREV_SURVIVE_' || P_RUNID || ' B ON  A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER AND A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TEMP_IFRS_IMP_DEFAULT_STATUS_' || P_RUNID || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE TEMP_IFRS_IMP_DEFAULT_STATUS_' || P_RUNID || ' AS 
    SELECT ''' || CAST(V_PREVMONTH AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE                              
    ,A.MASTERID                              
    ,A.ACCOUNT_NUMBER                              
    ,A.PRODUCT_CODE                              
    ,A.CUSTOMER_NUMBER                              
    ,A.BI_COLLECTABILITY                              
    ,NULL as BI_COLLECT_CIF                              
    ,A.DAY_PAST_DUE                
    ,NULL AS DPD_CIF
    ,A.DPD_FINAL                              
    ,NULL AS  DPD_FINAL_CIF                              
    ,CASE WHEN C.MASTERID IS NOT NULL THEN 1 ELSE 0 END WO_FLAG                              
    ,NULL AS WO_FLAG_CIF                              
    ,CASE WHEN (B.PREVIOUS_ACCOUNT_NUMBER IS NOT NULL OR F.ACCOUNT_NUMBER IS NOT NULL) THEN 1 ELSE 0 END AS  RESTRU_SIFAT_FLAG                              
    ,NULL AS RESTRU_SIFAT_FLAG_CIF                              
    ,CASE WHEN D.MASTERID IS NULL AND C.MASTERID IS NULL AND B.PREVIOUS_ACCOUNT_NUMBER IS NULL AND A.DAY_PAST_DUE <= 180 THEN 1 ELSE 0 END AS  FP_FLAG                              
    ,A.EXCHANGE_RATE                              
    ,A.PLAFOND                              
    ,A.OUTSTANDING                              
    ,A.MARGIN_RATE                              
    ,A.EIR                              
    ,A.SUB_SEGMENT                      
    ,A.FACILITY_NUMBER                      
    ,CASE WHEN D.MASTERID IS NULL AND C.MASTERID IS NULL AND B.PREVIOUS_ACCOUNT_NUMBER IS NULL  THEN 1 ELSE 0 END AS FP_FLAG_ORIG                                    
    ,CASE WHEN (RTRIM(LTRIM(E.FLAG_RESTRU_COVID19)) = ''Y'' OR RTRIM(LTRIM(F.FLAG_RESTRU_COVID19)) = ''Y'') THEN 1 ELSE 0 END  as FLAG_RESTRU_COVID19                  
    ,CASE                       
    WHEN                      
    ''' || CAST(V_PREVMONTH AS VARCHAR(10)) || '''::DATE < ''' || CAST(V_FLAG_SURVIVE_DATE AS VARCHAR(10)) || '''::DATE                      
    AND (                
    UPPER(LTRIM(RTRIM(E.SURVIVE_FLAG))) NOT IN (''Y'',''N'' )                
    OR UPPER(LTRIM(RTRIM(F.SURVIVE_FLAG))) NOT IN (''Y'',''N'' )                  
    OR E.SURVIVE_FLAG IS NULL OR F.SURVIVE_FLAG IS NULL)                
    AND (E.FLAG_RESTRU_COVID19 = ''Y'' OR F.FLAG_RESTRU_COVID19 = ''Y'')                    
    THEN 1                       
    WHEN                       
    (                
    (COALESCE(E.FLAG_RESTRU_COVID19,''N'') = ''N'' AND COALESCE(F.FLAG_RESTRU_COVID19,''N'') = ''N'')         
    )              
    THEN NULL
    WHEN                       
    (UPPER(LTRIM(RTRIM(E.SURVIVE_FLAG))) = ''Y'' OR UPPER(LTRIM(RTRIM(F.SURVIVE_FLAG))) = ''Y'')               
    THEN 1                          
    ELSE 0                       
    END AS SURVIVE_FLAG                  
    ,NULL AS FLAG_RESTRU_COVID19_CIF          
    ,NULL AS SURVIVE_FLAG_CIF
    FROM ' || V_TABLEINSERT3 || ' A                             
    LEFT JOIN CURR_RESTRU_SIFAT_PREVIOUS_ACCT_' || P_RUNID || ' B  ON A.ACCOUNT_NUMBER = B.PREVIOUS_ACCOUNT_NUMBER  AND A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER                          
    LEFT JOIN CURR_WO_' || P_RUNID || ' C ON A.MASTERID = C.MASTERID                       
    LEFT JOIN ' || V_TABLEINSERT2 || ' D ON A.MASTERID = D.MASTERID                       
    LEFT JOIN PREV_SURVIVE_RESTRU_' || P_RUNID || ' E ON A.ACCOUNT_NUMBER = E.PREVIOUS_ACCOUNT_NUMBER  AND A.CUSTOMER_NUMBER = E.CUSTOMER_NUMBER
    LEFT JOIN CURR_SURVIVE_RESTRU_' || P_RUNID || ' F ON A.ACCOUNT_NUMBER = F.ACCOUNT_NUMBER  AND A.CUSTOMER_NUMBER = F.CUSTOMER_NUMBER
    WHERE A.DATA_SOURCE <> ''LIMIT''';
    EXECUTE (V_STR_QUERY);
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS CIF_LEVEL_' || P_RUNID || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE CIF_LEVEL_' || P_RUNID || ' AS 
    SELECT CUSTOMER_NUMBER,SUB_SEGMENT, MAX(WO_FLAG) AS WO_FLAG_CIF, MAX(RESTRU_SIFAT_FLAG) AS RESTRU_SIFAT_FLAG_CIF, MIN(FP_FLAG) AS FP_FLAG_CIF                      
    ,MAX(BI_COLLECTABILITY) AS BI_COLLECT_CIF, MAX(DAY_PAST_DUE) AS DPD_CIF, MAX(DPD_FINAL) AS DPD_FINAL_CIF, MIN(FP_FLAG_ORIG) AS FP_FLAG_ORIG_CIF                   
    ,(FLAG_RESTRU_COVID19_CIF::INTEGER = MAX(FLAG_RESTRU_COVID19) ) AS FLAG_RESTRU_COVID19_CIF        
    ,(SURVIVE_FLAG_CIF::INTEGER = MIN(SURVIVE_FLAG) ) AS SURVIVE_FLAG_CIF                                                
    FROM TEMP_IFRS_IMP_DEFAULT_STATUS_' || P_RUNID || '
    GROUP BY CUSTOMER_NUMBER, SUB_SEGMENT, FLAG_RESTRU_COVID19_CIF, SURVIVE_FLAG_CIF';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE TEMP_IFRS_IMP_DEFAULT_STATUS_' || P_RUNID || ' A                              
    SET WO_FLAG_CIF = B.WO_FLAG_CIF
    ,RESTRU_SIFAT_FLAG_CIF = B.RESTRU_SIFAT_FLAG_CIF
    ,BI_COLLECT_CIF = B.BI_COLLECT_CIF
    ,DPD_CIF = B.DPD_CIF
    ,DPD_FINAL_CIF = B.DPD_FINAL_CIF                      
    ,FLAG_RESTRU_COVID19_CIF = B.FLAG_RESTRU_COVID19_CIF
    ,SURVIVE_FLAG_CIF = B.SURVIVE_FLAG_CIF                            
    FROM CIF_LEVEL_' || P_RUNID || ' B WHERE A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER AND A.SUB_SEGMENT = B.SUB_SEGMENT';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE TEMP_IFRS_IMP_DEFAULT_STATUS_' || P_RUNID || ' A                              
    SET FP_FLAG = B.FP_FLAG_CIF
    ,FP_FLAG_ORIG = B.FP_FLAG_ORIG_CIF                                  
    FROM CIF_LEVEL_' || P_RUNID || ' B WHERE A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER AND A.SUB_SEGMENT= B.SUB_SEGMENT                      
    AND A.SUB_SEGMENT IN (SELECT DISTINCT VALUE1 FROM TBLM_COMMONCODEDETAIL WHERE COMMONCODE = ''B154'')';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT4 || ' WHERE DOWNLOAD_DATE = ''' || CAST(V_PREVMONTH AS VARCHAR(10)) || '''::DATE';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT4 || '                              
    (                      
    DOWNLOAD_DATE                              
    ,MASTERID                              
    ,ACCOUNT_NUMBER                              
    ,PRODUCT_CODE                              
    ,CUSTOMER_NUMBER                              
    ,BI_COLLECTABILITY                              
    ,BI_COLLECT_CIF                              
    ,DAY_PAST_DUE                              
    ,DPD_CIF                              
    ,DPD_FINAL                              
    ,DPD_FINAL_CIF                              
    ,WO_FLAG                              
    ,WO_FLAG_CIF                              
    ,RESTRU_SIFAT_FLAG                              
    ,RESTRU_SIFAT_FLAG_CIF                              
    ,FP_FLAG                              
    ,EXCHANGE_RATE                              
    ,PLAFOND                              
    ,OUTSTANDING                         
    ,MARGIN_RATE                              
    ,EIR                      
    ,SUB_SEGMENT                      
    ,FP_FLAG_ORIG                      
    ,FACILITY_NUMBER                      
    ,FLAG_RESTRU_COVID19                      
    ,SURVIVE_FLAG            
    ,FLAG_RESTRU_COVID19_CIF                    
    ,SURVIVE_FLAG_CIF          
    )                              
    SELECT                               
    DOWNLOAD_DATE                     
    ,MASTERID                              
    ,ACCOUNT_NUMBER                              
    ,PRODUCT_CODE                              
    ,CUSTOMER_NUMBER                              
    ,BI_COLLECTABILITY                              
    ,BI_COLLECT_CIF::INTEGER                              
    ,DAY_PAST_DUE                              
    ,DPD_CIF::INTEGER                              
    ,DPD_FINAL                              
    ,DPD_FINAL_CIF::INTEGER                              
    ,WO_FLAG                              
    ,WO_FLAG_CIF::INTEGER                          
    ,RESTRU_SIFAT_FLAG                              
    ,RESTRU_SIFAT_FLAG_CIF::INTEGER                              
    ,FP_FLAG                              
    ,EXCHANGE_RATE                              
    ,PLAFOND                              
    ,OUTSTANDING                              
    ,MARGIN_RATE                              
    ,EIR                      
    ,SUB_SEGMENT                      
    ,FP_FLAG_ORIG                      
    ,FACILITY_NUMBER                      
    ,FLAG_RESTRU_COVID19                      
    ,SURVIVE_FLAG                      
    ,FLAG_RESTRU_COVID19_CIF::INTEGER                    
    ,SURVIVE_FLAG_CIF::INTEGER          
    FROM TEMP_IFRS_IMP_DEFAULT_STATUS_' || P_RUNID || '';
    EXECUTE (V_STR_QUERY);                                                                                                           

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    RAISE NOTICE 'SP_IFRS_IMP_INSERT_DEFAULT | AFFECTED RECORD : %', V_RETURNROWS2;
    -------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT4;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_IMP_INSERT_DEFAULT';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT4 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;