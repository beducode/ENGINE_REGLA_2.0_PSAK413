---- DROP PROCEDURE SP_IFRS_SYNC_CORPORATE_DATA;

CREATE OR REPLACE PROCEDURE SP_IFRS_SYNC_CORPORATE_DATA(
    IN P_RUNID VARCHAR(20) DEFAULT 'S_00000_0000', 
    IN P_DOWNLOAD_DATE DATE DEFAULT NULL,
    IN P_PRC VARCHAR(1) DEFAULT 'S')
LANGUAGE PLPGSQL 
AS $$
DECLARE
    ---- DATE
    V_CURRMONTH DATE;
    V_PREVDATE DATE;
    V_PREVMONTH DATE;
    V_CURRDATE DATE;
    V_LASTYEAR DATE;
    V_LASTYEARNEXTMONTH DATE;
    V_STARTDATEOFYEAR DATE;

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
    V_TABLEINSERT8 VARCHAR(100);
    V_TABLEINSERT9 VARCHAR(100);
    V_TABLEINSERT10 VARCHAR(100);
    V_TABLEINSERT11 VARCHAR(100);
    V_TABLEINSERT12 VARCHAR(100);
    V_TABLEINSERT13 VARCHAR(100);
    V_TABLEINSERT14 VARCHAR(100);
    V_TABLEINSERT15 VARCHAR(100);
    V_TABLEINSERT16 VARCHAR(100);
    V_TABLEINSERT17 VARCHAR(100);
    V_TABLEINSERT18 VARCHAR(100);

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
    V_CHECKROWS INT;
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
        V_TABLEINSERT1 := 'IFRS_PD_MASTERSCALE_CORP';
        V_TABLEINSERT3 := 'IFRS_RECOVERY_CORP';
        V_TABLEINSERT5 := 'IFRS_MASTER_LIMIT_CORP_UPLOAD';
        V_TABLEINSERT7 := 'IFRS_JUDGEMENT_RATING_TREASURY';
        V_TABLEINSERT9 := 'IFRS_PD_EXTERNAL_TREASURY';
        V_TABLEINSERT10 := 'IFRS_PD_EXTERNAL_MAPPING';
        V_TABLEINSERT13 := 'IFRS_RECOVERY_RATE_TREASURY';
        V_TABLEINSERT15 := 'IFRS_CUSTOMER_GRADING_CORP';
        V_TABLEINSERT17 := 'IFRS_ASSET_CLASSIFICATION_CORP';
    ELSE 
        V_TABLEINSERT1 := 'IFRS_PD_MASTERSCALE_CORP';
        V_TABLEINSERT3 := 'IFRS_RECOVERY_CORP';
        V_TABLEINSERT5 := 'IFRS_MASTER_LIMIT_CORP_UPLOAD';
        V_TABLEINSERT7 := 'IFRS_JUDGEMENT_RATING_TREASURY';
        V_TABLEINSERT9 := 'IFRS_PD_EXTERNAL_TREASURY';
        V_TABLEINSERT10 := 'IFRS_PD_EXTERNAL_MAPPING';
        V_TABLEINSERT13 := 'IFRS_RECOVERY_RATE_TREASURY';
        V_TABLEINSERT15 := 'IFRS_CUSTOMER_GRADING_CORP';
        V_TABLEINSERT17 := 'IFRS_ASSET_CLASSIFICATION_CORP';
    END IF;

    V_TABLEINSERT2 := 'TBLU_PD_MASTERSCALE';
    V_TABLEINSERT4 := 'TBLU_RECOVERY';
    V_TABLEINSERT6 := 'TBLU_LIMIT_CORPORATE';
    V_TABLEINSERT8 := 'TBLU_JUDGEMENT_RATING_TREASURY';
    V_TABLEINSERT11 := 'TBLU_PD_EXTERNAL_PEFINDO';
    V_TABLEINSERT12 := 'TBLU_PD_EXTERNAL_SNP';
    V_TABLEINSERT14 := 'TBLU_RECOVERY_RATE_TREASURY';
    V_TABLEINSERT16 := 'TBLU_CUSTOMER_GRADING';
    V_TABLEINSERT18 := 'TBLU_ASSET_CLASSIFICATION_CORP';

    IF P_DOWNLOAD_DATE IS NULL 
    THEN
        SELECT
            CURRDATE INTO V_CURRDATE
        FROM
            IFRS_PRC_DATE;
    ELSE        
        V_CURRDATE := P_DOWNLOAD_DATE;
    END IF;
    
    V_PREVDATE := V_CURRDATE - INTERVAL '1 DAY';
    V_CURRMONTH := F_EOMONTH(V_CURRDATE, 0, 'M', 'NEXT');
    V_PREVMONTH := F_EOMONTH(V_CURRDATE, 1, 'M', 'PREV');
    V_LASTYEAR := F_EOMONTH(V_CURRDATE, 1, 'Y', 'PREV');
    V_LASTYEARNEXTMONTH := F_EOMONTH(V_LASTYEAR, 1, 'M', 'NEXT');
    V_STARTDATEOFYEAR := (DATE_PART('YEAR', V_CURRDATE) || '-01-01')::DATE;
    
    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    -------- RECORD RUN_ID --------
    CALL SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, PG_BACKEND_PID(), CURRENT_DATE);
    -------- RECORD RUN_ID --------

    ----- MAIN LOGIC -----

    ----------------------------------------      
    ---- START IFRS_PD_MASTERSCALE_CORP ----      
    ----------------------------------------

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''';
    EXECUTE (V_STR_QUERY);

    V_CHECKROWS := 0;
    EXECUTE 'SELECT COUNT(*) FROM ' || V_TABLEINSERT2 || ' WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || ''' LIMIT 1 ' INTO V_CHECKROWS;

    IF(V_CHECKROWS > 0) THEN

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT ' || V_TABLEINSERT1 || '       
        (        
        DOWNLOAD_DATE        
        ,OBLIGOR_GRADE        
        ,PD        
        ,CREATEDBY        
        ,CREATEDDATE        
        ,CREATEDHOST         
        )        
        SELECT         
        DOWNLOAD_DATE        
        ,OBLIGOR_GRADE        
        ,PD::DOUBLE PRECISION        
        ,CREATEDBY        
        ,CREATEDDATE        
        ,CREATEDHOST         
        FROM ' || V_TABLEINSERT2 || '        
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''';
        EXECUTE (V_STR_QUERY);

    ELSE

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || '        
        (        
        DOWNLOAD_DATE        
        ,OBLIGOR_GRADE        
        ,PD        
        ,CREATEDBY        
        ,CREATEDDATE        
        ,CREATEDHOST         
        )        
        SELECT         
        ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''AS DOWNLOAD_DATE        
        ,OBLIGOR_GRADE        
        ,PD::DOUBLE PRECISION        
        ,CREATEDBY        
        ,CREATEDDATE        
        ,CREATEDHOST         
        FROM ' || V_TABLEINSERT2 || '        
        WHERE DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''';
        EXECUTE (V_STR_QUERY);

    END IF;

    ----------------------------------      
    ---- START IFRS_RECOVERY_CORP ----      
    ---------------------------------- 

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT3 || ' WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRMONTH AS VARCHAR(10)) || '''';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT3 || '        
    (        
    DOWNLOAD_DATE        
    ,DEFAULT_DATE        
    ,OS_AT_DEFAULT        
    ,CUSTOMER_NUMBER        
    ,CUSTOMER_NAME        
    ,PRODUCT_GROUP      
    ,CURRENCY      
    ,RECOVERY_DATE        
    ,NETT_RECOVERY        
    ,EIR_AT_DEFAULT        
    ,JAP_NON_JAP_IDENTIFIER        
    ,CREATEDBY        
    ,CREATEDDATE        
    ,CREATEDHOST        
    )        
    SELECT         
    CASE WHEN F_EOMONTH(DOWNLOAD_DATE::DATE, 0, ''M'', ''NEXT'') <= ''20110131''THEN ''20110131'' ELSE F_EOMONTH(DOWNLOAD_DATE::DATE, 0, ''M'', ''NEXT'') END AS DOWNLOAD_DATE      
    ,DEFAULT_DATE::DATE        
    ,OS_AT_DEFAULT::INT        
    ,CUSTOMER_NUMBER        
    ,CUSTOMER_NAME        
    ,PRODUCT_GROUP      
    ,CURRENCY        
    ,RECOVERY_DATE::DATE        
    ,NETT_RECOVERY::INT      
    ,OEIR_AT_DEFAULT::INT    
    ,JAP_NON_JAP_IDENTIFIER        
    ,CREATEDBY        
    ,CREATEDDATE        
    ,CREATEDHOST   
    FROM ' || V_TABLEINSERT4 ||'        
    WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRMONTH AS VARCHAR(10)) || '''';
    EXECUTE (V_STR_QUERY);

    ---------------------------------------------      
    ---- START IFRS_MASTER_LIMIT_CORP_UPLOAD ----      
    --------------------------------------------- 

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT5 || ' WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT5 || '      
    (      
    DOWNLOAD_DATE      
    ,DATA_SOURCE      
    ,LIMIT_FLAG      
    ,BRANCH_CODE      
    ,ACCOUNT_NUMBER      
    ,PRODUCT_CODE      
    ,CUSTOMER_NUMBER      
    ,CURRENCY      
    ,BI_COLLECTABILITY      
    ,MATURITY_DATE      
    ,PLAFOND      
    ,UNUSED_LIMIT      
    ,OUTSTANDING      
    )      
    SELECT      
    DOWNLOAD_DATE::DATE      
    ,''LIMIT_T24'' AS DATA_SOURCE      
    ,CASE FAC_COMMIT WHEN ''Y'' THEN 1 ELSE 0 END AS LIMIT_FLAG      
    ,''0800'' AS BRANCH_CODE      
    ,LIMIT_ID AS ACCOUNT_NUMBER      
    ,LIMIT_PRODUCT AS PRODUCT_CODE      
    ,CIF_NO AS CUSTOMER_NUMBER      
    ,LIMIT_CURRENCY AS CURRENCY      
    ,COLLECTIBILITY::INT AS BI_COLLECTABILITY      
    ,EXPIRY_DATE::DATE AS MATURITY_DATE      
    ,STG_LIMIT_AMT::NUMERIC AS PLAFOND      
    ,UNDRAWN_FINAL::NUMERIC AS UNUSED_LIMIT      
    ,STG_TOTAL_OS::NUMERIC AS OUTSTANDING      
    FROM ' || V_TABLEINSERT6 || '      
    WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRMONTH AS VARCHAR(10)) || '''';
    EXECUTE (V_STR_QUERY);


    ----------------------------------------------      
    ---- START IFRS_JUDGEMENT_RATING_TREASURY ----      
    ---------------------------------------------- 

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT7 || ' WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT7 || '      
    (      
    DOWNLOAD_DATE      
    ,MASTERID      
    ,EXTERNAL_RATING_AGENCY      
    ,EXTERNAL_RATING_CODE      
    )      
    SELECT      
    DOWNLOAD_DATE::DATE      
    ,DEAL_NO      
    ,EXTERNAL_RATING_AGENCY      
    ,EXTERNAL_RATING_CODE      
    FROM ' || V_TABLEINSERT8 || '      
    WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRMONTH AS VARCHAR(10)) || '''';
    EXECUTE (V_STR_QUERY);


    -----------------------------------------      
    ---- START IFRS_PD_EXTERNAL_TREASURY ----      
    ----------------------------------------- 

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT9 || ' WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRMONTH AS VARCHAR(10)) || ''' ';
    EXECUTE (V_STR_QUERY);

    V_CHECKROWS := 0;
    EXECUTE 'SELECT SUM(COUNTS) FROM (
    SELECT COUNT(*) AS COUNTS FROM ' || V_TABLEINSERT11 || ' WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''
    UNION ALL 
    SELECT COUNT(*) AS COUNTS FROM ' || V_TABLEINSERT12 || ' WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''
    ) A ' INTO V_CHECKROWS;

    IF (V_CHECKROWS > 0) THEN

            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT10 || ' WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT10 || '      
            (      
            DOWNLOAD_DATE      
            ,SEGMENT      
            ,PEFINDO_RATING_CODE      
            ,PEFINDO_PD_RATE      
            ,SNP_PD_RATE      
            )      
            SELECT       
            A.DOWNLOAD_DATE,      
            A.SEGMENT,      
            A.RATING_CODE AS PEFINDO_RATING_CODE,       
            CAST(A.CUMMULATIVE_PD AS DOUBLE PRECISION) AS PEFINDO_PD_RATE,       
            MAX(CAST(B.CUMMULATIVE_PD AS DOUBLE PRECISION)) AS SNP_PD_RATE      
            FROM ' || V_TABLEINSERT11 || ' A      
            JOIN ' || V_TABLEINSERT12 || ' B       
            ON A.DOWNLOAD_DATE = B.DOWNLOAD_DATE       
            AND A.SEGMENT = B.SEGMENT       
            AND A.REMAINING_TENOR_YEAR = B.REMAINING_TENOR_YEAR      
            WHERE A.REMAINING_TENOR_YEAR = ''1''       
            AND CAST(B.CUMMULATIVE_PD AS DOUBLE PRECISION) <= CAST(A.CUMMULATIVE_PD AS DOUBLE PRECISION)      
            AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''     
            GROUP BY A.DOWNLOAD_DATE, A.SEGMENT, A.RATING_AGENCY_CODE, A.RATING_CODE, A.CUMMULATIVE_PD      
            ORDER BY A.RATING_CODE ';
            EXECUTE (V_STR_QUERY);


            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT10 || ' A      
            SET SNP_RATING_CODE = B.RATING_CODE      
            FROM (      
            SELECT DOWNLOAD_DATE, CUMMULATIVE_PD, SEGMENT, MAX(RATING_CODE) AS RATING_CODE, REMAINING_TENOR_YEAR       
            FROM ' || V_TABLEINSERT12 || '       
            WHERE REMAINING_TENOR_YEAR = ''1'' AND DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''     
            GROUP BY DOWNLOAD_DATE, CUMMULATIVE_PD, SEGMENT, REMAINING_TENOR_YEAR      
            ) B      
            WHERE A.DOWNLOAD_DATE = B.DOWNLOAD_DATE       
            AND A.SNP_PD_RATE = B.CUMMULATIVE_PD      
            AND A.SEGMENT = B.SEGMENT      
            AND B.REMAINING_TENOR_YEAR = ''1'' AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT9 || '      
            (      
            DOWNLOAD_DATE      
            ,SEGMENT      
            ,RATING_CODE      
            ,REMAINING_TENOR_YEAR      
            ,CUMMULATIVE_PD      
            ,PEFINDO_RATING_CODE      
            ,SnP_RATING_CODE      
            ,PEFINDO_PD_RATE      
            ,SnP_PD_RATE       
            )      
            SELECT       
            A.DOWNLOAD_DATE      
            ,A.SEGMENT      
            ,RATING_CODE      
            ,REMAINING_TENOR_YEAR      
            ,(CAST(B.SNP_PD_RATE AS DOUBLE PRECISION) / 100) AS CUMMULATIVE_PD      
            ,B.PEFINDO_RATING_CODE      
            ,B.SnP_RATING_CODE      
            ,(CAST(B.PEFINDO_PD_RATE AS DOUBLE PRECISION) / 100) AS PEFINDO_PD_RATE      
            ,(CAST(B.SnP_PD_RATE AS DOUBLE PRECISION) / 100) AS SnP_PD_RATE      
            FROM ' || V_TABLEINSERT11 || ' A       
            LEFT JOIN ' || V_TABLEINSERT10 || ' B      
            ON A.DOWNLOAD_DATE = B.DOWNLOAD_DATE      
            AND A.SEGMENT = B.SEGMENT      
            AND A.REMAINING_TENOR_YEAR = 1      
            AND A.RATING_CODE = B.PEFINDO_RATING_CODE      
            WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRMONTH AS VARCHAR(10)) || '''     
            ORDER BY CAST(A.REMAINING_TENOR_YEAR AS INT), B.PEFINDO_RATING_CODE ';
            EXECUTE (V_STR_QUERY);


            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT9 || ' A      
            SET PEFINDO_RATING_CODE = B.PEFINDO_RATING_CODE,
            SnP_RATING_CODE = B.SnP_RATING_CODE,
            PEFINDO_PD_RATE = B.PEFINDO_PD_RATE,
            SnP_PD_RATE = B.SnP_PD_RATE      
            FROM (      
            SELECT DOWNLOAD_DATE, SEGMENT, REMAINING_TENOR_YEAR, PEFINDO_RATING_CODE, PEFINDO_PD_RATE, SNP_RATING_CODE, SNP_PD_RATE      
            FROM ' || V_TABLEINSERT9 || '      
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''     
            AND REMAINING_TENOR_YEAR = ''1''      
            ) B      
            WHERE A.DOWNLOAD_DATE = B.DOWNLOAD_DATE      
            AND A.SEGMENT = B.SEGMENT      
            AND A.RATING_CODE = B.PEFINDO_RATING_CODE      
            AND A.REMAINING_TENOR_YEAR > 1      
            AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''';
            EXECUTE (V_STR_QUERY);


            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT9 || ' A      
            SET CUMMULATIVE_PD = (CAST(B.CUMMULATIVE_PD AS DOUBLE PRECISION) / 100)      
            FROM TBLU_PD_EXTERNAL_SNP B      
            WHERE A.DOWNLOAD_DATE = B.DOWNLOAD_DATE      
            AND A.SEGMENT = B.SEGMENT      
            AND A.SnP_RATING_CODE = B.RATING_CODE      
            AND A.REMAINING_TENOR_YEAR = B.REMAINING_TENOR_YEAR      
            AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''';
            EXECUTE (V_STR_QUERY);


            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT9 || ' A      
            SET MARGINAL_PD = B.MARGINAL      
            FROM (      
            SELECT CAST(CAST(CUMMULATIVE_PD AS DOUBLE PRECISION) AS NUMERIC(32,6)) - CAST(COALESCE(LAG(CAST(CUMMULATIVE_PD AS DOUBLE PRECISION)) OVER (PARTITION BY DOWNLOAD_DATE, SEGMENT, RATING_CODE ORDER BY CAST(REMAINING_TENOR_YEAR AS INT)), 0) AS NUMERIC(32,6)) AS MARGINAL      
            , *      
            FROM ' || V_TABLEINSERT9 || '      
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''     
            ) B 
            WHERE A.DOWNLOAD_DATE = B.DOWNLOAD_DATE      
            AND A.SEGMENT = B.SEGMENT      
            AND A.RATING_CODE = B.RATING_CODE      
            AND A.REMAINING_TENOR_YEAR = B.REMAINING_TENOR_YEAR      
            AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''';
            EXECUTE (V_STR_QUERY);

        ELSE

            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT9 || '      
            (      
            DOWNLOAD_DATE      
            ,SEGMENT      
            ,RATING_CODE      
            ,REMAINING_TENOR_YEAR      
            ,CUMMULATIVE_PD      
            ,MARGINAL_PD      
            ,PEFINDO_RATING_CODE      
            ,SnP_RATING_CODE      
            ,PEFINDO_PD_RATE      
            ,SnP_PD_RATE        
            )      
            SELECT       
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE      
            ,SEGMENT      
            ,RATING_CODE      
            ,REMAINING_TENOR_YEAR      
            ,CUMMULATIVE_PD      
            ,MARGINAL_PD       
            ,PEFINDO_RATING_CODE      
            ,SnP_RATING_CODE      
            ,PEFINDO_PD_RATE      
            ,SnP_PD_RATE           
            FROM ' || V_TABLEINSERT9 || ' WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRMONTH AS VARCHAR(10)) || '''';
            EXECUTE (V_STR_QUERY);
    END IF;

    -------------------------------------------      
    ---- START TBLU_RECOVERY_RATE_TREASURY ----      
    -------------------------------------------

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT13 || ' WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_CHECKROWS := 0;
    EXECUTE 'SELECT COUNT(*) FROM ' || V_TABLEINSERT14 || '  WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || ''' ' INTO V_CHECKROWS;

    IF (V_CHECKROWS > 0) THEN

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT13 || '      
        (      
        DOWNLOAD_DATE      
        ,SEGMENT      
        ,RECOVERY_RATE       
        )      
        SELECT       
        DOWNLOAD_DATE      
        ,SEGMENT      
        ,CAST(RECOVERY_RATE AS DOUBLE PRECISION) / 100 AS RECOVERY_RATE      
        FROM ' || V_TABLEINSERT14 || '      
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRMONTH AS VARCHAR(10)) || ''' ';
        EXECUTE (V_STR_QUERY);

    ELSE

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT13 || '      
        (      
        DOWNLOAD_DATE      
        ,SEGMENT      
        ,RECOVERY_RATE       
        )      
        SELECT       
        ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE      
        ,SEGMENT      
        ,RECOVERY_RATE    
        FROM ' || V_TABLEINSERT13 || '      
        WHERE DOWNLOAD_DATE = ''' || CAST(V_PREVMONTH AS VARCHAR(10)) || ''' ';
        EXECUTE (V_STR_QUERY);

    END IF;

    ------------------------------------------      
    ---- START IFRS_CUSTOMER_GRADING_CORP ----      
    ------------------------------------------   

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT15 || ' WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || ' INSERT INTO ' || V_TABLEINSERT15 || '       
    (      
    DOWNLOAD_DATE      
    ,CUSTOMER_NUMBER        
    ,SANDI_BANK      
    ,OBLIGOR_GRADE      
    ,JAP_NON_JAP_IDENTIFIER      
    ,WATCH_LIST_FLAG      
    ,CREATEDBY      
    ,CREATEDDATE      
    )      
    SELECT      
    DOWNLOAD_DATE::DATE       
    ,CUSTOMER_NUMBER        
    ,SANDI_BANK           
    ,UPPER(MAX(OBLIGOR_GRADE))      
    ,MAX(JAP_NON_JAP_IDENTIFIER)      
    ,MAX(CASE WHEN WATCH_LIST_FLAG = 1 THEN 1 ELSE 0 END)      
    ,''SP_IFRS_SYNC_CORPORATE_DATA'' AS CREATEDBY      
    ,CURRENT_DATE AS CREATEDDATE      
    FROM ' || V_TABLEINSERT16 || '       
    WHERE DOWNLOAD_DATE::DATE  = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE     
    GROUP BY DOWNLOAD_DATE, CUSTOMER_NUMBER, SANDI_BANK  ';
    EXECUTE (V_STR_QUERY);


    ----------------------------------      
    ---- START IFRS_SYNC_TREASURY ----      
    ----------------------------------
    CALL SP_IFRS_SYNC_TREASURY(P_RUNID, V_CURRDATE, P_PRC);

    --------------------------------      
    ---- START SYNC ASSET CLASS CORP ----      
    --------------------------------   
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT17 || ' WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO  IFRS_ASSET_CLASSIFICATION_CORP (DOWNLOAD_DATE        
    ,FACILITY_NUMBER        
    ,SPPI_RESULT        
    ,BM_RESULT)        
    SELECT DOWNLOAD_DATE::DATE        
    ,FACILITY_NUMBER        
    ,SPPI_RESULT        
    ,CASE WHEN BM_RESULT = ''OTHERS'' THEN ''TRADE''        
    ELSE BM_RESULT END AS BM_RESULT        
    FROM ' || V_TABLEINSERT18 || ' WHERE DOWNLOAD_DATE::DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    ---- GET RECORD
    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    ----- MAIN LOGIC -----

    RAISE NOTICE 'SP_IFRS_SYNC_CORPORATE_DATA | | AFFECTED RECORD : %', V_RETURNROWS2;
    
    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT1;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_SYNC_CORPORATE_DATA';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT1 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    ------ ====== RESULT ======
    
END;

$$;