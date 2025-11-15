---- DROP PROCEDURE SP_IFRS_EXCEPTION_REPORT;

CREATE OR REPLACE PROCEDURE SP_IFRS_EXCEPTION_REPORT(
    IN P_RUNID VARCHAR(20) DEFAULT 'S_00000_0000',
    IN P_DOWNLOAD_DATE DATE DEFAULT NULL,
    IN P_PRC VARCHAR(1) DEFAULT 'S',
    IN P_FLAG VARCHAR(1) DEFAULT 'A')
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

    IF COALESCE(P_FLAG, NULL) IS NULL THEN
        P_FLAG := 'A';
    END IF;

    IF P_PRC = 'S' THEN 
        V_TABLENAME := 'TMP_IMA_' || P_RUNID || '';
        V_TABLEINSERT1 := 'IFRS_EXCEPTION_ACCOUNT_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_IMA_AMORT_CURR_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_IMA_IMP_CURR_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_JOURNAL_PARAM_' || P_RUNID || '';
        V_TABLEINSERT5 := 'IFRS_PRODUCT_PARAM_' || P_RUNID || '';
        V_TABLEINSERT6 := 'IFRS_TRANSACTION_DAILY_' || P_RUNID || '';
        V_TABLEINSERT7 := 'IFRS_TRANSACTION_PARAM_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'IFRS_EXCEPTION_ACCOUNT';
        V_TABLEINSERT2 := 'IFRS_IMA_AMORT_CURR';
        V_TABLEINSERT3 := 'IFRS_IMA_IMP_CURR';
        V_TABLEINSERT4 := 'IFRS_JOURNAL_PARAM';
        V_TABLEINSERT5 := 'IFRS_PRODUCT_PARAM';
        V_TABLEINSERT6 := 'IFRS_TRANSACTION_DAILY';
        V_TABLEINSERT7 := 'IFRS_TRANSACTION_PARAM';
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
    
    V_PREVMONTH := F_EOMONTH(V_PREVDATE, 1, 'M', 'PREV');
    V_CURRMONTH := F_EOMONTH(V_CURRDATE, 0, 'M', 'NEXT');
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
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT1 || ' AS SELECT * FROM IFRS_EXCEPTION_ACCOUNT WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT2 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT2 || ' AS SELECT * FROM IFRS_IMA_AMORT_CURR WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT3 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT3 || ' AS SELECT * FROM IFRS_IMA_IMP_CURR WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT4 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT4 || ' AS SELECT * FROM IFRS_JOURNAL_PARAM';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT5 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT5 || ' AS SELECT * FROM IFRS_PRODUCT_PARAM';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT5 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT5 || ' AS SELECT * FROM IFRS_PRODUCT_PARAM';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT6 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT6 || ' AS SELECT * FROM IFRS_TRANSACTION_DAILY';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT7 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT7 || ' AS SELECT * FROM IFRS_TRANSACTION_PARAM';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_EXCEPTION_REPORT', '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND FLAG = ''' || P_FLAG || ''' ';
    EXECUTE (V_STR_QUERY);

    IF P_FLAG = 'A' THEN 
        -- PRODUCT PARAMETER NOT DEFINED YET      
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || 'IFRS_MASTER_EXCEPTION' || ' C, ' || V_TABLEINSERT5 || ' D
            WHERE ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''IMA_PRODUCT''
                AND ' || V_TABLEINSERT1 || '.VALUE = D.PRD_CODE ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,             
                EXCEPTION_ID, 
                TABLE_NAME,  
                FIELD_NAME,   
                VALUE,          
                FLAG   
            ) SELECT DISTINCT            
                A.DOWNLOAD_DATE,
                A.DATA_SOURCE, 
                COALESCE(C.PKID,0),
                ''IFRS_MASTER_PRODUCT_PARAM'',
                ''PRD_CODE'',
                A.PRODUCT_CODE,          
                ''' || P_FLAG || '''          
            FROM ' || V_TABLEINSERT2 || ' A
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C 
                ON C.EXCEPTION_CODE = ''IMA_PRODUCT''
            LEFT JOIN ' || V_TABLEINSERT5 || ' D 
                ON A.DATA_SOURCE = D.DATA_SOURCE
                AND A.PRODUCT_TYPE = D.PRD_TYPE 
                AND A.PRODUCT_CODE = D.PRD_CODE
                AND (A.CURRENCY = D.CCY OR D.CCY = ''ALL'')            
            WHERE D.PRD_CODE IS NULL            
                AND A.PRODUCT_CODE NOT IN (
                    SELECT VALUE 
                    FROM ' || V_TABLEINSERT1 || ' X 
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y 
                    ON X.EXCEPTION_ID = Y.PKID 
                    WHERE Y.EXCEPTION_CODE = ''IMA_PRODUCT''
                )            
                AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        -- TRANSACTION PARAMETER NOT DEFINED YET    
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || V_TABLEINSERT7 || ' B, ' || 'IFRS_MASTER_EXCEPTION' || ' C
            WHERE ' || V_TABLEINSERT1 || '.VALUE = B.TRX_CODE
                AND ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''AMT_TRX01'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,
                EXCEPTION_ID,  
                TABLE_NAME,   
                FIELD_NAME,   
                VALUE,          
                FLAG   
            ) SELECT DISTINCT           
                X.DOWNLOAD_DATE,
                X.DATA_SOURCE,
                COALESCE(C.PKID,0),
                ''IFRS_TRANSACTION_PARAM'',
                ''TRX_CODE'', 
                X.TRX_CODE,          
                ''' || P_FLAG || '''  
            FROM ' || V_TABLEINSERT6 || ' X   
            LEFT JOIN ' || V_TABLEINSERT7 || ' B 
                ON (X.DATA_SOURCE = B.DATA_SOURCE OR COALESCE(B.DATA_SOURCE, ''ALL'') = ''ALL'')
                AND (X.PRD_CODE = B.PRD_CODE OR COALESCE(B.PRD_CODE, ''ALL'') = ''ALL'')
                AND X.TRX_CODE = B.TRX_CODE
                AND (X.CCY = B.CCY OR B.CCY = ''ALL'')
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C 
                ON C.EXCEPTION_CODE = ''AMT_TRX01''
            WHERE X.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE            
                AND X.TRX_CODE NOT IN (
                    SELECT VALUE 
                    FROM ' || V_TABLEINSERT1 || ' X 
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y 
                    ON X.EXCEPTION_ID = Y.PKID 
                    WHERE Y.EXCEPTION_CODE = ''AMT_TRX01''
                )            
                AND B.TRX_CODE IS NULL
            GROUP BY X.DOWNLOAD_DATE, COALESCE(C.PKID,0), X.TRX_CODE, X.DATA_SOURCE ';
        EXECUTE (V_STR_QUERY);

        -- JOURNAL PARAMETER NOT DEFINED YET EIR ACCRU DEBIT FEE  
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || V_TABLEINSERT4 || ' B, ' || 'IFRS_MASTER_EXCEPTION' || ' C
            WHERE ' || V_TABLEINSERT1 || '.VALUE = B.GL_CONSTNAME
                AND B.DRCR = ''D'' 
                AND B.JOURNALCODE = ''ACCRU''
                AND B.FLAG_CF = ''F''
                AND ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''AMT_JOURNAL_ACCRU_DB_FEE'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,  
                EXCEPTION_ID,
                TABLE_NAME,   
                FIELD_NAME,   
                VALUE,          
                FLAG   
            ) SELECT DISTINCT            
                A.DOWNLOAD_DATE,
                A.DATA_SOURCE,
                COALESCE(C.PKID,0),             
                ''IFRS_JOURNAL_PARAM'',   
                ''GL_CONSTNAME'',   
                A.GL_CONSTNAME,          
                ''' || P_FLAG || '''  
            FROM ' || V_TABLEINSERT2 || ' A
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C 
                ON C.EXCEPTION_CODE = ''AMT_JOURNAL_ACCRU_DB_FEE''
            LEFT JOIN ' || V_TABLEINSERT4 || ' D 
                ON A.GL_CONSTNAME = D.GL_CONSTNAME   
                AND (A.CURRENCY = D.CCY OR D.CCY = ''ALL'') 
                AND D.DRCR = ''D'' 
                AND D.JOURNALCODE = ''ACCRU''
                AND D.FLAG_CF = ''F''
            WHERE A.AMORT_TYPE = ''EIR''             
                AND D.GL_CONSTNAME IS NULL             
                AND A.GL_CONSTNAME NOT IN (
                    SELECT VALUE 
                    FROM ' || V_TABLEINSERT1 || ' X 
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y 
                    ON X.EXCEPTION_ID = Y.PKID 
                    WHERE Y.EXCEPTION_CODE = ''AMT_JOURNAL_ACCRU_DB_FEE''
                )            
                AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        -- JOURNAL PARAMETER NOT DEFINED YET EIR ACCRU CREDIT FEE     
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || V_TABLEINSERT4 || ' B, ' || 'IFRS_MASTER_EXCEPTION' || ' C
            WHERE ' || V_TABLEINSERT1 || '.VALUE = B.GL_CONSTNAME
                AND B.DRCR = ''C'' 
                AND B.JOURNALCODE = ''ACCRU''
                AND B.FLAG_CF = ''F''
                AND ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''AMT_JOURNAL_ACCRU_CR_FEE'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,   
                EXCEPTION_ID,
                TABLE_NAME,  
                FIELD_NAME,   
                VALUE,          
                FLAG
            ) SELECT DISTINCT             
                A.DOWNLOAD_DATE,
                A.DATA_SOURCE,
                COALESCE(C.PKID,0),             
                ''IFRS_JOURNAL_PARAM'',   
                ''GL_CONSTNAME'',   
                A.GL_CONSTNAME,          
                ''' || P_FLAG || '''  
            FROM ' || V_TABLEINSERT2 || ' A
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C 
                ON C.EXCEPTION_CODE = ''AMT_JOURNAL_ACCRU_CR_FEE''
            LEFT JOIN ' || V_TABLEINSERT4 || ' D 
                ON A.GL_CONSTNAME = D.GL_CONSTNAME   
                AND (A.CURRENCY = D.CCY OR D.CCY = ''ALL'') 
                AND D.DRCR = ''C'' 
                AND D.JOURNALCODE = ''ACCRU''
                AND D.FLAG_CF = ''F''
            WHERE             
                A.GL_CONSTNAME NOT IN (
                    SELECT VALUE 
                    FROM ' || V_TABLEINSERT1 || ' X 
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y 
                    ON X.EXCEPTION_ID = Y.PKID 
                    WHERE Y.EXCEPTION_CODE = ''AMT_JOURNAL_ACCRU_CR_FEE''
                )            
                AND A.AMORT_TYPE = ''EIR''             
                AND D.GL_CONSTNAME IS NULL            
                AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        -- JOURNAL PARAMETER NOT DEFINED YET EIR ACCRU DEBIT COST
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || V_TABLEINSERT4 || ' B, ' || 'IFRS_MASTER_EXCEPTION' || ' C
            WHERE ' || V_TABLEINSERT1 || '.VALUE = B.GL_CONSTNAME
                AND B.DRCR = ''D''
                AND B.JOURNALCODE = ''ACCRU''
                AND B.FLAG_CF = ''C''
                AND ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''AMT_JOURNAL_ACCRU_DB_COST'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || '
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,
                EXCEPTION_ID,
                TABLE_NAME,
                FIELD_NAME,
                VALUE,
                FLAG
            ) SELECT DISTINCT
                A.DOWNLOAD_DATE,
                A.DATA_SOURCE,
                COALESCE(C.PKID,0),
                ''IFRS_JOURNAL_PARAM'',
                ''GL_CONSTNAME'',
                A.GL_CONSTNAME,
                ''' || P_FLAG || '''
            FROM ' || V_TABLEINSERT2 || ' A
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C
                ON C.EXCEPTION_CODE = ''AMT_JOURNAL_ACCRU_DB_COST''
            LEFT JOIN ' || V_TABLEINSERT4 || ' D
                ON A.GL_CONSTNAME = D.GL_CONSTNAME
                AND (A.CURRENCY = D.CCY OR D.CCY = ''ALL'')
                AND D.DRCR = ''D''
                AND D.JOURNALCODE = ''ACCRU''
                AND D.FLAG_CF = ''C''
            WHERE
                A.GL_CONSTNAME NOT IN (
                    SELECT VALUE
                    FROM ' || V_TABLEINSERT1 || ' X
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y
                    ON X.EXCEPTION_ID = Y.PKID
                    WHERE Y.EXCEPTION_CODE = ''AMT_JOURNAL_ACCRU_DB_COST''
                )
                AND A.AMORT_TYPE = ''EIR''
                AND D.GL_CONSTNAME IS NULL
                AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        -- JOURNAL PARAMETER NOT DEFINED YET EIR ACCRU CREDIT COST
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || V_TABLEINSERT4 || ' B, ' || 'IFRS_MASTER_EXCEPTION' || ' C
            WHERE ' || V_TABLEINSERT1 || '.VALUE = B.GL_CONSTNAME
                AND B.DRCR = ''C''
                AND B.JOURNALCODE = ''ACCRU''
                AND B.FLAG_CF = ''C''
                AND ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''AMT_JOURNAL_ACCRU_CR_COST'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || '
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,
                EXCEPTION_ID,
                TABLE_NAME,
                FIELD_NAME,
                VALUE,
                FLAG
            ) SELECT DISTINCT
                A.DOWNLOAD_DATE,
                A.DATA_SOURCE,
                COALESCE(C.PKID,0),
                ''IFRS_JOURNAL_PARAM'',
                ''GL_CONSTNAME'',
                A.GL_CONSTNAME,
                ''' || P_FLAG || '''
            FROM ' || V_TABLEINSERT2 || ' A
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C
                ON C.EXCEPTION_CODE = ''AMT_JOURNAL_ACCRU_CR_COST''
            LEFT JOIN ' || V_TABLEINSERT4 || ' D
                ON A.GL_CONSTNAME = D.GL_CONSTNAME
                AND (A.CURRENCY = D.CCY OR D.CCY = ''ALL'')
                AND D.DRCR = ''C''
                AND D.JOURNALCODE = ''ACCRU''
                AND D.FLAG_CF = ''C''
            WHERE
                A.GL_CONSTNAME NOT IN (
                    SELECT VALUE
                    FROM ' || V_TABLEINSERT1 || ' X
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y
                    ON X.EXCEPTION_ID = Y.PKID
                    WHERE Y.EXCEPTION_CODE = ''AMT_JOURNAL_ACCRU_CR_COST''
                )
                AND A.AMORT_TYPE = ''EIR''
                AND D.GL_CONSTNAME IS NULL
                AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        -- JOURNAL PARAMETER NOT DEFINED YET SL ACCRU DEBIT FEE
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || V_TABLEINSERT4 || ' B, ' || 'IFRS_MASTER_EXCEPTION' || ' C
            WHERE ' || V_TABLEINSERT1 || '.VALUE = B.GL_CONSTNAME
                AND B.DRCR = ''D''
                AND B.JOURNALCODE = ''ACCRU_SL''
                AND B.FLAG_CF = ''F''
                AND ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''AMT_JOURNAL_ACCRU_SL_DB_FEE'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || '
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,
                EXCEPTION_ID,
                TABLE_NAME,
                FIELD_NAME,
                VALUE,
                FLAG
            ) SELECT DISTINCT
                A.DOWNLOAD_DATE,
                A.DATA_SOURCE,
                COALESCE(C.PKID,0),
                ''IFRS_JOURNAL_PARAM'',
                ''GL_CONSTNAME'',
                A.GL_CONSTNAME,
                ''' || P_FLAG || '''
            FROM ' || V_TABLEINSERT2 || ' A
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C
                ON C.EXCEPTION_CODE = ''AMT_JOURNAL_ACCRU_SL_DB_FEE''
            LEFT JOIN ' || V_TABLEINSERT4 || ' D
                ON A.GL_CONSTNAME = D.GL_CONSTNAME
                AND (A.CURRENCY = D.CCY OR D.CCY = ''ALL'')
                AND D.DRCR = ''D''
                AND D.JOURNALCODE = ''ACCRU_SL''
                AND D.FLAG_CF = ''F''
            WHERE
                A.GL_CONSTNAME NOT IN (
                    SELECT VALUE
                    FROM ' || V_TABLEINSERT1 || ' X
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y
                    ON X.EXCEPTION_ID = Y.PKID
                    WHERE Y.EXCEPTION_CODE = ''AMT_JOURNAL_ACCRU_SL_DB_FEE''
                )
                AND A.AMORT_TYPE = ''SL''
                AND D.GL_CONSTNAME IS NULL
                AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        -- JOURNAL PARAMETER NOT DEFINED YET SL ACCRU CREDIT FEE
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || V_TABLEINSERT4 || ' B, ' || 'IFRS_MASTER_EXCEPTION' || ' C
            WHERE ' || V_TABLEINSERT1 || '.VALUE = B.GL_CONSTNAME
                AND B.DRCR = ''C''
                AND B.JOURNALCODE = ''ACCRU_SL''
                AND B.FLAG_CF = ''F''
                AND ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''AMT_JOURNAL_ACCRU_SL_CR_FEE'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || '
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,
                EXCEPTION_ID,
                TABLE_NAME,
                FIELD_NAME,
                VALUE,
                FLAG
            ) SELECT DISTINCT
                A.DOWNLOAD_DATE,
                A.DATA_SOURCE,
                COALESCE(C.PKID,0),
                ''IFRS_JOURNAL_PARAM'',
                ''GL_CONSTNAME'',
                A.GL_CONSTNAME,
                ''' || P_FLAG || '''
            FROM ' || V_TABLEINSERT2 || ' A
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C
                ON C.EXCEPTION_CODE = ''AMT_JOURNAL_ACCRU_SL_CR_FEE''
            LEFT JOIN ' || V_TABLEINSERT4 || ' D
                ON A.GL_CONSTNAME = D.GL_CONSTNAME
                AND (A.CURRENCY = D.CCY OR D.CCY = ''ALL'')
                AND D.DRCR = ''C''
                AND D.JOURNALCODE = ''ACCRU_SL''
                AND D.FLAG_CF = ''F''
            WHERE
                A.GL_CONSTNAME NOT IN (
                    SELECT VALUE
                    FROM ' || V_TABLEINSERT1 || ' X
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y
                    ON X.EXCEPTION_ID = Y.PKID
                    WHERE Y.EXCEPTION_CODE = ''AMT_JOURNAL_ACCRU_SL_CR_FEE''
                )
                AND A.AMORT_TYPE = ''SL''
                AND D.GL_CONSTNAME IS NULL
                AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        -- JOURNAL PARAMETER NOT DEFINED YET SL ACCRU DEBIT COST
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || V_TABLEINSERT4 || ' B, ' || 'IFRS_MASTER_EXCEPTION' || ' C
            WHERE ' || V_TABLEINSERT1 || '.VALUE = B.GL_CONSTNAME
                AND B.DRCR = ''D''
                AND B.JOURNALCODE = ''ACCRU_SL''
                AND B.FLAG_CF = ''C''
                AND ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''AMT_JOURNAL_ACCRU_SL_DB_COST'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || '
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,
                EXCEPTION_ID,
                TABLE_NAME,
                FIELD_NAME,
                VALUE,
                FLAG
            ) SELECT DISTINCT
                A.DOWNLOAD_DATE,
                A.DATA_SOURCE,
                COALESCE(C.PKID,0),
                ''IFRS_JOURNAL_PARAM'',
                ''GL_CONSTNAME'',
                A.GL_CONSTNAME,
                ''' || P_FLAG || '''
            FROM ' || V_TABLEINSERT2 || ' A
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C
                ON C.EXCEPTION_CODE = ''AMT_JOURNAL_ACCRU_SL_DB_COST''
            LEFT JOIN ' || V_TABLEINSERT4 || ' D
                ON A.GL_CONSTNAME = D.GL_CONSTNAME
                AND (A.CURRENCY = D.CCY OR D.CCY = ''ALL'')
                AND D.DRCR = ''D''
                AND D.JOURNALCODE = ''ACCRU_SL''
                AND D.FLAG_CF = ''C''
            WHERE
                A.GL_CONSTNAME NOT IN (
                    SELECT VALUE
                    FROM ' || V_TABLEINSERT1 || ' X
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y
                    ON X.EXCEPTION_ID = Y.PKID
                    WHERE Y.EXCEPTION_CODE = ''AMT_JOURNAL_ACCRU_SL_DB_COST''
                )
                AND A.AMORT_TYPE = ''SL''
                AND D.GL_CONSTNAME IS NULL
                AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        -- JOURNAL PARAMETER NOT DEFINED YET SL ACCRU CREDIT COST
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || V_TABLEINSERT4 || ' B, ' || 'IFRS_MASTER_EXCEPTION' || ' C
            WHERE ' || V_TABLEINSERT1 || '.VALUE = B.GL_CONSTNAME
                AND B.DRCR = ''C''
                AND B.JOURNALCODE = ''ACCRU_SL''
                AND B.FLAG_CF = ''C''
                AND ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''AMT_JOURNAL_ACCRU_SL_CR_COST'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || '
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,
                EXCEPTION_ID,
                TABLE_NAME,
                FIELD_NAME,
                VALUE,
                FLAG
            ) SELECT DISTINCT
                A.DOWNLOAD_DATE,
                A.DATA_SOURCE,
                COALESCE(C.PKID,0),
                ''IFRS_JOURNAL_PARAM'',
                ''GL_CONSTNAME'',
                A.GL_CONSTNAME,
                ''' || P_FLAG || '''
            FROM ' || V_TABLEINSERT2 || ' A
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C
                ON C.EXCEPTION_CODE = ''AMT_JOURNAL_ACCRU_SL_CR_COST''
            LEFT JOIN ' || V_TABLEINSERT4 || ' D
                ON A.GL_CONSTNAME = D.GL_CONSTNAME
                AND (A.CURRENCY = D.CCY OR D.CCY = ''ALL'')
                AND D.DRCR = ''C''
                AND D.JOURNALCODE = ''ACCRU_SL''
                AND D.FLAG_CF = ''C''
            WHERE
                A.GL_CONSTNAME NOT IN (
                    SELECT VALUE
                    FROM ' || V_TABLEINSERT1 || ' X
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y
                    ON X.EXCEPTION_ID = Y.PKID
                    WHERE Y.EXCEPTION_CODE = ''AMT_JOURNAL_ACCRU_SL_CR_COST''
                )
                AND A.AMORT_TYPE = ''SL''
                AND D.GL_CONSTNAME IS NULL
                AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        -- JOURNAL PARAMETER NOT DEFINED YET SL ITCRG DEBIT FEE
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || V_TABLEINSERT4 || ' B, ' || 'IFRS_MASTER_EXCEPTION' || ' C
            WHERE ' || V_TABLEINSERT1 || '.VALUE = B.GL_CONSTNAME
                AND B.DRCR = ''D''
                AND B.JOURNALCODE LIKE ''ITRCG_SL''
                AND B.FLAG_CF = ''F''
                AND ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''AMT_JOURNAL_ITRCG_SL_DB_FEE'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || '
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,
                EXCEPTION_ID,
                TABLE_NAME,
                FIELD_NAME,
                VALUE,
                FLAG
            ) SELECT DISTINCT
                A.DOWNLOAD_DATE,
                A.DATA_SOURCE,
                COALESCE(C.PKID,0),
                ''IFRS_JOURNAL_PARAM'',
                ''GL_CONSTNAME'',
                A.GL_CONSTNAME,
                ''' || P_FLAG || '''
            FROM ' || V_TABLEINSERT2 || ' A
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C
                ON C.EXCEPTION_CODE = ''AMT_JOURNAL_ITRCG_SL_DB_FEE''
            LEFT JOIN ' || V_TABLEINSERT4 || ' D
                ON A.GL_CONSTNAME = D.GL_CONSTNAME
                AND (A.CURRENCY = D.CCY OR D.CCY = ''ALL'')
                AND D.DRCR = ''D''
                AND D.JOURNALCODE LIKE ''ITRCG_SL''
                AND D.FLAG_CF = ''F''
            WHERE
                A.GL_CONSTNAME NOT IN (
                    SELECT VALUE
                    FROM ' || V_TABLEINSERT1 || ' X
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y
                    ON X.EXCEPTION_ID = Y.PKID
                    WHERE Y.EXCEPTION_CODE = ''AMT_JOURNAL_ITRCG_SL_DB_FEE''
                )
                AND A.AMORT_TYPE = ''SL''
                AND D.GL_CONSTNAME IS NULL
                AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        -- JOURNAL PARAMETER NOT DEFINED YET SL ITCRG CREDIT FEE
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || V_TABLEINSERT4 || ' B, ' || 'IFRS_MASTER_EXCEPTION' || ' C
            WHERE ' || V_TABLEINSERT1 || '.VALUE = B.GL_CONSTNAME
                AND B.DRCR = ''C''
                AND B.JOURNALCODE LIKE ''ITRCG_SL''
                AND B.FLAG_CF = ''F''
                AND ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''AMT_JOURNAL_ITRCG_SL_CR_FEE'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || '
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,
                EXCEPTION_ID,
                TABLE_NAME,
                FIELD_NAME,
                VALUE,
                FLAG
            ) SELECT DISTINCT
                A.DOWNLOAD_DATE,
                A.DATA_SOURCE,
                COALESCE(C.PKID,0),
                ''IFRS_JOURNAL_PARAM'',
                ''GL_CONSTNAME'',
                A.GL_CONSTNAME,
                ''' || P_FLAG || '''
            FROM ' || V_TABLEINSERT2 || ' A
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C
                ON C.EXCEPTION_CODE = ''AMT_JOURNAL_ITRCG_SL_CR_FEE''
            LEFT JOIN ' || V_TABLEINSERT4 || ' D
                ON A.GL_CONSTNAME = D.GL_CONSTNAME
                AND (A.CURRENCY = D.CCY OR D.CCY = ''ALL'')
                AND D.DRCR = ''C''
                AND D.JOURNALCODE LIKE ''ITRCG_SL''
                AND D.FLAG_CF = ''F''
            WHERE
                A.GL_CONSTNAME NOT IN (
                    SELECT VALUE
                    FROM ' || V_TABLEINSERT1 || ' X
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y
                    ON X.EXCEPTION_ID = Y.PKID
                    WHERE Y.EXCEPTION_CODE = ''AMT_JOURNAL_ITRCG_SL_CR_FEE''
                )
                AND A.AMORT_TYPE = ''SL''
                AND D.GL_CONSTNAME IS NULL
                AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        -- JOURNAL PARAMETER NOT DEFINED YET SL ITCRG DEBIT COST
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || V_TABLEINSERT4 || ' B, ' || 'IFRS_MASTER_EXCEPTION' || ' C
            WHERE ' || V_TABLEINSERT1 || '.VALUE = B.GL_CONSTNAME
                AND B.DRCR = ''D''
                AND B.JOURNALCODE LIKE ''ITRCG_SL''
                AND B.FLAG_CF = ''C''
                AND ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''AMT_JOURNAL_ITRCG_SL_DB_COST'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || '
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,
                EXCEPTION_ID,
                TABLE_NAME,
                FIELD_NAME,
                VALUE,
                FLAG
            ) SELECT DISTINCT
                A.DOWNLOAD_DATE,
                A.DATA_SOURCE,
                COALESCE(C.PKID,0),
                ''IFRS_JOURNAL_PARAM'',
                ''GL_CONSTNAME'',
                A.GL_CONSTNAME,
                ''' || P_FLAG || '''
            FROM ' || V_TABLEINSERT2 || ' A
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C
                ON C.EXCEPTION_CODE = ''AMT_JOURNAL_ITRCG_SL_DB_COST''
            LEFT JOIN ' || V_TABLEINSERT4 || ' D
                ON A.GL_CONSTNAME = D.GL_CONSTNAME
                AND (A.CURRENCY = D.CCY OR D.CCY = ''ALL'')
                AND D.DRCR = ''D''
                AND D.JOURNALCODE LIKE ''ITRCG_SL''
                AND D.FLAG_CF = ''C''
            WHERE
                A.GL_CONSTNAME NOT IN (
                    SELECT VALUE
                    FROM ' || V_TABLEINSERT1 || ' X
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y
                    ON X.EXCEPTION_ID = Y.PKID
                    WHERE Y.EXCEPTION_CODE = ''AMT_JOURNAL_ITRCG_SL_DB_COST''
                )
                AND A.AMORT_TYPE = ''SL''
                AND D.GL_CONSTNAME IS NULL
                AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        -- JOURNAL PARAMETER NOT DEFINED YET SL ITCRG CREDIT COST
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || V_TABLEINSERT4 || ' B, ' || 'IFRS_MASTER_EXCEPTION' || ' C
            WHERE ' || V_TABLEINSERT1 || '.VALUE = B.GL_CONSTNAME
                AND B.DRCR = ''C''
                AND B.JOURNALCODE LIKE ''ITRCG_SL''
                AND B.FLAG_CF = ''C''
                AND ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''AMT_JOURNAL_ITRCG_SL_CR_COST'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || '
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,
                EXCEPTION_ID,
                TABLE_NAME,
                FIELD_NAME,
                VALUE,
                FLAG
            ) SELECT DISTINCT
                A.DOWNLOAD_DATE,
                A.DATA_SOURCE,
                COALESCE(C.PKID,0),
                ''IFRS_JOURNAL_PARAM'',
                ''GL_CONSTNAME'',
                A.GL_CONSTNAME,
                ''' || P_FLAG || '''
            FROM ' || V_TABLEINSERT2 || ' A
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C
                ON C.EXCEPTION_CODE = ''AMT_JOURNAL_ITRCG_SL_CR_COST''
            LEFT JOIN ' || V_TABLEINSERT4 || ' D
                ON A.GL_CONSTNAME = D.GL_CONSTNAME
                AND (A.CURRENCY = D.CCY OR D.CCY = ''ALL'')
                AND D.DRCR = ''C''
                AND D.JOURNALCODE LIKE ''ITRCG_SL''
                AND D.FLAG_CF = ''C''
            WHERE
                A.GL_CONSTNAME NOT IN (
                    SELECT VALUE
                    FROM ' || V_TABLEINSERT1 || ' X
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y
                    ON X.EXCEPTION_ID = Y.PKID
                    WHERE Y.EXCEPTION_CODE = ''AMT_JOURNAL_ITRCG_SL_CR_COST''
                )
                AND A.AMORT_TYPE = ''SL''
                AND D.GL_CONSTNAME IS NULL
                AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        -- JOURNAL PARAMETER NOT DEFINED YET EIR ITCRG DEBIT FEE
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || V_TABLEINSERT4 || ' B, ' || 'IFRS_MASTER_EXCEPTION' || ' C
            WHERE ' || V_TABLEINSERT1 || '.VALUE = B.GL_CONSTNAME
                AND B.DRCR = ''D''
                AND B.JOURNALCODE LIKE ''ITRCG''
                AND B.FLAG_CF = ''F''
                AND ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''AMT_JOURNAL_ITRCG_DB_FEE'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || '
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,
                EXCEPTION_ID,
                TABLE_NAME,
                FIELD_NAME,
                VALUE,
                FLAG
            ) SELECT DISTINCT
                A.DOWNLOAD_DATE,
                A.DATA_SOURCE,
                COALESCE(C.PKID,0),
                ''IFRS_JOURNAL_PARAM'',
                ''GL_CONSTNAME'',
                A.GL_CONSTNAME,
                ''' || P_FLAG || '''
            FROM ' || V_TABLEINSERT2 || ' A
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C
                ON C.EXCEPTION_CODE = ''AMT_JOURNAL_ITRCG_DB_FEE''
            LEFT JOIN ' || V_TABLEINSERT4 || ' D
                ON A.GL_CONSTNAME = D.GL_CONSTNAME
                AND (A.CURRENCY = D.CCY OR D.CCY = ''ALL'')
                AND D.DRCR = ''D''
                AND D.JOURNALCODE LIKE ''ITRCG''
                AND D.FLAG_CF = ''F''
            WHERE
                A.GL_CONSTNAME NOT IN (
                    SELECT VALUE
                    FROM ' || V_TABLEINSERT1 || ' X
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y
                    ON X.EXCEPTION_ID = Y.PKID
                    WHERE Y.EXCEPTION_CODE = ''AMT_JOURNAL_ITRCG_DB_FEE''
                )
                AND A.AMORT_TYPE = ''EIR''
                AND D.GL_CONSTNAME IS NULL
                AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        -- JOURNAL PARAMETER NOT DEFINED YET EIR ITCRG CREDIT FEE
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || V_TABLEINSERT4 || ' B, ' || 'IFRS_MASTER_EXCEPTION' || ' C
            WHERE ' || V_TABLEINSERT1 || '.VALUE = B.GL_CONSTNAME
                AND B.DRCR = ''C''
                AND B.JOURNALCODE LIKE ''ITRCG''
                AND B.FLAG_CF = ''F''
                AND ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''AMT_JOURNAL_ITRCG_CR_FEE'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || '
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,
                EXCEPTION_ID,
                TABLE_NAME,
                FIELD_NAME,
                VALUE,
                FLAG
            ) SELECT
                A.DOWNLOAD_DATE,
                A.DATA_SOURCE,
                COALESCE(C.PKID,0),
                ''IFRS_JOURNAL_PARAM'',
                ''GL_CONSTNAME'',
                A.GL_CONSTNAME,
                ''' || P_FLAG || '''
            FROM ' || V_TABLEINSERT2 || ' A
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C
                ON C.EXCEPTION_CODE = ''AMT_JOURNAL_ITRCG_CR_FEE''
            LEFT JOIN ' || V_TABLEINSERT4 || ' D
                ON A.GL_CONSTNAME = D.GL_CONSTNAME
                AND (A.CURRENCY = D.CCY OR D.CCY = ''ALL'')
                AND D.DRCR = ''C''
                AND D.JOURNALCODE LIKE ''ITRCG''
                AND D.FLAG_CF = ''F''
            WHERE
                A.GL_CONSTNAME NOT IN (
                    SELECT VALUE
                    FROM ' || V_TABLEINSERT1 || ' X
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y
                    ON X.EXCEPTION_ID = Y.PKID
                    WHERE Y.EXCEPTION_CODE = ''AMT_JOURNAL_ITRCG_CR_FEE''
                )
                AND A.AMORT_TYPE = ''EIR''
                AND D.GL_CONSTNAME IS NULL
                AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        -- JOURNAL PARAMETER NOT DEFINED YET EIR ITCRG DEBIT COST
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || V_TABLEINSERT4 || ' B, ' || 'IFRS_MASTER_EXCEPTION' || ' C
            WHERE ' || V_TABLEINSERT1 || '.VALUE = B.GL_CONSTNAME
                AND B.DRCR = ''D''
                AND B.JOURNALCODE LIKE ''ITRCG''
                AND B.FLAG_CF = ''C''
                AND ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''AMT_JOURNAL_ITRCG_DB_COST'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || '
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,
                EXCEPTION_ID,
                TABLE_NAME,
                FIELD_NAME,
                VALUE,
                FLAG
            ) SELECT DISTINCT
                A.DOWNLOAD_DATE,
                A.DATA_SOURCE,
                COALESCE(C.PKID,0),
                ''IFRS_JOURNAL_PARAM'',
                ''GL_CONSTNAME'',
                A.GL_CONSTNAME,
                ''' || P_FLAG || '''
            FROM ' || V_TABLEINSERT2 || ' A
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C
                ON C.EXCEPTION_CODE = ''AMT_JOURNAL_ITRCG_DB_COST''
            LEFT JOIN ' || V_TABLEINSERT4 || ' D
                ON A.GL_CONSTNAME = D.GL_CONSTNAME
                AND (A.CURRENCY = D.CCY OR D.CCY = ''ALL'')
                AND D.DRCR = ''D''
                AND D.JOURNALCODE LIKE ''ITRCG''
                AND D.FLAG_CF = ''C''
            WHERE
                A.GL_CONSTNAME NOT IN (
                    SELECT VALUE
                    FROM ' || V_TABLEINSERT1 || ' X
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y
                    ON X.EXCEPTION_ID = Y.PKID
                    WHERE Y.EXCEPTION_CODE = ''AMT_JOURNAL_ITRCG_DB_COST''
                )
                AND A.AMORT_TYPE = ''EIR''
                AND D.GL_CONSTNAME IS NULL
                AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        -- JOURNAL PARAMETER NOT DEFINED YET EIR ITCRG CREDIT COST
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || V_TABLEINSERT4 || ' B, ' || 'IFRS_MASTER_EXCEPTION' || ' C
            WHERE ' || V_TABLEINSERT1 || '.VALUE = B.GL_CONSTNAME
                AND B.DRCR = ''C''
                AND B.JOURNALCODE LIKE ''ITRCG''
                AND B.FLAG_CF = ''C''
                AND ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''AMT_JOURNAL_ITRCG_CR_COST'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || '
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,
                EXCEPTION_ID,
                TABLE_NAME,
                FIELD_NAME,
                VALUE,
                FLAG
            ) SELECT DISTINCT
                A.DOWNLOAD_DATE,
                A.DATA_SOURCE,
                COALESCE(C.PKID,0),
                ''IFRS_JOURNAL_PARAM'',
                ''GL_CONSTNAME'',
                A.GL_CONSTNAME,
                ''' || P_FLAG || '''
            FROM ' || V_TABLEINSERT2 || ' A
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C
                ON C.EXCEPTION_CODE = ''AMT_JOURNAL_ITRCG_CR_COST''
            LEFT JOIN ' || V_TABLEINSERT4 || ' D
                ON A.GL_CONSTNAME = D.GL_CONSTNAME
                AND (A.CURRENCY = D.CCY OR D.CCY = ''ALL'')
                AND D.DRCR = ''C''
                AND D.JOURNALCODE LIKE ''ITRCG''
                AND D.FLAG_CF = ''C''
            WHERE
                A.GL_CONSTNAME NOT IN (
                    SELECT VALUE
                    FROM ' || V_TABLEINSERT1 || ' X
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y
                    ON X.EXCEPTION_ID = Y.PKID
                    WHERE Y.EXCEPTION_CODE = ''AMT_JOURNAL_ITRCG_CR_COST''
                )
                AND A.AMORT_TYPE = ''EIR''
                AND D.GL_CONSTNAME IS NULL
                AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        -- AMT_IFRS9_CLASS IS null
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || 'IFRS_MASTER_EXCEPTION' || ' C
            WHERE ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''AMT_IFRS9_CLASS'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || '
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,
                EXCEPTION_ID,
                TABLE_NAME,
                FIELD_NAME,
                VALUE,
                FLAG
            ) SELECT DISTINCT
                DOWNLOAD_DATE,
                A.DATA_SOURCE,
                COALESCE(C.PKID,0),
                ''IFRS_MASTER_ACCOUNT'',
                ''IFRS9_CLASS'',
                IFRS9_CLASS,
                ''' || P_FLAG || '''
            FROM ' || V_TABLEINSERT2 || ' A
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C
                ON C.EXCEPTION_CODE = ''AMT_IFRS9_CLASS''
            WHERE A.IFRS9_CLASS IS NULL
                AND A.MASTERID NOT IN (
                    SELECT MASTERID
                    FROM ' || V_TABLEINSERT1 || ' X
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y
                    ON X.EXCEPTION_ID = Y.PKID
                    WHERE Y.EXCEPTION_CODE = ''AMT_IFRS9_CLASS''
                )
                AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                AND A.DATA_SOURCE <> ''LIMIT''
            GROUP BY DOWNLOAD_DATE, COALESCE(C.PKID,0), IFRS9_CLASS, A.DATA_SOURCE ';
        EXECUTE (V_STR_QUERY);

        -- IMA_GL_CONSTNAME IS null
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || 'IFRS_MASTER_EXCEPTION' || ' C
            WHERE ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''IMA_GL_CONSTNAME'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || '
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,
                EXCEPTION_ID,
                TABLE_NAME,
                FIELD_NAME,
                VALUE,
                FLAG
            ) SELECT
                A.DOWNLOAD_DATE,
                A.DATA_SOURCE,
                COALESCE(C.PKID,0),
                ''IFRS_MASTER_ACCOUNT'',
                ''GL_CONSTNAME'',
                A.GL_CONSTNAME,
                ''' || P_FLAG || '''
            FROM ' || V_TABLEINSERT2 || ' A
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C
                ON C.EXCEPTION_CODE = ''IMA_GL_CONSTNAME''
            WHERE A.GL_CONSTNAME IS NULL
                AND A.MASTERID NOT IN (
                    SELECT MASTERID
                    FROM ' || V_TABLEINSERT1 || ' X
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y
                    ON X.EXCEPTION_ID = Y.PKID
                    WHERE Y.EXCEPTION_CODE = ''IMA_GL_CONSTNAME''
                )
                AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            GROUP BY A.DOWNLOAD_DATE, COALESCE(C.PKID,0), A.GL_CONSTNAME, A.DATA_SOURCE ';
        EXECUTE (V_STR_QUERY);

        -- AMT_JOURNAL_RCLS
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || 'IFRS_MASTER_EXCEPTION' || ' C
            WHERE ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''AMT_JOURNAL_RCLS'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || '
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,
                EXCEPTION_ID,
                TABLE_NAME,
                FIELD_NAME,
                VALUE,
                FLAG
            ) SELECT
                A.DOWNLOAD_DATE,
                A.DATA_SOURCE,
                COALESCE(C.PKID,0),
                ''IFRS_JOURNAL_PARAM'',
                ''GL_CONSTNAME'',
                A.GL_CONSTNAME,
                ''' || P_FLAG || '''
            FROM ' || V_TABLEINSERT2 || ' A
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C
                ON C.EXCEPTION_CODE = ''AMT_JOURNAL_RCLS''
            LEFT JOIN ' || V_TABLEINSERT4 || ' D
                ON A.GL_CONSTNAME = D.GL_CONSTNAME
                AND (A.CURRENCY = D.CCY OR D.CCY = ''ALL'')
                AND D.JOURNALCODE = ''RCLS''
            WHERE D.GL_CONSTNAME IS NULL
                AND A.GL_CONSTNAME NOT IN (
                    SELECT VALUE
                    FROM ' || V_TABLEINSERT1 || ' X
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y
                    ON X.EXCEPTION_ID = Y.PKID
                    WHERE Y.EXCEPTION_CODE = ''AMT_JOURNAL_RCLS''
                )
                AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            GROUP BY A.DOWNLOAD_DATE, COALESCE(C.PKID,0), A.GL_CONSTNAME, A.DATA_SOURCE ';
        EXECUTE (V_STR_QUERY);

        -- AMT_JOURNAL_RCLV
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || 'IFRS_MASTER_EXCEPTION' || ' C
            WHERE ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''AMT_JOURNAL_RCLV'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || '
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,
                EXCEPTION_ID,
                TABLE_NAME,
                FIELD_NAME,
                VALUE,
                FLAG
            ) SELECT
                A.DOWNLOAD_DATE,
                A.DATA_SOURCE,
                COALESCE(C.PKID,0),
                ''IFRS_JOURNAL_PARAM'',
                ''GL_CONSTNAME'',
                A.GL_CONSTNAME,
                ''' || P_FLAG || '''
            FROM ' || V_TABLEINSERT2 || ' A
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C
                ON C.EXCEPTION_CODE = ''AMT_JOURNAL_RCLV''
            LEFT JOIN ' || V_TABLEINSERT4 || ' D
                ON A.GL_CONSTNAME = D.GL_CONSTNAME
                AND (A.CURRENCY = D.CCY OR D.CCY = ''ALL'')
                AND D.JOURNALCODE = ''RCLV''
            WHERE D.GL_CONSTNAME IS NULL
                AND A.GL_CONSTNAME NOT IN (
                    SELECT VALUE
                    FROM ' || V_TABLEINSERT1 || ' X
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y
                    ON X.EXCEPTION_ID = Y.PKID
                    WHERE Y.EXCEPTION_CODE = ''AMT_JOURNAL_RCLV''
                )
                AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            GROUP BY A.DOWNLOAD_DATE, COALESCE(C.PKID,0), A.GL_CONSTNAME, A.DATA_SOURCE ';
        EXECUTE (V_STR_QUERY);

    ELSIF P_FLAG = 'I' THEN 
        -- SEGMENT Still NULL
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || 'IFRS_MASTER_EXCEPTION' || ' C
            WHERE ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''SEGMENT'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || '
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,
                EXCEPTION_ID,
                MASTERID,
                TABLE_NAME,
                FIELD_NAME,
                VALUE,
                FLAG
            ) SELECT
                A.DOWNLOAD_DATE,
                A.DATA_SOURCE,
                COALESCE(C.PKID,0),
                MASTERID,
                ''IFRS_MASTER_ACCOUNT'',
                ''SEGMENT'',
                A.SEGMENT,
                ''' || P_FLAG || '''
            FROM ' || V_TABLEINSERT3 || ' A
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C
                ON C.EXCEPTION_CODE = ''SEGMENT''
            WHERE A.SEGMENT IS NULL
                AND A.MASTERID NOT IN (
                    SELECT MASTERID
                    FROM ' || V_TABLEINSERT1 || ' X
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y
                    ON X.EXCEPTION_ID = Y.PKID
                    WHERE Y.EXCEPTION_CODE = ''SEGMENT''
                )
                AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        -- EIR_SEGMENT STILL NULL  
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || 'IFRS_MASTER_EXCEPTION' || ' C
            WHERE ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''EIR_SEGMENT'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,
                EXCEPTION_ID,
                MASTERID,
                TABLE_NAME,   
                FIELD_NAME,
                VALUE,          
                FLAG        
            ) SELECT
                A.DOWNLOAD_DATE,
                A.DATA_SOURCE,
                COALESCE(C.PKID,0),
                MASTERID, 
                ''IFRS_MASTER_ACCOUNT'',
                ''EIR_SEGMENT'',
                A.EIR_SEGMENT,          
                ''' || P_FLAG || '''      
            FROM ' || V_TABLEINSERT3 || ' A   
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C 
                ON C.EXCEPTION_CODE = ''EIR_SEGMENT''
            WHERE A.EIR_SEGMENT IS NULL            
                AND A.MASTERID NOT IN (
                    SELECT MASTERID 
                    FROM ' || V_TABLEINSERT1 || ' X 
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y 
                    ON X.EXCEPTION_ID = Y.PKID 
                    WHERE Y.EXCEPTION_CODE = ''EIR_SEGMENT''
                )            
                AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        -- JOURNAL PARAMETER NOT DEFINED YET BIUW  
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || 'IFRS_MASTER_EXCEPTION' || ' C
            WHERE ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''IMP_JOUNAL_BIUW'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,   
                EXCEPTION_ID,             
                TABLE_NAME,   
                FIELD_NAME,   
                VALUE,          
                FLAG        
            ) SELECT DISTINCT
                A.DOWNLOAD_DATE, 
                A.DATA_SOURCE,  
                COALESCE(C.PKID,0),             
                ''IFRS_IMP_JOURNAL_DATA'',   
                ''GL_CONSTNAME'',   
                A.GL_CONSTNAME,          
                ''' || P_FLAG || '''        
            FROM ' || V_TABLEINSERT3 || ' A
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C 
                ON C.EXCEPTION_CODE = ''IMP_JOUNAL_BIUW''
            LEFT JOIN ' || V_TABLEINSERT4 || ' D 
                ON A.GL_CONSTNAME = D.GL_CONSTNAME 
                AND JOURNALCODE IN (''BIUW'')     
                AND (A.CURRENCY = D.CCY OR D.CCY = ''ALL'') 
            WHERE D.GL_CONSTNAME IS NULL             
                AND A.GL_CONSTNAME NOT IN (
                    SELECT VALUE 
                    FROM ' || V_TABLEINSERT1 || ' X 
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y 
                    ON X.EXCEPTION_ID = Y.PKID 
                    WHERE Y.EXCEPTION_CODE = ''IMP_JOUNAL_BIUW''
                )            
                AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        -- JOURNAL PARAMETER NOT DEFINED YET BKIP  
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || 'IFRS_MASTER_EXCEPTION' || ' C
            WHERE ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''IMP_JOUNAL_BKIP'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,  
                EXCEPTION_ID,
                TABLE_NAME,   
                FIELD_NAME,   
                VALUE,          
                FLAG        
            ) SELECT DISTINCT
                A.DOWNLOAD_DATE, 
                A.DATA_SOURCE, 
                COALESCE(C.PKID,0),
                ''IFRS_IMP_JOURNAL_DATA'',   
                ''GL_CONSTNAME'',   
                A.GL_CONSTNAME,          
                ''' || P_FLAG || '''        
            FROM ' || V_TABLEINSERT3 || ' A
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C 
                ON C.EXCEPTION_CODE = ''IMP_JOUNAL_BKIP''
            LEFT JOIN ' || V_TABLEINSERT4 || ' D 
                ON A.GL_CONSTNAME = D.GL_CONSTNAME 
                AND JOURNALCODE IN (''BKIP'')          
                AND (A.CURRENCY = D.CCY OR D.CCY = ''ALL'') 
            WHERE           
                D.GL_CONSTNAME IS NULL             
                AND A.GL_CONSTNAME NOT IN (
                    SELECT VALUE 
                    FROM ' || V_TABLEINSERT1 || ' X 
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y 
                    ON X.EXCEPTION_ID = Y.PKID 
                    WHERE Y.EXCEPTION_CODE = ''IMP_JOUNAL_BKIP''
                )            
                AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        -- JOURNAL PARAMETER NOT DEFINED YET BKPI  
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            USING ' || 'IFRS_MASTER_EXCEPTION' || ' C
            WHERE ' || V_TABLEINSERT1 || '.EXCEPTION_ID = C.PKID
                AND C.EXCEPTION_CODE = ''IMP_JOUNAL_BKPI'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
            (
                DOWNLOAD_DATE,
                DATA_SOURCE,   
                EXCEPTION_ID,             
                TABLE_NAME,   
                FIELD_NAME,   
                VALUE,          
                FLAG        
            ) SELECT DISTINCT
                A.DOWNLOAD_DATE, 
                A.DATA_SOURCE,  
                COALESCE(C.PKID,0),            
                ''IFRS_IMP_JOURNAL_DATA'',   
                ''GL_CONSTNAME'',   
                A.GL_CONSTNAME,          
                ''' || P_FLAG || '''        
            FROM ' || V_TABLEINSERT3 || ' A
            LEFT JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' C 
                ON C.EXCEPTION_CODE = ''IMP_JOUNAL_BKPI''
            LEFT JOIN ' || V_TABLEINSERT4 || ' D 
                ON A.GL_CONSTNAME = D.GL_CONSTNAME 
                AND JOURNALCODE IN (''BKPI'')          
                AND (A.CURRENCY = D.CCY OR D.CCY = ''ALL'') 
            WHERE D.GL_CONSTNAME IS NULL           
                AND A.GL_CONSTNAME NOT IN (
                    SELECT VALUE 
                    FROM ' || V_TABLEINSERT1 || ' X 
                    JOIN ' || 'IFRS_MASTER_EXCEPTION' || ' Y 
                    ON X.EXCEPTION_ID = Y.PKID 
                    WHERE Y.EXCEPTION_CODE = ''IMP_JOUNAL_BKPI''
                )            
                AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);
    END IF;

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_EXCEPTION_REPORT', '');

END;

$$;