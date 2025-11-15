---- DROP PROCEDURE SP_IFRS_IMP_RECOVERY_SCENARIO_DATA;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMP_RECOVERY_SCENARIO_DATA(
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
    V_TABLELGDCONFIG VARCHAR(100);

    ---- CONDITION
    V_RETURNROWS INT;
    V_RETURNROWS2 INT;
    V_TABLEDEST VARCHAR(100);
    V_COLUMNDEST VARCHAR(100);
    V_SPNAME VARCHAR(100);
    V_OPERATION VARCHAR(100);

    ---- VARIABLE PROCESS
    V_LGD_RULE_ID VARCHAR(50);                              
    V_DEFAULT_RULE_ID VARCHAR(50);                              
    V_SEGMENT VARCHAR(100);                             
    V_SUB_SEGMENT VARCHAR(100);                              
    V_GROUP_SEGMENT VARCHAR(100);                              
    V_CONDITION TEXT;                  
    V_LAG VARCHAR(1);                   
    V_CALC_METHOD VARCHAR(50);

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
        V_TABLEINSERT4 := 'IFRS_RECOVERY_SCENARIO_DATA_' || P_RUNID || '';
        V_TABLELGDCONFIG := 'IFRS_LGD_RULES_CONFIG_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLENAME_MON := 'IFRS_MASTER_ACCOUNT_MONTHLY';
        V_TABLEINSERT1 := 'TMP_IFRS_ECL_IMA';
        V_TABLEINSERT2 := 'IFRS_IMA_IMP_CURR';
        V_TABLEINSERT3 := 'IFRS_IMA_IMP_PREV';
        V_TABLEINSERT4 := 'IFRS_RECOVERY_SCENARIO_DATA';
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

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLELGDCONFIG || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLELGDCONFIG || ' AS SELECT * FROM IFRS_LGD_RULES_CONFIG';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT4 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT4 || ' AS SELECT * FROM IFRS_RECOVERY_SCENARIO_DATA WHERE 0=1';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======
    
    -------- ====== BODY ======
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS REC_' || P_RUNID || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE REC_' || P_RUNID || ' AS 
    SELECT * FROM ' || V_TABLEINSERT4 || ' WHERE 1=2';
    EXECUTE (V_STR_QUERY);

    FOR V_LGD_RULE_ID, V_SEGMENT, V_SUB_SEGMENT, V_GROUP_SEGMENT, V_CONDITION, V_LAG, V_CALC_METHOD IN
        EXECUTE 'SELECT A.PKID, B.SEGMENT, B.SUB_SEGMENT, B.GROUP_SEGMENT, B.CONDITION, A.LAG_1MONTH_FLAG, UPPER(A.CALC_METHOD) AS CALC_METHOD          
        FROM ' || V_TABLELGDCONFIG || ' A                              
        INNER JOIN IFRS_SCENARIO_SEGMENT_GENERATE_QUERY B                              
        ON A.SEGMENTATION_ID = B.RULE_ID                              
        WHERE B.SEGMENT_TYPE = ''LGD_SEGMENT''                       
        AND IS_DELETE = 0                       
        AND ACTIVE_FLAG = 1                       
        AND A.CUT_OFF_DATE <= CASE WHEN A.LAG_1MONTH_FLAG = 1 THEN ''' || CAST(V_PREVMONTH AS VARCHAR(10)) || '''::DATE ELSE ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE END          
        ORDER BY PKID'
    LOOP
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO REC_' || P_RUNID || '                              
        (                                   
        DOWNLOAD_DATE                              
        ,LGD_RULE_ID                              
        ,LGD_RULE_NAME                              
        ,DEFAULT_RULE_ID                              
        ,MASTERID                              
        ,ACCOUNT_NUMBER                              
        ,CUSTOMER_NUMBER                              
        ,SEGMENT                              
        ,SUB_SEGMENT                              
        ,GROUP_SEGMENT                           
        ,LGD_METHOD                              
        ,LGD_UNIQUE_ID                              
        ,CALC_METHOD                              
        ,RECOVERY_AMOUNT                          
        ,BI_COLLECTABILITY                              
        ,DAY_PAST_DUE                             
        ,CREATEDBY                              
        ,CREATEDDATE          
        )                              
        SELECT                  
        A.DOWNLOAD_DATE                              
        ,B.PKID AS LGD_RULE_ID                              
        ,B.LGD_RULE_NAME              
        ,B.DEFAULT_RULE_ID                              
        ,A.MASTERID                              
        ,A.ACCOUNT_NUMBER                              
        ,A.CUSTOMER_NUMBER                              
        ,''' || V_SEGMENT ||''' AS SEGMENT                              
        ,''' || V_SUB_SEGMENT || ''' AS SUB_SEGMENT                              
        ,''' || V_GROUP_SEGMENT || ''' AS GROUP_SEGMENT          
        ,LGD_METHOD                              
        ,CASE CALC_METHOD WHEN ''CUSTOMER'' THEN A.CUSTOMER_NUMBER WHEN ''ACCOUNT'' THEN A.MASTERID END AS LGD_UNIQUE_ID          
        ,UPPER(CALC_METHOD) AS CALC_METHOD                    
        ,A.RECOVERY_AMOUNT          
        ,A.BI_COLLECTABILITY                              
        ,A.DAY_PAST_DUE           
        ,''ADMIN'' AS CREATEDBY                            
        ,CURRENT_DATE AS CREATEDDATE                    
        FROM IFRS_MASTER_WO_RECOVERY A                              
        JOIN ' || V_TABLELGDCONFIG || ' B ON B.PKID = ' || V_LGD_RULE_ID || '                 
        WHERE A.DOWNLOAD_DATE = CASE WHEN B.LAG_1MONTH_FLAG = 1 THEN ''' || CAST(V_PREVMONTH AS VARCHAR(10)) || '''::DATE          
        ELSE ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE END
        ' || CASE WHEN V_GROUP_SEGMENT LIKE '%JENIUS%' THEN 'AND A.CUSTOMER_NUMBER NOT IN (SELECT DISTINCT CUSTOMER_NUMBER FROM IFRS_EXCLUDE_JENIUS) ' ELSE '' END || 'AND ' || REPLACE(V_CONDITION,'"','') || '';
        EXECUTE (V_STR_QUERY);
    
    END LOOP;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT4 || ' A                            
    USING ' || V_TABLELGDCONFIG || ' B                            
    WHERE A.LGD_RULE_ID = B.PKID                            
    AND DOWNLOAD_DATE = CASE WHEN B.LAG_1MONTH_FLAG = 1 THEN ''' || CAST(V_PREVMONTH AS VARCHAR(10)) || '''::DATE ELSE ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE END          
    AND B.ACTIVE_FLAG = 1 AND B.IS_DELETE = 0';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT4 || '                        
    (                        
    DOWNLOAD_DATE          
    ,LGD_RULE_ID          
    ,LGD_RULE_NAME          
    ,DEFAULT_RULE_ID          
    ,MASTERID          
    ,SEGMENT          
    ,SUB_SEGMENT          
    ,GROUP_SEGMENT          
    ,ACCOUNT_NUMBER          
    ,CUSTOMER_NUMBER          
    ,LGD_METHOD          
    ,CALC_METHOD          
    ,RECOVERY_AMOUNT          
    ,BI_COLLECTABILITY          
    ,DAY_PAST_DUE          
    ,LGD_UNIQUE_ID                        
    )               
    SELECT                         
    DOWNLOAD_DATE          
    ,LGD_RULE_ID          
    ,LGD_RULE_NAME          
    ,DEFAULT_RULE_ID          
    ,MASTERID          
    ,SEGMENT          
    ,SUB_SEGMENT          
    ,GROUP_SEGMENT          
    ,ACCOUNT_NUMBER          
    ,CUSTOMER_NUMBER          
    ,LGD_METHOD          
    ,CALC_METHOD          
    ,RECOVERY_AMOUNT          
    ,BI_COLLECTABILITY          
    ,DAY_PAST_DUE          
    ,LGD_UNIQUE_ID                       
    FROM REC_' || P_RUNID || '';
    EXECUTE (V_STR_QUERY); 

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    RAISE NOTICE 'SP_IFRS_IMP_RECOVERY_SCENARIO_DATA | AFFECTED RECORD : %', V_RETURNROWS2;
    -------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT4;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_IMP_RECOVERY_SCENARIO_DATA';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT4 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;