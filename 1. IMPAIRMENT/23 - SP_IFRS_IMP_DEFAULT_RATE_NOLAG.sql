---- DROP PROCEDURE SP_IFRS_IMP_DEFAULT_RATE_NOLAG;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMP_DEFAULT_RATE_NOLAG(
    IN P_RUNID VARCHAR(20) DEFAULT 'S_00000_0000',
    IN P_DOWNLOAD_DATE DATE DEFAULT NULL,
    IN P_PRC VARCHAR(1) DEFAULT 'S')
LANGUAGE PLPGSQL AS $$
DECLARE
    ---- DATE
    V_PREVDATE DATE;
    V_CURRDATE DATE;
    V_LASTYEAR DATE;
    V_CURRMONTH DATE;
    V_LASTYEARNEXTMONTH DATE;

    ---- QUERY   
    V_STR_QUERY TEXT;

    ---- TABLE LIST       
    V_TABLENAME VARCHAR(100);
    V_TABLEINSERT1 VARCHAR(100);
    V_TABLEINSERT2 VARCHAR(100);
    V_TABLEINSERT3 VARCHAR(100);
    
    ---- VARIABLE PROCESS
    V_SEGMENT RECORD;
    
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
        V_TABLEINSERT1 := 'IFRS_IMP_DEFAULT_RATE_NOLAG_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_IMP_DEFAULT_RATE_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_PD_SCENARIO_DATA_NOLAG_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'IFRS_IMP_DEFAULT_RATE_NOLAG';
        V_TABLEINSERT2 := 'IFRS_IMP_DEFAULT_RATE';
        V_TABLEINSERT3 := 'IFRS_PD_SCENARIO_DATA_NOLAG';
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
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT1 || ' AS SELECT * FROM IFRS_IMP_DEFAULT_RATE_NOLAG WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT2 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT2 || ' AS SELECT * FROM IFRS_IMP_DEFAULT_RATE WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' WHERE DOWNLOAD_DATE = ''' || CAST(V_LASTYEAR AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (
            DOWNLOAD_DATE 
            ,PROJECTION_DATE 
            ,PD_RULE_ID 
            ,PD_RULE_NAME 
            ,SEGMENT 
            ,CALC_METHOD 
            ,TOTAL_ACCOUNT 
            ,TOTAL_PERFORMING 
            ,TOTAL_12M_DEFAULT 
            ,TOTAL_OS 
            ,TOTAL_OS_PERFORMING 
            ,TOTAL_OS_12M_DEFAULT 
            ,ODR_RATE 
            ,CREATEDBY 
            ,CREATEDDATE 
        ) SELECT 
            DOWNLOAD_DATE
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS PROJECTION_DATE 
            ,PD_RULE_ID 
            ,MAX(PD_RULE_NAME) 
            ,MAX(SEGMENT) 
            ,MAX(A.CALC_METHOD) 
            ,COUNT(1) AS TOTAL_ACCOUNT 
            ,SUM(CASE WHEN BUCKET_ID = 0 OR BUCKET_ID = B.MAX_BUCKET THEN 0 ELSE 1 END) AS TOTAL_PERFORMING 
            ,SUM(CASE WHEN NEXT_12M_DEFAULT_FLAG = 1 AND A.BUCKET_ID <> MAX_BUCKET AND BUCKET_ID <> 0  THEN 1  ELSE 0 END) AS TOTAL_12M_DEFAULT 
            ,SUM(OUTSTANDING) AS TOTAL_OS 
            ,SUM(CASE WHEN BUCKET_ID = 0 OR BUCKET_ID = B.MAX_BUCKET  THEN 0 ELSE A.OUTSTANDING END) AS TOTAL_OS_PERFORMING 
            ,SUM(CASE WHEN NEXT_12M_DEFAULT_FLAG = 1 AND A.BUCKET_ID <> MAX_BUCKET AND BUCKET_ID <> 0 THEN A.OUTSTANDING ELSE 0 END) AS TOTAL_OS_12M_DEFAULT 
            ,NULL AS ODR_RATE 
            ,''SP_IFRS_IMP_DEFAULT_RATE_NOLAG'' AS CREATEDBY 
            ,CURRENT_TIMESTAMP AS CREATEDDATE 
        FROM (
            SELECT 
                DOWNLOAD_DATE         	                                    AS DOWNLOAD_DATE 
                ,MAX(PD_RULE_ID)	                                        AS PD_RULE_ID 
                ,MAX(PD_RULE_NAME)	                                        AS PD_RULE_NAME 
                ,MAX(DEFAULT_RULE_ID)	                                    AS DEFAULT_RULE_ID 
                ,MAX(BUCKET_GROUP)	                                        AS BUCKET_GROUP 
                ,MAX(MASTERID)	                                            AS MASTERID 
                ,MAX(SEGMENT)	                                            AS SEGMENT 
                ,MAX(SUB_SEGMENT)	                                        AS SUB_SEGMENT 
                ,MAX(GROUP_SEGMENT)	                                        AS GROUP_SEGMENT 
                ,MAX(ACCOUNT_NUMBER)	                                    AS ACCOUNT_NUMBER 
                ,MAX(CUSTOMER_NUMBER)	                                    AS CUSTOMER_NUMBER 
                ,MAX(PD_METHOD)	                                            AS PD_METHOD 
                ,MAX(CALC_METHOD)	                                        AS CALC_METHOD 
                ,SUM(CALC_AMOUNT)	                                        AS CALC_AMOUNT 
                ,MAX(BUCKET_ID)	                                            AS BUCKET_ID 
                ,SUM(OUTSTANDING)	                                        AS OUTSTANDING 
                ,MAX(IMPAIRED_FLAG)	                                        AS IMPAIRED_FLAG 
                ,MAX(CASE WHEN DEFAULT_FLAG = 1 THEN 1 ELSE 0 END)	        AS DEFAULT_FLAG 
                ,MAX(LIFETIME)	                                            AS LIFETIME 
                ,SUM(FAIR_VALUE_AMOUNT)	                                    AS FAIR_VALUE_AMOUNT 
                ,MAX(BI_COLLECTABILITY)	                                    AS BI_COLLECTABILITY 
                ,MAX(RATING_CODE)	                                        AS RATING_CODE 
                ,MAX(DAY_PAST_DUE)	                                        AS DAY_PAST_DUE 
                ,MAX(CREATEDBY)	                                            AS CREATEDBY 
                ,MAX(CREATEDDATE)	                                        AS CREATEDDATE 
                ,PD_UNIQUE_ID	                                            AS PD_UNIQUE_ID 
                ,MAX(CASE WHEN NEXT_12M_DEFAULT_FLAG = 1 THEN 1 ELSE 0 END) AS NEXT_12M_DEFAULT_FLAG 
                ,MAX(DPD_CIF)	                                            AS DPD_CIF 
                ,MAX(BUCKET_ID_ORIG)	                                    AS BUCKET_ID_ORIG 
            FROM ' || V_TABLEINSERT3 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_LASTYEAR AS VARCHAR(10)) || '''::DATE 
            GROUP BY DOWNLOAD_DATE, PD_UNIQUE_ID 
        ) A 
        JOIN (
            SELECT BUCKET_GROUP, MAX(BUCKET_ID) AS MAX_BUCKET 
            FROM IFRS_BUCKET_DETAIL GROUP BY BUCKET_GROUP 
        ) B 
        ON A.BUCKET_GROUP = B.BUCKET_GROUP 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_LASTYEAR AS VARCHAR(10)) || '''::DATE 
        GROUP BY DOWNLOAD_DATE, PD_RULE_ID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET ODR_RATE = CASE WHEN TOTAL_PERFORMING = 0 THEN 0 ELSE CAST(TOTAL_12M_DEFAULT AS DOUBLE PRECISION) / CAST(TOTAL_PERFORMING AS DOUBLE PRECISION) END 
        FROM IFRS_PD_RULES_CONFIG B 
        WHERE A.PD_RULE_ID = B.PKID 
        AND A.DOWNLOAD_DATE = ''' || CAST(V_LASTYEAR AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' 
        SET ODR_RATE = 0.0003 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_LASTYEAR AS VARCHAR(10)) || '''::DATE 
        AND ODR_RATE < 0.0003 ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT2 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_LASTYEAR AS VARCHAR(10)) || '''::DATE 
        AND CREATEDBY = ''SP_IFRS_IMP_DEFAULT_RATE_NOLAG'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
        (
            DOWNLOAD_DATE 
            ,PROJECTION_DATE 
            ,PD_RULE_ID 
            ,PD_RULE_NAME 
            ,SEGMENT 
            ,CALC_METHOD 
            ,TOTAL_ACCOUNT 
            ,TOTAL_PERFORMING 
            ,TOTAL_12M_DEFAULT 
            ,TOTAL_OS 
            ,TOTAL_OS_PERFORMING 
            ,TOTAL_OS_12M_DEFAULT 
            ,ODR_RATE 
            ,CREATEDBY 
            ,CREATEDDATE 
            ,CREATEDHOST 
            ,UPDATEDBY 
            ,UPDATEDDATE 
            ,UPDATEDHOST 
        ) SELECT 
            DOWNLOAD_DATE 
            ,PROJECTION_DATE 
            ,PD_RULE_ID 
            ,PD_RULE_NAME 
            ,SEGMENT 
            ,CALC_METHOD 
            ,TOTAL_ACCOUNT 
            ,TOTAL_PERFORMING 
            ,TOTAL_12M_DEFAULT 
            ,TOTAL_OS 
            ,TOTAL_OS_PERFORMING 
            ,TOTAL_OS_12M_DEFAULT 
            ,ODR_RATE 
            ,CREATEDBY 
            ,CREATEDDATE 
            ,CREATEDHOST 
            ,UPDATEDBY 
            ,UPDATEDDATE 
            ,UPDATEDHOST 
        FROM ' || V_TABLEINSERT1 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_LASTYEAR AS VARCHAR(10)) || '''::DATE 
        AND CREATEDBY = ''SP_IFRS_IMP_DEFAULT_RATE_NOLAG'' ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    RAISE NOTICE 'SP_IFRS_IMP_DEFAULT_RATE_NOLAG | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT2;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_IMP_DEFAULT_RATE_NOLAG';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT2 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;