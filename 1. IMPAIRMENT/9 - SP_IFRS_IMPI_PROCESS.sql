---- DROP PROCEDURE SP_IFRS_IMPI_PROCESS;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMPI_PROCESS(
    IN P_RUNID VARCHAR(20) DEFAULT 'S_00000_0000',
    IN P_DOWNLOAD_DATE DATE DEFAULT NULL,
    IN P_PRC VARCHAR(1) DEFAULT 'S')
LANGUAGE PLPGSQL AS $$
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
    V_TABLENAME_AMORT VARCHAR(100);
    V_TMPTABLE1 VARCHAR(100);
    V_TMPTABLE2 VARCHAR(100);
    V_TMPTABLE3 VARCHAR(100);
    V_TMPTABLE4 VARCHAR(100);
    V_TABLEINSERT1 VARCHAR(100);
    V_TABLEINSERT2 VARCHAR(100);
    V_TABLEINSERT3 VARCHAR(100);
    V_TABLEINSERT4 VARCHAR(100);
    V_TABLEINSERT5 VARCHAR(100);

    ---- VARIABLE PROCESS
    V_SEGMENT RECORD;
    V_SEGMENT_RULE RECORD;
    V_CONDITION TEXT;

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
        V_TABLENAME_AMORT := 'TMP_IMA_AMORT_' || P_RUNID || '';
        V_TMPTABLE1 := '_IMA_' || P_RUNID || '';
        V_TMPTABLE2 := '_IMA_CUSTOMER_' || P_RUNID || '';
        V_TMPTABLE3 := '_ECL_' || P_RUNID || '';
        V_TMPTABLE4 := '_ECL_INDIVIDUAL_' || P_RUNID || '';
        V_TABLEINSERT1 := 'IFRS_SCENARIO_GENERATE_QUERY_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_IMP_IA_SCENARIO_DATA_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_IMP_IA_MASTER_HIST_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_IMP_IA_MASTER_' || P_RUNID || '';
        V_TABLEINSERT5 := 'IFRS_ECL_INDIVIDUAL_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLENAME_AMORT := 'IFRS_IMA_AMORT_CURR';
        V_TMPTABLE1 := '_IMA';
        V_TMPTABLE2 := '_IMA_CUSTOMER';
        V_TMPTABLE3 := '_ECL';
        V_TMPTABLE4 := '_ECL_INDIVIDUAL';
        V_TABLEINSERT1 := 'IFRS_SCENARIO_GENERATE_QUERY';
        V_TABLEINSERT2 := 'IFRS_IMP_IA_SCENARIO_DATA';
        V_TABLEINSERT3 := 'IFRS_IMP_IA_MASTER_HIST';
        V_TABLEINSERT4 := 'IFRS_IMP_IA_MASTER';
        V_TABLEINSERT5 := 'IFRS_ECL_INDIVIDUAL';
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

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLENAME_AMORT || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLENAME_AMORT || ' AS SELECT * FROM IFRS_IMA_AMORT_CURR';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT1 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT1 || ' AS SELECT * FROM IFRS_SCENARIO_GENERATE_QUERY';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT2 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT2 || ' AS SELECT * FROM IFRS_IMP_IA_SCENARIO_DATA';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT3 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT3 || ' AS SELECT * FROM IFRS_IMP_IA_MASTER_HIST';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT4 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT4 || ' AS SELECT * FROM IFRS_IMP_IA_MASTER';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT5 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT5 || ' AS SELECT * FROM IFRS_ECL_INDIVIDUAL';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======
    
    -------- ====== BODY ======
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT2 || ' 
        WHERE EFFECTIVE_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND DATA_SOURCE NOT IN (''LOAN_T24'', ''TRADE_T24'') ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT3 || ' 
        WHERE EFFECTIVE_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND IMPAIRED_FLAG = ''C'' AND OVERRIDE_FLAG = ''A'' AND DATA_SOURCE NOT IN (''LOAN_T24'', ''TRADE_T24'') ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TMPTABLE1;
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || ' 
        CREATE TABLE ' || V_TMPTABLE1 || ' AS 
        SELECT
            DOWNLOAD_DATE
            ,MASTERID
            ,CUSTOMER_NUMBER
            ,CASE WHEN FACILITY_NUMBER IS NULL THEN MASTERID ELSE FACILITY_NUMBER END AS FACILITY_NUMBER
            ,PLAFOND
            ,DATA_SOURCE
            ,SOURCE_SYSTEM
        FROM ' || V_TABLENAME_AMORT || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND DATA_SOURCE NOT IN (''LOAN_T24'', ''TRADE_T24'') ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TMPTABLE2;
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || ' 
        CREATE TABLE ' || V_TMPTABLE2 || ' AS 
        SELECT
            DOWNLOAD_DATE
            ,CUSTOMER_NUMBER
            ,SUM(PLAFOND) AS PLAFOND_CIF
            ,DATA_SOURCE
            ,SOURCE_SYSTEM
        FROM (
            SELECT
                DOWNLOAD_DATE
                ,CUSTOMER_NUMBER
                ,FACILITY_NUMBER
                ,MAX(PLAFOND) AS PLAFOND
                ,DATA_SOURCE
                ,SOURCE_SYSTEM
            FROM ' || V_TMPTABLE1 || ' 
            GROUP BY DOWNLOAD_DATE, CUSTOMER_NUMBER, FACILITY_NUMBER, DATA_SOURCE, SOURCE_SYSTEM
        ) AS A
        GROUP BY DOWNLOAD_DATE, CUSTOMER_NUMBER, DATA_SOURCE, SOURCE_SYSTEM 
        ORDER BY CUSTOMER_NUMBER ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || '
        UPDATE ' || V_TABLENAME_AMORT || ' A 
        SET
            PLAFOND_CIF = B.PLAFOND_CIF 
            ,IMPAIRED_FLAG = ''C''
        FROM ' || V_TMPTABLE2 || ' B 
        WHERE A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER 
        AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE 
        AND A.DATA_SOURCE = B.DATA_SOURCE 
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND A.DATA_SOURCE NOT IN (''LOAN_T24'', ''TRADE_T24'') ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || ' 
        UPDATE ' || V_TABLENAME || ' A 
        SET
            PLAFOND_CIF = B.PLAFOND_CIF 
            ,IMPAIRED_FLAG = B.IMPAIRED_FLAG 
        FROM ' || V_TABLENAME_AMORT || ' B 
        WHERE A.MASTERID = B.MASTERID 
        AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE 
        AND A.SOURCE_SYSTEM = B.SOURCE_SYSTEM 
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND A.DATA_SOURCE NOT IN (''LOAN_T24'', ''TRADE_T24'') ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' WHERE RULE_TYPE = ''INDIVIDUAL_RULE''';
    EXECUTE (V_STR_QUERY);

    FOR V_SEGMENT IN
        EXECUTE ('SELECT DISTINCT
            CASE WHEN UPDATED_TABLE = ''IFRS_MASTER_ACCOUNT'' THEN ''' || V_TABLENAME_AMORT || ''' ELSE UPDATED_TABLE END AS UPDATED_TABLE 
            ,UPDATED_COLUMN
            ,RULE_TYPE
            ,CASE WHEN TABLE_NAME = ''IFRS_MASTER_ACCOUNT'' THEN ''' || V_TABLENAME_AMORT || ''' ELSE TABLE_NAME END AS TABLE_NAME 
            ,A.RULE_NAME
            ,A.PKID AS RULE_ID
        FROM IFRS_SCENARIO_RULES_HEADER A
        INNER JOIN IFRS_SCENARIO_RULES_DETAIL B ON A.PKID = B.RULE_ID
        WHERE A.IS_DELETE = 0 AND B.IS_DELETE = 0 AND RULE_TYPE = ''INDIVIDUAL_RULE''')
    LOOP
        V_CONDITION := ' ';

        FOR V_SEGMENT_RULE IN
            EXECUTE 'SELECT
                TRIM(''A."'' || COALESCE(COLUMN_NAME, '''') || ''"'') AS COLUMN_NAME
                ,TRIM(DATA_TYPE) AS DATA_TYPE
                ,TRIM(OPERATOR) AS OPERATOR
                ,TRIM(VALUE1) AS VALUE1
                ,TRIM(VALUE2) AS VALUE2
                ,QUERY_GROUPING AS QG
                ,TRIM(AND_OR_CONDITION) AS AOC
                ,LAG(QUERY_GROUPING, 1, MIN_QG) OVER (PARTITION BY RULE_ID ORDER BY QUERY_GROUPING, SEQUENCE) AS PREV_QG
                ,LEAD(QUERY_GROUPING, 1, MAX_QG) OVER (PARTITION BY RULE_ID ORDER BY QUERY_GROUPING, SEQUENCE) AS NEXT_QG
                ,JML
                ,RN
                ,PKID
            FROM (SELECT
                MIN(QUERY_GROUPING) OVER (PARTITION BY RULE_ID) AS MIN_QG
                ,MAX(QUERY_GROUPING) OVER (PARTITION BY RULE_ID) AS MAX_QG
                ,ROW_NUMBER() OVER (PARTITION BY RULE_ID ORDER BY QUERY_GROUPING, SEQUENCE) AS RN
                ,COUNT(0) OVER (PARTITION BY RULE_ID) AS JML
                ,COLUMN_NAME
                ,DATA_TYPE
                ,OPERATOR
                ,VALUE1
                ,VALUE2
                ,QUERY_GROUPING
                ,RULE_ID
                ,AND_OR_CONDITION
                ,PKID
                ,SEQUENCE
            FROM IFRS_SCENARIO_RULES_DETAIL
            WHERE RULE_ID = ' || V_SEGMENT.RULE_ID || ' 
            AND IS_DELETE = 0 ) A'
        LOOP
            V_CONDITION := COALESCE(V_CONDITION, '');
            
            -- ADD LOGICAL OPERATOR
            V_CONDITION := V_CONDITION || ' ' || COALESCE(
                CASE
                    WHEN V_SEGMENT_RULE.QG > V_SEGMENT_RULE.PREV_QG THEN 'OR'
                    WHEN V_SEGMENT_RULE.QG = V_SEGMENT_RULE.PREV_QG THEN V_SEGMENT_RULE.AOC
                END, '');

            -- ADD OPENING PARANTHESIS
            V_CONDITION := V_CONDITION || ' ' || COALESCE(
                CASE
                    WHEN V_SEGMENT_RULE.QG <> V_SEGMENT_RULE.PREV_QG THEN '('
                    ELSE ' '
                END, '');

            -- ADD CONDITION
            V_CONDITION := V_CONDITION || COALESCE(
                CASE
                    WHEN UPPER(V_SEGMENT_RULE.DATA_TYPE) IN ('NUMBER', 'DECIMAL', 'NUMERIC', 'DOUBLE PRECISION', 'INT') THEN
                    CASE
                        WHEN V_SEGMENT_RULE.OPERATOR IN ('=', '<>', '>', '<', '>=', '<=') THEN 
                            COALESCE(V_SEGMENT_RULE.COLUMN_NAME, '') || ' ' ||
                            COALESCE(V_SEGMENT_RULE.OPERATOR, '') || ' ' ||
                            COALESCE(V_SEGMENT_RULE.VALUE1, '')
                        WHEN UPPER(V_SEGMENT_RULE.OPERATOR) = 'BETWEEN' THEN
                            COALESCE(V_SEGMENT_RULE.COLUMN_NAME, '') || ' ' ||
                            COALESCE(V_SEGMENT_RULE.OPERATOR, '') || ' ' ||
                            COALESCE(V_SEGMENT_RULE.VALUE1, '') || ' ' ||
                            'AND ' || COALESCE(V_SEGMENT_RULE.VALUE2, '')
                        WHEN UPPER(V_SEGMENT_RULE.OPERATOR) IN ('IN', 'NOT IN') THEN
                            COALESCE(V_SEGMENT_RULE.COLUMN_NAME, '') || ' ' ||
                            COALESCE(V_SEGMENT_RULE.OPERATOR, '') || ' ' ||
                            '(' || COALESCE(V_SEGMENT_RULE.VALUE1, '') || ')'
                        ELSE 'xxx'
                    END
                    WHEN UPPER(V_SEGMENT_RULE.DATA_TYPE) IN ('DATE', 'DATETIME') THEN
                    CASE
                        WHEN V_SEGMENT_RULE.OPERATOR IN ('=', '<>', '>', '<', '>=', '<=') THEN 
                            COALESCE(V_SEGMENT_RULE.COLUMN_NAME, '') || ' ' ||
                            COALESCE(V_SEGMENT_RULE.OPERATOR, '') || ' ' ||
                            'DATE(''' || COALESCE(REPLACE(V_SEGMENT_RULE.VALUE1, ' ', '/'), '') || ''')::DATE'
                        WHEN UPPER(V_SEGMENT_RULE.OPERATOR) = 'BETWEEN' THEN
                            COALESCE(V_SEGMENT_RULE.COLUMN_NAME, '') || ' ' ||
                            COALESCE(V_SEGMENT_RULE.OPERATOR, '') || ' ' ||
                            'DATE(''' || COALESCE(REPLACE(V_SEGMENT_RULE.VALUE1, ' ', '/'), '') || ''')::DATE' ||
                            ' AND ' ||
                            'DATE(''' || COALESCE(REPLACE(V_SEGMENT_RULE.VALUE2, ' ', '/'), '') || ''')::DATE'
                        ELSE 'xXx'
                    END
                    WHEN UPPER(V_SEGMENT_RULE.DATA_TYPE) IN ('CHAR', 'CHARACTER', 'VARCHAR', 'VARCHAR2', 'BIT') THEN
                    CASE
                        WHEN V_SEGMENT_RULE.OPERATOR = '=' THEN
                            COALESCE(V_SEGMENT_RULE.COLUMN_NAME, '') || ' ' ||
                            COALESCE(V_SEGMENT_RULE.OPERATOR, '') || ' ' ||
                            '''' || COALESCE(V_SEGMENT_RULE.VALUE1, '') || ''''
                        WHEN UPPER(V_SEGMENT_RULE.OPERATOR) = 'BETWEEN' THEN
                            COALESCE(V_SEGMENT_RULE.COLUMN_NAME, '') || ' ' ||
                            COALESCE(V_SEGMENT_RULE.OPERATOR, '') || ' ' ||
                            '''' || COALESCE(V_SEGMENT_RULE.VALUE1, '') || '''' ||
                            ' AND ' ||
                            '''' || COALESCE(V_SEGMENT_RULE.VALUE2, '') || ''''
                        WHEN UPPER(V_SEGMENT_RULE.OPERATOR) IN ('IN', 'NOT IN') THEN
                            COALESCE(V_SEGMENT_RULE.COLUMN_NAME, '') || ' ' ||
                            COALESCE(V_SEGMENT_RULE.OPERATOR, '') || ' ' ||
                            '(''' || COALESCE(REPLACE(V_SEGMENT_RULE.VALUE1, ',', ''','''), '') || ''')'
                        ELSE 'XXX'
                    END
                    ELSE 'XxX'
                END, '');

            -- ADD CLOSING PARANTHESIS
            V_CONDITION := V_CONDITION || COALESCE(
                CASE
                    WHEN V_SEGMENT_RULE.QG <> V_SEGMENT_RULE.NEXT_QG
                    OR V_SEGMENT_RULE.RN = V_SEGMENT_RULE.JML THEN ')'
                    ELSE ' '
                END, '');

        END LOOP;

        V_CONDITION := '(' || COALESCE(LTRIM(SUBSTRING(V_CONDITION FROM 6 FOR LENGTH(TRIM(TRAILING FROM V_CONDITION)))), '');

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || '
            SELECT
                A.MASTERID 
                ,A.DOWNLOAD_DATE AS EFFECTIVE_DATE 
                ,A.CUSTOMER_NUMBER 
                ,CUSTOMER_NAME 
                ,PRODUCT_GROUP 
                ,PRODUCT_CODE 
                ,CASE WHEN A.FACILITY_NUMBER IS NULL THEN A.MASTERID ELSE A.FACILITY_NUMBER END AS FACILITY_NUMBER 
                ,A.LOAN_DUE_DATE AS MATURITY_DATE 
                ,CURRENCY 
                ,INTEREST_RATE 
                ,EIR 
                ,AVG_EIR 
                ,CASE WHEN A.DATA_SOURCE = ''LIMIT_T24'' THEN UNUSED_AMOUNT ELSE OUTSTANDING END AS OUTSTANDING 
                ,PLAFOND 
                ,DAY_PAST_DUE 
                ,BI_COLLECTABILITY 
                ,DPD_CIF 
                ,RESTRUCTURE_COLLECT_FLAG 
                ,BI_COLLECT_CIF 
                ,B.PLAFOND_CIF
                ,' || V_SEGMENT.RULE_ID || ' AS IA_RULE_ID 
                ,A.DATA_SOURCE 
                ,A.SOURCE_SYSTEM 
            FROM ' || V_SEGMENT.UPDATED_TABLE || ' AS A 
            JOIN ' || V_TMPTABLE2 || ' AS B 
            ON A.DOWNLOAD_DATE = B.DOWNLOAD_DATE 
            AND A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER 
            AND A.DATA_SOURCE = B.DATA_SOURCE 
            AND A.SOURCE_SYSTEM = B.SOURCE_SYSTEM 
            WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND A.DATA_SOURCE <> ''LIMIT'' AND A.DATA_SOURCE NOT IN (''LOAN_T24'', ''TRADE_T24'') ';
        V_STR_QUERY := V_STR_QUERY || ' AND (' || REPLACE(V_CONDITION, '"', '') || ')';

        V_STR_QUERY := 'INSERT INTO ' || V_TABLEINSERT2 || ' 
            (
                MASTERID 
                ,EFFECTIVE_DATE 
                ,CUSTOMER_NUMBER 
                ,CUSTOMER_NAME 
                ,PRODUCT_GROUP 
                ,PRODUCT_CODE 
                ,FACILITY_NUMBER 
                ,MATURITY_DATE 
                ,CURRENCY 
                ,INTEREST_RATE 
                ,EIR 
                ,AVG_EIR 
                ,OUTSTANDING 
                ,PLAFOND 
                ,DAY_PAST_DUE 
                ,BI_COLLECTABILITY 
                ,DPD_CIF 
                ,RESTRUCTURE_COLLECT_FLAG 
                ,BI_COLLECT_CIF 
                ,PLAFOND_CIF 
                ,IA_RULE_ID 
                ,DATA_SOURCE 
                ,SOURCE_SYSTEM
            ) ' || V_STR_QUERY;
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (
            RULE_ID
            ,RULE_NAME
            ,RULE_TYPE
            ,TABLE_NAME
            ,PD_RULES_QRY_RESULT
            ,CREATEDBY
            ,CREATEDDATE
        ) VALUES (
            ''' || V_SEGMENT.RULE_ID || '''
            ,''' || V_SEGMENT.RULE_NAME || '''
            ,''' || V_SEGMENT.RULE_TYPE || '''
            ,''' || V_SEGMENT.TABLE_NAME || '''
            ,''' || V_CONDITION || '''
            ,''SP_IFRS_IMP_IA_PROCESS''
            ,CURRENT_TIMESTAMP::DATE
        )';

    END LOOP;

    IF (DATE_PART('DAY', V_CURRDATE) BETWEEN 1 AND 26)
    THEN
        -- INSERT TO IFRS_IMP_IA_MASTER FROM SCENARIO DATA, WHICH IS NOT IN IFRS_IMP_IA_MASTER ITSELF
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT4 || ' 
            (
                MASTERID 
                ,EFFECTIVE_DATE 
                ,CUSTOMER_NUMBER 
                ,CUSTOMER_NAME 
                ,PRODUCT_GROUP 
                ,PRODUCT_CODE 
                ,MATURITY_DATE 
                ,CURRENCY 
                ,INTEREST_RATE 
                ,EIR 
                ,AVG_EIR 
                ,OUTSTANDING 
                ,PLAFOND 
                ,DAY_PAST_DUE 
                ,BI_COLLECTABILITY 
                ,IMPAIRED_FLAG 
                ,DPD_CIF 
                ,RESTRUCTURE_COLLECT_FLAG 
                ,BI_COLLECT_CIF 
                ,OVERRIDE_FLAG 
                ,BEING_EDITED 
                ,PLAFOND_CIF 
                ,DATA_SOURCE 
                ,SOURCE_SYSTEM 
            ) SELECT
                MASTERID 
                ,EFFECTIVE_DATE 
                ,CUSTOMER_NUMBER 
                ,CUSTOMER_NAME 
                ,PRODUCT_GROUP 
                ,PRODUCT_CODE 
                ,MATURITY_DATE 
                ,CURRENCY 
                ,INTEREST_RATE 
                ,EIR 
                ,AVG_EIR 
                ,OUTSTANDING 
                ,PLAFOND 
                ,DAY_PAST_DUE 
                ,BI_COLLECTABILITY 
                ,IMPAIRED_FLAG 
                ,DPD_CIF 
                ,RESTRUCTURE_COLLECT_FLAG 
                ,BI_COLLECT_CIF 
                ,OVERRIDE_FLAG 
                ,BEING_EDITED 
                ,PLAFOND_CIF 
                ,DATA_SOURCE 
                ,SOURCE_SYSTEM 
            FROM ' || V_TABLEINSERT2 || ' 
            WHERE EFFECTIVE_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND MASTERID NOT IN (SELECT MASTERID FROM ' || V_TABLEINSERT4 || ') ';
        EXECUTE (V_STR_QUERY);

        -- INSERT TO IFRS_IMP_IA_MASTER_HIST FOR INITIALIZE
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT3 || ' 
            (
                MASTERID 
                ,EFFECTIVE_DATE 
                ,CUSTOMER_NUMBER 
                ,CUSTOMER_NAME 
                ,PRODUCT_GROUP 
                ,PRODUCT_CODE 
                ,MATURITY_DATE 
                ,CURRENCY 
                ,INTEREST_RATE 
                ,EIR 
                ,AVG_EIR 
                ,OUTSTANDING 
                ,PLAFOND 
                ,DAY_PAST_DUE 
                ,BI_COLLECTABILITY 
                ,IMPAIRED_FLAG 
                ,DPD_CIF 
                ,RESTRUCTURE_COLLECT_FLAG 
                ,BI_COLLECT_CIF 
                ,OVERRIDE_FLAG 
                ,BEING_EDITED 
                ,PLAFOND_CIF 
                ,DATA_SOURCE 
                ,SOURCE_SYSTEM
            ) SELECT
                MASTERID 
                ,EFFECTIVE_DATE 
                ,CUSTOMER_NUMBER 
                ,CUSTOMER_NAME 
                ,PRODUCT_GROUP 
                ,PRODUCT_CODE 
                ,MATURITY_DATE 
                ,CURRENCY 
                ,INTEREST_RATE 
                ,EIR 
                ,AVG_EIR 
                ,OUTSTANDING 
                ,PLAFOND 
                ,DAY_PAST_DUE 
                ,BI_COLLECTABILITY 
                ,IMPAIRED_FLAG 
                ,DPD_CIF 
                ,RESTRUCTURE_COLLECT_FLAG 
                ,BI_COLLECT_CIF 
                ,OVERRIDE_FLAG 
                ,BEING_EDITED 
                ,PLAFOND_CIF 
                ,DATA_SOURCE 
                ,SOURCE_SYSTEM 
            FROM ' || V_TABLEINSERT2 || ' 
            WHERE EFFECTIVE_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND MASTERID NOT IN (SELECT MASTERID FROM ' || V_TABLEINSERT4 || ') ';
        EXECUTE (V_STR_QUERY);

        -- INSERT TO IFRS_IMP_IA_MASTER_HIST WHICH IS CHANGE TO COLLECTIVE
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT3 || ' 
            (
                MASTERID 
                ,DCFID 
                ,DOWNLOAD_DATE 
                ,EFFECTIVE_DATE 
                ,CUSTOMER_NUMBER 
                ,CUSTOMER_NAME 
                ,PRODUCT_GROUP 
                ,PRODUCT_CODE 
                ,MATURITY_DATE 
                ,CURRENCY 
                ,INTEREST_RATE 
                ,EIR 
                ,AVG_EIR 
                ,OUTSTANDING 
                ,PLAFOND 
                ,DAY_PAST_DUE 
                ,BI_COLLECTABILITY 
                ,IMPAIRED_FLAG 
                ,DPD_CIF 
                ,RESTRUCTURE_COLLECT_FLAG 
                ,BI_COLLECT_CIF 
                ,METHOD 
                ,REMARKS 
                ,MANAGER_NAME 
                ,MANAGER_TELEPHONE 
                ,MANAGER_HANDPHONE 
                ,REMARKS_A 
                ,REMARKS_B 
                ,REMARKS_C 
                ,REMARKS_D 
                ,REMARKS_E 
                ,REMARKS_E1 
                ,REMARKS_E2 
                ,REMARKS_F 
                ,REMARKS_F1 
                ,STATUS 
                ,BEING_EDITED 
                ,OVERRIDE_FLAG 
                ,NPV_AMOUNT 
                ,ECL_AMOUNT 
                ,UNWINDING_AMOUNT 
                ,CREATEDBY 
                ,CREATEDDATE 
                ,CREATEDHOST 
                ,PLAFOND_CIF 
                ,DATA_SOURCE 
                ,SOURCE_SYSTEM 
            ) SELECT 
                MASTERID 
                ,DCFID 
                ,DOWNLOAD_DATE 
                ,EFFECTIVE_DATE 
                ,CUSTOMER_NUMBER 
                ,CUSTOMER_NAME 
                ,PRODUCT_GROUP 
                ,PRODUCT_CODE 
                ,MATURITY_DATE 
                ,CURRENCY 
                ,INTEREST_RATE 
                ,EIR 
                ,AVG_EIR 
                ,OUTSTANDING 
                ,PLAFOND 
                ,DAY_PAST_DUE 
                ,BI_COLLECTABILITY 
                ,''C'' AS IMPAIRED_FLAG 
                ,DPD_CIF 
                ,RESTRUCTURE_COLLECT_FLAG 
                ,BI_COLLECT_CIF 
                ,METHOD 
                ,REMARKS 
                ,MANAGER_NAME 
                ,MANAGER_TELEPHONE 
                ,REMARKS_A 
                ,REMARKS_B 
                ,REMARKS_C 
                ,REMARKS_D 
                ,REMARKS_E 
                ,REMARKS_E1 
                ,REMARKS_E2 
                ,REMARKS_F 
                ,REMARKS_F1 
                ,''APPROVE_OVERRIDE'' AS STATUS
                ,BEING_EDITED 
                ,''A'' AS OVERRIDE_FLAG 
                ,NPV_AMOUNT 
                ,ECL_AMOUNT 
                ,UNWINDING_AMOUNT 
                ,CREATEDBY 
                ,CREATEDDATE 
                ,CREATEDHOST 
                ,PLAFOND_CIF 
                ,DATA_SOURCE 
                ,SOURCE_SYSTEM 
            FROM ' || V_TABLEINSERT4 || ' 
            WHERE MASTERID NOT IN 
            (
                SELECT MASTERID
                FROM ' || V_TABLEINSERT2 || ' 
                WHERE EFFECTIVE_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ) ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT4 || ' 
            WHERE MASTERID NOT IN 
            (
                SELECT MASTERID
                FROM ' || V_TABLEINSERT2 || ' 
                WHERE EFFECTIVE_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ) ';
        EXECUTE (V_STR_QUERY);
    END IF;

    -- UP TO DATING INFORMATION DATA
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT4 || ' A
        SET 
            CUSTOMER_NUMBER = B.CUSTOMER_NUMBER 
            ,CUSTOMER_NAME = B.CUSTOMER_NAME 
            ,PRODUCT_CODE = B.PRODUCT_CODE 
            ,PRODUCT_GROUP = B.PRODUCT_GROUP 
            ,MATURITY_DATE = B.LOAN_DUE_DATE 
            ,CURRENCY = B.CURRENCY 
            ,INTEREST_RATE = B.INTEREST_RATE 
            ,EIR = B.EIR 
            ,AVG_EIR = B.AVG_EIR 
            ,OUTSTANDING = CASE WHEN A.DATA_SOURCE = ''LIMIT_T24'' THEN B.UNUSED_AMOUNT ELSE B.OUTSTANDING END 
            ,PLAFOND = B.PLAFOND 
            ,DAY_PAST_DUE = B.DAY_PAST_DUE 
            ,DPD_CIF = B.DPD_CIF 
            ,BI_COLLECTABILITY = B.BI_COLLECTABILITY 
            ,BI_COLLECT_CIF = B.BI_COLLECT_CIF 
            ,RESTRUCTURE_COLLECT_FLAG = B.RESTRUCTURE_COLLECT_FLAG 
            ,UPDATEDBY = ''SP_IFRS_IMPI_PROCESS'' 
            ,UPDATEDDATE = CURRENT_TIMESTAMP::DATE 
        FROM ' || V_TABLENAME_AMORT || ' B 
        WHERE A.MASTERID = B.MASTERID ';
    EXECUTE (V_STR_QUERY);

    -- IF CURRENT DATE IS EOMONTH
    IF (V_CURRDATE = F_EOMONTH(V_CURRDATE, 0, 'M', 'NEXT')) THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TMPTABLE3 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || ' 
            CREATE TABLE ' || V_TMPTABLE3 || ' AS 
            SELECT 
                F_EOMONTH(B.DOWNLOADDATE, 0, ''M'', ''NEXT'') AS DOWNLOAD_DATE
                ,A.MASTERID 
                ,CASE WHEN (A.OUTSTANDING - SUM(B.NPV)) < 0 THEN 0 ELSE (A.OUTSTANDING - SUM(B.NPV)) END AS ECL_AMOUNT 
                ,CASE WHEN (A.OUTSTANDING - SUM(B.NPV)) < 0 THEN 0 ELSE (A.OUTSTANDING - SUM(B.NPV)) END AS ECL_AMOUNT_BFL 
            FROM
            (
                SELECT * FROM ' || V_TABLEINSERT4 || ' 
                WHERE EFFECTIVE_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ) A 
            JOIN TBLT_PAYMENTEXPECTED B 
            ON A.DCFID = B.DCFID 
            WHERE F_EOMONTH(B.DOWNLOADDATE, 0, ''M'', ''NEXT'') = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            GROUP BY F_EOMONTH(B.DOWNLOADDATE, 0, ''M'', ''NEXT''), A.MASTERID, A.OUTSTANDING ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT5 || ' 
            WHERE DOWNLOAD_DATE >= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT5 || ' 
            (
                DOWNLOAD_DATE 
                ,MASTERID 
                ,ECL_AMOUNT 
                ,ECL_AMOUNT_BFL 
            ) SELECT
                DOWNLOAD_DATE 
                ,MASTERID 
                ,ECL_AMOUNT 
                ,ECL_AMOUNT_BFL 
            FROM ' || V_TMPTABLE3 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TMPTABLE4 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TMPTABLE4 || ' AS 
            SELECT * FROM ' || V_TMPTABLE3 || ' WHERE 1=2 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TMPTABLE4 || ' 
            (DOWNLOAD_DATE, MASTERID) 
            SELECT MAX(DOWNLOAD_DATE) AS DOWNLOAD_DATE, MASTERID 
            FROM IFRS_ECL_INDIVIDUAL GROUP BY MASTERID ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TMPTABLE4 || ' A 
            SET
                ECL_AMOUNT = B.ECL_AMOUNT 
                ,ECL_AMOUNT_BFL = B.ECL_AMOUNT_BFL 
            FROM IFRS_ECL_INDIVIDUAL B 
            WHERE A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
            AND A.MASTERID = B.MASTERID ';
        EXECUTE (V_STR_QUERY);

        -- INDIVIDUAL WHICH IS LATEST DCF UPLOAD DATE < CURRENT_DATE
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TMPTABLE3 || ' 
            (
                DOWNLOAD_DATE
                ,MASTERID
                ,ECL_AMOUNT
                ,ECL_AMOUNT_BFL
            ) SELECT 
                F_EOMONTH(B.DOWNLOADDATE, 0, ''M'', ''NEXT'') AS DOWNLOAD_DATE 
                ,A.MASTERID 
                ,CASE WHEN C.ECL_AMOUNT > A.OUTSTANDING THEN A.OUTSTANDING ELSE C.ECL_AMOUNT END AS ECL_AMOUNT
                ,CASE WHEN C.ECL_AMOUNT_BFL > A.OUTSTANDING THEN A.OUTSTANDING ELSE C.ECL_AMOUNT_BFL END AS ECL_AMOUNT_BFL
            FROM 
            (
                SELECT * FROM ' || V_TABLEINSERT4 || ' 
                WHERE EFFECTIVE_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ) A 
            JOIN 
            (SELECT DISTINCT DOWNLOADDATE, DCFID FROM TBLT_PAYMENTEXPECTED) B 
            ON A.DCFID = B.DCFID 
            JOIN 
            (
                SELECT
                    X.MASTERID
                    ,X.ECL_AMOUNT
                    ,X.ECL_AMOUNT_BFL
                FROM ' || V_TMPTABLE4 || ' X 
                JOIN (SELECT * FROM ' || V_TABLEINSERT4 || ' WHERE EFFECTIVE_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE) Y 
                ON X.MASTERID = Y.MASTERID 
            ) C 
            ON A.MASTERID = C.MASTERID
            WHERE B.DOWNLOADDATE < ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        -- INDIVIDUAL BUT NOT HAVE DCF
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TMPTABLE3 || ' 
            (
                DOWNLOAD_DATE
                ,MASTERID
                ,ECL_AMOUNT
                ,ECL_AMOUNT_BFL
            ) SELECT 
                F_EOMONTH(EFFECTIVE_DATE, 0, ''M'', ''NEXT'') AS DOWNLOAD_DATE 
                ,MASTERID 
                ,OUTSTANDING AS ECL_AMOUNT
                ,OUTSTANDING AS ECL_AMOUNT_BFL
            FROM ' || V_TABLEINSERT4 || ' 
            WHERE DCFID IS NULL 
            AND EFFECTIVE_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'ALTER TABLE ' || V_TMPTABLE3 || ' 
            ADD UNWINDING_AMOUNT NUMERIC(32, 6) DEFAULT 0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TMPTABLE3 || ' A 
            SET UNWINDING_AMOUNT = B.UNWINDING_AMOUNT 
            FROM 
            (
                SELECT 
                    F_EOMONTH(A.EFFECTIVE_DATE, 0, ''M'', ''NEXT'') AS DOWNLOAD_DATE
                    ,A.MASTERID
                    ,SUM(B.UNWINDING_AMOUNT) AS UNWINDING_AMOUNT
                FROM (SELECT * FROM ' || V_TABLEINSERT4 || ' WHERE EFFECTIVE_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE) A 
                JOIN TBLT_PAYMENTEXPECTED B 
                ON A.DCFID = B.DCFID 
                WHERE F_EOMONTH(B.EFFECTIVE_DATE_FD, 0, ''M'', ''NEXT'') <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
                GROUP BY F_EOMONTH(A.EFFECTIVE_DATE, 0, ''M'', ''NEXT''), A.MASTERID
            ) B 
            WHERE A.DOWNLOAD_DATE = B.DOWNLOAD_DATE 
            AND A.MASTERID = B.MASTERID ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT4 || ' A 
            SET
                ECL_AMOUNT = B.ECL_AMOUNT 
                ,UNWINDING_AMOUNT = CASE WHEN COALESCE(B.UNWINDING_AMOUNT, 0) > B.ECL_AMOUNT THEN B.ECL_AMOUNT ELSE COALESCE(B.UNWINDING_AMOUNT, 0) END 
            FROM ' || V_TMPTABLE3 || ' B 
            WHERE A.MASTERID = B.MASTERID ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLENAME || ' A 
            SET
                ECL_AMOUNT = CASE WHEN COALESCE(A.IFRS9_CLASS, '''') = ''FVTPL'' THEN 0 ELSE B.ECL_AMOUNT END 
                ,ECL_AMOUNT_BFL = CASE WHEN COALESCE(A.IFRS9_CLASS, '''') = ''FVTPL'' THEN 0 ELSE B.ECL_AMOUNT_BFL END 
                ,IA_UNWINDING_AMOUNT = CASE WHEN COALESCE(A.IFRS9_CLASS, '''') = ''FVTPL'' THEN 0 ELSE CASE WHEN B.UNWINDING_AMOUNT > B.ECL_AMOUNT THEN B.ECL_AMOUNT ELSE B.UNWINDING_AMOUNT END END 
            FROM ' || V_TMPTABLE3 || ' B 
            WHERE A.MASTERID = B.MASTERID 
            AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT3 || ' 
            WHERE CREATEDBY = ''IFRS_IMP_IA_MASTERE_HIST'' 
            AND F_EOMONTH(DOWNLOAD_DATE, 0, ''M'', ''NEXT'') = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT3 || ' 
            (
                MASTERID 
                ,DCFID 
                ,DOWNLOAD_DATE 
                ,EFFECTIVE_DATE 
                ,CUSTOMER_NUMBER 
                ,CUSTOMER_NAME 
                ,PRODUCT_GROUP 
                ,PRODUCT_CODE 
                ,MATURITY_DATE 
                ,CURRENCY 
                ,INTEREST_RATE 
                ,EIR 
                ,AVG_EIR 
                ,OUTSTANDING 
                ,PLAFOND 
                ,DAY_PAST_DUE 
                ,BI_COLLECTABILITY 
                ,IMPAIRED_FLAG 
                ,DPD_CIF 
                ,RESTRUCTURE_COLLECT_FLAG 
                ,BI_COLLECT_CIF 
                ,METHOD 
                ,REMARKS 
                ,MANAGER_NAME 
                ,MANAGER_TELEPHONE 
                ,MANAGER_HANDPHONE 
                ,REMARKS_A 
                ,REMARKS_B 
                ,REMARKS_C 
                ,REMARKS_D 
                ,REMARKS_E 
                ,REMARKS_E1 
                ,REMARKS_E2 
                ,REMARKS_F 
                ,REMARKS_F1 
                ,STATUS 
                ,BEING_EDITED 
                ,OVERRIDE_FLAG 
                ,NPV_AMOUNT 
                ,ECL_AMOUNT 
                ,UNWINDING_AMOUNT 
                ,CREATEDBY 
                ,CREATEDDATE 
                ,CREATEDHOST 
                ,PLAFOND_CIF 
                ,SOURCE_SYSTEM 
            ) SELECT 
                MASTERID 
                ,NULL AS DCFID 
                ,DOWNLOAD_DATE 
                ,EFFECTIVE_DATE 
                ,CUSTOMER_NUMBER 
                ,CUSTOMER_NAME 
                ,PRODUCT_GROUP 
                ,PRODUCT_CODE 
                ,MATURITY_DATE 
                ,CURRENCY 
                ,INTEREST_RATE 
                ,EIR 
                ,AVG_EIR 
                ,OUTSTANDING 
                ,PLAFOND 
                ,DAY_PAST_DUE 
                ,BI_COLLECTABILITY 
                ,IMPAIRED_FLAG 
                ,DPD_CIF 
                ,RESTRUCTURE_COLLECT_FLAG 
                ,BI_COLLECT_CIF 
                ,METHOD 
                ,REMARKS 
                ,MANAGER_NAME 
                ,MANAGER_TELEPHONE 
                ,MANAGER_HANDPHONE 
                ,REMARKS_A 
                ,REMARKS_B 
                ,REMARKS_C 
                ,REMARKS_D 
                ,REMARKS_E 
                ,REMARKS_E1 
                ,REMARKS_E2 
                ,REMARKS_F 
                ,REMARKS_F1 
                ,STATUS 
                ,BEING_EDITED 
                ,OVERRIDE_FLAG 
                ,NPV_AMOUNT 
                ,ECL_AMOUNT 
                ,UNWINDING_AMOUNT 
                ,''SP_IFRS_IMPI_PROCESS'' AS CREATEDBY 
                ,CURRENT_TIMESTAMP::DATE AS CREATEDDATE 
                ,CREATEDHOST 
                ,PLAFOND_CIF 
                ,SOURCE_SYSTEM
            FROM ' || V_TABLEINSERT4 || ' 
            WHERE DCFID IS NOT NULL 
            AND F_EOMONTH(DOWNLOAD_DATE, 0, ''M'', ''NEXT'') <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);
    END IF;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLENAME_AMORT || ' 
        SET IMPAIRED_FLAG = CASE WHEN B.MASTERID IS NOT NULL THEN ''I'' ELSE ''C'' END 
        FROM ' || V_TABLENAME_AMORT || ' A 
        LEFT JOIN 
        (
            SELECT EFFECTIVE_DATE, MASTERID 
            FROM ' || V_TABLEINSERT4 || ' 
            WHERE EFFECTIVE_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND MASTERID NOT IN 
            (
                SELECT MASTERID 
                FROM IFRS_ECL_EXCLUSION 
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ) 
        ) B 
        ON A.DOWNLOAD_DATE >= F_EOMONTH(B.EFFECTIVE_DATE, 0, ''M'', ''NEXT'') 
        AND A.MASTERID = B.MASTERID 
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND A.DATA_SOURCE NOT IN (''LOAN_T24'', ''LIMIT_T24'', ''TRADE_T24'') ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLENAME || ' 
        SET IMPAIRED_FLAG = CASE WHEN B.MASTERID IS NOT NULL THEN ''I'' ELSE ''C'' END 
        FROM ' || V_TABLENAME || ' A 
        LEFT JOIN 
        (
            SELECT EFFECTIVE_DATE, MASTERID 
            FROM ' || V_TABLEINSERT4 || ' 
            WHERE EFFECTIVE_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND MASTERID NOT IN 
            (
                SELECT MASTERID 
                FROM IFRS_ECL_EXCLUSION 
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ) 
        ) B 
        ON A.DOWNLOAD_DATE >= F_EOMONTH(B.EFFECTIVE_DATE, 0, ''M'', ''NEXT'') 
        AND A.MASTERID = B.MASTERID 
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);
    
    -- RAISE NOTICE 'SP_IFRS_IMPI_PROCESS | AFFECTED RECORD : %', V_RETURNROWS2;
    -------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLENAME;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_IMPI_PROCESS';
    V_OPERATION = 'UPDATE';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLENAME || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;