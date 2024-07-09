CREATE OR REPLACE PROCEDURE CSVToSQL (
    v_file_name IN VARCHAR2,
    v_table_name IN VARCHAR2
) AS
    v_directory_name VARCHAR2(255) := 'MYDATA'; -- CSV�ļ�����Ŀ¼
    v_line VARCHAR2(4000); -- ���ڶ�ȡÿһ������
    v_first_line BOOLEAN := TRUE; -- ��ʶ�Ƿ��ǵ�һ��
    v_col_list SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST(); -- �����б�
    v_col_type SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST(); -- �������б�
    v_separator VARCHAR2(1) := ','; -- CSV�ļ��ָ���

    v_file UTL_FILE.FILE_TYPE; -- �ļ����
    v_row_count NUMBER := 0; -- �Ѵ�����������
    v_max_sample_rows CONSTANT NUMBER := 5; -- ���ڷ����������������
    v_table_col_count NUMBER; -- �������
    v_imported_rows NUMBER; --���յ����¼����

    -- ���������ݷָ����ָ��ַ���Ϊ�б�
    FUNCTION split_string(p_str IN VARCHAR2, p_sep IN VARCHAR2)
        RETURN SYS.ODCIVARCHAR2LIST
    IS
        l_list SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST();
        l_start NUMBER := 1;
        l_index NUMBER;
    BEGIN
        LOOP
            l_index := INSTR(p_str, p_sep, l_start);
            EXIT WHEN l_index = 0;
            l_list.EXTEND;
            l_list(l_list.COUNT) := SUBSTR(p_str, l_start, l_index - l_start);
            l_start := l_index + LENGTH(p_sep);
        END LOOP;
        l_list.EXTEND;
        l_list(l_list.COUNT) := SUBSTR(p_str, l_start);
        RETURN l_list;
    END split_string;

    -- �������ж��Ƿ�����Ч������
    FUNCTION is_valid_column_name(p_name IN VARCHAR2) RETURN BOOLEAN IS
    BEGIN
        IF p_name IS NULL OR LENGTH(TRIM(p_name)) = 0 THEN
            RETURN FALSE;
        END IF;

        -- �����в��ܰ����ո��Ʊ����
        FOR i IN 1..LENGTH(p_name) LOOP
            IF ASCII(SUBSTR(p_name, i, 1)) IN (9, 10, 13, 32) THEN
                RETURN FALSE;
            END IF;
        END LOOP;

        RETURN TRUE;
    END is_valid_column_name;

    -- ����������ַ����Ƿ�Ϊ����
    FUNCTION is_number(p_str IN VARCHAR2) RETURN BOOLEAN IS
        l_num NUMBER;
    BEGIN
        l_num := TO_NUMBER(p_str);
        RETURN TRUE;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN FALSE;
    END is_number;

    -- ����������ַ����Ƿ����С����
    FUNCTION has_decimal(p_str IN VARCHAR2) RETURN BOOLEAN IS
    BEGIN
        RETURN INSTR(p_str, '.') > 0;
    END has_decimal;

    -- ����������ַ����Ƿ�Ϊ��ѧ������
    FUNCTION is_scientific_notation(p_str IN VARCHAR2) RETURN BOOLEAN IS
    BEGIN
        RETURN REGEXP_LIKE(p_str, '^[+-]?[0-9]+(\.[0-9]+)?[eE][+-]?[0-9]+$');
    END is_scientific_notation;

BEGIN
    -- ����DBMS_OUTPUT��������С
    DBMS_OUTPUT.ENABLE(91000000);

    -- �����Ƿ���ڲ�ɾ�����б�
    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE ' || v_table_name;
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -942 THEN -- ORA-00942: �����ͼ������
                RAISE;
            END IF;
    END;

    -- ��CSV�ļ�
    v_file := UTL_FILE.FOPEN(v_directory_name, v_file_name, 'R');

    -- ��ȡ��������������������
    WHILE v_row_count < v_max_sample_rows LOOP
        BEGIN
            -- ��ȡһ������
            UTL_FILE.GET_LINE(v_file, v_line);
            v_row_count := v_row_count + 1;

            IF v_first_line THEN
                -- ��ȡ��һ�У���ȡ����
                v_col_list := split_string(v_line, v_separator);
                FOR i IN 1..v_col_list.COUNT LOOP
                    v_col_type.EXTEND;
                    v_col_type(i) := 'VARCHAR2(4000)'; -- Ĭ��ΪVARCHAR2����
                END LOOP;
                v_first_line := FALSE;
            ELSE
                -- ���������У�ȷ���е���������
                DECLARE
                    v_values SYS.ODCIVARCHAR2LIST := split_string(v_line, v_separator);
                    v_index NUMBER;
                    v_is_number BOOLEAN;
                    v_has_decimal BOOLEAN;
                    v_is_scientific BOOLEAN;
                BEGIN
                    FOR v_index IN 1..v_values.COUNT LOOP
                        IF v_values(v_index) IS NOT NULL THEN
                            -- ��鵱ǰֵ�Ƿ�����ֵ����
                            v_is_number := is_number(v_values(v_index));
                            -- ��鵱ǰֵ�Ƿ����С����
                            v_has_decimal := has_decimal(v_values(v_index));
                            -- ��鵱ǰֵ�Ƿ��ǿ�ѧ������
                            v_is_scientific := is_scientific_notation(v_values(v_index));
                            
                            -- �����ǰֵ����ֵ�����Ҳ�������ѧ�����������ǵ�ǰ��ΪNUMBER����
                            -- ����ǿ�ѧ����������ΪVARCHAR2����
                            IF v_is_number AND NOT v_is_scientific AND v_has_decimal THEN
                                v_col_type(v_index) := 'NUMBER';
                            ELSE
                                v_col_type(v_index) := 'VARCHAR2(4000)';
                            END IF;
                        END IF;
                    END LOOP;
                END;
            END IF;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                EXIT; -- �ļ���ȡ���
        END;
    END LOOP;

    -- �ر��ļ�
    UTL_FILE.FCLOSE(v_file);

    -- ������
    DECLARE
        v_column_name VARCHAR2(30);
        v_include_comma BOOLEAN := FALSE;
        v_create_sql VARCHAR2(4000) := 'CREATE TABLE ' || v_table_name || ' (';
    BEGIN
        FOR i IN 1..v_col_list.COUNT LOOP
            v_column_name := TRIM(v_col_list(i));
            IF is_valid_column_name(v_column_name) THEN
                v_create_sql := v_create_sql || v_column_name || ' ' || v_col_type(i);
                IF i < v_col_list.COUNT THEN
                    v_create_sql := v_create_sql || ', ';
                END IF;
                v_include_comma := TRUE;
            END IF;
        END LOOP;

        -- �Ƴ�ĩβ���ܴ��ڵĶ��źͿո�
        IF v_include_comma THEN
            v_create_sql := REGEXP_REPLACE(v_create_sql, ',\s*$', '');
        END IF;

        v_create_sql := v_create_sql || ')';

        -- ������
        DBMS_OUTPUT.PUT_LINE('���ݱ�ɹ�������' || v_create_sql);
        EXECUTE IMMEDIATE v_create_sql;
    END;

    -- ��ȡĿ��������
    SELECT COUNT(*) INTO v_table_col_count
    FROM USER_TAB_COLUMNS
    WHERE TABLE_NAME = UPPER(v_table_name);

    -- ��CSV�ļ��Բ�������
    v_file := UTL_FILE.FOPEN(v_directory_name, v_file_name, 'R');
    v_first_line := TRUE;

    -- ��������
    BEGIN
        LOOP
            UTL_FILE.GET_LINE(v_file, v_line);

            IF v_first_line THEN
                v_first_line := FALSE; -- ������һ�У������У�
            ELSE
                -- ����������
                DECLARE
                    v_values SYS.ODCIVARCHAR2LIST := split_string(v_line, v_separator);
                    TYPE t_value_list IS TABLE OF VARCHAR2(4000) INDEX BY PLS_INTEGER;
                    v_value_list t_value_list;
                    v_insert_sql VARCHAR2(32767);
                    v_clean_value VARCHAR2(4000);
                    v_include_comma BOOLEAN := FALSE;
                BEGIN
                    -- �����������
                    v_insert_sql := 'INSERT INTO ' || v_table_name || ' VALUES (';

                    FOR i IN 1..LEAST(v_values.COUNT, v_table_col_count) LOOP
                        IF v_values.EXISTS(i) THEN
                            IF v_col_type(i) = 'NUMBER' THEN
                                -- ������ֵ�����ֶΣ���ϴ���ɼ��ַ�
                                IF v_values(i) IS NOT NULL THEN
                                    v_clean_value := TRIM(REPLACE(REPLACE(v_values(i), CHR(10), ''), CHR(13), ''));
                                    v_insert_sql := v_insert_sql || TO_NUMBER(v_clean_value);
                                ELSE
                                    v_insert_sql := v_insert_sql || 'NULL';
                                END IF;
                            ELSE
                                -- �����ı������ֶΣ�ȷ�����ַ���ʹ�����������Ž���ת��
                                IF v_values(i) IS NOT NULL THEN
                                    v_clean_value := TRIM(REPLACE(REPLACE(REPLACE(v_values(i), CHR(10), ''), CHR(13), ''), ' ', ''));
                                    v_insert_sql := v_insert_sql || '''' || REPLACE(v_clean_value, '''', '''''') || '''';
                                ELSE
                                    v_insert_sql := v_insert_sql || 'NULL';
                                END IF;
                            END IF;
                        ELSE
                            v_insert_sql := v_insert_sql || 'NULL';
                        END IF;

                        -- ��Ӷ��ŷָ����������һ����֮��
                        IF i < LEAST(v_values.COUNT, v_table_col_count) THEN
                            v_insert_sql := v_insert_sql || ', ';
                        END IF;
                    END LOOP;

                    v_insert_sql := v_insert_sql || ')';

                    -- ִ�в������
                    EXECUTE IMMEDIATE v_insert_sql;
                    END;
            END IF;
        END LOOP;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            NULL; -- �ļ���ȡ���
    END;
        -- �ر��ļ�
    UTL_FILE.FCLOSE(v_file);
    -- �ύ����
    COMMIT;

    BEGIN
          EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_table_name INTO v_imported_rows;
        DBMS_OUTPUT.PUT_LINE('�������¼����' || v_imported_rows);
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('����: ' || SQLERRM);
    END;

END CSVToSQL;
/
