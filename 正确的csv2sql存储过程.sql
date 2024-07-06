CREATE OR REPLACE PROCEDURE CSVToSQL_ok (
    v_file_name IN VARCHAR2,
    v_table_name IN VARCHAR2
) AS
    v_directory_name VARCHAR2(255) := 'LS_DIR'; -- CSV�ļ�����Ŀ¼
    v_line VARCHAR2(4000); -- ���ڶ�ȡÿһ������
    v_first_line BOOLEAN := TRUE; -- ��ʶ�Ƿ��ǵ�һ��
    v_col_list SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST(); -- �����б�
    v_col_type SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST(); -- �������б�
    v_separator VARCHAR2(1) := ','; -- CSV�ļ��ָ���

    v_file UTL_FILE.FILE_TYPE; -- �ļ����
    v_row_count NUMBER := 0; -- �Ѵ�����������
    v_max_sample_rows CONSTANT NUMBER := 5; -- ���ڷ����������������

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
    END;

    -- ����������ַ����Ƿ�Ϊ����
    FUNCTION is_number(p_str IN VARCHAR2) RETURN BOOLEAN IS
        l_num NUMBER;
    BEGIN
        l_num := TO_NUMBER(p_str);
        RETURN TRUE;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN FALSE;
    END;

    -- ����������ַ����Ƿ����С����
    FUNCTION has_decimal(p_str IN VARCHAR2) RETURN BOOLEAN IS
    BEGIN
        RETURN INSTR(p_str, '.') > 0;
    END;

BEGIN
    -- ����DBMS_OUTPUT��������С
    DBMS_OUTPUT.ENABLE(1000000);

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
                BEGIN
                    FOR v_index IN 1..v_values.COUNT LOOP
                        IF v_values(v_index) IS NOT NULL THEN
                            -- ��鵱ǰֵ�Ƿ�����ֵ����
                            v_is_number := is_number(v_values(v_index));
                            -- ��鵱ǰֵ�Ƿ����С����
                            v_has_decimal := has_decimal(v_values(v_index));
                            -- �����ǰֵ����ֵ�����Ұ���С���㣬���ǵ�ǰ��ΪNUMBER����
                            IF v_is_number AND v_has_decimal THEN
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
        v_create_sql VARCHAR2(4000) := 'CREATE TABLE ' || v_table_name || ' (';
    BEGIN
        FOR i IN 1..v_col_list.COUNT LOOP
            -- ��ȡ�������ȣ���֤��Oracle��ʶ������������
            v_create_sql := v_create_sql || SUBSTR(REPLACE(v_col_list(i), ' ', '_'), 1, 30) || ' ' || v_col_type(i);
            IF i < v_col_list.COUNT THEN
                v_create_sql := v_create_sql || ', ';
            END IF;
        END LOOP;
        v_create_sql := v_create_sql || ')';

        EXECUTE IMMEDIATE v_create_sql;
    END;

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
                BEGIN
                    -- �����������
                    v_insert_sql := 'INSERT INTO ' || v_table_name || ' VALUES (';
                    FOR i IN 1..v_col_list.COUNT LOOP
                        IF v_values.EXISTS(i) THEN
                            IF v_col_type(i) = 'NUMBER' THEN
                                -- ������ֵ�����ֶ�
                                IF v_values(i) IS NOT NULL THEN
                                    v_insert_sql := v_insert_sql || TO_NUMBER(v_values(i));
                                ELSE
                                    v_insert_sql := v_insert_sql || 'NULL';
                                END IF;
                            ELSE
                                -- �����ı������ֶΣ�ȷ�����ַ���ʹ�����������Ž���ת��
                                IF v_values(i) IS NOT NULL THEN
                                    v_insert_sql := v_insert_sql || '''' || REPLACE(v_values(i), '''', '''''') || '''';
                                ELSE
                                    v_insert_sql := v_insert_sql || 'NULL';
                                END IF;
                            END IF;
                        ELSE
                            v_insert_sql := v_insert_sql || 'NULL';
                        END IF;
                        IF i < v_col_list.COUNT THEN
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

    -- �ύ����
    COMMIT;

    -- �ر��ļ�
    UTL_FILE.FCLOSE(v_file);

    DBMS_OUTPUT.PUT_LINE('�����ѳɹ����뵽��' || v_table_name);
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('����: ' || SQLERRM);
        IF UTL_FILE.IS_OPEN(v_file) THEN
            UTL_FILE.FCLOSE(v_file);
        END IF;
END;
/
