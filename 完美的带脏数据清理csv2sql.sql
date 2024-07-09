CREATE OR REPLACE PROCEDURE CSVToSQL (
    v_file_name IN VARCHAR2,
    v_table_name IN VARCHAR2
) AS
    v_directory_name VARCHAR2(255) := 'MYDATA'; -- CSV文件所在目录
    v_line VARCHAR2(4000); -- 用于读取每一行数据
    v_first_line BOOLEAN := TRUE; -- 标识是否是第一行
    v_col_list SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST(); -- 列名列表
    v_col_type SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST(); -- 列类型列表
    v_separator VARCHAR2(1) := ','; -- CSV文件分隔符

    v_file UTL_FILE.FILE_TYPE; -- 文件句柄
    v_row_count NUMBER := 0; -- 已处理行数计数
    v_max_sample_rows CONSTANT NUMBER := 5; -- 用于分析的最大样本行数
    v_table_col_count NUMBER; -- 表的列数
    v_imported_rows NUMBER; --最终导入记录条数

    -- 函数：根据分隔符分割字符串为列表
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

    -- 函数：判断是否是有效的列名
    FUNCTION is_valid_column_name(p_name IN VARCHAR2) RETURN BOOLEAN IS
    BEGIN
        IF p_name IS NULL OR LENGTH(TRIM(p_name)) = 0 THEN
            RETURN FALSE;
        END IF;

        -- 列名中不能包含空格、制表符等
        FOR i IN 1..LENGTH(p_name) LOOP
            IF ASCII(SUBSTR(p_name, i, 1)) IN (9, 10, 13, 32) THEN
                RETURN FALSE;
            END IF;
        END LOOP;

        RETURN TRUE;
    END is_valid_column_name;

    -- 函数：检查字符串是否为数字
    FUNCTION is_number(p_str IN VARCHAR2) RETURN BOOLEAN IS
        l_num NUMBER;
    BEGIN
        l_num := TO_NUMBER(p_str);
        RETURN TRUE;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN FALSE;
    END is_number;

    -- 函数：检查字符串是否包含小数点
    FUNCTION has_decimal(p_str IN VARCHAR2) RETURN BOOLEAN IS
    BEGIN
        RETURN INSTR(p_str, '.') > 0;
    END has_decimal;

    -- 函数：检查字符串是否为科学计数法
    FUNCTION is_scientific_notation(p_str IN VARCHAR2) RETURN BOOLEAN IS
    BEGIN
        RETURN REGEXP_LIKE(p_str, '^[+-]?[0-9]+(\.[0-9]+)?[eE][+-]?[0-9]+$');
    END is_scientific_notation;

BEGIN
    -- 增大DBMS_OUTPUT缓冲区大小
    DBMS_OUTPUT.ENABLE(91000000);

    -- 检查表是否存在并删除已有表
    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE ' || v_table_name;
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -942 THEN -- ORA-00942: 表或视图不存在
                RAISE;
            END IF;
    END;

    -- 打开CSV文件
    v_file := UTL_FILE.FOPEN(v_directory_name, v_file_name, 'R');

    -- 读取并分析列名和数据类型
    WHILE v_row_count < v_max_sample_rows LOOP
        BEGIN
            -- 读取一行数据
            UTL_FILE.GET_LINE(v_file, v_line);
            v_row_count := v_row_count + 1;

            IF v_first_line THEN
                -- 读取第一行，提取列名
                v_col_list := split_string(v_line, v_separator);
                FOR i IN 1..v_col_list.COUNT LOOP
                    v_col_type.EXTEND;
                    v_col_type(i) := 'VARCHAR2(4000)'; -- 默认为VARCHAR2类型
                END LOOP;
                v_first_line := FALSE;
            ELSE
                -- 分析数据行，确定列的数据类型
                DECLARE
                    v_values SYS.ODCIVARCHAR2LIST := split_string(v_line, v_separator);
                    v_index NUMBER;
                    v_is_number BOOLEAN;
                    v_has_decimal BOOLEAN;
                    v_is_scientific BOOLEAN;
                BEGIN
                    FOR v_index IN 1..v_values.COUNT LOOP
                        IF v_values(v_index) IS NOT NULL THEN
                            -- 检查当前值是否是数值类型
                            v_is_number := is_number(v_values(v_index));
                            -- 检查当前值是否包含小数点
                            v_has_decimal := has_decimal(v_values(v_index));
                            -- 检查当前值是否是科学计数法
                            v_is_scientific := is_scientific_notation(v_values(v_index));
                            
                            -- 如果当前值是数值类型且不包含科学计数法，则标记当前列为NUMBER类型
                            -- 如果是科学计数法则标记为VARCHAR2类型
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
                EXIT; -- 文件读取完毕
        END;
    END LOOP;

    -- 关闭文件
    UTL_FILE.FCLOSE(v_file);

    -- 创建表
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

        -- 移除末尾可能存在的逗号和空格
        IF v_include_comma THEN
            v_create_sql := REGEXP_REPLACE(v_create_sql, ',\s*$', '');
        END IF;

        v_create_sql := v_create_sql || ')';

        -- 创建表
        DBMS_OUTPUT.PUT_LINE('数据表成功创建：' || v_create_sql);
        EXECUTE IMMEDIATE v_create_sql;
    END;

    -- 获取目标表的列数
    SELECT COUNT(*) INTO v_table_col_count
    FROM USER_TAB_COLUMNS
    WHERE TABLE_NAME = UPPER(v_table_name);

    -- 打开CSV文件以插入数据
    v_file := UTL_FILE.FOPEN(v_directory_name, v_file_name, 'R');
    v_first_line := TRUE;

    -- 插入数据
    BEGIN
        LOOP
            UTL_FILE.GET_LINE(v_file, v_line);

            IF v_first_line THEN
                v_first_line := FALSE; -- 跳过第一行（列名行）
            ELSE
                -- 处理数据行
                DECLARE
                    v_values SYS.ODCIVARCHAR2LIST := split_string(v_line, v_separator);
                    TYPE t_value_list IS TABLE OF VARCHAR2(4000) INDEX BY PLS_INTEGER;
                    v_value_list t_value_list;
                    v_insert_sql VARCHAR2(32767);
                    v_clean_value VARCHAR2(4000);
                    v_include_comma BOOLEAN := FALSE;
                BEGIN
                    -- 构建插入语句
                    v_insert_sql := 'INSERT INTO ' || v_table_name || ' VALUES (';

                    FOR i IN 1..LEAST(v_values.COUNT, v_table_col_count) LOOP
                        IF v_values.EXISTS(i) THEN
                            IF v_col_type(i) = 'NUMBER' THEN
                                -- 处理数值类型字段，清洗不可见字符
                                IF v_values(i) IS NOT NULL THEN
                                    v_clean_value := TRIM(REPLACE(REPLACE(v_values(i), CHR(10), ''), CHR(13), ''));
                                    v_insert_sql := v_insert_sql || TO_NUMBER(v_clean_value);
                                ELSE
                                    v_insert_sql := v_insert_sql || 'NULL';
                                END IF;
                            ELSE
                                -- 处理文本类型字段，确保空字符串使用两个单引号进行转义
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

                        -- 添加逗号分隔符，除最后一个列之外
                        IF i < LEAST(v_values.COUNT, v_table_col_count) THEN
                            v_insert_sql := v_insert_sql || ', ';
                        END IF;
                    END LOOP;

                    v_insert_sql := v_insert_sql || ')';

                    -- 执行插入语句
                    EXECUTE IMMEDIATE v_insert_sql;
                    END;
            END IF;
        END LOOP;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            NULL; -- 文件读取完毕
    END;
        -- 关闭文件
    UTL_FILE.FCLOSE(v_file);
    -- 提交事务
    COMMIT;

    BEGIN
          EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_table_name INTO v_imported_rows;
        DBMS_OUTPUT.PUT_LINE('共导入记录数：' || v_imported_rows);
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('错误: ' || SQLERRM);
    END;

END CSVToSQL;
/
