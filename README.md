# autocsv2sql
自动化csv转sql脚本
v_file_name与v_table_name需要自定义，ls_dir目录要先通过CREATE DIRECTORY ls_dir AS '/home/oracle/ls_dir';命令建立
csv文件生成时要进行基本的格式化，我的定义为凡是有小数点的数字，将被定义为数值型，其它皆为字符型
个人采用100万条随机数据测试通过
#

DECLARE
    v_directory_name VARCHAR2(255) := 'LS_DIR'; -- 目录名称，需事先创建
    v_file_name VARCHAR2(255) := 'sh_dy2.csv'; -- CSV文件名
    v_table_name VARCHAR2(255) := 'sh_dy2'; -- 生成的表名
    v_line VARCHAR2(4000); -- 用于读取每一行数据
    v_first_line BOOLEAN := TRUE; -- 用于标识是否是第一行
    v_col_list SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST(); -- 列名列表
    v_col_type SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST(); -- 列类型列表
    v_separator VARCHAR2(1) := ','; -- CSV文件中的分隔符

    v_file UTL_FILE.FILE_TYPE;
    v_row_count NUMBER := 0; -- 用于计数读取的行数
    v_max_sample_rows CONSTANT NUMBER := 5; -- 用于定义样本行数，这里调整为更大的值，提高样本分析的准确性

    -- 用于分割字符串的函数
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

    -- 检查字符串是否为数字
    FUNCTION is_number(p_str IN VARCHAR2) RETURN BOOLEAN IS
        l_num NUMBER;
    BEGIN
        l_num := TO_NUMBER(p_str);
        RETURN TRUE;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN FALSE;
    END;

    -- 检查字符串是否包含小数点
    FUNCTION has_decimal(p_str IN VARCHAR2) RETURN BOOLEAN IS
    BEGIN
        RETURN INSTR(p_str, '.') > 0;
    END;

BEGIN
    -- 增加DBMS_OUTPUT缓冲区大小
    DBMS_OUTPUT.ENABLE(1000000);

    -- 检查表是否存在并删除已有的表
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
            UTL_FILE.GET_LINE(v_file, v_line);
            v_row_count := v_row_count + 1;

            IF v_first_line THEN
                -- 读取第一行，提取列名
                v_col_list := split_string(v_line, v_separator);
                FOR i IN 1..v_col_list.COUNT LOOP
                    v_col_type.EXTEND;
                    v_col_type(i) := 'VARCHAR2(4000)'; -- 默认设置为VARCHAR2类型
                END LOOP;
                v_first_line := FALSE;
            ELSE
                -- 分析数据行，确定列的数据类型
                DECLARE
                    v_values SYS.ODCIVARCHAR2LIST := split_string(v_line, v_separator);
                BEGIN
                    FOR i IN 1..v_values.COUNT LOOP
                        IF v_values(i) IS NOT NULL THEN
                            IF has_decimal(v_values(i)) THEN
                                v_col_type(i) := 'NUMBER'; -- 含有小数点的字段定义为NUMBER类型
                            ELSE
                                v_col_type(i) := 'VARCHAR2(4000)'; -- 其他字段定义为VARCHAR2(4000)类型
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
        v_create_sql VARCHAR2(4000) := 'CREATE TABLE ' || v_table_name || ' (';
    BEGIN
        FOR i IN 1..v_col_list.COUNT LOOP
            -- 截取列名长度，保证在Oracle标识符长度限制内
            v_create_sql := v_create_sql || SUBSTR(REPLACE(v_col_list(i), ' ', '_'), 1, 30) || ' ' || v_col_type(i);
            IF i < v_col_list.COUNT THEN
                v_create_sql := v_create_sql || ', ';
            END IF;
        END LOOP;
        v_create_sql := v_create_sql || ')';

        EXECUTE IMMEDIATE v_create_sql;
    END;

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
                BEGIN
                    -- 构建插入语句
                    v_insert_sql := 'INSERT INTO ' || v_table_name || ' VALUES (';
                    FOR i IN 1..v_col_list.COUNT LOOP
                        IF v_values.EXISTS(i) THEN
                            IF v_col_type(i) = 'NUMBER' THEN
                                v_insert_sql := v_insert_sql || TO_NUMBER(v_values(i));
                            ELSE
                                v_insert_sql := v_insert_sql || '''' || REPLACE(v_values(i), '''', '''''') || '''';
                            END IF;
                        ELSE
                            v_insert_sql := v_insert_sql || 'NULL';
                        END IF;
                        IF i < v_col_list.COUNT THEN
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

    -- 提交事务
    COMMIT;

    -- 关闭文件
    UTL_FILE.FCLOSE(v_file);

    DBMS_OUTPUT.PUT_LINE('数据已成功导入到表：' || v_table_name);
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('错误: ' || SQLERRM);
        IF UTL_FILE.IS_OPEN(v_file) THEN
            UTL_FILE.FCLOSE(v_file);
        END IF;
END;
/
