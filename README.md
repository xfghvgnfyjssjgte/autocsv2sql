# 项目名称 csv2sql

- 构建自动化的csv转sql 表格脚本

## 特性

- 替代plsql中的手动文本导入表格的功能，大大减少工作量，提高工作效率
- csv文件生成时要进行基本的格式化，我的定义为凡是有小数点的数字，将被定义为数值型，其它皆为字符型
- 个人采用100万条随机数据测试通过
- 3个不同的版本，最终的是带脏数据过滤功能的

## 使用方法
- v_file_name与v_table_name需要自定义，ls_dir目录要先通过CREATE DIRECTORY ls_dir AS '/home/oracle/ls_dir';命令建立
-  plsql 直接调用
