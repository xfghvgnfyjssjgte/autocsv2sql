# autocsv2sql
自动化csv转sql脚本
v_file_name与v_table_name需要自定义，ls_dir目录要先通过CREATE DIRECTORY ls_dir AS '/home/oracle/ls_dir';命令建立
csv文件生成时要进行基本的格式化，我的定义为凡是有小数点的数字，将被定义为数值型，其它皆为字符型
个人采用100万条随机数据测试通过

