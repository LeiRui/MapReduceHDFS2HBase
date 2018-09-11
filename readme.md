https://docs.qingcloud.com/product/big_data/hbase/README.html

使用 MapReduce 导入数据有三种方案：
1. 直接书写 MapReduce 使用 HBase 提供的 JAVA API 从 HDFS 导入到 HBase 表。
2. 书写 MapReduce 将 HDFS 中数据转化为 HFile 格式，再使用 HBase 的 BulkLoad 工具导入到 HBase 表。
3. 使用 HBase ImportTsv 工具将格式化的 HDFS 数据导入到 HBase 表。

这里的代码是上述教程中的1示例代码。（主要是项目化在pom那块花费了一些时间）

执行命令：

hadoop jar mapreduceHBaseHDFS.jar  /user/mrtest0911

执行成功后可简单通过测试一中的 HBase Shell 来验证数据。

记得jps确认resourceManager运行，不运行的话尝试start-yarn.sh
