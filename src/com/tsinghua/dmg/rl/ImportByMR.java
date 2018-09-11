// https://docs.qingcloud.com/product/big_data/hbase/README.html
/*
    使用 MapReduce 导入数据有三种方案：
    1. 直接书写 MapReduce 使用 HBase 提供的 JAVA API 从 HDFS 导入到 HBase 表。
    2. 书写 MapReduce 将 HDFS 中数据转化为 HFile 格式，再使用 HBase 的 BulkLoad 工具导入到 HBase 表。
    3. 使用 HBase ImportTsv 工具将格式化的 HDFS 数据导入到 HBase 表。

    这里是1。

    hadoop jar mapreduceHBaseHDFS.jar  /user/mrtest0911

    执行成功后可简单通过测试一中的 HBase Shell 来验证数据。

 */
package com.tsinghua.dmg.rl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
//先创建表，在 Map 中完成数据解析，在 Reduce 中完成入库。Reduce的个数相当于入库线程数。
public class ImportByMR {

    private static String table = "test_import";

    private static class ImportByMRMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] sp = value.toString().split(" ");
            if (sp.length < 2) {
                return;
            }
            context.write(new Text(sp[0]), new Text(sp[1]));
        }
    }

    private static class ImportByMRReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            byte[] bRowKey = key.toString().getBytes();
            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(bRowKey);

            for (Text t : value) {
                Put p = new Put(bRowKey);
                p.setDurability(Durability.SKIP_WAL);
                p.addColumn("content".getBytes(), "a".getBytes(), t.toString().getBytes());
                context.write(rowKey, p);
            }
        }
    }

    private static void createTable(Configuration conf) throws IOException {
        TableName tableName = TableName.valueOf(table);
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        if (admin.tableExists(tableName)) {
            System.out.println("table exists!recreating.......");
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        HTableDescriptor htd = new HTableDescriptor(tableName);
        HColumnDescriptor tcd = new HColumnDescriptor("content");
        htd.addFamily(tcd);
        admin.createTable(htd);
    }

    public static void main(String[] argv) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        File file = new File("/home/publiccluster/software/hbase-1.4.7/conf/hbase-site.xml");
        FileInputStream in = new FileInputStream(file);
        conf.addResource(in);
        createTable(conf);
        System.out.println("!!!!!!!!!!!!!!!!!remainingArgs[0]:");
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, argv);
        String[] remainingArgs = optionParser.getRemainingArgs();
        System.out.println("!!!!!!!!!!!!!!!!!remainingArgs[0]: "+remainingArgs[0]);

        Job job = Job.getInstance(conf, ImportByMR.class.getSimpleName());
        job.setJarByClass(ImportByMR.class);
        job.setMapperClass(ImportByMRMapper.class);
        TableMapReduceUtil.initTableReducerJob(table, ImportByMRReducer.class, job);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Mutation.class);
        job.setNumReduceTasks(1); //可自行修改 job.setNumReduceTasks() 中 Reduce 数目
        FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}