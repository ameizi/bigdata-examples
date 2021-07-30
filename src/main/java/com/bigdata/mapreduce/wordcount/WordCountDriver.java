package com.bigdata.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 描述 job 信息
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration();

        // 此参数表示将mr job以本地模式运行
        // conf.set("mapreduce.framework.name","local");
        // 指定MapReduce程序的运行平台为yarn
        // conf.set("mapreduce.framework.name","yarn");
        // 此参数表示mr job所要访问的文件系统为本地文件系统
        // conf.set("fs.defaultFS","file:///tmp");

        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCountDriver.class);

        // 指定本次job所要用的mapper类和reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 指定本次job的mapper类和reducer类输出结果的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 指定本次job读取源数据时所需要用的输入组件：源文件在本地文件系统中，用TextInputFormat
        job.setInputFormatClass(TextInputFormat.class);
        // 指定本次job输出数据时所需要用的输出组件：输出到本地文件系统中，用TextOutputFormat
        job.setOutputFormatClass(TextOutputFormat.class);

        // 指定reduce task运行实例的个数
        job.setNumReduceTasks(1);

        // 判断结果输出路径是否已存在，如果已经存在，则删除。以免在测试阶段需要反复手动删除输出目录
        FileSystem fs = FileSystem.get(conf);
        Path out = new Path("./data-output/wc-output");
        if (fs.exists(out)) {
            fs.delete(out, true);
        }

        // 指定job输入数据目录为本地文件系统目录
        FileInputFormat.setInputPaths(job, new Path("./data-input/wc-input"));
        // 指定job输出结果目录为本地文件系统目录
        FileOutputFormat.setOutputPath(job, out);

        // 核心代码： 提交jar包给yarn
        // job.submit();  // 提交完任务，客户端就退出了
        // 提交完任务，客户端打印集群中的运行进度信息，并等待最终运行状态
        boolean completion = job.waitForCompletion(true);
        System.exit(completion ? 0 : 1);
    }

}
