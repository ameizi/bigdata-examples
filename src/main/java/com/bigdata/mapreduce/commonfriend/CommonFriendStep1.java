package com.bigdata.mapreduce.commonfriend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CommonFriendStep1 {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(CommonFriendStep1.class);

        // 设置job的mapper类
        job.setMapperClass(CommonFriendStep1Mapper.class);
        // 设置job的reducer类
        job.setReducerClass(CommonFriendStep1Reducer.class);

        // 设置map阶段输出的key：value数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // 设置reduce阶段输出的key：value数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 判断结果输出路径是否已存在，如果已经存在，则删除。以免在测试阶段需要反复手动删除输出目录
        FileSystem fs = FileSystem.get(conf);
        Path out = new Path("./data-output/friend-output1");
        if (fs.exists(out)) {
            fs.delete(out, true);
        }

        // 设置数据输入输出路径
        FileInputFormat.setInputPaths(job, new Path("./data-input/friend-input"));
        FileOutputFormat.setOutputPath(job, out);

        // 提交job给yarn或者local runner来运行
        job.waitForCompletion(true);

    }

}
