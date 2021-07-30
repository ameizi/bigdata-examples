package com.bigdata.mapreduce.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IndexStep2Driver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(IndexStep2Driver.class);

        job.setMapperClass(IndexStep2Mapper.class);
        job.setReducerClass(IndexStep2Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem fs = FileSystem.get(conf);
        Path out = new Path("./data-output/index-output2");
        if (fs.exists(out)) {
            fs.delete(out, true);
        }

        FileInputFormat.setInputPaths(job, new Path("./data-output/index-output1"));
        FileOutputFormat.setOutputPath(job, out);

        boolean completion = job.waitForCompletion(true);
        System.exit(completion ? 0 : 1);
    }

}
