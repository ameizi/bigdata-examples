package com.bigdata.mapreduce.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IndexStep2Mapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // c++-c.txt	2
        String[] split = line.split("\t");// {"c++-c.txt","2"}
        String[] words = split[0].split("-"); // {"c++","c.txt"}
        context.write(new Text(words[0]), new Text(words[1] + "-" + split[1]));
    }
}
