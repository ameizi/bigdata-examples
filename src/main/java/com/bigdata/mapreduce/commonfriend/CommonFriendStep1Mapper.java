package com.bigdata.mapreduce.commonfriend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CommonFriendStep1Mapper extends Mapper<LongWritable, Text, Text, Text> {

    // 输入数据形式如：A:B,C,D,F,E,O
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        // 将maptask所传入的一行数据按照冒号切分
        String[] split = value.toString().split(":");
        // 得到数据中的用户
        String user = split[0];
        // 得到数据中的"好友们"
        String[] friends = split[1].split(",");

        // 将每一个"好友"作为key，用户作为value，返回给maptask
        for (String f : friends) {
            context.write(new Text(f), new Text(user));
        }
    }

}
