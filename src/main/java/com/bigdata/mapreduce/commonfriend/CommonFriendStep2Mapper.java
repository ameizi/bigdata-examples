package com.bigdata.mapreduce.commonfriend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CommonFriendStep2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    /**
     * 上一个job所产生的数据是本次job读取的数据： B-C	A
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 将数据按制表符切分
        String[] split = value.toString().split("\t");
        // 将切出来的B-C用户对作为key，共同好友A作为value
        // 返回给map task
        context.write(new Text(split[0]), new Text(split[1]));
    }
}
