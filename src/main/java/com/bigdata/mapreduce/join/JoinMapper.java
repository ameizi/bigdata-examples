package com.bigdata.mapreduce.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class JoinMapper extends Mapper<LongWritable, Text, Text, JoinBean> {

    private String fileName;

    @Override
    protected void setup(Context context) throws IOException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Text outKey = new Text();
        JoinBean outValue = new JoinBean();
        if ("customer.txt".equals(fileName)) {
            // 客户数据
            String[] split = line.split("\t");
            // 客户id
            outKey.set(split[0]);
            outValue.set(split[0], split[1], split[2], split[3], "NULL", "NULL", fileName);
        } else if ("order.txt".equals(fileName)) {
            // 订单数据
            String[] split = line.split("\t");
            // 客户id
            outKey.set(split[1]);
            outValue.set(split[1], "NULL", "NULL", "NULL", split[0], split[2], fileName);
        }
        context.write(outKey, outValue);
    }
}
