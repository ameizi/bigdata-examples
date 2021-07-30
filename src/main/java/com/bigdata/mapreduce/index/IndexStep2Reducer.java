package com.bigdata.mapreduce.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IndexStep2Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        int count = 0;
        for (Text value : values) {
            count += Integer.parseInt(value.toString().split("-")[1]);
            builder.append(value).append(" ");
        }
        context.write(new Text(key + "(" + count + ")"), new Text(builder.toString().trim()));
    }

}
