package com.hadoop;
import java.util.*;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class processUnit {

    //mapper class
    public static class  E_EMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>
    {
       //Map funnction
        public void  map(LongWritable key, Text value,OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException{
            if (value == null) ;
            String[] splits = value.toString().split("\\s+");
            String year = splits[0];
            String lasttoken = splits[splits.length - 1];

            int avgprice = Integer.parseInt(lasttoken);
            output.collect(new Text(year), new IntWritable(avgprice));
        }
    }

    //Reducer class
    public static class E_EReduce extends MapReduceBase implements Reducer< Text, IntWritable, Text, IntWritable > {

        //Reduce function
        public void reduce( Text key, Iterator <IntWritable> values,
                            OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int maxavg = 30;
            int val = Integer.MIN_VALUE;

            while (values.hasNext()) {
                if((val = values.next().get())>maxavg) {
                    output.collect(key, new IntWritable(val));
                }
            }
        }
    }

    //Main function
    public static void main(String args[])throws Exception {
        JobConf conf = new JobConf(processUnit.class);

        conf.setJobName("max_eletricityunits");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(E_EMapper.class);
        conf.setCombinerClass(E_EReduce.class);
        conf.setReducerClass(E_EReduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}

