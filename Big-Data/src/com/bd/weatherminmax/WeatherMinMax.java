package com.bd.weatherminmax;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherMinMax {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		private Text dateTxt = new Text();
		private Text minMaxTxt = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			if (value != null && value.toString() != null && value.toString().trim().length() > 0) {
				String[] input = value.toString().split("\\s+");

				String date = input[1];
				Double min = new Double(input[5]);
				Double max = new Double(input[6]);

				if (min < 10.0) {
					dateTxt.set(date);
					context.write(dateTxt, new Text("Cold Day"));
				} else if (max > 40.0) {
					dateTxt.set(date);
					context.write(dateTxt, new Text("Hot Day"));
				} else {
					dateTxt.set(date);
					context.write(dateTxt, new Text("Normal Day"));
				}
			}

		}
	}

	// public static class IntSumReducer extends Reducer<Text, Text, Text, Text>
	// {
	// private Text dateTxt = new Text();
	// private Text dayStatusTxt = new Text();
	//
	// public void reduce(Text key, Iterable<IntWritable> values, Context
	// context) throws IOException, InterruptedException {
	// int sum = 0;
	// for (IntWritable val : values) {
	// sum += val.get();
	// }
	// result.set(sum);
	// context.write(key, result);
	// }
	// }

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "weather max");
		job.setJarByClass(WeatherMinMax.class);
		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		// job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
