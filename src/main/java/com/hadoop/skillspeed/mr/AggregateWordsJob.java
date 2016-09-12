package com.hadoop.skillspeed.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AggregateWordsJob {

	private static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String words[] = line.split("\t");
			context.write(new Text(words[0]), new DoubleWritable(Double.parseDouble(words[2])));
		}
	}

	private static class Reduce extends Reducer<Text, DoubleWritable, Text, AggregateWritable> {
		@Override
		protected void reduce(Text text, Iterable<DoubleWritable> counts, Reducer<Text, DoubleWritable, Text, AggregateWritable>.Context context)
				throws IOException, InterruptedException {
			Double sum = 0d;
			int length = 0;
			Double max = Double.MIN_VALUE;
			Double min = Double.MAX_VALUE;
			for (DoubleWritable count : counts) {
				sum += count.get();
				if (max < count.get()) {
					max = count.get();
				}
				if (min > count.get()) {
					min = count.get();
				}
				length++;
			}
			AggregateData aggregateData = new AggregateData();
			aggregateData.setSum(sum);
			aggregateData.setCount(length * 1.0);
			aggregateData.setMin(min);
			aggregateData.setMax(max);
			AggregateWritable aggregateWritable = new AggregateWritable(aggregateData);
			context.write(text, aggregateWritable);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "AGGREGATEWords");

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(26);
		
		job.setPartitionerClass(CustomPartitioner.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		Path out = new Path(args[1]);
		out.getFileSystem(conf).deleteOnExit(out);
		job.waitForCompletion(true);
	}
}
