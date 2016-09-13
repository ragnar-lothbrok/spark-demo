package com.hadoop.skillspeed.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AggregateWordsJobCombiner {

	private static class Map extends Mapper<LongWritable, Text, Text, AggregateWritable> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, AggregateWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String words[] = line.split("\t");
			AggregateData aggregateData = new AggregateData();
			AggregateWritable aggregateWritable = new AggregateWritable(aggregateData);
			aggregateData.setMax(Double.parseDouble(words[2]));
			context.write(new Text(words[0]), aggregateWritable);
		}
	}

	private static class Combiner extends Reducer<Text, AggregateWritable, Text, AggregateWritable> {
		@Override
		protected void reduce(Text text, Iterable<AggregateWritable> counts, Reducer<Text, AggregateWritable, Text, AggregateWritable>.Context context)
				throws IOException, InterruptedException {
			Double sum = 0d;
			int length = 0;
			Double max = Double.MIN_VALUE;
			Double min = Double.MAX_VALUE;
			for (AggregateWritable count : counts) {
				sum += count.getAggregateData().getMax();
				if (max < count.getAggregateData().getMax()) {
					max = count.getAggregateData().getMax();
				}
				if (min > count.getAggregateData().getMax()) {
					min = count.getAggregateData().getMax();
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

	private static class Reduce extends Reducer<Text, AggregateWritable, Text, AggregateWritable> {
		@Override
		protected void reduce(Text text, Iterable<AggregateWritable> counts,
				Reducer<Text, AggregateWritable, Text, AggregateWritable>.Context context) throws IOException, InterruptedException {
			Double max = Double.MIN_VALUE;
			Double min = Double.MAX_VALUE;
			AggregateData aggregateData = new AggregateData();
			for (AggregateWritable count : counts) {
				aggregateData.setCount(aggregateData.getCount() + count.getAggregateData().getCount());
				aggregateData.setSum(aggregateData.getSum() + count.getAggregateData().getSum());
				if (count.getAggregateData().getMax() > max) {
					max = count.getAggregateData().getMax();
				}
				if (count.getAggregateData().getMin() < min) {
					min = count.getAggregateData().getMin();
				}
			}
			aggregateData.setMin(min);
			aggregateData.setMax(max);
			AggregateWritable aggregateWritable = new AggregateWritable(aggregateData);
			context.write(text, aggregateWritable);
		}
	}

	public static void main(String[] args) {

		try {
			Configuration conf = new Configuration();

			Job job = Job.getInstance(conf, "AGGREGATEWords");

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(AggregateWritable.class);

			job.setMapperClass(Map.class);
			job.setCombinerClass(Combiner.class);
			job.setReducerClass(Reduce.class);
//			job.setNumReduceTasks(26);
//
//			job.setPartitionerClass(CustomPartitioner.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			Path out = new Path(args[1]);
			out.getFileSystem(conf).deleteOnExit(out);
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
