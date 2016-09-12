package com.hadoop.skillspeed.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		String word = key.toString();
		char letter = word.toLowerCase().charAt(0);
		return (((int)letter) % 26)+1;
	}
}