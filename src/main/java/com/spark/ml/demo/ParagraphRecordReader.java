package com.spark.ml.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ParagraphRecordReader extends RecordReader<LongWritable, Text> {
	private long end;
	private boolean stillInChunk = true;

	private LongWritable key = new LongWritable();
	private Text value = new Text();

	private FSDataInputStream fsin;
	private DataOutputBuffer buffer = new DataOutputBuffer();

	private byte[] endTag = "\n\r\n".getBytes();

	public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		FileSplit split = (FileSplit) inputSplit;
		Configuration conf = taskAttemptContext.getConfiguration();
		Path path = split.getPath();
		FileSystem fs = path.getFileSystem(conf);

		fsin = fs.open(path);
		long start = split.getStart();
		end = split.getStart() + split.getLength();
		fsin.seek(start);

		if (start != 0) {
			readUntilMatch(endTag, false);
		}
	}

	public boolean nextKeyValue() throws IOException {
		if (!stillInChunk)
			return false;

		boolean status = readUntilMatch(endTag, true);

		value = new Text();
		value.set(buffer.getData(), 0, buffer.getLength());
		key = new LongWritable(fsin.getPos());
		buffer.reset();

		if (!status) {
			stillInChunk = false;
		}

		return true;
	}

	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	public void close() throws IOException {
		fsin.close();
	}

	private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
		int i = 0;
		while (true) {
			int b = fsin.read();
			if (b == -1)
				return false;
			if (withinBlock)
				buffer.write(b);
			if (b == match[i]) {
				i++;
				if (i >= match.length) {
					return fsin.getPos() < end;
				}
			} else
				i = 0;
		}
	}

}