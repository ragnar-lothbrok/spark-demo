package com.hadoop.skillspeed.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class AggregateWritable implements Writable {

	private AggregateData aggregateData;

	public AggregateWritable(AggregateData aggregateData) {
		super();
		this.aggregateData = aggregateData;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(aggregateData.getCount());
		out.writeDouble(aggregateData.getMax());
		out.writeDouble(aggregateData.getMin());
		out.writeDouble(aggregateData.getSum());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		aggregateData.setCount(in.readDouble());
		aggregateData.setMax(in.readDouble());
		aggregateData.setMin(in.readDouble());
		aggregateData.setSum(in.readDouble());
	}

	@Override
	public String toString() {
		return "AggregateWritable [aggregateData=" + aggregateData + "]";
	}

}
