package com.hadoop.skillspeed.mr;

import java.io.Serializable;

public class AggregateData implements Serializable {

	private static final long serialVersionUID = 1L;

	private Double count;
	private Double sum;
	private Double max;
	private Double min;

	public Double getCount() {
		return count;
	}

	public void setCount(Double count) {
		this.count = count;
	}

	public Double getSum() {
		return sum;
	}

	public void setSum(Double sum) {
		this.sum = sum;
	}

	public Double getMax() {
		return max;
	}

	public void setMax(Double max) {
		this.max = max;
	}

	public Double getMin() {
		return min;
	}

	public void setMin(Double min) {
		this.min = min;
	}

	@Override
	public String toString() {
		return "AggregateData [count=" + count + ", sum=" + sum + ", max=" + max + ", min=" + min + "]";
	}

}
