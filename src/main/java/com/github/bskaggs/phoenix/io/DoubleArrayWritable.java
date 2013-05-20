package com.github.bskaggs.phoenix.io;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

public class DoubleArrayWritable extends ArrayWritable {

	public DoubleArrayWritable() {
		super(DoubleWritable.class);
	}
	
	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		boolean first = true;
		for (Writable w : get()) {
			if (!first) {
				result.append(' ');
			}
			result.append(((DoubleWritable) w).get());
			first = false;
		}
		return result.toString();
	}
}
