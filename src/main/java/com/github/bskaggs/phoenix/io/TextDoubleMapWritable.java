package com.github.bskaggs.phoenix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class TextDoubleMapWritable extends HashMap<Text, Double> implements Writable{
	@Override
	public void readFields(DataInput in) throws IOException {
		clear();
		int num = WritableUtils.readVInt(in);
		
		for (int i = 0; i < num; i++) {
			Text key = new Text();
			key.readFields(in);
			Double value = new Double(in.readDouble());
			put(key, value);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, size());
		for(Entry<Text, Double> entry : entrySet()) {
			entry.getKey().write(out);
			out.writeDouble(entry.getValue());
		}
	}
}
