package com.github.bskaggs.phoenix.io;

import gnu.trove.iterator.TObjectLongIterator;
import gnu.trove.map.hash.TObjectLongHashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class TextLongMapWritable extends TObjectLongHashMap<Text> implements Writable{
	
	@Override
	public void readFields(DataInput in) throws IOException {
		clear();
		int num = WritableUtils.readVInt(in);
		
		for (int i = 0; i < num; i++) {
			Text key = new Text();
			key.readFields(in);
			long value = WritableUtils.readVLong(in);
			put(key, value);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, size());
		TObjectLongIterator<Text> it = iterator();
		while (it.hasNext()) {
			it.advance();
			it.key().write(out);
			WritableUtils.writeVLong(out, it.value());
		}
	}
}
