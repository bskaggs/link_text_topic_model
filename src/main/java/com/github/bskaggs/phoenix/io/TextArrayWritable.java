package com.github.bskaggs.phoenix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class TextArrayWritable implements Writable {
	public Text[] array;
	
	public TextArrayWritable() {
		
	}
	
	public TextArrayWritable(Collection<String> strings) {
		array = new Text[strings.size()];
		int i = 0;
		for (String s : strings) {
			array[i++] = new Text(s);
		}
	}
	
	public TextArrayWritable(String[] strings) {
		this(Arrays.asList(strings));
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int size = WritableUtils.readVInt(in);
		if (array == null || size != array.length) {
			array = new Text[size];
		}
		for (int i = 0; i < size; i++) {
			(array[i] = new Text()).readFields(in);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, array.length);
		for (Text t : array) {
			t.write(out);
		}
	}
	
	@Override
	public String toString() {
		return Arrays.toString(array);
	}

}
