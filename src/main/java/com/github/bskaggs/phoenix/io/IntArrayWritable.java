package com.github.bskaggs.phoenix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class IntArrayWritable implements Writable {
	public int[] array;
	
	public IntArrayWritable() {
		
	}
	
	public IntArrayWritable(int[] array) {
		this.array = array;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int size = WritableUtils.readVInt(in);
		if (array == null || size != array.length) {
			array = new int[size];
		}
		for (int i = 0; i < size; i++) {
			array[i] = WritableUtils.readVInt(in);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, array.length);
		for (int i : array) {
			WritableUtils.writeVInt(out, i);
		}
	}
	
	@Override
	public String toString() {
		return Arrays.toString(array);
	}

}
