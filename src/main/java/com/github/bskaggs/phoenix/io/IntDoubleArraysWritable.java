package com.github.bskaggs.phoenix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class IntDoubleArraysWritable implements Writable {
	private int[] intArray;
	private double[] doubleArray;
	
	public IntDoubleArraysWritable() {
		intArray = new int[0];
		doubleArray = new double[0];
	}
	
	public IntDoubleArraysWritable(int[] intArray, double[] doubleArray) {
		super();
		this.intArray = intArray;
		this.doubleArray = doubleArray;
	}

	public double[] getDoubleArray() {
		return doubleArray;
	}
	public int[] getIntArray() {
		return intArray;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int size = WritableUtils.readVInt(in);
		if (intArray == null || intArray.length != size) {
			intArray = new int[size];
			doubleArray = new double[size];
		}
		for (int i = 0; i < size; i++) {
			intArray[i] = in.readInt();
			doubleArray[i] = in.readDouble();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, intArray.length);
		for (int i = 0; i < intArray.length; i++) {
			out.writeInt(intArray[i]);
			out.writeDouble(doubleArray[i]);
		}
	}
	
	@Override
	public String toString() {
		StringBuilder result = new StringBuilder('[');
		
		for (int i = 0; i < intArray.length; i++) {
			if (i != 0) {
				result.append(", ");
			}
			result.append(intArray[i]);
			result.append(':');
			result.append(doubleArray[i]);
		}
		result.append(']');
		return result.toString();
	}

}
