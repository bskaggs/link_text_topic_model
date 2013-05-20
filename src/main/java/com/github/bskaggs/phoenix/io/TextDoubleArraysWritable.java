package com.github.bskaggs.phoenix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class TextDoubleArraysWritable implements Writable {
	private Text[] textArray;
	private double[] doubleArray;
	
	public TextDoubleArraysWritable() {
		textArray = new Text[0];
		doubleArray = new double[0];
	}
	
	public TextDoubleArraysWritable(Text[] textArray, double[] doubleArray) {
		super();
		this.textArray = textArray;
		this.doubleArray = doubleArray;
	}

	public double[] getDoubleArray() {
		return doubleArray;
	}
	public Text[] getTextArray() {
		return textArray;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int size = WritableUtils.readVInt(in);
		if (textArray == null || textArray.length != size) {
			textArray = new Text[size];
			doubleArray = new double[size];
		}
		for (int i = 0; i < size; i++) {
			textArray[i] = new Text();
			textArray[i].readFields(in);
			doubleArray[i] = in.readDouble();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, textArray.length);
		for (int i = 0; i < textArray.length; i++) {
			textArray[i].write(out);
			out.writeDouble(doubleArray[i]);
		}
	}
	
	@Override
	public String toString() {
		StringBuilder result = new StringBuilder('[');
		
		for (int i = 0; i < textArray.length; i++) {
			if (i != 0) {
				result.append(", ");
			}
			result.append(textArray[i]);
			result.append(':');
			result.append(doubleArray[i]);
		}
		result.append(']');
		return result.toString();
	}

}
