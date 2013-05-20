package com.github.bskaggs.phoenix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPairWritable implements WritableComparable<TextPairWritable>{
	public Text first;
	public Text second;
	
	public TextPairWritable() {
		this("", "");
	}
	public TextPairWritable(Text first, Text second) {
		this.first = first;
		this.second = second;
	}
	
	public TextPairWritable(String first, String second) {
		this(new Text(first), new Text(second));
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	@Override
	public int compareTo(TextPairWritable other) {
		int res = first.compareTo(other.first);
		if (res != 0)
			return res;
		return second.compareTo(other.second);
	}
	
	@Override
	public boolean equals(Object obj) {
		return compareTo((TextPairWritable) obj) == 0;
	}
	
	@Override
	public int hashCode() {
		return first.hashCode() * 37 + second.hashCode();
	}

	@Override
	public String toString() {
		return Arrays.toString(new Text[] {first, second});
	}
}
