/**
 * 
 */
package com.github.bskaggs.phoenix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

public class LongPairWritable implements Writable {
	public long first;
	public long second;
	public LongPairWritable() {
	}
	
	public LongPairWritable(long first, long second) {
		this.first = first;
		this.second = second;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readLong();
		second = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(first);
		out.writeLong(second);
	}

	@Override
	public String toString() {
		return Arrays.toString(new long[] { first, second } );
	}
}