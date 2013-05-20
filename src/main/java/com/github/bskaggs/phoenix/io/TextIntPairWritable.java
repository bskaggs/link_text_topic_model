package com.github.bskaggs.phoenix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class TextIntPairWritable implements WritableComparable<TextIntPairWritable>{
	private Text text;
	private int val;
	
	
	public TextIntPairWritable() {
		text = new Text("");
		val = 0;
	}
	
	
	
	public TextIntPairWritable(Text text, int val) {
		super();
		this.text = text;
		this.val = val;
	}



	@Override
	public void readFields(DataInput in) throws IOException {
		text.readFields(in);
		val = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		text.write(out);
		out.writeInt(val);
	}



	public Text getText() {
		return text;
	}



	public int getVal() {
		return val;
	}



	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((text == null) ? 0 : text.hashCode());
		result = prime * result + val;
		return result;
	}



	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TextIntPairWritable other = (TextIntPairWritable) obj;
		if (text == null) {
			if (other.text != null)
				return false;
		} else if (!text.equals(other.text))
			return false;
		if (val != other.val)
			return false;
		return true;
	}



	@Override
	public int compareTo(TextIntPairWritable o) {
		int res = text.compareTo(o.text);
		if (res != 0)
			return res;
		if (val < o.val)
			return -1;
		if (val > o.val)
			return 1;
		return 0;
	}
	
	@Override
	public String toString() {
		return "[" + text + "," + val + "]";
	}

}
