/**
 * 
 */
package com.github.bskaggs.phoenix;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Link implements WritableComparable<Link> {
	public Text from = new Text();
	public Text to = new Text();
	
	public Link() {
		this("", "");
	}
	public Link(String from, String to) {
		this(new Text(from), new Text(to));
	}
	public Link(Text from, Text to) {
		this.from = from;
		this.to = to;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		from.readFields(in);
		to.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		from.write(out);
		to.write(out);
		
	}

	@Override
	public int compareTo(Link other) {
		int res = from.compareTo(other.from);
		if (res == 0) {
			return to.compareTo(other.to);
		}
		return res;
	}
	
	@Override
	public String toString() {
		return Arrays.toString(new Text[]{from, to});
	}
	@Override
	public boolean equals(Object obj) {
		Link other = (Link) obj;
		return from.equals(other.from) && to.equals(other.to);
	}
	@Override
	public int hashCode() {
		return from.hashCode() * 31 + to.hashCode();
	}
}