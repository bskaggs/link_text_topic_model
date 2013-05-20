/**
 * 
 */
package com.github.bskaggs.phoenix.sampler;

import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

final public class Dictionary {
	final private TObjectIntHashMap<Text> numbers;
	final private TIntObjectHashMap<Text> reverse;
	final private SequenceFile.Writer writer;
	private int current = 1;
	
	private IntWritable value = new IntWritable();
	
	public Dictionary(SequenceFile.Writer writer, int capacity) {
		this.writer = writer;
		this.numbers = new TObjectIntHashMap<Text>(capacity);
		this.reverse = null;
	}
	
	public Dictionary(SequenceFile.Reader reader) throws IOException {
		this(reader, false);
	}
	
	public Dictionary(SequenceFile.Reader reader, boolean reverse) throws IOException {
		this.writer = null;
		this.numbers = new TObjectIntHashMap<Text>();
		if (!reverse) {
			this.reverse = null;
		} else {
			this.reverse = new TIntObjectHashMap<Text>();
		}
		Text text;
		
		while (reader.next(text = new Text(), value)) {
			numbers.put(text, value.get());
			if (reverse) {
				this.reverse.put(value.get(), text);
			}
		}
	}
	public int get(Text text) throws IOException {
		int res = numbers.get(text);
		if (res == 0) {
			res = current++;
			numbers.put(new Text(text), res);
			value.set(res);
			writer.append(text, value);
		}
		return res;
	}
	
	public int getOnly(Text text) {
		return numbers.get(text);
	}
	
	public int getOnly(String text) {
		return getOnly (new Text(text));
	}
	
	public Text getReverse(int val) {
		return reverse.get(val);
	}

}