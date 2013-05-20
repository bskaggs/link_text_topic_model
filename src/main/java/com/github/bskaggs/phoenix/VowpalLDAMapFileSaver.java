package com.github.bskaggs.phoenix;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.github.bskaggs.phoenix.io.DoubleArrayWritable;
/**
 * Saves the result of running  gensim's LDA code to a MapFile
 * @author brad
 * Arg 0 is the sequence file with the labels
 * Arg 1 is the map file output dir
 */
public class VowpalLDAMapFileSaver extends Configured implements Tool{
		
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(args[0]), conf);
		WritableComparable<?> key = (WritableComparable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in, Charset.forName("UTF-8")));
		MapFile.Writer out = new MapFile.Writer(conf, fs, args[1], Text.class, DoubleArrayWritable.class);
		DoubleArrayWritable outValue = new DoubleArrayWritable();
		Writable[] outArray = null;
		
		int num = 0;
		while (reader.next(key)) {
			if (num++ % 1000 == 0) {
				System.out.println("Saved " + num);
			}
			String line = in.readLine();
			if (line == null)
				break;
			if (outArray == null) {
				outArray = new Writable[line.split("\\s").length];
				for (int i = 0; i < outArray.length; i++) {
					outArray[i] = new DoubleWritable();
				}
				outValue.set(outArray);
			}
			Scanner sc = new Scanner(line);
			int pos = 0;
			while (pos < outArray.length) {
				((DoubleWritable) outArray[pos++]).set(sc.nextDouble());
			}
			out.append(key, outValue);
		}
		reader.close();
		out.close();
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new VowpalLDAMapFileSaver(), args));
	}
}