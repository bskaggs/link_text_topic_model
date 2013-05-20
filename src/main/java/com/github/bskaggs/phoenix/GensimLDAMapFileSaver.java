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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.github.bskaggs.phoenix.io.DoubleArrayWritable;
/**
 * Saves the result of running  gensim's LDA code to a MapFile
 * @author brad
 * Arg 0 is the text file in HDFS with the labels
 * Arg 1 is the sequence file output dir
 */
public class GensimLDAMapFileSaver extends Configured implements Tool{
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		//reader of 
		BufferedReader labelReader = new BufferedReader(new InputStreamReader(fs.open(new Path(args[0])), Charset.forName("UTF-8")));
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in, Charset.forName("UTF-8")));
		
		SequenceFile.Writer out = SequenceFile.createWriter(fs, conf, new Path(args[1]), Text.class, DoubleArrayWritable.class);
		DoubleArrayWritable outValue = new DoubleArrayWritable();
		Writable[] outArray = null;
		
		int num = 0;
		String inLine;
		Text label = new Text();
		while ((inLine = in.readLine()) != null) {
			if (++num % 10000 == 0) {
				System.out.println("Saved " + num);
			}
			
			label.set(labelReader.readLine()); 
			if (outArray == null) {
				outArray = new Writable[inLine.split("\\s").length - 1];
				for (int i = 0; i < outArray.length; i++) {
					outArray[i] = new DoubleWritable();
				}
				outValue.set(outArray);
			}
			Scanner sc = new Scanner(inLine);
			//burn the document number
			sc.nextInt();
			int pos = 0;
			while (pos < outArray.length) {
				((DoubleWritable) outArray[pos++]).set(sc.nextDouble());
			}
			out.append(label, outValue);
		}
		labelReader.close();
		out.close();
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new GensimLDAMapFileSaver(), args));
	}
}