package com.github.bskaggs.phoenix.sampler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.github.bskaggs.phoenix.io.IntDoubleArraysWritable;
import com.github.bskaggs.phoenix.io.TextDoubleArraysWritable;
import com.github.bskaggs.phoenix.io.TextIntPairWritable;
import com.github.bskaggs.phoenix.io.TextPairWritable;


@SuppressWarnings("deprecation")
public class DecompressResults extends Configured implements Tool{
	@Override
	public int run(String[] args) throws Exception {
		Path working = new Path(args[0]);
		
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		
		Reader articleDictionaryReader = new SequenceFile.Reader(fs,  new Path(working, "article_dictionary"), conf);
		Dictionary articleDictionary = new Dictionary(articleDictionaryReader, true);
		
		Path resultsPath = new Path(working, "sampler_results");
		Reader resultsReader = new SequenceFile.Reader(fs, resultsPath, conf);
		
		Writer decompressedWriter = SequenceFile.createWriter(fs, conf, new Path(working, "unsorted_decompressed_sampler_results"), TextPairWritable.class, TextDoubleArraysWritable.class,  CompressionType.BLOCK);
		
		TextIntPairWritable keyPair = new TextIntPairWritable();
		IntDoubleArraysWritable valuePair = new IntDoubleArraysWritable();
		
		while (resultsReader.next(keyPair, valuePair)) {
			Text dabText = articleDictionary.getReverse(keyPair.getVal());
			int[] nums = valuePair.getIntArray();
			Text[] textArray = new Text[nums.length];
			for (int i = 0; i < nums.length; i++) {
				textArray[i] = articleDictionary.getReverse(nums[i]);
			}
			double[] probs = valuePair.getDoubleArray();
			decompressedWriter.append(new TextPairWritable(keyPair.getText(), dabText), new TextDoubleArraysWritable(textArray, probs));
		}
		decompressedWriter.close();
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new DecompressResults(), args));
	}
}
