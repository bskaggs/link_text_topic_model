package com.github.bskaggs.phoenix.sampler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Writer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.github.bskaggs.phoenix.io.TextDoubleArraysWritable;
import com.github.bskaggs.phoenix.io.TextPairWritable;
import com.github.bskaggs.phoenix.sampler.SparseCountFactory.Entry;
import com.github.bskaggs.phoenix.sampler.SparseCountFactory.SparseCounts;


public class AverageSamples extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String rootDir = args[0];
		FileSystem fs = FileSystem.get(conf); 
	
		final int start = args.length > 1 ? Integer.parseInt(args[1]) : 0;
		final int end = args.length > 2 ? Integer.parseInt(args[2]) : Integer.MAX_VALUE;
		
		FileStatus[] statuses = fs.listStatus(new Path(rootDir, "sampler_working"), new PathFilter() {
			@Override
			public boolean accept(Path path) {
				if (path.getName().matches("\\A\\d+\\Z")) {
					int iteration = Integer.parseInt(path.getName());
					return (start <= iteration && iteration <= end);
				}
				return false;
			}
		});
		
		Writer writer = new MapFile.Writer(conf, fs, rootDir + "/sorted_decompressed_sampler_results/part-r-00000", TextPairWritable.class, TextDoubleArraysWritable.class, CompressionType.BLOCK);
		
		System.out.println("Files to read: " + statuses.length);
		
		
		for (FileStatus status : statuses) {
			System.out.println(status.getPath().getName());
			Reader reader = new SequenceFile.Reader(fs, status.getPath(), conf);
			addFile(reader);
			reader.close();
		}
		
		Reader articleDictionaryReader = new SequenceFile.Reader(fs,  new Path(rootDir, "article_dictionary"), conf);
		Dictionary articleDictionary = new Dictionary(articleDictionaryReader, true);
		
		
		List<Text> names = new ArrayList<Text>(counts.keySet());
		Collections.sort(names);
		
		Map<TextPairWritable, TextDoubleArraysWritable> buffer = new TreeMap<TextPairWritable, TextDoubleArraysWritable>();
		long articleCount = 0;
		for (Text name : names) 
		{
			articleCount++;
			if (articleCount % 100000 == 0) {
				System.out.println(articleCount);
			}
			
			SparseCounts[] articleCounts = counts.get(name);
			int[] articleCorrespondingDabs = correspondingDabs.get(name);
			for (int i = 0; i < articleCorrespondingDabs.length; i++) {
				long sum = 0;
				int numEntries = 0;
				for (Entry entry : articleCounts[i]) {
					sum += entry.count;
					if (articleDictionary.getReverse(entry.topic) != null) {
						numEntries++;
					}
				}
				
				if (numEntries > 0) {
					double[] doubleArray = new double[numEntries];
					Text[] textArray = new Text[numEntries];
					int j = 0;
					for (Entry entry : articleCounts[i]) {
						Text text = articleDictionary.getReverse(entry.topic);
						if (text != null) {
							doubleArray[j] = entry.count / ((double) sum);
							textArray[j] = text;
							j++;
						}
					}
					
					Text dabText = articleDictionary.getReverse(articleCorrespondingDabs[i]);
					TextDoubleArraysWritable arrays = new TextDoubleArraysWritable(textArray, doubleArray);
					buffer.put(new TextPairWritable(name, dabText), arrays);
				}
			}

			//output sorted buffer
			for (Map.Entry<TextPairWritable, TextDoubleArraysWritable> entry : buffer.entrySet()) {
				writer.append(entry.getKey(), entry.getValue());
			}
			buffer.clear();
		}
		
		writer.close();
		return 0;
	}
	
	private HashMap<Text, SparseCounts[]> counts = new HashMap<Text, SparseCounts[]>();
	private HashMap<Text, int[]> correspondingDabs = new HashMap<Text, int[]>();
	
	private SparseCountFactory factory = new SparseCountFactory(1L << 31);
	private void addFile(Reader reader) throws IOException {
		Text articleName = new Text();
		LatentAndVisibleArticle document = new LatentAndVisibleArticle();
		long articleCount = 0;
		while(reader.next(articleName, document)) {
			articleCount++;
			if (articleCount % 100000 == 0) {
				System.out.println(articleCount);
			}
			SparseCounts[] articleCounts = counts.get(articleName); 
			int ambiguousLength = document.ambiguousLength;
			if (articleCounts == null) {
				Text nameCopy = new Text(articleName);
				int[] articleCorrespondingDabs = Arrays.copyOf(document.ambiguousLinkTargets, ambiguousLength);
				correspondingDabs.put(nameCopy, articleCorrespondingDabs);
				articleCounts = new SparseCounts[ambiguousLength];
				for (int i = 0; i < ambiguousLength; i ++) {
					articleCounts[i] = factory.create();
				}
				counts.put(nameCopy, articleCounts);
			}
			int[] sampledTargets = document.ambiguousLinkSampledTargets;
			for (int i = 0; i < ambiguousLength; i++) {
				articleCounts[i].increment(sampledTargets[i]);
			}
			//System.out.println(articleName + "\t" + Arrays.deepToString(articleCounts));
		}
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new AverageSamples(), args));
	}
}
