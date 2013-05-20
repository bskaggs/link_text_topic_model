/**
 * 
 */
package com.github.bskaggs.phoenix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.github.bskaggs.phoenix.io.TextDoubleMapWritable;

public class RandomWalkMapper extends Mapper<Text, Text, Text, TextDoubleMapWritable> {
	private double alpha;
	private double tolerance;
	private Reader linkFile;
	
	private final static class EntryComparator implements Comparator<Map.Entry<Text,Double>> {
		public int compare(Map.Entry<Text, Double> o1, Map.Entry<Text, Double> o2) {
			return o1.getValue().compareTo(o2.getValue());
		}
	}
	
	private final int MAX_ENTRIES = 1000;
	private Map<Text, List<Text>> linkCache = new LinkedHashMap<Text, List<Text>>(MAX_ENTRIES+1, .75f, true) {
		protected boolean removeEldestEntry(Map.Entry<Text,java.util.List<Text>> eldest) {
			return size() > MAX_ENTRIES;
		}
	};
	
	private Text linksText = new Text();
	private List<Text> getLinks(Text source) throws IOException {
		List<Text> cachedResult = linkCache.get(source);
		if (cachedResult != null) {
			return cachedResult;
		}
		if (linkFile.get(source, linksText) == null) {
			List<Text> result = Collections.emptyList();
			linkCache.put(source, result);
			return result;
		}
		List<Text> result = new ArrayList<Text>();
		byte[] bytes = linksText.getBytes();
		int valid = linksText.getLength();
		
		int lastTab = -1;
		byte tab = '\t';
		for (int i = 0; i < valid; i++) {
			if (bytes[i] == tab) {
				byte[] newBytes = new byte[(i - lastTab) - 1];
				System.arraycopy(bytes, lastTab + 1, newBytes, 0, newBytes.length);
				result.add(new Text(newBytes));
				lastTab = i;
			}
		}
		byte[] newBytes = new byte[(valid - lastTab) - 1];
		System.arraycopy(bytes, lastTab + 1, newBytes, 0, newBytes.length);
		result.add(new Text(newBytes));
		
		linkCache.put(new Text(source), result);
		return result;
	}
	
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		alpha = conf.getFloat("phoenix.alpha", .95f);
		tolerance = conf.getFloat("phoenix.tolerance", 0.001f);
		linkFile = new MapFile.Reader(FileSystem.get(conf), new Path(conf.get("phoenix.links_path"), "part-r-00000").toString(), conf);
	}
	
	
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		TextDoubleMapWritable result = new TextDoubleMapWritable();
		Map<Text, Double> qVals = new HashMap<Text, Double>();
		RandomWalkMapper.EntryComparator comp = new EntryComparator();		
		qVals.put(key, 1d);
		
		while (!qVals.isEmpty()) {
			context.progress();
			Entry<Text, Double> entry = Collections.max(qVals.entrySet(), comp);
			Text i = entry.getKey();
			double w = entry.getValue();
			qVals.remove(i);
			Double previous = result.get(i);
			if (previous != null) {
				result.put(i, previous + alpha * w);
			} else {
				result.put(i, alpha * w);
			}
			if (w < tolerance) {
				continue;
			}
			List<Text> links = getLinks(i);
			int size = links.size();
			if (size > 0) {
				double weight = (1 - alpha) * w / size;
				for (Text link : links) {
					previous = qVals.get(link);
					if (previous != null) {
						qVals.put(link, previous + weight);
					} else {
						qVals.put(link, weight);
					}
				}
			}
		}
		context.write(key, result);
	}
}