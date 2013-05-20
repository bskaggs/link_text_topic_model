package com.github.bskaggs.phoenix;

import java.io.IOException;
import java.io.StringReader;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import com.github.bskaggs.phoenix.io.TextPairWritable;
import com.github.bskaggs.phoenix.lucene.AlmostStandardAnalyzer;

public class LinkProbabilityFinder extends Configured implements Tool {
	
	public static class LinkProbabilityMapper extends Mapper<Text, Text, TextPairWritable, LongWritable> {
		private final Pattern tabSplitter = Pattern.compile("\t");
	
		private final TextPairWritable tpw = new TextPairWritable();
		private final LongWritable one = new LongWritable(1L);

		private Analyzer analyzer;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Class<? extends Analyzer> analyzerClass = context.getConfiguration().getClass("phoenix.analyzer", AlmostStandardAnalyzer.class, Analyzer.class);
			analyzer = ReflectionUtils.newInstance(analyzerClass, context.getConfiguration());
		}
		
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = tabSplitter.split(value.toString());
			tpw.second.set(parts[0]);
			for (int i = 1; i < parts.length; i++) {
				StringReader reader = new StringReader(parts[i]);
				TokenStream stream = analyzer.reusableTokenStream("text", reader);
				TermAttribute termAttribute = stream.getAttribute(TermAttribute.class);
				StringBuilder result = new StringBuilder();
				boolean first = true;
				while (stream.incrementToken()) {
					if (!first) {
						result.append(' ');
					}
					result.append(termAttribute.term());
					first = false;
				}
				if (result.length() > 0) {
					tpw.first.set(result.toString());
					context.write(tpw, one);
				}
			}
		}
	}
	
	public static class SameFirstPartitioner extends Partitioner<TextPairWritable, LongWritable> {
		@Override
		public int getPartition(TextPairWritable key, LongWritable value, int numPartitions) {
			return key.first.hashCode() % numPartitions;
		}
	}
	
	public static class SameFirstGroupingComparator extends WritableComparator {
		protected SameFirstGroupingComparator() {
			super(TextPairWritable.class, true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			TextPairWritable t1 = (TextPairWritable) a;
			TextPairWritable t2 = (TextPairWritable) b;
			return t1.first.compareTo(t2.first);
		}
	}
	
	public static class LinkProbabilityCombiner extends Reducer<TextPairWritable, LongWritable, TextPairWritable, LongWritable> {
		protected void reduce(TextPairWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable value : values) {
				sum += value.get();
			}
			context.write(key, new LongWritable(1L));
		}
	}
	
	public static class LinkProbabilityReducer extends Reducer<TextPairWritable, LongWritable, Text, Text> {
		protected void reduce(TextPairWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			Text last = null;
			long sum = 0;
			StringBuilder result = new StringBuilder();
			for (LongWritable value : values) {
				if (last != null && !key.second.equals(last)) {
					if (result.length() > 0) {
						result.append('\t');
					}
					result.append(last).append('\t').append(sum);
					sum = 0;
				}
				if (last == null) {
					last = new Text();
				}
				sum += value.get();
				last.set(key.second);
			}
			if (result.length() > 0) {
				result.append('\t');
			}
			result.append(last).append('\t').append(sum);
			context.write(key.first, new Text(result.toString()));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		boolean good;
		
		Path working = new Path(args[0]);
		Path linkPath = new Path(working, "sorted_out_links");
		Path finalPath = new Path(working, "grouped_link_text");
		FileSystem fs = FileSystem.get(getConf());

		if (fs.exists(finalPath)) {
			System.err.println(finalPath + " exists; please delete it.");
			return -1;
		}
		
		Job job = new Job(getConf());
		job.setJarByClass(getClass());
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, new Path(linkPath, "data"));
		
		job.setMapperClass(LinkProbabilityMapper.class);
		job.setMapOutputKeyClass(TextPairWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setCombinerClass(LinkProbabilityCombiner.class);
		
		job.setPartitionerClass(SameFirstPartitioner.class);
		
		job.setReducerClass(LinkProbabilityReducer.class);

		job.setNumReduceTasks(1);
		
		job.setGroupingComparatorClass(SameFirstGroupingComparator.class);
		

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(MapFileOutputFormat.class);
		MapFileOutputFormat.setCompressOutput(job, true);
		MapFileOutputFormat.setOutputPath(job, finalPath);
		good = job.waitForCompletion(true);
		
		if (!good) {
			return -1;
		}
	
		return 0;
		
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new LinkProbabilityFinder(), args));
	}	
}
