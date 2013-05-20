package com.github.bskaggs.phoenix;

import java.io.IOException;
import java.io.StringReader;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import com.github.bskaggs.phoenix.io.TextLongMapWritable;
import com.github.bskaggs.phoenix.lucene.AlmostStandardAnalyzer;


public class LinkTextIndexer extends Configured implements Tool {
	public static class IndexMapper extends Mapper<Link, Text, Text, Text> {
		private Pattern tabSplitter = Pattern.compile("\t");
		private AlmostStandardAnalyzer analyzer;
	
		
		protected void setup(Context context) throws IOException ,InterruptedException {
			analyzer = new AlmostStandardAnalyzer();	
		}
		
		@Override
		protected void map(Link key, Text value, Context context) throws IOException, InterruptedException {
			for (String part : tabSplitter.split(value.toString())) {
				TokenStream stream = analyzer.tokenStream(null, new StringReader(part));
				TermAttribute termAttribute = stream.getAttribute(TermAttribute.class);
				
				StringBuilder result = new StringBuilder();
				if (stream.incrementToken()) {
					result.append(termAttribute.term());
				}
				while (stream.incrementToken()) {
					result.append(' ').append(termAttribute.term());
				}
				
				
				if (result.length() > 0) {
					context.write(new Text(result.toString()), new Text(key.to));
				}
			}
		}
	}
	
	public static class IndexReducer extends Reducer<Text, Text, Text, TextLongMapWritable> {
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			TextLongMapWritable map = new TextLongMapWritable();
			for (Text value : values) {
				map.adjustOrPutValue(new Text(value), 1L, 1L);
			}
			context.write(key, map);
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Path working = new Path(args[0]);
		Path inPath = new Path(working, "sorted_fixed_out_links");
		Path outPath = new Path(working, "sorted_link_text_index");
		
		Job job = new Job(getConf());
		job.setJarByClass(getClass());
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, inPath);
		job.setMapperClass(IndexMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(IndexReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TextLongMapWritable.class);
		job.setOutputFormatClass(MapFileOutputFormat.class);
		MapFileOutputFormat.setCompressOutput(job, true);
		MapFileOutputFormat.setOutputPath(job, outPath);
		return job.waitForCompletion(true) ? 0 : -1;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new LinkTextIndexer(), args));
	}	
}
