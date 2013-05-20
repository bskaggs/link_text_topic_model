package com.github.bskaggs.phoenix.sampler;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import com.github.bskaggs.phoenix.Link;
import com.github.bskaggs.phoenix.lucene.AlmostStandardAnalyzer;


public class LinkCompressor extends Configured implements Tool {

	public static class ArticleNameMapper extends Mapper<Link, Text, Text, NullWritable> {
		protected void map(Link key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key.from, NullWritable.get());
			context.write(key.to, NullWritable.get());
		}
	}
	
	public static class ArticleTextMapper extends Mapper<Link, Text, Text, NullWritable> {
		private AlmostStandardAnalyzer analyzer;
		protected void setup(Context context) throws IOException ,InterruptedException {
			analyzer = new AlmostStandardAnalyzer();
		}
		protected void map(Link key, Text value, Context context) throws IOException, InterruptedException {
			for (String textString : value.toString().split("\t")) {
				TokenStream stream = analyzer.tokenStream(null, new StringReader(textString));
				TermAttribute termAttribute = stream.getAttribute(TermAttribute.class);
				
				StringBuilder result = new StringBuilder();
				if (stream.incrementToken()) {
					result.append(termAttribute.term());
				}
				while (stream.incrementToken()) {
					result.append(' ').append(termAttribute.term());
				}
				
				context.write(new Text(result.toString()), NullWritable.get());
			}
		}
	}
	
	public static class DuplicateTextCombiner extends Reducer<Text, NullWritable, Text, NullWritable> {
		protected void reduce(Text key, java.lang.Iterable<NullWritable> values, Context context) throws IOException ,InterruptedException {
			context.write(key, NullWritable.get());
		}
	}
	
	public static class NumberReducer extends Reducer<Text, NullWritable, Text, IntWritable> {
		private int id;
		protected void setup(Context context) throws IOException, InterruptedException {
			id = 1;
		}
		protected void reduce(Text key, java.lang.Iterable<NullWritable> values, Context context) throws IOException ,InterruptedException {
			context.write(key, new IntWritable(id++));
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		{
			Job job = new Job(conf, "Build article dictionary");
		}
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new LinkCompressor(), args));
	}

}
