package com.github.bskaggs.phoenix;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LinkSimplifier extends Configured implements Tool {
	public static class LinkSimplifierMapper extends Mapper<Link, Text, Text, Text> {
		protected void map(Link key, Text value, Context context) throws IOException ,InterruptedException {
			context.write(key.from, key.to);
		}
	}
	public static class ReverseLinkSimplifierMapper extends Mapper<Link, Text, Text, Text> {
		protected void map(Link key, Text value, Context context) throws IOException ,InterruptedException {
			context.write(key.to, key.from);
		}
	}
	
	public static class LinkSimplifierReducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Set<Text> links = new TreeSet<Text>();
			for (Text value : values) {
				links.add(new Text(value));
			}
			StringBuilder result = new StringBuilder();
			boolean first = true;
			for (Text link : links) {
				if (!first) {
					result.append('\t');
				}
				result.append(link);
				first = false;
			}
			context.write(key, new Text(result.toString()));
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		boolean good;
		
		Path working = new Path(args[0]);
		Path fixedPath = new Path(working, "sorted_fixed_out_links");
		Path simpleOutPath = new Path(working, "sorted_simple_out_links");
		Path simpleInPath = new Path(working, "sorted_simple_in_links");
		
		FileSystem fs = FileSystem.get(getConf());
		
		if (fs.exists(simpleOutPath)) {
			System.err.println(simpleOutPath + " exists; please delete it.");
			return -1;
		}
		if (fs.exists(simpleInPath)) {
			System.err.println(simpleInPath + " exists; please delete it.");
			return -1;
		}	
		Job job1 = new Job(getConf());
		job1.setJarByClass(getClass());
		job1.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job1, new Path(new Path(fixedPath, "part-r-00000"), "data"));
		job1.setMapperClass(LinkSimplifierMapper.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setNumReduceTasks(1);
		job1.setReducerClass(LinkSimplifierReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setOutputFormatClass(MapFileOutputFormat.class);
		MapFileOutputFormat.setOutputPath(job1, simpleOutPath);
		//MapFileOutputFormat.setCompressOutput(job1, true);
		//MapFileOutputFormat.setOutputCompressorClass(job1,GzipCodec.class);
		good = job1.waitForCompletion(true);
		if (!good) {
			return -2;
		}
		
		Job job = new Job(getConf());
		job.setJarByClass(getClass());
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, new Path(new Path(fixedPath, "part-r-00000"), "data"));
		job.setMapperClass(ReverseLinkSimplifierMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(LinkSimplifierReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(MapFileOutputFormat.class);
		MapFileOutputFormat.setOutputPath(job, simpleInPath);
		//MapFileOutputFormat.setCompressOutput(job, true);
		//MapFileOutputFormat.setOutputCompressorClass(job,GzipCodec.class);
		good = job.waitForCompletion(true);
		if (!good) {
			return -2;
		}
		return 0;
		
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new LinkSimplifier(), args));
	}	
}
