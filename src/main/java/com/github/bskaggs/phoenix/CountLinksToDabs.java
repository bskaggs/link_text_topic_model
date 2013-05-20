package com.github.bskaggs.phoenix;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CountLinksToDabs extends Configured implements Tool {
	public final static String DAB_PATH = "dab_path";
	public static class LinkCountMapper extends Mapper<Text, Text, LongWritable, LongWritable> {
		private Set<Text> dabs = new HashSet<Text>();
		
		protected void setup(Context context) throws IOException ,InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			MapFile.Reader dabReader = new MapFile.Reader(fs, conf.get(DAB_PATH), conf);
			Text key;
			Text value = new Text();
			while (dabReader.next(key = new Text(), value)) {
				dabs.add(key);
			}
		}

		private static LongWritable ONE = new LongWritable(1);
		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			if (dabs.contains(key)) {
				int count = value.toString().split("\t").length;
				context.write(new LongWritable(count), ONE);
			}
		}
	}
	public static class LinkCountReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
		@Override
		protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable value : values) {
				sum += value.get();
			}
			context.write(key, new LongWritable(sum));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		boolean good;
		Path working = new Path(args[0]);
		
		Job job = new Job(getConf());
		Configuration conf = job.getConfiguration();
		conf.set(DAB_PATH, new Path(working, "sorted_dabs").toString());
		
		Path input = new Path(working, "sorted_simple_in_links");
		Path output = new Path(working, "dab_link_count_counts");

		job.setJarByClass(getClass());
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, new Path(input, "part-r-00000/data"));
		job.setMapperClass(LinkCountMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setCombinerClass(LinkCountReducer.class);
		job.setReducerClass(LinkCountReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(MapFileOutputFormat.class);
		MapFileOutputFormat.setCompressOutput(job, true);
		MapFileOutputFormat.setOutputPath(job, output);
		good = job.waitForCompletion(true);
		if (!good) {
			return -1;
		}
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new CountLinksToDabs(), args));
	}	
}
