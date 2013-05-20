package com.github.bskaggs.phoenix;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.github.bskaggs.phoenix.io.TextPairWritable;

public class CountLinkText extends Configured implements Tool {
	public static class LinkCountMapper extends Mapper<Link, Text, TextPairWritable, LongWritable> {
		private static LongWritable ONE = new LongWritable(1);
		private static Text EMPTY = new Text("");
		@Override
		protected void map(Link key, Text value, Context context) throws IOException, InterruptedException {
			context.write(new TextPairWritable(new Text(key.to), new Text(value)), ONE);
			//record total count
			context.write(new TextPairWritable(new Text(key.to), EMPTY), ONE);
		}
	}
	public static class LinkCountReducer extends Reducer<TextPairWritable, LongWritable, TextPairWritable, LongWritable> {
		@Override
		protected void reduce(TextPairWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
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
		Path input = new Path(working, "sorted_fixed_out_links");
		Path output = new Path(working, "sorted_in_link_counts");


		Job job = new Job(getConf());
		job.setJarByClass(getClass());
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, new Path(input, "part-r-00000/data"));
		job.setMapperClass(LinkCountMapper.class);
		job.setMapOutputKeyClass(TextPairWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setCombinerClass(LinkCountReducer.class);
		job.setReducerClass(LinkCountReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(TextPairWritable.class);
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
		System.exit(ToolRunner.run(new CountLinkText(), args));
	}	
}
