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

import com.github.bskaggs.phoenix.io.TextArrayWritable;


public class CountDabClassSizes extends Configured implements Tool {
	public static class DabCountMapper extends Mapper<Text, TextArrayWritable, LongWritable, LongWritable> {
		private static LongWritable ONE = new LongWritable(1);
		@Override
		protected void map(Text key, TextArrayWritable value, Context context) throws IOException, InterruptedException {
			context.write(new LongWritable(value.array.length), ONE);
		}
	}
	public static class DabCountReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
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
		Path input = new Path(working, "sorted_dabs_list");
		Path output = new Path(working, "dab_list_sizes");


		Job job = new Job(getConf());
		job.setJarByClass(getClass());
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, new Path(input, "data"));
		job.setMapperClass(DabCountMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setCombinerClass(DabCountReducer.class);
		job.setReducerClass(DabCountReducer.class);
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
		System.exit(ToolRunner.run(new CountDabClassSizes(), args));
	}	
}
