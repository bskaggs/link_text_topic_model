package com.github.bskaggs.phoenix;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.github.bskaggs.phoenix.io.TextDoubleMapWritable;

public class RandomWalker extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new RandomWalker(), args));
	}

	@Override
	public int run(String[] args) throws Exception {

		Path root = new Path(args[0]);
		Job job = new Job(getConf());
		job.setJobName("Random walk");
		job.setJarByClass(this.getClass());		
		Path linkPath = new Path(root, "sorted_simple_out_links");
		job.getConfiguration().set("phoenix.links_path", linkPath.toString());
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, linkPath);
		job.setMapperClass(RandomWalkMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TextDoubleMapWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TextDoubleMapWritable.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(MapFileOutputFormat.class);

		MapFileOutputFormat.setOutputPath(job, new Path(root, "sorted_random_walk_weights"));
		MapFileOutputFormat.setCompressOutput(job, true);
		return job.waitForCompletion(true) ? 0 : -1;
	}
}
