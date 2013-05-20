package com.github.bskaggs.phoenix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SequenceFileSorter extends Configured implements Tool{

	private FileSystem fs;
	
	public static class SwapMapper<K,V> extends Mapper<K, V, V, K> {
		protected void map(K key, V value, Context context) throws java.io.IOException ,InterruptedException {
			context.write(value, key);
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf);
		Path input = SequenceFileInputFormat.getInputPaths(job)[0];
		fs = FileSystem.get(conf);
		Reader tempReader = new SequenceFile.Reader(fs, input, conf);
		Class<?> keyClass = tempReader.getKeyClass();
		Class<?> valueClass = tempReader.getValueClass();
	
		
		
		
		tempReader.close();
		
	 	job.setJarByClass(this.getClass());
		job.setInputFormatClass(SequenceFileInputFormat.class);
		boolean swap = conf.getBoolean("phoenix.sort.swap", false);
		if (swap) {
			Class<?> temp = keyClass;
			keyClass = valueClass;
			valueClass = temp;
			job.setMapperClass(SwapMapper.class);
		} else {
			job.setMapperClass(Mapper.class);
		}
		job.setMapOutputKeyClass(keyClass);
		job.setMapOutputValueClass(valueClass);
		job.setNumReduceTasks(1);
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(keyClass);
		job.setOutputValueClass(valueClass);
		job.setOutputFormatClass(MapFileOutputFormat.class);
		MapFileOutputFormat.setCompressOutput(job, true);
		return job.waitForCompletion(true) ? 0 : -1;
		
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new SequenceFileSorter(), args));
	}

}
