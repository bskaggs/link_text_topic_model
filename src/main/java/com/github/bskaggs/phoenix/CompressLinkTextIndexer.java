package com.github.bskaggs.phoenix;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.github.bskaggs.phoenix.io.IntArrayWritable;
import com.github.bskaggs.phoenix.io.TextLongMapWritable;
import com.github.bskaggs.phoenix.sampler.Dictionary;


public class CompressLinkTextIndexer extends Configured implements Tool {
	public static class IndexMapper extends Mapper<Text, TextLongMapWritable, IntWritable, IntArrayWritable> {
			
		private Dictionary articleDictionary;
		private Dictionary textDictionary;

		protected void setup(Context context) throws IOException ,InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			Path tableDir = new Path(conf.get("tabledir"));
			articleDictionary = new Dictionary(new SequenceFile.Reader(fs, new Path(tableDir, "article_dictionary"), conf));
			textDictionary = new Dictionary(new SequenceFile.Reader(fs, new Path(tableDir, "text_dictionary"), conf));
		}
		
		@Override
		protected void map(Text key, TextLongMapWritable value, Context context) throws IOException, InterruptedException {
			int textId = textDictionary.getOnly(key);
			int[] articleIds = new int[value.size()];
			int pos = 0;
			for (Text article : value.keySet()) {
				articleIds[pos++] = articleDictionary.getOnly(article);
			}
			context.write(new IntWritable(textId), new IntArrayWritable(articleIds));
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		Path working = new Path(args[0]);
		Path inPath = new Path(working, "sorted_link_text_index");
		Path outPath = new Path(working, "compressed_link_text_index");
		
		Job job = new Job(getConf());
		job.getConfiguration().set("tabledir", args[0]);
		job.setJarByClass(getClass());
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, inPath);
		job.setMapperClass(IndexMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntArrayWritable.class);
		
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntArrayWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputPath(job, outPath);
		return job.waitForCompletion(true) ? 0 : -1;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new CompressLinkTextIndexer(), args));
	}	
}
