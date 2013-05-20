package com.github.bskaggs.phoenix;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LinkFixer extends Configured implements Tool {
	public static class LinkFixerMapper extends Mapper<Text, Text, Link, Text> {
		private Pattern tabSplitter = Pattern.compile("\t");
	
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			Link link = new Link();
			link.from.set(key);
			String[] parts = tabSplitter.split(value.toString());
			link.to.set(parts[0]);
			
			StringBuilder labels = new StringBuilder();
			for (int i = 1; i < parts.length; i++) {
				if (i != 1) {
					labels.append('\t');
				}
				labels.append(parts[i]);
			}
			context.write(link, new Text(labels.toString()));
		}
	}
	
	public static class LinkFixerReducer extends Reducer<Link, Text, Link, Text> {
		private Reader redirectsReader;
		private Text redirectFrom;
		private Text redirectTo;
		private Counter fixedCounter;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			
			Path redirectsPath = new Path(conf.get("phoenix.redirects_path"));
			redirectsReader = new SequenceFile.Reader(FileSystem.get(conf), redirectsPath, conf);
			redirectFrom = new Text("");
			redirectTo = new Text("");
			fixedCounter = context.getCounter("phoenix", "redirects_fixed");
		}
		
		protected void reduce(Link key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuilder labels = new StringBuilder();
			boolean first = true; 
			for (Text value : values) {
				if (!first) {
					labels.append('\t');
				}
				labels.append(value.toString());
				first = false;
			}
			
			int order = 0;
			boolean notFinished = true;
			while ((order = redirectFrom.compareTo(key.to)) < 0 && (notFinished = redirectsReader.next(redirectFrom, redirectTo))) {
				//skip through file until matching or after redirect
			}
			
			boolean isRedirect = (order == 0 && notFinished);
			if (isRedirect) {
				//we've got a redirect!
				fixedCounter.increment(1L);
				key.to.set(redirectTo);
			}
			
			Text combinedLabels = new Text(labels.toString());
			context.write(key, combinedLabels);
		}
		
		protected void cleanup(Context context) throws IOException ,InterruptedException {
			redirectsReader.close();
		}
	}
	
	public static class SimpleLinkFixerReducer extends Reducer<Link, Text, Link, Text> {
		protected void reduce(Link key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuilder labels = new StringBuilder();
			boolean first = true; 
			for (Text value : values) {
				if (!first) {
					labels.append('\t');
				}
				labels.append(value.toString());
				first = false;
			}
			context.write(key, new Text(labels.toString()));
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		boolean good;
		boolean compress = !System.getProperty("os.name").equals("Mac OS X");
		
		Path working = new Path(args[0]);
		Path partialPath = new Path(working, "unsorted_fixed_out_links");
		Path finalPath = new Path(working, "sorted_fixed_out_links");
		FileSystem fs = FileSystem.get(getConf());
		
		
		if (fs.exists(partialPath)) {
			System.err.println(partialPath + " exists; please delete it.");
			return -1;
		}
		if (fs.exists(finalPath)) {
			System.err.println(finalPath + " exists; please delete it.");
			return -1;
		}
		
		Path redirectsPath = new Path(new Path(working, "sorted_redirects"), "data");

		Job job = new Job(getConf());
		job.setJarByClass(getClass());
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, new Path(new Path(working, "sorted_out_links"), "data"));
		job.getConfiguration().set("phoenix.redirects_path", redirectsPath.toString());
		job.setMapperClass(LinkFixerMapper.class);
		job.setMapOutputKeyClass(Link.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setSortComparatorClass(LinkToComparator.class);
		job.setReducerClass(LinkFixerReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Link.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputPath(job, partialPath);
		good = job.waitForCompletion(true);
		
		if (!good) {
			return -1;
		}
	
		FileStatus[] statuses = fs.listStatus(partialPath, new PathFilter() {
			@Override
			public boolean accept(Path path) {
				return path.getName().startsWith("part");
			}
		});
		
		Path[] partialPaths = new Path[statuses.length];
		int i = 0;
		for (FileStatus status : statuses) {
			partialPaths[i++] = status.getPath();
		}
		
		Job job2 = new Job(getConf());
		job2.setJarByClass(getClass());
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job2, partialPaths);
		job2.setMapperClass(Mapper.class);
		job2.setMapOutputKeyClass(Link.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setNumReduceTasks(1);
		job2.setReducerClass(SimpleLinkFixerReducer.class);
		job2.setOutputKeyClass(Link.class);
		job2.setOutputValueClass(Text.class);
		job2.setOutputFormatClass(MapFileOutputFormat.class);
		MapFileOutputFormat.setOutputPath(job2, finalPath);
		if (compress) {
			MapFileOutputFormat.setCompressOutput(job2, compress);
			MapFileOutputFormat.setOutputCompressorClass(job2,GzipCodec.class);
		}
		good = job2.waitForCompletion(true);
		if (!good) {
			return -2;
		}
			
		fs.delete(partialPath, true);

		return 0;
		
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new LinkFixer(), args));
	}	
}
