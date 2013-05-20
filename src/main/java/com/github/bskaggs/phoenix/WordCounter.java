package com.github.bskaggs.phoenix;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import com.github.bskaggs.phoenix.io.LongPairWritable;

@SuppressWarnings("deprecation")
public class WordCounter extends Configured implements Tool {
	public static class WordCountMapper extends Mapper<Text, Text, Text, LongPairWritable> {
		private Counter docCounter;
		private Analyzer analyzer;
		private Counter wordCounter;
		public final static Charset utf8 = Charset.forName("UTF-8");
		protected void setup(Context context) throws IOException, InterruptedException {
			docCounter = context.getCounter("phoenix", "documents");
			wordCounter = context.getCounter("phoenix", "words");
			Class<? extends Analyzer> analyzerClass = context.getConfiguration().getClass("phoenix.analyzer", SimpleAnalyzer.class, Analyzer.class);
			analyzer = ReflectionUtils.newInstance(analyzerClass, context.getConfiguration());
		}
		
		private Text wordText = new Text();
		private LongPairWritable pair = new LongPairWritable(1L, 0L);
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			docCounter.increment(1);
			InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(value.getBytes(), 0, value.getLength()), utf8);
			TokenStream stream = analyzer.reusableTokenStream("text", reader);
			TermAttribute termAttribute = stream.getAttribute(TermAttribute.class);
			
			Map<String, Long> words = new HashMap<String, Long>();
			while (stream.incrementToken()) {
				String word = termAttribute.term();
				Long old = words.get(word);
				if (old == null) {
					words.put(word, 1L);
				} else {
					words.put(word, old + 1L);
				}
				wordCounter.increment(1L);
			}
			
			for (Map.Entry<String, Long> entry : words.entrySet()) {
				wordText.set(entry.getKey());
				pair.second = entry.getValue();
				context.write(wordText, pair);
			}
		}
	}
	
	public static class WordCountReducer extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
		private LongPairWritable pair = new LongPairWritable();
		protected void reduce(Text key, Iterable<LongPairWritable> values, Context context) throws IOException, InterruptedException {
			pair.first = 0L;
			pair.second = 0L;
			for (LongPairWritable value : values) {
				pair.first += value.first;
				pair.second += value.second;
			}
			context.write(key, pair);
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		boolean good;
		
		Path working = new Path(args[0]);
		Path sortedTextPath = new Path(working, "sorted_plain_text");
		Class<? extends Analyzer> analyzerClass = getConf().getClass("phoenix.analyzer", SimpleAnalyzer.class, Analyzer.class);
		String analyzerName = analyzerClass.getSimpleName();
		Path wordCountsPath = new Path(new Path(working, "word_counts"), analyzerName);
		
		Job job1 = new Job(getConf());
		job1.setJarByClass(getClass());
		job1.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job1, new Path(sortedTextPath, "data"));
		job1.setMapperClass(WordCountMapper.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(LongPairWritable.class);
		job1.setCombinerClass(WordCountReducer.class);
		job1.setNumReduceTasks(1);
		job1.setReducerClass(WordCountReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(LongPairWritable.class);
		job1.setOutputFormatClass(MapFileOutputFormat.class);
		MapFileOutputFormat.setOutputPath(job1, wordCountsPath);
		good = job1.waitForCompletion(true);
		if (!good) {
			return -2;
		}
		long wordCount = job1.getCounters().getGroup("phoenix").findCounter("words").getValue();
		long docCount = job1.getCounters().getGroup("phoenix").findCounter("documents").getValue();
		
		FileSystem fs = FileSystem.get(getConf());
		PrintWriter out = new PrintWriter(fs.create(new Path(new Path(working, "total_word_counts"), analyzerName), true));
		out.println(docCount);
		out.println(wordCount);
		out.close();
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new WordCounter(), args));
	}	
}
