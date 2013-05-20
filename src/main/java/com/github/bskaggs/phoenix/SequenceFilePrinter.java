package com.github.bskaggs.phoenix;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

public class SequenceFilePrinter extends Configured implements Tool{
	private Writer out;
	private Text outKey = new Text("");
	private IntWritable outValue = new IntWritable();
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		Reader reader = new SequenceFile.Reader(fs, new Path(args[0]), conf);
		Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		
		Class<? extends Analyzer> analyzerClass = getConf().getClass("phoenix.analyzer", SimpleAnalyzer.class, Analyzer.class);
		Analyzer analyzer = ReflectionUtils.newInstance(analyzerClass, conf);

		if (args.length > 1) {
			out = SequenceFile.createWriter(fs, conf, new Path(args[1]), Text.class, IntWritable.class);
		}
		
		Pattern whitespace = Pattern.compile("\\s+", Pattern.MULTILINE);
		while (reader.next(key, value)) {
			String valueString = getParameters(analyzer, value);
			if (valueString.length() > 0) {
				System.out.print("| ");
				//System.out.println(whitespace.matcher(key.toString()).replaceAll(" ").trim());
				System.out.println(valueString);
			}
		}
		reader.close();
		out.close();
		
		return 0;
	}
	
	private Map<String, Integer> words = new HashMap<String, Integer>();
	private int wordCount = 0;
	
	public String getParameters(Analyzer analyzer, Object w) throws IOException {
		TokenStream stream = analyzer.reusableTokenStream("text", new StringReader(w.toString()));
		TermAttribute termAttribute = stream.getAttribute(TermAttribute.class);
		Map<Integer, Integer> counts = new HashMap<Integer, Integer>();
		while (stream.incrementToken()) {
			String word = termAttribute.term();
			Integer id = words.get(word);
			if (id == null) {
				words.put(word, id = wordCount++);
				if (out != null) {
					outKey.set(word);
					outValue.set(id);
					out.append(outKey, outValue);
				}
			}
			
			Integer lastCount = counts.get(id);
			if (lastCount == null) {
				lastCount = 0;
			}
			counts.put(id, lastCount + 1);
		}
		StringBuilder result = new StringBuilder();
		boolean first = true;
		for (Entry<Integer, Integer> entry : counts.entrySet()) {
			if (first)
				first = false;
			else
				result.append(' ');
			result.append(entry.getKey()).append(':').append(entry.getValue());
		}
		return result.toString();
	}
	
	public String getString(Analyzer analyzer, Object w) throws IOException {
		TokenStream stream = analyzer.reusableTokenStream("text", new StringReader(w.toString()));
		TermAttribute termAttribute = stream.getAttribute(TermAttribute.class);
		StringBuilder result = new StringBuilder();
		while (stream.incrementToken()) {
			if (result.length() > 0) {
				result.append(' ');
			}
			result.append(termAttribute.term());
		}
		return result.toString();
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new SequenceFilePrinter(), args));
	}
}
