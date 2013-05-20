package com.github.bskaggs.phoenix.sampler;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.hash.TLongHashSet;

import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import com.github.bskaggs.phoenix.Link;
import com.github.bskaggs.phoenix.lucene.AlmostStandardAnalyzer;


@SuppressWarnings("deprecation")
public class CompressLinks extends Configured implements Tool{
	@Override
	public int run(String[] args) throws Exception {
		TLongHashSet ambiguous = new TLongHashSet();
		Path working = new Path(args[0]);
		
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		Writer textDictionaryWriter = SequenceFile.createWriter(fs, conf, new Path(working, "text_dictionary"), Text.class, IntWritable.class, CompressionType.BLOCK);
		Dictionary textDictionary = new Dictionary(textDictionaryWriter, 1);//, 60000000);
		
		Writer articleDictionaryWriter = SequenceFile.createWriter(fs, conf, new Path(working, "article_dictionary"), Text.class, IntWritable.class, CompressionType.BLOCK);
		Dictionary articleDictionary = new Dictionary(articleDictionaryWriter,1);//, 5000000);
		
		System.out.println("Adding dabs...");
		Path dabPath = new Path(new Path(working, "sorted_dabs"), "data");
		Reader dabReader = new SequenceFile.Reader(fs, dabPath, conf);
		Text dab = new Text();
		while (dabReader.next(dab)) {
			ambiguous.add(articleDictionary.get(dab));
		}
		dabReader.close();
		
		Path inputLinksPath = new Path(new Path(new Path(working, "sorted_fixed_out_links"), "part-r-00000"), "data");
		Reader linkReader = new SequenceFile.Reader(fs, inputLinksPath, conf);
		MapFile.Writer writer = new MapFile.Writer(conf, fs, new Path(working, "sorted_compressed_fixed_out_links").toString(), Text.class, VisibleArticle.class, CompressionType.BLOCK);
		
		int lastFrom = -1;
		Text lastFromText = new Text();
		Link link = new Link();
		Text textText = new Text();
		
		VisibleArticle va = new VisibleArticle();
		TIntArrayList regularLinkTargets = new TIntArrayList();
		TIntArrayList regularLinkTexts= new TIntArrayList();
		TIntArrayList ambiguousLinkTargets = new TIntArrayList();
		TIntArrayList ambiguousLinkTexts= new TIntArrayList();
		Text tt = new Text();
		int count = 0;
		
		AlmostStandardAnalyzer analyzer = new AlmostStandardAnalyzer();
		System.out.println("Adding links...");
		while (linkReader.next(link, textText)) {
			int from = articleDictionary.get(link.from);
			int to = articleDictionary.get(link.to);

			if (lastFrom != -1 && lastFrom != from) {
				count++;
				if (count % 1000 == 0) {
					System.out.println(count);
				}
 				va.regularLinkTargets = regularLinkTargets.toArray();
 				va.regularLinkTexts = regularLinkTexts.toArray();
 				va.regularLength = regularLinkTargets.size();
 				
 				va.ambiguousLinkTargets = ambiguousLinkTargets.toArray();
 				va.ambiguousLinkTexts = ambiguousLinkTexts.toArray();
 				va.ambiguousLength = ambiguousLinkTargets.size();
 				writer.append(lastFromText, va);
 				
 				regularLinkTargets.clear();
 				regularLinkTexts.clear();
 				ambiguousLinkTargets.clear();
 				ambiguousLinkTexts.clear();
 			}
			
			TIntArrayList linkTargets;
			TIntArrayList linkTexts;
			if (ambiguous.contains(to)) {
				linkTargets = ambiguousLinkTargets;
				linkTexts = ambiguousLinkTexts;
			} else {
				linkTargets = regularLinkTargets;
				linkTexts = regularLinkTexts;
			}
			
			for (String textString : textText.toString().split("\t")) {
				TokenStream stream = analyzer.tokenStream(null, new StringReader(textString));
				TermAttribute termAttribute = stream.getAttribute(TermAttribute.class);
				
				StringBuilder result = new StringBuilder();
				if (stream.incrementToken()) {
					result.append(termAttribute.term());
				}
				while (stream.incrementToken()) {
					result.append(' ').append(termAttribute.term());
				}
				
				tt.set(result.toString());
				int text = textDictionary.get(tt);
				linkTexts.add(text);
				linkTargets.add(to);
			}
			
			lastFrom = from;
			lastFromText.set(link.from);
 		}
 		
		if (lastFrom != -1) {
			va.regularLinkTargets = regularLinkTargets.toArray();
			va.regularLinkTexts = regularLinkTexts.toArray();
			va.regularLength = regularLinkTargets.size();

			va.ambiguousLinkTargets = ambiguousLinkTargets.toArray();
			va.ambiguousLinkTexts = ambiguousLinkTexts.toArray();
			va.ambiguousLength = ambiguousLinkTargets.size();
			writer.append(lastFromText, va);
		}
		writer.close();
		articleDictionaryWriter.close();
		textDictionaryWriter.close();
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new CompressLinks(), args));
	}
}
