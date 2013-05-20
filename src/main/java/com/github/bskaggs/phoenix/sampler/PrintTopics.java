package com.github.bskaggs.phoenix.sampler;

import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TIntIntProcedure;
import gnu.trove.procedure.TIntObjectProcedure;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.github.bskaggs.phoenix.io.IntDoubleArraysWritable;
import com.github.bskaggs.phoenix.io.TextDoubleArraysWritable;
import com.github.bskaggs.phoenix.io.TextIntPairWritable;
import com.github.bskaggs.phoenix.io.TextPairWritable;


@SuppressWarnings("deprecation")
public class PrintTopics extends Configured implements Tool{
	@Override
	public int run(String[] args) throws Exception {
		Path working = new Path(args[0]);
		
		TIntObjectHashMap<TIntIntHashMap> counts = new TIntObjectHashMap<TIntIntHashMap>(5000000);
		
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		
		
		Path samplerPath = new Path(working, "sampler_working");
		Path inputPath = new Path(samplerPath, args[1]);
		Reader resultsReader = new SequenceFile.Reader(fs, inputPath, conf);
		
		Reader articleDictionaryReader = new SequenceFile.Reader(fs,  new Path(working, "article_dictionary"), conf);
		final Dictionary articleDictionary = new Dictionary(articleDictionaryReader, true);
		
		Text key = new Text();
		LatentAndVisibleArticle value = new LatentAndVisibleArticle();
		
    int numRead = 0;
		while (resultsReader.next(key, value)) {
      if (numRead++ % 100000 == 0) {
        System.err.println(numRead);
      }

			for (int i = 0; i < value.regularLength; i++) {
				int target = value.regularLinkTargets[i];
				if (target >= 0) {
					TIntIntHashMap count = counts.get(target);
					if (count == null) {
						counts.put(target, count = new TIntIntHashMap(10));
					}
					int topic = value.regularLinkTopics[i];
					if (topic >= 0) {
						count.adjustOrPutValue(topic, 1, 1);
					}
				}
			}
			for (int i = 0; i < value.ambiguousLength; i++) {
				int target = value.ambiguousLinkSampledTargets[i];
				if (target >= 0) {
					TIntIntHashMap count = counts.get(target);
					if (count == null) {
						counts.put(target, count = new TIntIntHashMap(10));
					}
					int topic = value.ambiguousLinkTopics[i];
					if (topic >= 0) {
						count.adjustOrPutValue(topic, 1, 1);
					}
				}
			}
		}
		
		final Text label = new Text();
		final TIntIntProcedure topicPrinter = new TIntIntProcedure() {
			@Override
			public boolean execute(int topic, int count) {
				if (count > 10) {
					System.out.println(topic + "\t" + count + "\t" + label);
				}
				return true;
			}
		};
		
		counts.forEachEntry(new TIntObjectProcedure<TIntIntHashMap>() {
			@Override
			public boolean execute(int key, TIntIntHashMap value) {
				label.set(articleDictionary.getReverse(key));
				value.forEachEntry(topicPrinter);
				return true;
			}
		});

		return 0;
	}


	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new PrintTopics(), args));
	}
}
