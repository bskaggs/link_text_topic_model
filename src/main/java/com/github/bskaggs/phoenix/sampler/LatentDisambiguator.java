package com.github.bskaggs.phoenix.sampler;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.procedure.TObjectProcedure;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.Lock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.github.bskaggs.phoenix.io.IntArrayWritable;
import com.github.bskaggs.phoenix.io.IntDoubleArraysWritable;
import com.github.bskaggs.phoenix.io.TextIntPairWritable;
import com.github.bskaggs.phoenix.sampler.SparseCountFactory.Entry;
import com.github.bskaggs.phoenix.sampler.SparseCountFactory.SparseCounts;


public final class LatentDisambiguator extends Configured implements Tool {
	private final static ThreadLocal<FastCache> threadCache = new ThreadLocal<LatentDisambiguator.FastCache>();

	private final class SamplerCallable implements Callable<LatentAndVisibleArticle> {
		private final Text articleName;
		private final LatentAndVisibleArticle document;
		private final Writer writer;
		private int iterations;

		public SamplerCallable(Text articleName, LatentAndVisibleArticle document, Writer writer, int iterations) {
			this.articleName = articleName;
			this.document = document;
			this.writer = writer;
			this.iterations = iterations;
		}

		@Override
		public LatentAndVisibleArticle call() throws Exception {
			FastCache cache = threadCache.get();
			if (cache == null) {
				cache = new FastCache();
				threadCache.set(cache);
			}
			SparseCounts docTopics = articleTopicCountFactory.create();

			int rl = document.regularLength;
			int[] rTopics = document.regularLinkTopics;
			for (int i = 0; i < rl; i++) {
				docTopics.increment(rTopics[i]);
			}
			int al = document.ambiguousLength;
			int[] aTopics = document.ambiguousLinkTopics;
			for (int i = 0; i < al; i++) {
				if (aTopics[i] != -1) {
					docTopics.increment(aTopics[i]);
				}
			}
			cache.setDocTopics(docTopics);
			
			for (int i = 0; i < iterations; i++) {
				sparseSampleDocumentTopics(docTopics, cache, document.regularLinkTargets, rTopics, rl);
				sparseSampleDocumentTopics(docTopics, cache, document.ambiguousLinkSampledTargets, aTopics, al);
				sampleDocumentArticles(document.ambiguousLinkSampledTargets, document.ambiguousLinkTargets, document.ambiguousLinkTexts, document.ambiguousLinkTopics, al);
			}
			if (writer != null) {
				synchronized (writer) {
					writer.append(articleName, document);
				}
			}

			// for optimization of alpha and beta
			long sum = 0;
			for (Entry entry : docTopics) {
				sum += entry.count;
				synchronized (topicLengthCounts[entry.topic]) {
					topicLengthCounts[entry.topic].adjustOrPutValue(entry.count, 1, 1);
				}
			}
			synchronized (lengthCounts) {
				lengthCounts.adjustOrPutValue(sum, 1, 1);
			}
			return document;
		}
	}

	public static final String KEEP_RATE = "keep_rate";
	public static final int DEFAULT_KEEP_RATE = 10;
	public static final String MAX_ITERATION = "max_iteration";
	public static final String SAVE_PROBABILITIES = "save_probabilities";
	public static final int DEFAULT_MAX_ITERATION = 1000;

	private int numTopics;
	private int vocabularySize = 0;
	private int textSize = 0;

	private boolean dabTexts = false;
	private double gamma;
	private double textGamma;
	private double beta;
	private double betaV;
	private double[] alpha;
	private AtomicLongArray topicCounts;
	private Random rand;

	private TLongLongHashMap lengthCounts = new TLongLongHashMap();
	private TLongLongHashMap[] topicLengthCounts;

	private final TIntObjectHashMap<SparseCountFactory.SparseCounts> articleTopicCounts = new TIntObjectHashMap<SparseCountFactory.SparseCounts>(20000000, 0.75f);
	private final TIntObjectHashMap<SparseCountFactory.SparseCounts> articleTextCounts = new TIntObjectHashMap<SparseCountFactory.SparseCounts>(20000000, 0.75f);
	private TIntObjectMap<int[]> dabPossibleArticles = new TIntObjectHashMap<int[]>();

	private final boolean doCompress = !System.getProperty("os.name").equals("Mac OS X");
	private final CompressionCodec compressionCodec = new GzipCodec();
	private final CompressionType compressionType = CompressionType.BLOCK;

	private Path workingDir;
	private FileSystem fs;
	private Configuration conf;

	private SparseCountFactory articleTopicCountFactory;
	private SparseCountFactory articleTextCountFactory = new SparseCountFactory(1L << 32);
	private boolean updateTextCounts;
	private boolean updateArticleTopicCounts = true;

	public boolean isDab(int articleId) {
		return dabPossibleArticles.containsKey(articleId);
	}
	
	private void loadDabPossibilities(SequenceFile.Reader reader, TIntSet allowed) throws IOException {
		IntWritable key = new IntWritable();
		IntArrayWritable value = new IntArrayWritable();
		while (reader.next(key, value)) {
			if (allowed == null || allowed.contains(key.get())) {
				int[] possibleArticles = Arrays.copyOf(value.array, value.array.length);
				dabPossibleArticles.put(key.get(), possibleArticles);
				for (int possibleArticle : possibleArticles) {
					articleTopicCounts.put(possibleArticle, articleTopicCountFactory.create());
					articleTextCounts.put(possibleArticle, articleTextCountFactory.create());
				}
			}
		}
		System.out.println("Dab possibilities loaded: " + dabPossibleArticles.size());
	}

	private void loadInitialCounts(SequenceFile.Reader reader) throws IOException {
		Text articleName;
		VisibleArticle va = new VisibleArticle();
		topicCounts = new AtomicLongArray(numTopics);

		Path initialPath = new Path(workingDir, "0");
		SequenceFile.Writer initialWriter;
		if (doCompress) {
			initialWriter = SequenceFile.createWriter(fs, conf, initialPath, Text.class, LatentAndVisibleArticle.class, compressionType, compressionCodec);
		} else {
			initialWriter = SequenceFile.createWriter(fs, conf, initialPath, Text.class, LatentAndVisibleArticle.class);
		}
		textSize = 0;
		int articleCount = 0;
		while (reader.next(articleName = new Text(), va)) {
			articleCount++;
			if (articleCount % 100000 == 0) {
				System.out.println(Arrays.toString(new int[] { articleCount, articleTextCounts.size(), articleTopicCounts.size(), dabPossibleArticles.size() }));
			}
			LatentAndVisibleArticle document = new LatentAndVisibleArticle(va, rand, numTopics);
			addToCounts(document, true);
			initialWriter.append(articleName, document);
		}
		initialWriter.close();
	}

	private void loadCounts(SequenceFile.Reader reader) throws IOException {
		Text articleName = new Text();
		LatentAndVisibleArticle document = new LatentAndVisibleArticle();
		topicCounts = new AtomicLongArray(numTopics);
		textSize = 0;
		int articleCount = 0;
		while (reader.next(articleName, document) && articleCount < limit) {
			articleCount++;
			if (articleCount % 100000 == 0) {
				System.out.println(Arrays.toString(new int[] { articleCount, articleTextCounts.size(), articleTopicCounts.size(), dabPossibleArticles.size() }));
			}
			addToCounts(document, false);
		}
	}

	public void randomizeAmbiguousTargets(LatentAndVisibleArticle document) {
		int al = document.ambiguousLength;
		int[] aLTargets = document.ambiguousLinkTargets;
		int[] aLTexts = document.ambiguousLinkTexts;
		int[] aLTopics = document.ambiguousLinkTopics;
		int[] aLSampled = document.ambiguousLinkSampledTargets;
		
		for (int i = 0; i < al; i++) {
			int[] possibleList = dabPossibleArticles.get(dabTexts ? aLTexts[i] : aLTargets[i]);
			if (possibleList == null || possibleList.length == 0) {
				aLTopics[i] = -1;
				continue;
			}
			aLSampled[i] = possibleList[rand.nextInt(possibleList.length)];
		}
	}
	private void addToCounts(LatentAndVisibleArticle document, boolean randomize) {
		int[] rLTargets = document.regularLinkTargets;
		int[] rLTexts = document.regularLinkTexts;
		int[] rLTopics = document.regularLinkTopics;
		int rl = document.regularLength;
		for (int i = 0; i < rl; i++) {
			int text = rLTexts[i];
			int article = rLTargets[i];
			vocabularySize = Math.max(vocabularySize, article);
			textSize = Math.max(textSize, text);
			int topic = rLTopics[i];

			topicCounts.incrementAndGet(topic);
			incrementArticleTopic(article, topic);
			incrementArticleText(article, text);
		}

		int al = document.ambiguousLength;
		int[] aLTargets = document.ambiguousLinkTargets;
		int[] aLTexts = document.ambiguousLinkTexts;
		int[] aLTopics = document.ambiguousLinkTopics;
		int[] aLSampled = document.ambiguousLinkSampledTargets;
		for (int i = 0; i < al; i++) {
			int topic = aLTopics[i];
			int[] possibleList = dabPossibleArticles.get(dabTexts ? aLTexts[i] : aLTargets[i]);
			if (possibleList == null || possibleList.length == 0) {
				aLTopics[i] = -1;
				continue;
			}

			int article;
			if (randomize) {
				article = aLSampled[i] = possibleList[rand.nextInt(possibleList.length)];
			} else {
				article = aLSampled[i];
			}
			int text = aLTexts[i];

			topicCounts.incrementAndGet(topic);
			if (updateArticleTopicCounts) {
				incrementArticleTopic(article, topic);
			}
			if (updateTextCounts) {
				incrementArticleText(article, text);
			}
		}
	}

	private final class FastCache {
		private double q;
		private double s;
		private double r;
		private final double[] denoms;
		private final double[] qParts;
		private final double[] qTops;
		private final double[] rParts;
		private final double[] sParts;
		private final TIntHashSet touched = new TIntHashSet();
		private SparseCountFactory.SparseCounts docTopics;

		public FastCache() {
			denoms = new double[numTopics];
			qParts = new double[numTopics];
			qTops = new double[numTopics];
			rParts = new double[numTopics];
			sParts = new double[numTopics];
			reset();
		}

		public void reset() {
			s = 0;
			for (int topic = 0; topic < numTopics; topic++) {
				double denom = (denoms[topic] = 1 / (betaV + topicCounts.get(topic)));
				s += (sParts[topic] = alpha[topic] * beta * denom);
				qParts[topic] = (qTops[topic] = alpha[topic]) * denom;
			}

			if (docTopics != null) {
				// reuse the new values
				SparseCounts oldDocTopics = docTopics;
				docTopics = null;
				setDocTopics(oldDocTopics);
			}
		}

		public void setDocTopics(SparseCountFactory.SparseCounts dT) {
			if (this.docTopics != null) {
				TIntIterator it = touched.iterator();
				while (it.hasNext()) {
					int topic = it.next();
					qTops[topic] = alpha[topic]; // reseting qTops to original value
					rParts[topic] = 0; // resetting rParts to 0;
					updateTopic(topic);
				}
			}

			this.docTopics = dT;
			touched.clear();

			r = 0;
			for (Entry entry : docTopics) {
				touched.add(entry.topic);
				qTops[entry.topic] += entry.count;
				updateTopic(entry.topic);
				r += (rParts[entry.topic] = beta * entry.count * denoms[entry.topic]);
			}
		}

		/*
		 * Update the counts that don't rely on the document topic counts
		 * directly
		 */
		public void updateTopic(int topic) {
			s -= sParts[topic];
			double denom = (denoms[topic] = 1 / (betaV + topicCounts.get(topic)));
			s += (sParts[topic] = alpha[topic] * beta * denom);
			qParts[topic] = qTops[topic] * denom; // qTops should be changed
													// before using this
		}

		public int sample(SparseCountFactory.SparseCounts articleTopicCount) {
			int newTopic = -1;
			do {
				Lock readLock = articleTopicCount.getLock().readLock();
				readLock.lock();

				q = 0;
				for (Entry entry : articleTopicCount) {
					q += qParts[entry.topic] * entry.count;
				}

				if (r < 0 || s < 0 || q < 0) {
					System.out.printf("Reseting variables... q:%d r:%d s:%d => ", q, r, s);
					readLock.unlock();
					reset();
					System.out.printf("q:%d r:%d s:%d\n", q, r, s);
					continue;
				}

				double sample = (q + r + s) * rand.nextDouble();
				if (sample < q) {
					for (Entry entry : articleTopicCount) {
						sample -= qParts[entry.topic] * entry.count;
						if (sample <= 0) {
							newTopic = entry.topic;
							break;
						}
					}
				} else {
					sample -= q;
					if (sample < r) {
						for (Entry entry : docTopics) {
							sample -= rParts[entry.topic];
							if (sample <= 0) {
								newTopic = entry.topic;
								break;
							}
						}
					} else {
						sample -= r;
						for (int t = 0; t < numTopics; t++) {
							sample -= sParts[t];
							if (sample <= 0) {
								newTopic = t;
								break;
							}
						}
					}
				}
				readLock.unlock();
			} while (newTopic == -1);
			return newTopic;
		}

		public void incrementTopic(int topic) {
			touched.add(topic);
			qTops[topic]++;
			updateTopic(topic);
			r -= rParts[topic];
			r += (rParts[topic] = beta * docTopics.rawIncrement(topic) * denoms[topic]);
		}

		public void decrementTopic(int topic) {
			// touched.add(topic); already there from start or previous
			// increment
			qTops[topic]--;
			updateTopic(topic);
			r -= rParts[topic];
			r += (rParts[topic] = beta * docTopics.rawDecrement(topic) * denoms[topic]);
		}
	}

	private void sparseSampleDocumentTopics(SparseCountFactory.SparseCounts docTopics, FastCache cache, int[] articles, int[] topics, int rl) {
		for (int i = 0; i < rl; i++) {
			int article = articles[i];

			// make sure we keep the current topic information local
			{
				int topic = topics[i];
				if (topic == -1) {
					continue;
				}

				/** modification **/
				if (updateArticleTopicCounts) {
					decrementArticleTopic(article, topic);
					topicCounts.decrementAndGet(topic);
				}
				cache.decrementTopic(topic); // changes doc counts as well
			}

			SparseCounts articleTopicCount = articleTopicCounts.get(article);
			if (articleTopicCount == null) {
				articleTopicCount = articleTopicCountFactory.create();
				articleTextCounts.put(article, articleTopicCount);
			}
			int newTopic = cache.sample(articleTopicCount);

			if (updateArticleTopicCounts) {
				articleTopicCount.increment(newTopic);
				topicCounts.incrementAndGet(newTopic);
			}
			cache.incrementTopic(newTopic); // changes doc counts as well
			topics[i] = newTopic;
		}
	}

	private void sampleDocumentArticles(int[] ambiguousLinkSampledTargets, int[] ambiguousLinkTargets, int[] ambiguousLinkTexts, int ambiguousLinkTopics[], int al) {
		for (int i = 0; i < al; i++) {
			int topic = ambiguousLinkTopics[i];
			if (topic == -1) {
				continue;
			}
			int article = ambiguousLinkSampledTargets[i];
			int text = ambiguousLinkTexts[i];

			if (updateArticleTopicCounts) {
				decrementArticleTopic(article, topic);
			}
			if (updateTextCounts) {
				decrementArticleText(article, text);
			}

			 
			int[] possible = dabPossibleArticles.get(dabTexts ? ambiguousLinkTexts[i] : ambiguousLinkTargets[i]);
			double[] p = new double[possible.length];
			double pSum = 0;
			for (int a = 0; a < possible.length; a++) {
				int possibleArticle = possible[a];

				SparseCounts atc = articleTextCounts.get(possibleArticle);
				Lock lock = atc.getLock().readLock();
				lock.lock();
				double textContrib = (atc.rawGet(topic) + gamma) / (atc.rawGet(Integer.MAX_VALUE) + textGamma);
				lock.unlock();
				double articleContrib = articleTopicCounts.get(possibleArticle).get(topic) + beta;
				pSum += (p[a] = textContrib * articleContrib);

			}
			double pp = pSum * rand.nextDouble();

			int newArticle = 0;
			for (int a = 0; a < possible.length; a++) {
				double pa = p[a];
				if (pp < pa) {
					newArticle = possible[a];
					break;
				}
				pp -= pa;
			}

			ambiguousLinkSampledTargets[i] = newArticle;
			
			if (updateArticleTopicCounts) {
				incrementArticleTopic(newArticle, topic);
			}
			if (updateTextCounts) {
				incrementArticleText(newArticle, text);
			}

		}
	}

	private void emitAllProbabilities(SequenceFile.Reader reader, SequenceFile.Writer writer) throws IOException {
		LatentAndVisibleArticle article = new LatentAndVisibleArticle();
		Text articleName = new Text();
		int articleCount = 0;
		while (reader.next(articleName, article)) {
			articleCount++;
			if (articleCount % 100000 == 0) {
				System.out.println(articleCount);
			}
			emitDocumentProbabilities(articleName, article, writer);
		}
		writer.close();
	}

	private void emitDocumentProbabilities(Text articleName, LatentAndVisibleArticle article, SequenceFile.Writer writer) throws IOException {
		for (Object[] entry : calculateDocumentProbabilities(articleName, article)) {
			writer.append(entry[0], entry[1]);
		}
	}
	
	public IntDoubleArraysWritable sampleAndEmitProbabilities(String articleNameString, int index, int[] targets, int[] texts, boolean[] ambiguous, int iterations) {
		Text articleName = new Text(articleNameString);
		updateArticleTopicCounts = false;
		updateTextCounts = false;
		
		try {
			int[] regularLinkTargets = new int[targets.length];
			int[] regularLinkTexts = new int[texts.length];
			int[] ambiguousLinkTargets = new int[targets.length];
			int[] ambiguousLinkTexts = new int[texts.length];
			
			int rl = 0;
			int al = 0;
			int aIndex = -1;
			for (int i = 0; i < targets.length; i++) {
				if (ambiguous[i]) {
					if (i == index) {
						aIndex = al;
					}
					ambiguousLinkTargets[al] = targets[i];
					ambiguousLinkTexts[al] = texts[i];
					al++;
					
				} else {
					regularLinkTargets[rl] = targets[i];
					regularLinkTexts[rl] = texts[i];
					rl++;
				}
			}
			
			VisibleArticle va = new VisibleArticle();
			va.regularLength = rl;
			va.regularLinkTargets = regularLinkTargets;
			va.regularLinkTexts = regularLinkTexts;
			va.ambiguousLength = al;
			va.ambiguousLinkTexts = ambiguousLinkTexts;
			va.ambiguousLinkTargets = ambiguousLinkTargets;
			
			LatentAndVisibleArticle article = new LatentAndVisibleArticle(va, rand, numTopics);
			randomizeAmbiguousTargets(article);
//System.out.println(articleName + " " + article + " " + iterations);
			article = new SamplerCallable(articleName, article, null, iterations).call();
//System.out.println(article);
			IntDoubleArraysWritable interestingResult = (IntDoubleArraysWritable) calculateDocumentProbabilities(articleName, article).get(aIndex)[1];
			return interestingResult;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public List<Object[]> calculateDocumentProbabilities(Text articleName, LatentAndVisibleArticle article) throws IOException {
		List<Object[]> results = new ArrayList<Object[]>();
		int[] docTopics = new int[numTopics];
		for (int i = 0; i < article.ambiguousLength; i++) {
			int t = article.ambiguousLinkTopics[i];
			if (t != -1)
				docTopics[t]++;
		}
		for (int i = 0; i < article.regularLength; i++) {
			docTopics[article.regularLinkTopics[i]]++;
		}
		
//System.out.println(Arrays.toString(docTopics));

		for (int i = 0; i < article.ambiguousLength; i++) {
			int currentTopic = article.ambiguousLinkTopics[i];
			if (currentTopic == -1) {
				System.out.println("Skip " + i);

				continue;
			}
			int currentArticle = article.ambiguousLinkSampledTargets[i];

			int[] possible = dabPossibleArticles.get(dabTexts ? article.ambiguousLinkTexts[i] : article.ambiguousLinkTargets[i]);

			// calculate probility of linking to each possible article
			double[] p = new double[possible.length];
			for (int j = 0; j < possible.length; j++) {
				int possibleArticle = possible[j];
				long[] atArray = articleTopicCounts.get(possibleArticle).toArray();

				for (int possibleTopic = 0; possibleTopic < numTopics; possibleTopic++) {
					long at = atArray[possibleTopic];
					long tc = topicCounts.get(possibleTopic);
					int dc = docTopics[possibleTopic];
					if (possibleTopic == currentTopic) {
						tc--;
						dc--;
						if (possibleArticle == currentArticle) {
							at--;
						}
					}
					p[j] += (dc + alpha[possibleTopic]) * (at + beta) / (tc + betaV);
				}
			}

			// multiply in probability of linking with the given text
			int text = article.ambiguousLinkTexts[i];
			double pSum = 0;
			for (int j = 0; j < possible.length; j++) {
				int possibleArticle = possible[j];
				long articleTextCount = fetchArticleText(possibleArticle, text);
				long totalArticleCount = fetchArticleCounts(possibleArticle) - 1;
				if (possibleArticle == currentArticle) {
					articleTextCount--;
				}

				double textProbability = (articleTextCount + gamma) / (totalArticleCount + textGamma);
				p[j] *= textProbability;
				pSum += p[j];
			}

			for (int j = 0; j < possible.length; j++) {
				p[j] /= pSum;
			}
			results.add(new Object[] {new TextIntPairWritable(articleName, article.ambiguousLinkTargets[i]), new IntDoubleArraysWritable(possible, p)});
		}
		return results;
	}

	private long fetchArticleCounts(int article) {
		return fetchArticleText(article, Integer.MAX_VALUE);
	}

	private long fetchArticleText(int article, int text) {
		return articleTextCounts.get(article).get(text);
	}

	private void incrementArticleText(int article, int text) {
		SparseCounts counts = articleTextCounts.get(article);
		if (counts == null) {
			counts = articleTextCountFactory.create();
			articleTextCounts.put(article, counts);
		}
		Lock lock = counts.getLock().writeLock();
		lock.lock();
		counts.rawIncrement(text);
		counts.rawIncrement(Integer.MAX_VALUE);
		lock.unlock();
	}

	private void decrementArticleText(int article, int text) {
		SparseCounts counts = articleTextCounts.get(article);
		Lock lock = counts.getLock().writeLock();
		lock.lock();
		counts.rawDecrement(text);
		counts.rawDecrement(Integer.MAX_VALUE);
		lock.unlock();
	}

	private void incrementArticleTopic(int article, int topic) {
		SparseCounts counts = articleTopicCounts.get(article);
		if (counts == null) {
			counts = articleTopicCountFactory.create();
			articleTopicCounts.put(article, counts);
		}
		counts.increment(topic);
	}

	private void decrementArticleTopic(int article, int topic) {
		articleTopicCounts.get(article).decrement(topic);
	}

	private void iterate(SequenceFile.Reader reader, SequenceFile.Writer writer) throws IOException, InterruptedException, ExecutionException {
		int articleCount = 0;

		int numThreads = 8;
		ExecutorService executor = Executors.newFixedThreadPool(numThreads, new NamingThreadFactory("sampler"));
		ExecutorCompletionService<LatentAndVisibleArticle> completionService = new ExecutorCompletionService<LatentAndVisibleArticle>(executor);
		Text articleName;
		LatentAndVisibleArticle document = new LatentAndVisibleArticle();
		while (reader.next(articleName = new Text(), document)) {
			articleCount++;
			if (articleCount % 100000 == 0) {
				System.out.println(articleCount);
			}
			completionService.submit(new SamplerCallable(articleName, document, writer, 1));
			if (articleCount > numThreads) {
				document = completionService.take().get();
			} else {
				document = new LatentAndVisibleArticle();
			}
		}

		System.out.println("Shutting down samplers...");
		executor.shutdown();
		System.out.println("Waiting for samplers to finish...");
		while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
			System.out.println("Still waiting for samplers to finish...");
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Path root = new Path(args[0]);
		
		int startIteration = -1;
		if (args.length > 1) {
			startIteration = Integer.parseInt(args[1]);
		}
		startIteration = loadCountsAndSettings(startIteration, root);
		
		boolean saveProbabilites = conf.getBoolean(SAVE_PROBABILITIES, false);
		System.out.println("Save probabilities: " + saveProbabilites);
		if (saveProbabilites) {
			Path zeroPath = new Path(workingDir, Integer.toString(startIteration));
			SequenceFile.Reader zeroReader = new SequenceFile.Reader(fs, zeroPath, conf);

			Path writePath = new Path(root, "sampler_results");
			SequenceFile.Writer probabilityWriter;
			if (doCompress) {
				probabilityWriter = SequenceFile.createWriter(fs, conf, writePath, TextIntPairWritable.class, IntDoubleArraysWritable.class, compressionType, compressionCodec);
			} else {
				probabilityWriter = SequenceFile.createWriter(fs, conf, writePath, TextIntPairWritable.class, IntDoubleArraysWritable.class);
			}
			emitAllProbabilities(zeroReader, probabilityWriter);
			return 0;
		}

		int keepRate = conf.getInt(KEEP_RATE, DEFAULT_KEEP_RATE);
		int maxIteration = conf.getInt(MAX_ITERATION, DEFAULT_MAX_ITERATION);
		
		long start;
		for (int i = startIteration; i < maxIteration; i++) {
			start = System.currentTimeMillis();

			// reset counts to zero
			lengthCounts.clear();
			for (int t = 0; t < numTopics; t++) {
				topicLengthCounts[t].clear();
			}

			Path zeroPath = new Path(workingDir, Integer.toString(i));
			Path onePath = new Path(workingDir, Integer.toString(i + 1));
			SequenceFile.Reader zeroReader = new SequenceFile.Reader(fs, zeroPath, conf);
			SequenceFile.Writer oneWriter;
			if (doCompress) {
				oneWriter = SequenceFile.createWriter(fs, conf, onePath, Text.class, LatentAndVisibleArticle.class, compressionType, compressionCodec);
			} else {
				oneWriter = SequenceFile.createWriter(fs, conf, onePath, Text.class, LatentAndVisibleArticle.class);
			}
			iterate(zeroReader, oneWriter);
			zeroReader.close();
			oneWriter.close();
			
			long stop = System.currentTimeMillis();
			System.out.println("Iteration time: " + ((stop - start) / 1000f));

			boolean optimize = (i >= 50) && (i % 10 == 0);
			// update variables as needed?
			if (optimize) {
				// alpha
				{
					long alphaStart = System.currentTimeMillis();
					alpha = Dirichlet.optimize(alpha, lengthCounts, topicLengthCounts, true);

					long alphaStop = System.currentTimeMillis();
					System.out.println("Alpha optimization time: " + ((alphaStop - alphaStart) / 1000f));
				}
				// beta
				{
					long betaStart = System.currentTimeMillis();

					TLongLongHashMap topicSizesHashMap = new TLongLongHashMap();
					for (int t = 0; t < numTopics; t++) {
						topicSizesHashMap.adjustOrPutValue(topicCounts.get(t), 1, 1);
					}
					final TLongLongHashMap topicWordCountsHashMap = new TLongLongHashMap();
					articleTopicCounts.forEachValue(new TObjectProcedure<SparseCounts>() {
						@Override
						public boolean execute(SparseCounts counts) {
							for (Entry e : counts) {
								topicWordCountsHashMap.adjustOrPutValue(e.count, 1, 1);
							}
							return true;
						}
					});

					beta = Dirichlet.optimizeSymmetric(beta, vocabularySize, topicSizesHashMap, topicWordCountsHashMap, true);
					betaV = beta * vocabularySize;
					long betaStop = System.currentTimeMillis();
					System.out.println("Beta optimization time: " + ((betaStop - betaStart) / 1000f));

				}

				// gamma
				{
					long gammaStart = System.currentTimeMillis();
					final TLongLongHashMap textSizesHashMap = new TLongLongHashMap();
					final TLongLongHashMap textWordCountsHashMap = new TLongLongHashMap();
					articleTextCounts.forEachValue(new TObjectProcedure<SparseCounts>() {
						@Override
						public boolean execute(SparseCounts counts) {
							int length = 0;
							for (Entry e : counts) {
								textWordCountsHashMap.adjustOrPutValue(e.count, 1, 1);
								length += e.count;
							}
							textSizesHashMap.adjustOrPutValue(length, 1, 1);
							return true;
						}
					});

					gamma = Dirichlet.optimizeSymmetric(gamma, textSize, textSizesHashMap, textWordCountsHashMap, true);
					textGamma = gamma * textSize;
					long gammaStop = System.currentTimeMillis();
					System.out.println("Gamma optimization time: " + ((gammaStop - gammaStart) / 1000f));
				}
			}

			// save new configuration settings in case they have changed
			Configuration newConf = new Configuration(false);
			newConf.setBoolean("disambiguate_by_text", dabTexts);
			newConf.setInt("num_topics", numTopics);
			newConf.setFloat("beta", (float) beta);
			newConf.setFloat("gamma", (float) gamma);
			String[] newAlphaStrings = new String[alpha.length];
			for (int t = 0; t < alpha.length; t++) {
				newAlphaStrings[t] = Double.toString(alpha[t]);
			}
			newConf.setStrings("alpha", newAlphaStrings);
			newConf.setBoolean("update_text_counts", updateTextCounts);
			Path newXmlPath = new Path(workingDir, Integer.toString(i + 1) + ".xml");
			FSDataOutputStream newXml = fs.create(newXmlPath);
			newConf.writeXml(newXml);
			newXml.close();

			if (i % keepRate != 0 && i != startIteration) {
				if (fs.exists(zeroPath)) {
					fs.delete(zeroPath, true);
				}
				Path oldXmlPath = new Path(workingDir, Integer.toString(i) + ".xml");
				if (fs.exists(oldXmlPath)) {
					fs.delete(oldXmlPath, true);
				}
			}
		}
		return 0;
	}

	//maximum to load
	private int limit = Integer.MAX_VALUE;
	public void setLimit(int limit) {
		this.limit = limit;
	}
	public int loadCountsAndSettings(int intialStartIteration, Path root) throws IOException {
		conf = getConf();
		fs = FileSystem.get(conf);
		rand = new Random();

		workingDir = new Path(root, "sampler_working");

		// load in appropriate configuration for the iteration
		if (intialStartIteration != -1) {
			Path xmlPath = new Path(workingDir, Integer.toString(intialStartIteration) + ".xml");
			if (fs.exists(xmlPath)) {
				FSDataInputStream xmlStream = fs.open(xmlPath);
				conf.addResource(xmlStream);
			}
		} else {
			//startIteration = 0;
		}

		// read settings from configuration
		numTopics = conf.getInt("num_topics", 100);
		beta = conf.getFloat("beta", 0.02f);
		gamma = conf.getFloat("gamma", 0.02f);
		alpha = new double[numTopics];
		updateTextCounts = conf.getBoolean("update_text_counts", true);
		String[] alphaStrings = conf.getStrings("alpha");
		if (alphaStrings == null) {
			double symmetricAlpha = conf.getFloat("symmetric_alpha", .02f);
			Arrays.fill(alpha, symmetricAlpha);
		} else {
			for (int t = 0; t < numTopics; t++) {
				alpha[t] = Double.parseDouble(alphaStrings[t]);
			}
		}

		articleTopicCountFactory = new SparseCountFactory(numTopics);
		topicLengthCounts = new TLongLongHashMap[numTopics];
		for (int t = 0; t < numTopics; t++) {
			topicLengthCounts[t] = new TLongLongHashMap();
		}

		long loadStart = System.currentTimeMillis();

		dabTexts = conf.getBoolean("disambiguate_by_text", false);
		System.out.println("Disambiguate by text: " + dabTexts);

		Path dabPath;
		if (dabTexts) {
			TIntSet allowed = new TIntHashSet();
			Path compressedPath = new Path(root, "sorted_compressed_fixed_out_links/data");
			
			System.out.println("Loading necessary texts...");
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, compressedPath, conf);
			VisibleArticle va = new VisibleArticle();
			Text text = new Text();
			while (reader.next(text, va)) {
				int al = va.ambiguousLength;
				for (int i = 0; i < al; i++) {
					allowed.add(va.ambiguousLinkTexts[i]);
				}
			}
			System.out.println("Texts needed: " + allowed.size());
			reader.close();
			
			FileStatus[] statuses = fs.listStatus(new Path(root, "compressed_link_text_index"), new PathFilter() {
				@Override
				public boolean accept(Path path) {
					return path.getName().startsWith("part");
				}
			});
			for (FileStatus status : statuses) {
				System.out.println("Loading " + status.getPath().toString() + "...");
				SequenceFile.Reader dabReader = new SequenceFile.Reader(fs, status.getPath(), conf);
				loadDabPossibilities(dabReader, allowed);
				dabReader.close();
			}
			allowed.clear();
		} else {
			dabPath = new Path(root, "compressed_dabs_list");
			SequenceFile.Reader dabReader = new SequenceFile.Reader(fs, dabPath, conf);
			loadDabPossibilities(dabReader, null);
			dabReader.close();
		}
		
		int startIteration;
		if (intialStartIteration > -1) {
			Path zeroPath = new Path(workingDir, Integer.toString(intialStartIteration));
			SequenceFile.Reader zeroReader = new SequenceFile.Reader(fs, zeroPath, conf);
			loadCounts(zeroReader);
			zeroReader.close();
			startIteration = intialStartIteration;
		} else {
			startIteration = 0;
			Path compressedPath = new Path(root, "sorted_compressed_fixed_out_links/data");
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, compressedPath, conf);
			loadInitialCounts(reader);
			reader.close();
		}

		// need to get vocabulary size and text size before this will work
		betaV = beta * vocabularySize;
		textGamma = gamma * textSize;

		long start = System.currentTimeMillis();
		System.out.println("Load time: " + ((start - loadStart) / 1000f));
		return startIteration;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new LatentDisambiguator(), args));
	}
}
