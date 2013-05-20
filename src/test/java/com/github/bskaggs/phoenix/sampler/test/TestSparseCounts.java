package com.github.bskaggs.phoenix.sampler.test;

import gnu.trove.map.hash.TLongLongHashMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import com.github.bskaggs.phoenix.sampler.SparseCountFactory;
import com.github.bskaggs.phoenix.sampler.SparseCountFactory.Entry;
import com.github.bskaggs.phoenix.sampler.SparseCountFactory.SparseCounts;

import junit.framework.TestCase;

public class TestSparseCounts extends TestCase{
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
	}
	
	@Test
	public void testBits() {
		SparseCountFactory factory = new SparseCountFactory(7);
		assertEquals(3, factory.getTopicShift());
		assertEquals(8, factory.getTopicMult());
		assertEquals(7, factory.getTopicMask());
	
		factory = new SparseCountFactory(8);
		assertEquals(3, factory.getTopicShift());
		assertEquals(8, factory.getTopicMult());
		assertEquals(7, factory.getTopicMask());
	
		factory = new SparseCountFactory(9);
		assertEquals(4, factory.getTopicShift());
		assertEquals(16, factory.getTopicMult());
		assertEquals(15, factory.getTopicMask());
	
	}
	
	private void checkOrder(SparseCounts counts) {
		long lastCount = Long.MAX_VALUE;
		int lastTopic = Integer.MAX_VALUE;
		for (Entry e: counts) {
			assertTrue(lastCount >= e.count);
			if (lastCount == e.count) {
				assertTrue(lastTopic > e.topic);
			}
			lastCount = e.count;
			lastTopic = e.topic;
		}
	}
	
	private void checkNumber(SparseCounts counts, Map<Integer, Long> numAdded) {
		List<Long>values = new ArrayList<Long>(numAdded.values());
		Collections.sort(values);
		Collections.reverse(values);
		
		int pos = 0;
		for (Entry e: counts) {
			assertEquals(numAdded.get(e.topic).longValue(), e.count);
			assertEquals(values.get(pos++).longValue(), e.count);
		}
		assertEquals(numAdded.size(), counts.getSize());
	}
	
	private void checkTopics(SparseCounts counts, Map<Integer, Long> numAdded) {
		Set<Integer> topics = new HashSet<Integer>();
		for (Entry e: counts) {
			topics.add(e.topic);
		}
		assertEquals(numAdded.keySet(), topics);
	}

	@Test
	public void testAdd() {
		SparseCountFactory factory = new SparseCountFactory(10);
		SparseCounts counts = factory.create();
		Map<Integer, Long> numAdded = new HashMap<Integer, Long>();
		for (int t : new int[] { 3,5,7,3,4,5,5,7,5,4,2,3,4,5,6,5,5,5}) {
			counts.increment(t);
			Long last = numAdded.get(t);
			if (last == null) {
				last = 0L;
			}
			numAdded.put(t, last + 1);
			
			checkNumber(counts, numAdded);
			checkTopics(counts, numAdded);
			checkOrder(counts);
		}
		assertEquals(6, counts.getSize());
		
		for (int t : new int[] { 3,5,7,3,4,5,5 }) {
			counts.decrement(t);
			long last = numAdded.get(t);
			if (last == 1) {
				numAdded.remove(t);
			} else {
				numAdded.put(t, last - 1);
			}
			checkNumber(counts, numAdded);
			checkTopics(counts, numAdded);
			checkOrder(counts);
		}
	}
	
	public void testDelete() {
		SparseCountFactory factory = new SparseCountFactory(10);
		SparseCounts counts = factory.create();
		
		Map<Integer, Long> numAdded = new HashMap<Integer, Long>();
		for (int t : new int[] { 3,5,7,3,4,5,5,7,5,4,2,3,4,5,6,5,5,5}) {
			counts.increment(t);
			Long last = numAdded.get(t);
			if (last == null) {
				last = 0L;
			}
			numAdded.put(t, last + 1);
		}
		
		for (int t : new int[] { 3,5,7,3,6,4,5,5 }) {
			counts.decrement(t);
			long last = numAdded.get(t);
			if (last == 1) {
				numAdded.remove(t);
			} else {
				numAdded.put(t, last - 1);
			}
			checkNumber(counts, numAdded);
			checkTopics(counts, numAdded);
			checkOrder(counts);
		}
	}
	
	public void testConcurrent() throws InterruptedException {
		final int numTopics = 128;
		SparseCountFactory factory = new SparseCountFactory(numTopics);
		final SparseCounts counts = factory.create();
		
		class RandomAdder implements Runnable {
			List<Integer> added = new ArrayList<Integer>();
			Random rand;
			public RandomAdder(int num) {
				rand = new Random(num);
			}
			@Override
			public void run() {
				for (int i = 0; i < 10000; i++) {
					int t = rand.nextInt(numTopics);
					counts.increment(t);
					added.add(t);
				}
			}
		};
		
		class RandomSubtracter implements Runnable {
			List<Integer> added;
			Random rand;
			public RandomSubtracter(List<Integer> added) {
				this.added = added;
			}
			@Override
			public void run() {
				Collections.shuffle(added);
				for (int t : added) {
					counts.decrement(t);
				}
			}
		};
		
		RandomAdder[] adders = new RandomAdder[2000];
		Thread[] threads = new Thread[adders.length];
		for(int j = 0; j < adders.length; j++) {
			adders[j] = new RandomAdder(j);
			threads[j] = new Thread(adders[j]);
			threads[j].start();
		}
		for(int j = 0; j < adders.length; j++) {
			threads[j].join();
		}

		Map<Integer, Long> numAdded = new HashMap<Integer, Long>();
		for (RandomAdder adder : adders) {
			for (int t : adder.added) {
				Long last = numAdded.get(t);
				if (last == null) {
					last = 0L;
				}
				numAdded.put(t, last + 1);
			}
		}
		
		checkNumber(counts, numAdded);
		checkTopics(counts, numAdded);
		checkOrder(counts);
		
		RandomSubtracter[] subtractors = new RandomSubtracter[adders.length];
		for(int j = 0; j < adders.length; j++) {
			subtractors[j] = new RandomSubtracter(adders[j].added);
			threads[j] = new Thread(subtractors[j]);
			threads[j].start();
		}
		for(int j = 0; j < adders.length; j++) {
			threads[j].join();
		}
		

		checkNumber(counts, Collections.EMPTY_MAP);
		checkTopics(counts, Collections.EMPTY_MAP);
		checkOrder(counts);
	}
	
	public void testRandom() {
		int numTopics = 128;
		Random rand = new Random(10);
		SparseCountFactory factory = new SparseCountFactory(numTopics);
		SparseCounts counts = factory.create();
		
		Map<Integer, Long> numAdded = new HashMap<Integer, Long>();
		List<Integer> topics = new ArrayList<Integer>();
		for (int i = 0; i < 10000; i++) {
			topics.add(rand.nextInt(numTopics));
		}
		for (int t : topics) {
			counts.increment(t);
			Long last = numAdded.get(t);
			if (last == null) {
				last = 0L;
			}
			numAdded.put(t, last + 1);
			
			checkNumber(counts, numAdded);
			checkTopics(counts, numAdded);
			checkOrder(counts);
		}
		
		Collections.shuffle(topics);
		
		
		for (int t : topics) {
			counts.decrement(t);
			long last = numAdded.get(t);
			if (last == 1) {
				numAdded.remove(t);
			} else {
				numAdded.put(t, last - 1);
			}
			checkNumber(counts, numAdded);
			checkTopics(counts, numAdded);
			checkOrder(counts);
		}
		
	}
	
	public void testTimeSparse() {
		int numTopics = 1000;
		Random rand = new Random(0);
		SparseCountFactory factory = new SparseCountFactory(numTopics);
		SparseCounts counts = factory.create();
		for (int i = 0; i < 1000000; i++) {
			counts.increment((long) rand.nextInt(numTopics));
		}
	}
	
	
	public void testTimeDense() {
		int numTopics = 1000;
		Random rand = new Random(0);
		TLongLongHashMap counts = new TLongLongHashMap();
		for (int i = 0; i < 1000000; i++) {
			counts.adjustOrPutValue(rand.nextInt(numTopics), 1, 1);
		}
	}

}
