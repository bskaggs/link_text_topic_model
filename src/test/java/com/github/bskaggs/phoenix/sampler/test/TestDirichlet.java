package com.github.bskaggs.phoenix.sampler.test;

import gnu.trove.map.hash.TLongLongHashMap;

import java.util.Arrays;
import java.util.Random;

import com.github.bskaggs.phoenix.sampler.Dirichlet;

import junit.framework.TestCase;

public class TestDirichlet extends TestCase {
	public void testOptimize() throws Exception {
		Random rand = new Random(1);
		double[] params = new double[] {1, 2, 3, 10, .2, .001};
		
		TLongLongHashMap lengthCounts = new TLongLongHashMap();
		TLongLongHashMap[] topicLengthCounts = new TLongLongHashMap[params.length];
		for (int i = 0; i < params.length; i++) {
			topicLengthCounts[i] = new TLongLongHashMap();
		}
		
		for (int i = 1; i < 10000; i++) {
			int length = rand.nextInt(20) + 20;
			int[] counts = new int[params.length];
			double[] theta = Dirichlet.dirichlet(rand, params);
			for (int j = 0; j < length; j++) {
				int t = Dirichlet.multinomial(rand, theta);
				counts[t]++;
			}
			for (int t = 0; t < params.length; t++) {
				topicLengthCounts[t].adjustOrPutValue(counts[t], 1, 1);
			}
			lengthCounts.adjustOrPutValue(length, 1, 1);
		}
		
		double[] putative = new double[params.length];
		Arrays.fill(putative, 1);
		double[] result = Dirichlet.optimize(putative, lengthCounts, topicLengthCounts, true);
		System.out.println(Arrays.toString(result));
	}
	
	public void testOptimizeSymmetric() throws Exception {
		Random rand = new Random(1);
		double[] params = new double[10000];
		Arrays.fill(params, .33);
		
		TLongLongHashMap lengthCounts = new TLongLongHashMap();
		TLongLongHashMap topicLengthCounts = new TLongLongHashMap();
		
		for (int i = 1; i < 1000; i++) {
			int length = rand.nextInt(20) + 20;
			int[] counts = new int[params.length];
			double[] theta = Dirichlet.dirichlet(rand, params);
			for (int j = 0; j < length; j++) {
				int t = Dirichlet.multinomial(rand, theta);
				counts[t]++;
			}
			for (int t = 0; t < params.length; t++) {
				topicLengthCounts.adjustOrPutValue(counts[t], 1, 1);
			}
			lengthCounts.adjustOrPutValue(length, 1, 1);
		}
		
		double putative = 1;
		double result = Dirichlet.optimizeSymmetric(putative, params.length, lengthCounts, topicLengthCounts, true);
		System.out.println(result);
	}
}
