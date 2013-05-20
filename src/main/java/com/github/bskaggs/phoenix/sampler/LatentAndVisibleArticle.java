package com.github.bskaggs.phoenix.sampler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.io.WritableUtils;

public class LatentAndVisibleArticle extends VisibleArticle{
	public int[] regularLinkTopics;
	public int[] ambiguousLinkTopics;
	public int[] ambiguousLinkSampledTargets;
	
	public LatentAndVisibleArticle() {
		super();
	}
	public LatentAndVisibleArticle(VisibleArticle va, Random rand, int numTopics) {
		super(va);
		regularLinkTopics = new int[regularLength];
		for (int i = 0; i < regularLength; i++) {
			regularLinkTopics[i] = rand.nextInt(numTopics);
		}
		ambiguousLinkTopics = new int[ambiguousLength];
		for (int i = 0; i < ambiguousLength; i++) {
			ambiguousLinkTopics[i] = rand.nextInt(numTopics);
		}
		
		ambiguousLinkSampledTargets = new int[ambiguousLength];
		//fill in afterward
		Arrays.fill(ambiguousLinkSampledTargets, -1);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		if (regularLinkTopics == null || regularLinkTopics.length < regularLength) {
			regularLinkTopics = new int[regularLength];
		}
		for (int i = 0; i < regularLength; i++) {
			regularLinkTopics[i] = WritableUtils.readVInt(in);
		}
		
		if (ambiguousLinkTopics == null || ambiguousLinkTopics.length < ambiguousLength) {
			ambiguousLinkTopics = new int[ambiguousLength];
		}
		for (int i = 0; i < ambiguousLength; i++) {
			ambiguousLinkTopics[i] = WritableUtils.readVInt(in);
		}
		
		if (ambiguousLinkSampledTargets == null || ambiguousLinkSampledTargets.length < ambiguousLength) {
			ambiguousLinkSampledTargets = new int[ambiguousLength];
		}
		for (int i = 0; i < ambiguousLength; i++) {
			ambiguousLinkSampledTargets[i] = WritableUtils.readVInt(in);
		}
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		for (int i = 0; i < regularLength; i++) {
			WritableUtils.writeVInt(out, regularLinkTopics[i]);
		}
		for (int i = 0; i < ambiguousLength; i++) {
			WritableUtils.writeVInt(out, ambiguousLinkTopics[i]);
		}
		for (int i = 0; i < ambiguousLength; i++) {
			WritableUtils.writeVInt(out, ambiguousLinkSampledTargets[i]);
		}
	}
	
	@Override	public String toString() {
		StringBuilder res = new StringBuilder();
		try {
			res.append("<");
			res.append(Arrays.toString(Arrays.copyOf(regularLinkTargets, regularLength)));
			res.append(';');
			res.append(Arrays.toString(Arrays.copyOf(regularLinkTexts, regularLength)));
			res.append(';');
			res.append(Arrays.toString(Arrays.copyOf(regularLinkTopics, regularLength)));
			
			res.append(">;<");
			res.append(Arrays.toString(Arrays.copyOf(ambiguousLinkTargets, ambiguousLength)));
			res.append(';');
			res.append(Arrays.toString(Arrays.copyOf(ambiguousLinkTexts, ambiguousLength)));
			res.append(';');
			res.append(Arrays.toString(Arrays.copyOf(ambiguousLinkSampledTargets, ambiguousLength)));
			res.append(';');
			res.append(Arrays.toString(Arrays.copyOf(ambiguousLinkTopics, ambiguousLength)));
			res.append('>');
		} catch (Exception e) {
			res.append(e.toString());
		}
		return res.toString();
	}
}
