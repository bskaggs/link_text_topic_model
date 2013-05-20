package com.github.bskaggs.phoenix.sampler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class VisibleArticle implements Writable{
	public int[] regularLinkTargets;
	public int[] regularLinkTexts;
	public int regularLength;
	
	public int[] ambiguousLinkTargets;
	public int[] ambiguousLinkTexts;
	public int ambiguousLength;
	
	public VisibleArticle() {
	}
	
	public VisibleArticle(VisibleArticle va) {
		regularLength = va.regularLength;
		ambiguousLength = va.ambiguousLength;
		
		regularLinkTargets = Arrays.copyOf(va.regularLinkTargets, regularLength);
		regularLinkTexts = Arrays.copyOf(va.regularLinkTexts, regularLength);
		ambiguousLinkTargets = Arrays.copyOf(va.ambiguousLinkTargets, ambiguousLength);
		ambiguousLinkTexts = Arrays.copyOf(va.ambiguousLinkTexts, ambiguousLength);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int rl = regularLength = WritableUtils.readVInt(in);
		if (regularLinkTargets == null || regularLinkTargets.length < rl) {
			regularLinkTargets = new int[rl];
			regularLinkTexts = new int[rl];
		}
		for (int i = 0; i < rl; i++) {
			regularLinkTargets[i] = WritableUtils.readVInt(in);
			regularLinkTexts[i] = WritableUtils.readVInt(in);
		}
		
		int al = ambiguousLength = WritableUtils.readVInt(in);
		if (ambiguousLinkTargets == null || ambiguousLinkTargets.length < al) {
			ambiguousLinkTargets = new int[al];
			ambiguousLinkTexts = new int[al];
		}
		for (int i = 0; i < al; i++) {
			ambiguousLinkTargets[i] = WritableUtils.readVInt(in);
			ambiguousLinkTexts[i] = WritableUtils.readVInt(in);
		}
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, regularLength);
		for (int i = 0; i < regularLength; i++) {
			WritableUtils.writeVInt(out, regularLinkTargets[i]);
			WritableUtils.writeVInt(out, regularLinkTexts[i]);
		}
		WritableUtils.writeVInt(out, ambiguousLength);
		for (int i = 0; i < ambiguousLength; i++) {
			WritableUtils.writeVInt(out, ambiguousLinkTargets[i]);
			WritableUtils.writeVInt(out, ambiguousLinkTexts[i]);
		}
	}
	
	@Override
	public String toString() {
		StringBuilder res = new StringBuilder();
		try {
			res.append(Arrays.toString(Arrays.copyOf(regularLinkTargets, regularLength)));
			res.append(';');
			res.append(Arrays.toString(Arrays.copyOf(regularLinkTexts, regularLength)));
			res.append(';');
			res.append(Arrays.toString(Arrays.copyOf(ambiguousLinkTargets, ambiguousLength)));
			res.append(';');
			res.append(Arrays.toString(Arrays.copyOf(ambiguousLinkTexts, ambiguousLength)));
		} catch (Exception e) {
			res.append(e.toString());
		}
		return res.toString();
	}
}
