package com.github.bskaggs.phoenix;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class LinkToComparator extends WritableComparator {

	public LinkToComparator() {
		super(Link.class, true);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		Link l1 = (Link) w1;
		Link l2 = (Link) w2;
		
		int res = l1.to.compareTo(l2.to);
		if (res == 0) {
			return l1.from.compareTo(l2.from);
		}
		return res;
	}
	

}
