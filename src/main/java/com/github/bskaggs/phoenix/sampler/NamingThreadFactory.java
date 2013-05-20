package com.github.bskaggs.phoenix.sampler;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class NamingThreadFactory implements ThreadFactory {
	private int count = 0;
	private final String prefix;
	private final ThreadFactory factory;
	
	public NamingThreadFactory(String prefix, ThreadFactory factory) {
		this.prefix = prefix;
		this.factory = factory;
	}
	
	public NamingThreadFactory(String prefix) {
		this(prefix, Executors.defaultThreadFactory());
	}
	
	@Override
	public Thread newThread(Runnable r) {				
		Thread t = factory.newThread(r);
		t.setName(prefix + "-" + (count++));
		return t;
	}
}