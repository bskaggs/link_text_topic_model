package com.github.bskaggs.phoenix.sampler;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.io.Writable;

final public class CompressedCorpus<K extends Writable, V extends Writable> implements Runnable{
	public static final class Reader<K extends Writable, V extends Writable> {
		private DataInputStream in;
		public Reader(byte[] array, int offset, int length) {
			try {
				in = new DataInputStream(new GZIPInputStream(new ByteArrayInputStream(array, offset, length)));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public Reader(byte[] array) {
			this(array, 0, array.length);
		}
		public boolean next(K key, V value) {
			try {
				key.readFields(in);
				value.readFields(in);
				return true;
			} catch (EOFException e) {
		
			} catch (IOException e) {
				e.printStackTrace();
			}
			return false;
		}
		
		
	}
	final private class Entry {
		public K key;
		public V value;
		
		public Entry(K key, V value) {
			this.key = key;
			this.value = value;
		}
	}
	
	final private BlockingQueue<Entry> queue = new ArrayBlockingQueue<Entry>(10000);
	final private ByteArrayOutputStream baos = new ByteArrayOutputStream();
	private DataOutputStream out;
	private Semaphore ready = new Semaphore(0); 
	
	public CompressedCorpus() {
		try {
			out = new DataOutputStream(new GZIPOutputStream(baos));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void run() {
		int count = 0;
		while (true) {
			count++;
			Entry entry = null;
			try {
				entry = queue.take();
			} catch (InterruptedException e) {
				try {
					//drain queue and write the result;
					while((entry = queue.poll()) != null) {
						entry.key.write(out);
						entry.value.write(out);
					}
					out.flush();
					out.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				Thread.currentThread().interrupt();
				ready.release(1);
				return;
			}
			
			try {
				entry.key.write(out);
				entry.value.write(out);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	//called from main thread
	public void write(K key, V value) {
		try {
			queue.put(new Entry(key, value));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public Reader<K,V> getReader() {
		try {
			ready.acquire(); //wait for everything to finish
			return new Reader<K,V>(baos.toByteArray());
		} catch (InterruptedException e) {
			e.printStackTrace();
			return null;
		}
	}

}
