package com.github.bskaggs.phoenix.sampler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SparseCountFactory {
	public final static class Entry {
		public long count;
		public int topic;
		
		@Override
		public String toString() {
			return Arrays.toString(new Long[] {count, (long) topic});
		}
	}
	
	private long topicMask;
	private long topicShift;
	private long topicMult;
	private long numTopics;
	
	public SparseCountFactory(long nt) {
		topicMult = Long.highestOneBit(nt - 1) * 2;
		topicMask = topicMult - 1;
		topicShift = Long.numberOfTrailingZeros(topicMult);
		numTopics = nt;
	}
	
	public long getTopicMask() {
		return topicMask;
	}
	public long getTopicShift() {
		return topicShift;
	}
	public long getTopicMult() {
		return topicMult;
	}
	
	public final class SparseCounts implements Iterable<Entry>{
		private long[] array;
		private int size;
		private ReadWriteLock lock;
		
		public SparseCounts() {
			array = new long[3];
			lock = new ReentrantReadWriteLock();
			size = 0;
		}
		
		public void increment(long topic) {
			lock.writeLock().lock();
			rawIncrement(topic);
			lock.writeLock().unlock();
		}
		
		public long rawIncrement(long topic) {
			//System.out.println("Inserting " + topic);
			int i;
			for (i = 0; i < size; i++) {
				//System.out.println("i=" + i + ", array[i]=" + array[i] + ", array[i] & topicMask=" + (array[i] & topicMask));
				if ((array[i] & topicMask) == topic) {
					//System.out.println("Found!");
					array[i] += topicMult;
					break;
				}
			}
		
			if (i == size) {
				//System.out.println("Not found, adding");
				if (size == array.length) {
					//System.out.println("Copying to larger array");	
					array = Arrays.copyOf(array, size + 7);
				}
				array[size++] = topicMult + topic;
				//System.out.println("New value: " + array[size - 1]);
			}
			
			long res = array[i] >> topicShift;
			//percolate
			while(i > 0 && array[i] > array[i-1]) {
				long temp = array[i];
				array[i] = array[i-1];
				array[i-1] = temp;
				
				i--;
			}
			
			return res;
		}

	
		public void decrement(long topic) {
			lock.writeLock().lock();
			rawDecrement(topic);
			lock.writeLock().unlock();
		}
		
		public long rawDecrement(long topic) {
			int i;
			for (i = 0; i < size; i++) {
				if ((array[i] & topicMask) == topic) {
					array[i] -= topicMult;
					break;
				}
			}
			
			if (i < size) {
				//percolate
				while(i < size - 1 && array[i] < array[i+1]) {
					long temp = array[i];
					array[i] = array[i+1];
					array[i+1] = temp;
					i++;
				}
				long res = array[i] >> topicShift;
				if (array[i] < topicMult) { //purge zero
					size--;
					if (array.length - size > 7) {
						array = Arrays.copyOf(array, size + 1);
					}
				}
				return res;
			}
			
			//shouldn't happen!
			throw new IllegalArgumentException("Shouldn't decrement something not there!");
		}

		@Override
		public Iterator<Entry> iterator() {
			return new Iterator<Entry>() {
				private int i = -1;
				private final Entry entry = new Entry();

				@Override
				public boolean hasNext() {
					return i + 1 < size;
				}

				@Override
				public Entry next() {
					i++;
					entry.topic = (int) (array[i] & topicMask);
					entry.count = array[i] >> topicShift;
					return entry;
				}

				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
			};
		}

		public int getSize() {
			return size;
		}


		
		@Override
		public String toString() {
			lock.readLock().lock();
			List<String> entries = new ArrayList<String>();
			for (Entry entry : this) {
				entries.add(entry.toString());
			}
			lock.readLock().unlock();
			return entries.toString();
		}
		
		public long[] toArray() {
			return toArray(new long[(int) numTopics]);
		}

		public long[] toArray(long[] counts) {
			lock.readLock().lock();
			for (Entry entry : this) {
				counts[entry.topic] = entry.count;
			}
			lock.readLock().unlock();
			return counts;
		}

		public long get(int topic) {
			lock.readLock().lock();
			long count = rawGet(topic);
			lock.readLock().unlock();
			return count;
		}
		
		public long rawGet(int topic) {
			long count = 0;
			for (int i = 0; i < size; i++) {
				if ((array[i] & topicMask) == topic) {
					count = array[i] >> topicShift;
					break;
				}
			}
			return count;
		}
		
		public ReadWriteLock getLock() {
			return lock;
		}

	}

	public SparseCounts create() {
		return new SparseCounts();
	}
}
