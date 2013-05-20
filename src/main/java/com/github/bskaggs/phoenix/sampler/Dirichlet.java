package com.github.bskaggs.phoenix.sampler;

import java.util.Arrays;
import java.util.Random;

import gnu.trove.map.hash.TLongLongHashMap;

public class Dirichlet {

	public static double optimizeSymmetric(double param, int dimensions, TLongLongHashMap lengthCounts, TLongLongHashMap topicLengthCounts, boolean debug) {
		
		double difference;
		int steps = 0;

		param *= dimensions;
		do {
			steps++;
			difference = 0;
	
			double s = 0;
			{
				double d = 0;
				long maxLength = 0;
				for (long l : lengthCounts.keys()) {
					maxLength = Math.max(maxLength, l);
				}
				for (long n = 1; n <= maxLength; n++) {
					long count = lengthCounts.get(n);
					d += 1 / (param + n - 1);
					s += count * d;
				}
			}
			
			double newParam;
			{
				double d = 0;
				double ak = param / dimensions;
				double sk = 0;
				long maxLength = 0;
				for (long l : topicLengthCounts.keys()) {
					maxLength = Math.max(maxLength, l);
				}
				for (long n = 1; n <= maxLength; n++) {
					long count = topicLengthCounts.get(n);
					d += 1 / (ak + n - 1 );
					sk += count * d;
				}
				newParam = ak * sk / (s);
			}
			difference = Math.abs(newParam - param);
			
			param = newParam;
			difference /= param;
			System.out.println(steps + ":" + param);
		} while (difference > 0.0001);
		param /= dimensions;
		if (debug) {
			System.out.println("Converged in " + steps + " steps to " + param);
		}
		return param;
	}
		
	public static double[] optimize(double[] alpha, TLongLongHashMap lengthCounts, TLongLongHashMap[] topicLengthCounts, boolean debug) {
		int numTopics = alpha.length;
		double[] newAlpha = new double[alpha.length];
		
		double difference;
		int steps = 0;
		double alphaSum = 0;
		alphaSum = 0;
		for (double a : alpha) {
			alphaSum += a;
		}
		do {
			steps++;
			difference = 0;
	
			double s = 0;
			{
				double d = 0;
				long maxLength = 0;
				for (long l : lengthCounts.keys()) {
					maxLength = Math.max(maxLength, l);
				}
				for (long n = 1; n <= maxLength; n++) {
					long count = lengthCounts.get(n);
					d += 1 / (alphaSum + n - 1);
					s += count * d;
				}
			}
			
			for (int t = 0; t < numTopics; t++) {
				double d = 0;
				double ak = alpha[t];
				double sk = 0;
				long maxLength = 0;
				TLongLongHashMap lc = topicLengthCounts[t];
				for (long l : lc.keys()) {
					maxLength = Math.max(maxLength, l);
				}
				for (long n = 1; n <= maxLength; n++) {
					long count = lc.get(n);
					d += 1 / (ak + n - 1 );
					sk += count * d;
				}
				newAlpha[t] = alpha[t] * sk / s;
				difference += Math.abs(newAlpha[t] - alpha[t]);
			}
			
			{
				double[] temp = alpha;
				alpha = newAlpha;
				newAlpha = temp;
			}
			
			alphaSum = 0;
			for (double a : alpha) {
				alphaSum += a;
			}
			difference /= alphaSum;
		} while (difference > 0.0001);
		if (debug) {
			System.out.println("Converged in " + steps + " steps to " + alphaSum);
		}
		return alpha;
	}
	
	public static double gamma(Random rng, double k, double theta) {
		boolean accept = false;
		if (k < 1) {
			// Weibull algorithm
			double c = (1 / k);
			double d = ((1 - k) * Math.pow(k, (k / (1 - k))));
			double u, v, z, e, x;
			do {
				u = rng.nextDouble();
				v = rng.nextDouble();
				z = -Math.log(u);
				e = -Math.log(v);
				x = Math.pow(z, c);
				if ((z + e) >= (d + x)) {
					accept = true;
				}
			} while (!accept);
			return (x * theta);
		} else {
			// Cheng's algorithm
			double b = (k - Math.log(4));
			double c = (k + Math.sqrt(2 * k - 1));
			double lam = Math.sqrt(2 * k - 1);
			double cheng = (1 + Math.log(4.5));
			double u, v, x, y, z, r;
			do {
				u = rng.nextDouble();
				v = rng.nextDouble();
				y = ((1 / lam) * Math.log(v / (1 - v)));
				x = (k * Math.exp(y));
				z = (u * v * v);
				r = (b + (c * y) - x);
				if ((r >= ((4.5 * z) - cheng)) || (r >= Math.log(z))) {
					accept = true;
				}
			} while (!accept);
			return (x * theta);
		}
	}
	
	public static double[] dirichlet(Random random, double[] alpha, double alphaSum, double[] result) {
		double sum = 0;
		for (int i = 0; i < alpha.length; i++) {
			sum += (result[i] = gamma(random, alpha[i], 1));
		}
		for (int i = 0; i < result.length; i++) {
			result[i] /= sum;
		}
		return result;
	}
	
	public static double[] dirichlet(Random random, double alpha, int size) {
		double[] a = new double[size];
		Arrays.fill(a, alpha);
		return dirichlet(random, a);
	}
	
	public static double[] dirichlet(Random random, double[] alpha) {
		double sum = 0;
		for (double a : alpha) {
			sum += a;
		}
		return dirichlet(random, alpha, sum, new double[alpha.length]);
	}
	
	public static int multinomial(Random random, double[] params, double sum) {
		double r = random.nextDouble() / sum;
		for (int i = 0; i < params.length; i++) {
			if (params[i] >= r) {
				return i;
			}
			r -= params[i];
		}
		return params.length - 1;
	}
	
	public static int multinomial(Random random, double[] params) {
		double sum = 0;
		for (double a : params) {
			sum += a;
		}
		return multinomial(random, params, sum);
	}
}
