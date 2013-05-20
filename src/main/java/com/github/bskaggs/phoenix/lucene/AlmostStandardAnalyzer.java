package com.github.bskaggs.phoenix.lucene;

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.Version;

public class AlmostStandardAnalyzer extends Analyzer{
	@Override
	public TokenStream tokenStream(String fieldName, Reader reader) {
		return new LowerCaseFilter(new StandardFilter(new StandardTokenizer(Version.LUCENE_30, reader)));
	}
}
