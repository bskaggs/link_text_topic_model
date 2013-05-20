package com.github.bskaggs.phoenix.sampler;

import org.apache.hadoop.io.Text;

public class ArticlePair {
	public Text articleName;
	public LatentAndVisibleArticle document;
	public ArticlePair(Text articleName, LatentAndVisibleArticle document) {
		this.articleName = articleName;
		this.document = document;
	}
}