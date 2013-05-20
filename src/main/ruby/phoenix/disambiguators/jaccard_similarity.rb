#!/usr/bin/env ruby
require 'rubygems'
require 'phoenix/wiki/wiki'
require 'phoenix/evaluation_framework'
require 'set'
require 'phoenix/text/tfidf'
require 'cache'

class JaccardSimilarity < EvaluationFramework

  def initialize(options)
    super(options)
    @wiki = Wiki.new(options.root)
    @last_dab = nil
    analyzer = java_import(options.analyzer_name).new
    doc_count = @wiki.get_document_count(options.table, analyzer.java_class)
    puts "Doc count: #{doc_count}"
    df_file = @wiki.open_document_frequency(options.table, analyzer.java_class)
    
    @tfidf = WikiTFIDF.new(@wiki, df_file, analyzer, doc_count)
    @plain_text_file = @wiki.open_plain_text(options.table)
    @vector_cache = Cache.new(nil, nil, 100000)
  end

  def self.register_options(o, options)
  end

  def vectorize(article)
    @vector_cache.fetch(article) do |key|
      Set.new(@tfidf.tokenize(@wiki.get_plain_text(@plain_text_file, article) || ""))
    end
  end

  def jaccard_similarity(v1, v2)
    num = v1.intersection(v2).size
    denom = v1.union(v2).size
    denom = 1 if denom == 0
    num.to_f / denom
  end

  #find class of links with the most inlinks
  def score(class_id, dab_id, from_id)
    unless @last_dab == class_id
      @vectors = @classes[class_id].map { |links| links.map { |link| vectorize(link) } }
      @last_dab = class_id
    end
    vector = vectorize(from_id)
    @vectors.map { |vs| vs.map { |v| jaccard_similarity(v,vector) }.max}
  end

  def disambiguate(class_id, dab_id, text, from_id)
    rank_by(class_id, dab_id, text, from_id).last
  end
  
  def rank_by(class_id, dab_id, text, from_id)
    lengths = score(class_id, dab_id, from_id)
    order = (0...lengths.length).sort_by { |a| lengths[a] }
    scores = order.map { |o| lengths[o] }
    return order, scores
  end
end
