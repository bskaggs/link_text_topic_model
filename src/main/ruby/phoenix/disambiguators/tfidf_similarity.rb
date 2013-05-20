#!/usr/bin/env ruby
require 'rubygems'
require 'phoenix/wiki/wiki'
require 'phoenix/evaluation_framework'
require 'set'
require 'phoenix/text/tfidf'

class WikiTFIDF < TFIDF
  def initialize(wiki, df_file, analyzer, doc_count)
    super(analyzer, doc_count)
    @wiki = wiki
    @df_file = df_file
    @cache = {}
  end

  def get_word_document_count(term)
    res = @cache[term]
    return res if res
    res = @wiki.get_word_document_count(@df_file, term)
    @cache[term] = res
    res
  end
end

class TFIDFDocumentSimilarity < EvaluationFramework

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
  end

  def self.register_options(o, options)
    options.analyzer_name = "org.apache.lucene.analysis.SimpleAnalyzer"
    o.on("--analyzer ANALYZER", String, "Lucene analyzer. Default = #{ options.analyzer_name }") { |a| options.analyzer_name = a} 
  end

  #find class of links with the most inlinks
  def score(class_id, dab_id, from_id)
    unless @last_dab == class_id
      @vectors = @classes[class_id].map { |links| links.map { |link| @tfidf.vectorize(@wiki.get_plain_text(@plain_text_file, link) || "") } }
      @last_dab = class_id
    end
    vector = @tfidf.vectorize(@wiki.get_plain_text(@plain_text_file, from_id) || "")
    @vectors.map { |vs| vs.map { |v| @tfidf.similarity(v,vector) }.max}
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
