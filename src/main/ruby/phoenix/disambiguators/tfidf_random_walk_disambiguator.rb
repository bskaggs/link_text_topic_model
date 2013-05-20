#!/usr/bin/env ruby
require 'rubygems'
require 'phoenix/wiki/wiki'
require 'phoenix/evaluation_framework'
require 'set'
require 'peach'
require 'phoenix/graph/random_walk'
require 'cache'
class TFIDFRandomWalkSimilarity < EvaluationFramework
  def initialize(options)
    super(options)
    @wiki = Wiki.new(options.root)
    out_links_file = @wiki.open_out_links(options.table)
    @out_links = @wiki.load_raw_links(out_links_file)

    @alpha = options.random_walk_alpha
    analyzer = java_import(options.analyzer_name).new
    doc_count = @wiki.get_document_count(options.table, analyzer.java_class)
    df_file = @wiki.open_document_frequency(options.table, analyzer.java_class)
    @tfidf = WikiTFIDF.new(@wiki, df_file, analyzer, doc_count)
    @plain_text_file = @wiki.open_plain_text(options.table)
    @vector_cache = Cache.new(nil, nil, 100000)

    $logger.info("Alpha: #{@alpha}")
  end
  
  def self.register_options(o, options)
    #piggy back off of normal random walk
    #options.random_walk_alpha = 0.5
    #o.on("--alpha ALPHA", Float, "Alpha value. Default = #{ options.random_walk_alpha }") { |a| options.random_walk_alpha = a} 
  end
  
  def vectorize(article)
    @vector_cache.fetch(article) do |key|
      @tfidf.vectorize(@wiki.get_plain_text(@plain_text_file, article) || "")
    end
  end
  #find class of links with the most inlinks
  #dab 
  def score(class_id, dab, from_id)
    retain = @classes[class_id].map { |x| x.to_a}.flatten
    values = BookmarkColorer.color(from_id, @alpha, 0.0000001, retain) do |article|
      edges = (@out_links[article] || "").split(/\t/)
      edges.delete(dab)
    
      vector = vectorize(article)
      vectors = edges.map { |link| vectorize(link) }
      similarities = vectors.map { |v| @tfidf.similarity(v,vector) }
      [edges, similarities]
    end
    @classes[class_id].map { |vs| vs.map { |v| values[v] || 0 }.max}
  end

  def disambiguate(class_id, dab_id, text, from_id)
    scores = score(class_id, dab_id, from_id)
    (0...scores.length).max { |a,b| scores[a] <=> scores[b] }
  end
end
