#!/usr/bin/env ruby
require 'rubygems'
require 'phoenix/wiki/wiki'
require 'phoenix/evaluation_framework'
require 'set'
require 'phoenix/text/tfidf'

class TFIDFInlinkDocumentSimilarity < EvaluationFramework

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
    @links = @wiki.open_in_links(options.table)
  end

  def self.register_options(o, options)
    options.analyzer_name = "org.apache.lucene.analysis.SimpleAnalyzer"
    o.on("--analyzer ANALYZER", String, "Lucene analyzer. Default = #{ options.analyzer_name }") { |a| options.analyzer_name = a} 
  end

  #find class of links with the most inlinks
  def disambiguate(dab_id, from_id)
    unless @last_dab == dab_id
      @vectors = @classes[dab_id].map do |targets| 
        possibles = targets.inject(Set.new) { |s,t| s.merge(@wiki.get_links(@links, t)) }
        
        if possibles.size > 100
          possibles = Set.new(possibles.to_a.sort_by { rand }[0...100])
        end
        possibles.merge(targets)

        p possibles.size
        possibles.map { |link| @tfidf.vectorize(@wiki.get_plain_text(@plain_text_file, link) || "") }
      end

      @last_dab = dab_id
    end
    vector = @tfidf.vectorize(@wiki.get_plain_text(@plain_text_file, from_id) || "")
    scores = @vectors.map { |vs| vs.map { |v| @tfidf.similarity(v,vector) }.max}
    (0...scores.length).max { |a,b| scores[a] <=> scores[b] }

  end
end
