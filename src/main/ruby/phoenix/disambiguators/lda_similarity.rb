#!/usr/bin/env ruby
require 'rubygems'
require 'phoenix/wiki/wiki'
require 'phoenix/evaluation_framework'
require 'set'

class LDADocumentSimilarity < EvaluationFramework
  def initialize(options)
    super(options)
    @wiki = Wiki.new(options.root)
    @last_dab = nil
    @divergence_type = options.divergence || :kl
    @use_neighbors = (options.neighbors == true)
    @lda_file = @wiki.open_lda(options.table)
    @outlinks = @wiki.open_out_links(options.table)
  end

  def self.register_options(o, options)
    metrics = [:kl, :lk, :irad, :l1, :l2]
    o.on("--divergence TYPE", metrics, "Similarity divergence for LDA (#{metrics.inspect})") do |d|
      options.divergence = d
    end
    o.on("--neighbors", "Use similarity to other links in source article, not just article itself.") do |n|
      options.neighbors = true
    end
  end

  #find class of links with the most inlinks
  def score(class_id, dab_id, from_id)
    unless @last_dab == class_id
      @vectors = @classes[class_id].map { |links| links.map { |link| 
        res = normalize(@wiki.get_lda(@lda_file, link))
        res = Array.new(res.length, 1.0 / res.length) if link == dab_id
        res
      } }
      @last_dab = class_id
    end

    if @use_neighbors
      links = ((@wiki.get_links(@outlinks, from_id) - [from_id, dab_id])).map { |l| r = normalize(@wiki.get_lda(@lda_file, l));  r }
    else
      links = [normalize(@wiki.get_lda(@lda_file, from_id))]
    end
    scores = @vectors.map do |vs| 
      vs.map do |v| 
        links.map { |l| divergence(v,l) }.min || (1.0/0)
      end.min || (1.0/0)
    end
    scores
  end

  def normalize(v)
    sum = v.inject { |s,x| s + x }
    v.map { |x| x/sum }
  end

  def divergence(v1, v2)
    case @divergence_type
      when :l1
        v1.zip(v2).inject(0) { |s, arr| s + (arr[0] - arr[1]).abs }
      when :l2
        v1.zip(v2).inject(0) { |s, arr| sdif = Math.sqrt(arr[0]) - Math.sqrt(arr[1]); s + sdif * sdif }
      when :kl
        v1.zip(v2).inject(0) { |s, arr| s + arr[0] * Math.log(arr[0] / arr[1]) }  # kl divergence
      when :lk
        v1.zip(v2).inject(0) { |s, arr| s + arr[1] * Math.log(arr[1] / arr[0]) }  # other kl divergence
      when :irad
        v1.zip(v2).inject(0) { |s, arr| s + (arr[1] * Math.log(arr[1] / (arr[1] + arr[0]))) + (arr[0] * Math.log(arr[0] / (arr[1] + arr[0]))) }  # Information radius
    end
  end
  
  def disambiguate(class_id, dab_id, text, from_id)
    rank_by(class_id, dab_id, text, from_id).last
  end
  
  def rank_by(class_id, dab_id, text, from_id)
    lengths = score(class_id, dab_id, from_id)
    order = (0...lengths.length).sort_by { |a| -lengths[a] }
    scores = order.map { |o| -lengths[o] }
    return order, scores
  end
end
