#!/usr/bin/env ruby
require 'rubygems'
require 'phoenix/wiki/wiki'
require 'phoenix/evaluation_framework'
require 'set'
require 'peach'
require 'phoenix/graph/random_walk'
class RandomWalkSimilarity < EvaluationFramework
  def initialize(options)
    super(options)
    @wiki = Wiki.new(options.root)
    out_links_file = @wiki.open_out_links(options.table)
    @out_links = @wiki.load_raw_links(out_links_file)

    @alpha = options.random_walk_alpha
    @bayes = options.random_walk_bayes
    @in_link_counts = @wiki.open_in_link_counts(options.table) if @bayes
    @full_links = @wiki.open_full_links(options.table) if @bayes
    @radius = options.radius
    $logger.info("Alpha: #{@alpha}")
  end
  
  def self.register_options(o, options)
    options.random_walk_alpha = 0.5
    o.on("--alpha ALPHA", Float, "Alpha value. Default = #{ options.random_walk_alpha }") { |a| options.random_walk_alpha = a} 
    o.on("--bayes", "Use as prior for link probability") { |a| options.random_walk_bayes = a} 
    o.on("--radius", "Use information radius as score") { |a| options.radius = a}
  end
  
  #find class of links with the most inlinks
  #dab 
  def score(class_id, dab, from_id)
    retain = @classes[class_id].map { |x| x.to_a}.flatten
    values = BookmarkColorer.color(from_id, @alpha, 0.0000001, retain) do |article|
      edges = (@out_links[article] || "").split(/\t/)
      edges.delete(dab)
      [edges, Array.new(edges.length, 1)]
    end
    #p values
    if @bayes
      text = @wiki.get_full_link(@full_links, from_id, dab)
      #@classes[dab].map { |vs| vs.map { |v| (values[v] || 0) * @wiki.get_in_link_count(@in_link_counts, v, text) / (@wiki.get_in_link_count(@in_link_counts,v, "").to_f) }.max}
      res = @classes[class_id].map do |vs| 
        (vs.map do |v|
          if text
            val = values[v] || 0
            #p [v, val, text]
            text_contrib = @wiki.get_in_link_count(@in_link_counts, v, text)
            total = @wiki.get_in_link_count(@in_link_counts, v, "").to_f
            total = 1 if total == 0
            #p [val, text_contrib, total]
            val * text_contrib / total
          else
            0
          end
        end).max
      end
      res
    elsif @radius
      @classes[class_id].pmap do |vs| 
        (vs.map do |v|
          other_values = BookmarkColorer.color(v, @alpha, 0.0001) do |article|
            edges =(@out_links[article] || "").split(/\t/)
            edges.delete(dab)
            [edges, Array.new(edges.length, 1)]
          end

          avg_values = Hash.new { |h,k| h[k] = 0}
          [values, other_values].each do |h|
            h.keys.each do |k|
              avg_values[k] += h[k] / 2
            end
          end

          -(kl(values, avg_values) + kl(other_values, avg_values)) / 2
        end).max
      end
    else
      res = @classes[class_id].map { |vs| vs.map { |v| values[v] || 0 }.max}
      #p res
      res
    end
  end

  def kl(h1, h2)
    h1.keys.inject(0) do |s, k|
      s + (h1[k] * Math.log(h1[k] / h2[k]))
    end
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
