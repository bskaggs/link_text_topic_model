#!/usr/bin/env ruby
require 'rubygems'
require 'phoenix/wiki/wiki'
require 'phoenix/evaluation_framework'
require 'set'

class GibbsDisambiguator < EvaluationFramework
  def initialize(options)
    super(options)
    @wiki = Wiki.new(options.root)
    @last_dab = nil
    @gibbs_file = @wiki.open_gibbs(options.table)
  end

  def self.register_options(o, options)
  end

  #find class of links with the most inlinks
  def score(class_id, dab_id, from_id)
    scores = @wiki.get_gibbs(@gibbs_file, from_id, dab_id)
    res = @classes[class_id].map { |links| links.map {|link| (scores[link] == 1.0 ? 0 : scores[link])  || 0}.max }
    res
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
