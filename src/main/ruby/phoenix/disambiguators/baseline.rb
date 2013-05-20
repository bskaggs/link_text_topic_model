#!/usr/bin/env ruby
require 'rubygems'
require 'phoenix/wiki/wiki'
require 'phoenix/evaluation_framework'
require 'phoenix/hadoop'
class Baseline < EvaluationFramework
  def initialize(options)
    super(options)
    @wiki = Wiki.new(options.root)
    @last_dab_id = nil
    @last_lengths = nil

    @in_links = @wiki.open_in_links(options.table)
  end
  
  #find class of links with the most inlinks
  def score(class_id, from_id)
    unless @last_class_id == class_id
      options = @classes[class_id]
      unscaled_lengths = options.map do |titles|
        in_set = titles.inject(Set.new) do |s, title| 
          s.merge(@wiki.get_links(@in_links, title))
        end
        in_set.size
      end
      total = unscaled_lengths.inject { |s,l| s + l }
      total = 1 if total == 0
      lengths = unscaled_lengths.map { |l| l.to_f / total }
      @last_lengths = lengths
    else
      lengths = @last_lengths
    end
    @last_class_id = class_id
    lengths
  end

  def disambiguate(class_id, dab_id, text, from_id)
    rank_by(class_id, dab_id, text, from_id).last
  end
  
  def rank_by(class_id, dab_id, text, from_id)
    lengths = score(class_id, from_id)
    order = (0...lengths.length).sort_by { |a| lengths[a] }
    scores = order.map { |o| lengths[o] }
    return order, scores
  end
end
