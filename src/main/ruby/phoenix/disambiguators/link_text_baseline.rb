#!/usr/bin/env ruby
require 'rubygems'
require 'phoenix/wiki/wiki'
require 'phoenix/evaluation_framework'
require 'phoenix/hadoop'
class LinkTextBaseline < EvaluationFramework
  def initialize(options)
    super(options)
    @wiki = Wiki.new(options.root)
    @last_dab_id = nil
    @last_lengths = nil

    @in_links = @wiki.open_link_text_index(options.table)
  end
  
  #find class of links with the most inlinks
  def score(class_id, from_id)
    unless @last_class_id == class_id
      options = @classes[class_id]
      counts = @wiki.get_link_counts_by_text(@in_links, class_id)
      unscaled_lengths = options.map do |titles|
        count = titles.inject(0) do |s, title| 
          s + counts[title]
        end
        count
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
