#!/usr/bin/env ruby
require 'rubygems'
require 'phoenix/wiki/wiki'
require 'phoenix/evaluation_framework'
require 'phoenix/hadoop'
class WeightedCombinationDisambiguator < EvaluationFramework
  def initialize(options)
    super(options)
    @scorers = options.scorer_classes.map { |c| Kernel.const_get(c).new(options) }
    @weights = options.scorer_weights
  end
 
  def self.register_options(o, options)
    o.on("--scorers SCORER1,SCORER2", Array, "Scorers.") { |s| options.scorer_classes = s }
    o.on("--weights WEIGHT1,WEIGHT2", Array, "Weights.") { |w| options.scorer_weights = w.map { |x| x.to_f } }
  end

  def disambiguate(dab_id, from_id)
    final = nil
    @scorers.zip(@weights).each do |scorer, w|
      scores = scorer.score(dab_id, from_id)
      final ||= Array.new(scores.length, 0)
      scores.each_with_index { |s,i| final[i] += s * w }
    end
    (0...final.length).max { |a,b| final[a] <=> final[b] }
  end
end
