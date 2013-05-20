#!/usr/bin/env ruby
require 'rubygems'
require 'phoenix/wiki/wiki'
require 'phoenix/evaluation_framework'
require 'set'
class LinkRelatedness < EvaluationFramework
  def initialize(options)
    super(options)
    @wiki = Wiki.new(options.root)
    @last_dab = nil
    @logW = Math.log(3900000) #Math.log(@wiki.document_count(0))
    @inlinks = @wiki.open_in_links(options.table)
    @outlinks = @wiki.open_out_links(options.table)
    @smooth = options.smooth
    @average = options.average || false
    @selection = options.selection
  end
  
  def self.register_options(o, options)
    options.direction = :in
    o.on("--outlinks", "Use outlinks rather than inlinks") { |o| options.direction = :out }
    o.on("--smooth [AMOUNT]", Float, "Use additive smoothing for size of sets; default for smoothing is plus one smoothing") { |o| options.smooth = o || 1 }
    o.on("--selection AMOUNT", Float, "Use selection algorithm rather than max; amount is position in list: 0 = min, 1 = max") { |o| options.selection = o }
    o.on("--average", "Use average of links instead of max") { |o| options.average = true }
  end

  #find class of links with the most inlinks
  def score(class_id, dab_id, from_id)
    unless @last_dab == class_id
      @vectors = @classes[class_id].map { |links| links.map { |link| [link, Set.new(@wiki.get_links(@inlinks, link))] } }
      @last_dab == class_id
    end

    links = ((@wiki.get_links(@outlinks, from_id) - [dab_id]) + [from_id]).map { |l| [l, @wiki.get_links(@inlinks, l)] }
    pos = -1
    scores = @vectors.map do |vs| 
      if @classes[class_id][pos += 1].include?(dab_id)
        -999
      else
        vs.map do |v| 
          if v[0] == from_id
            0
          else
            combine_scores(links.map { |l| v[0] == l[0] ? 0 : relatedness(v[1], l[1]) } ) || 0
          end
        end.max || 0
      end
    end
  end

  def combine_scores(a)
    if @average
      if a.size > 0
        a.inject(0) { |s, x| s + x} / a.length
      else
        0
      end
    elsif @selection 
      sorted = a.sort
      pos = (a.length * @selection).floor
      sorted[pos]
    else
      a.max
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

  def relatedness(v1, v2)
    if @smooth
      sizes = [v1.size + @smooth, v2.size + @smooth]
      overlap = v1.intersection(v2).size
      return 0 if overlap == 0
    else
      sizes = [v1.size, v2.size]
      overlap = v1.intersection(v2).size
      return 0 if overlap == 0
    end
    rel =(Math.log(sizes.max) - Math.log(overlap)) /
      (@logW - Math.log(sizes.min))
    Math.exp(-rel)
  end
end
