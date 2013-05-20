#!/usr/bin/env ruby
require 'set'
require 'optparse'
require 'logger'
require 'ostruct'

class EvaluationFramework
  def self.inherited(child)
    EvaluationFramework.registered_plugins << child
  end
  @registered_plugins = []
  class << self; attr_reader :registered_plugins end

  attr_reader :classes
  def initialize(options)
    #load classes
    @classes = Hash.new([])
    File.foreach(options.class_file) do |l|
      parts = eval(l)
      @classes[parts[0]] = parts[1].map { |opt| Set.new(opt) }
    end
    @options = options
  end
 
  def self.sort(data)
    data
    #data.sort_by { |parts| parts[1] }
  end

  def evaluate
    correct = 0
    total = 0
    line_count = (lines = File.readlines(@options.correct_file).map { |x| eval(x) }).size
    $logger.info("Data count: #{line_count}") if $verbose
    data_count = 0
    
    
    scores_correct = []
    EvaluationFramework.sort(lines).each do |parts|
      if parts.length == 4
        dab_id, from, old_links, new_links = *parts
        text = nil
        class_id = dab_id
      else
        dab_id, text, from, old_links, new_links = *parts
        class_id = text
      end
      data_count += 1
      next if data_count <= @options.skip
      guess_order, guess_scores = rank_by(class_id, dab_id, text, from) 
      best = guess_order[-[@options.top, guess_order.length].min..-1]
      guessed_links = Set.new
      best.each { |g| guessed_links.merge(@classes[class_id][g] || []) }
      changed_links = new_links - old_links
      correct_guess = !guessed_links.intersection(changed_links).empty?
      scores_correct << [guess_scores.last, correct_guess] if guess_scores.last
      correct += 1 if correct_guess 
      $logger.info "#{data_count}(#{correct})/#{line_count}\t#{from}->#{dab_id}\tGuessed: #{best.inspect}:#{guessed_links.to_a.inspect}#{guess_scores.last}\tValid: #{changed_links}\tCorrect: #{correct_guess}" if $verbose && (data_count % $verbose == 0)
    end
    scores_correct = scores_correct.sort_by { |pair| -pair[0] }
    
    return correct, data_count, scores_correct
  end

  def EvaluationFramework.generate_svm_rank_file(scorers, options)
    line_count = (lines = File.readlines(options.correct_file).map { |x| eval(x) }).size
    $logger.info("Data count: #{line_count}") if $verbose
    data_count = 0
    classes = scorers[0].classes
    File.open(options.svm_rank_file, "w") do |f|
      sort(lines).each do |parts|
        dab_id, from, old_links, new_links = *parts
        data_count += 1
        next if data_count <= options.skip
        changed_links = new_links - old_links
        #1 for possible guesses
        #2 for bad guesses
        scores = scorers.map { |s| s.score(dab_id, from) }.transpose
        classes[dab_id].each_with_index do |klass, i|
          target_string = klass.intersection(changed_links).empty? ? "2" : "1"
          qid_string = "qid:#{data_count}" 
          score_string = ""
          scores[i].each_with_index { |s,j| score_string << " #{j+1}:#{s}" }
          f.puts [target_string, qid_string, score_string, "#", from, '->', dab_id, '->', klass.to_a.sort].join(' ')
        end
      end
    end
  end


  def EvaluationFramework.generate_scores(scorers, options)
    line_count = (lines = File.readlines(options.correct_file).map { |x| eval(x) }).size
    $logger.info("Data count: #{line_count}") if $verbose
    data_count = 0
    classes = scorers[0].classes
    File.open(options.out_score_file, "a") do |f|
      f.puts "#Starting..."
      f.puts "##{scorers.map { |s| s.class }.join(', ')}"
      lines.each do |parts|
        dab_id, from_id, old_links, new_links = *parts
        data_count += 1
        next if data_count <= options.skip
        changed_links = new_links - old_links
        scores = scorers.map { |s| s.score(dab_id, from_id) }.transpose
        valid_answers = classes[dab_id].map { |guessed_links| !guessed_links.intersection(changed_links).empty? } 
        $logger.info "#{data_count}/#{line_count}: #{from_id}->#{dab_id}: #{scores.inspect}, #{valid_answers.inspect}" if $verbose && (data_count % $verbose == 0)
        f.puts [dab_id, from_id, scores, valid_answers].inspect
      end
    end
  end

  def disambiguate(dab, text, from)
    raise "need to implement disambiguation method"
  end
  
  def self.register_options(o, options)

  end
end

