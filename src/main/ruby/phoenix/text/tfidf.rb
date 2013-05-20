require 'rubygems'
require 'ostruct'
require 'set'
require 'phoenix/wiki/wiki'
require 'java'

class TFIDF
  $CLASSPATH << File.join(File.dirname(__FILE__), "../../lucene-core-3.0.3.jar")
  import java.io.InputStreamReader
  import java.io.ByteArrayInputStream
  import org.apache.lucene.analysis.tokenattributes.TermAttribute
  import java.nio.charset.Charset

  attr_accessor :term_counts
  def initialize(analyzer, document_count)
    @analyzer = analyzer
    @document_count = document_count
    @utf8 = Charset.for_name("UTF-8")
    @scaled = true
  end

  def tokenize(text)
    reader = InputStreamReader.new(ByteArrayInputStream.new(text.to_java_bytes),@utf8)
    stream = @analyzer.reusable_token_stream("text", reader)
    term_attribute = stream.get_attribute(TermAttribute.java_class)

    words = []
    while (stream.increment_token) 
      words << term_attribute.term
    end
    words
  end

  #used for scoring
  def vectorize(text)
    v = Hash.new { |h, k| h[k] = 0 }
    tokenize(text).each do |term|
      v[term] += 1 
    end
    scale(v) 
  end

  def group_vectorize(texts)
    terms = Set.new
    vectors = texts.map do |text|
      v = Hash.new { |h, k| h[k] = 0 }
      tokenize(text).each do |term|
        v[term] += 1 
      end
      terms.merge(v.keys)
      v
    end
    
    idfs = get_document_counts(terms)
    vectors.map{ |v| scale(v, idfs) }
  end

  def get_document_counts(terms)
    counts = Hash.new(0)
    terms.each do |term|
      counts[term] = get_word_document_count(term)
    end
    counts
  end

  def get_word_document_count(term)
    raise "Implement this"
  end

  def scale(v, document_counts = get_document_counts(v.keys) )
    vv = {}
    div = 0
    v.each do |term, tf|
      df = document_counts[term]
      #x = (1 + Math.log(tf)) * (@document_count / df)
      x = (1 + Math.log(tf)) * Math.log(@document_count / df)
      vv[term] = x
      div += x * x
    end

    #scale if nonzero
    if @scaled && div > 0
      div = Math.sqrt(div)
      vv.keys.each do |term|
        vv[term] /= div
      end
    end
    vv
  end

  def similarity(v1, v2)
    common = v1.keys & v2.keys #set intersection
    score = 0
    common.each do |term|
      score += v1[term] * v2[term]
    end
    score
  end
end
