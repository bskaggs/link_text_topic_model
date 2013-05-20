require 'rubygems'

require 'phoenix/wiki/links'
require 'phoenix/hadoop'
require 'java'

class Wiki
  import org.apache.hadoop.io.MapWritable
  import org.apache.hadoop.fs.FileSystem
  import org.apache.hadoop.fs.Path
  import org.apache.hadoop.io.SequenceFile
  import org.apache.hadoop.io.MapFile
  import org.apache.hadoop.io.Text
  import org.apache.hadoop.io.LongWritable
  import org.apache.hadoop.io.IntWritable
  import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner
  import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat
  import java.io.BufferedReader
  import java.io.InputStreamReader

  java_import "phoenix.LongPairWritable"
  java_import "phoenix.TextPairWritable"
  JLink = Java::phoenix.Link
  java_import "phoenix.io.TextDoubleArraysWritable"
  java_import "phoenix.DoubleArrayWritable"
  java_import "phoenix.io.TextLongMapWritable"
  java_import "phoenix.AlmostStandardAnalyzer"
  import org.apache.lucene.analysis.tokenattributes.TermAttribute
  import java.io.StringReader

  def initialize(root)
    @root = Path.new(root)
    @conf = Hadoop.configuration
    @fs = FileSystem.get(@conf)
    @utf8 = java.nio.charset.Charset.for_name("UTF-8")
  end

  def get_document_count(table, analyzer_class)
    file = Path.new(Path.new(Path.new(@root, table), "total_word_counts"), analyzer_class.simple_name)
    fin = BufferedReader.new(InputStreamReader.new(@fs.open(file), @utf8))
    doc_count = fin.read_line.to_i
    fin.close
    doc_count
  end

  def get_word_document_count(df_file, term)
    key = Text.new(term.to_java_bytes)
    value = LongPairWritable.new
    pair = df_file.get(key, value)
    pair.first
  end

  def open_document_frequency(table, analyzer_class)
    file = Path.new(Path.new(Path.new(Path.new(@root, table), "word_counts"), analyzer_class.simple_name), "part-r-00000").to_s
    MapFile::Reader.new(@fs, file, @conf)
  end

  def namespaces
    return @ns if @ns
    create_namespaces
    @ns
  end

  def inverse_namespaces
    return @ins if @ins
    create_namespaces
    @ins
  end

  def open_plain_text(table)
    file = Path.new(Path.new(@root, table), "sorted_plain_text").to_s
    MapFile::Reader.new(@fs, file, @conf)
  end
  
  def open_lda(table)
    file = Path.new(Path.new(@root, table), "sorted_lda/part-r-00000").to_s
    MapFile::Reader.new(@fs, file, @conf)
  end

  def open_gibbs(table)
    file = Path.new(Path.new(@root, table), "sorted_decompressed_sampler_results/part-r-00000").to_s
    MapFile::Reader.new(@fs, file, @conf)
  end

  def open_in_link_counts(table)
    file = Path.new(Path.new(@root, table), "sorted_in_link_counts/part-r-00000").to_s
    MapFile::Reader.new(@fs, file, @conf)
  end

  def open_in_links(table)
    file = Path.new(Path.new(@root, table), "sorted_simple_in_links/part-r-00000").to_s
    MapFile::Reader.new(@fs, file, @conf)
  end
  
  def open_out_links(table)
    file = Path.new(Path.new(@root, table), "sorted_simple_out_links/part-r-00000").to_s
    MapFile::Reader.new(@fs, file, @conf)
  end
  
  def open_full_links(table)
    file = Path.new(Path.new(@root, table), "sorted_fixed_out_links/part-r-00000").to_s
    MapFile::Reader.new(@fs, file, @conf)
  end

  def open_link_text_index(table)
    file = Path.new(Path.new(@root, table), "sorted_link_text_index/part-r-00000").to_s
    MapFile::Reader.new(@fs, file, @conf)
  end
  
  def open_dab_list(table)
    file = Path.new(Path.new(@root, table), "sorted_dabs").to_s
    MapFile::Reader.new(@fs, file, @conf)
  end

  def open_article_dictionary(table)
    MapFileOutputFormat.getReaders(Path.new(Path.new(@root, table), "article_dictionary_sorted"), @conf)
  end
  def open_reverse_article_dictionary(table)
    MapFileOutputFormat.getReaders(Path.new(Path.new(@root, table), "reverse_article_dictionary_sorted"), @conf)
  end
  
  def open_text_dictionary(table)
    MapFileOutputFormat.getReaders(Path.new(Path.new(@root, table), "text_dictionary_sorted"), @conf)
  end

  def get_dictionary_number(readers, string)
    res = MapFileOutputFormat.getEntry(readers, @partitioner ||= HashPartitioner.new, Text.new(string.to_java_bytes), IntWritable.new)
    if res
      return res.get.to_i
    else
      return -1
    end
  end
  
  def get_reverse_dictionary_name(readers, num)
    res = MapFileOutputFormat.getEntry(readers, @partitioner ||= HashPartitioner.new, IntWritable.new(num), Text.new)
    if res
      return res.to_s
    else
      return nil
    end
  end
  
  def get_links(links_file, name)
    links_bytes = links_file.get(Text.new(name.to_java_bytes), Text.new)
    return [] unless links_bytes
    String.from_java_bytes(links_bytes.get_bytes).split(/\t/)
  end
  
  def load_raw_links(links_file)
    result = {}
    text = Text.new
    links = Text.new
    count = 0
    while links_file.next(text, links)
      result[text.to_s] = links.to_s
      #puts text if (count += 1) % 100000 == 0
    end
    result
  end
 
  def get_full_link(links_file, from, to)
    link = JLink.new(from.to_java_string, to.to_java_string)
    res = links_file.get(link, Text.new)
    return nil unless res
    return res.to_s
  end

  def get_full_links(links_file, name)
    link = JLink.new(name.to_java_string, "".to_java_string)
    links_file.seek(link)
    text = Text.new
    links = []
    while links_file.next(link, text) && link.from.to_s == name
      links << [link.to.to_s, text.to_s]
    end
    return links
  end
 
  def normalize_text(text)
    stream = AlmostStandardAnalyzer.new.tokenStream(nil, StringReader.new(text.to_java_string))
    termAttribute = stream.getAttribute(TermAttribute.java_class);
    result = []
    while (stream.incrementToken())
      result << termAttribute.term().to_s
    end
    result.join(" ")
  end

  def get_links_by_text(links_file, name)
    set = Set.new
    res = links_file.get(Text.new(name.to_java_string), TextLongMapWritable.new)
    if res
      res.keySet.each { |t| set << t.to_s }
    end
    set
  end
  
  def get_link_counts_by_text(links_file, name)
    set = Hash.new{|h,k| h[k] = 0 }
    res = links_file.get(Text.new(name.to_java_string), TextLongMapWritable.new)
    if res
      res.keySet.each { |t| set[t.to_s] = res.get(t).to_i }
    end
    set
  end
  
  def get_plain_text(text_file, name)
    text_bytes = text_file.get(Text.new(name.to_java_bytes), Text.new)
    return nil unless text_bytes
    String.from_java_bytes(text_bytes.get_bytes)
  end
  
  def get_lda(file, name)
    array = file.get(Text.new(name.sub(/^en:/, '').to_java_bytes), DoubleArrayWritable.new)
    return Array.new(100, 0.01) unless array
    array.get.map { |dw| dw.get }
  end

  def get_gibbs(file, name, dab)
    array = file.get(TextPairWritable.new(name.to_java_string, dab.to_java_string), TextDoubleArraysWritable.new)
    return {} unless array

    text_array = array.text_array
    double_array = array.double_array
    res = {}
    text_array.size.times do |i|
      res[text_array[i].to_s] = double_array[i]
    end
    res
  end

  def get_in_link_count(file, to, text)
    res = file.get(TextPairWritable.new(to.to_java_string, text.to_java_string), LongWritable.new)
    return 0 unless res
    res.get()
  end

  DEFAULT_NS = {
    -2 => "media",
    -1 => "special",
    1 => "talk",
    2 => "user",
    3 => "user talk",
    4 => "project",
    5 => "project talk",
    6 => "file",
    7 => "file talk",
    8 => "mediawiki",
    9 => "mediawiki talk",
    10 => "template",
    11 => "template talk",
    12 => "help",
    13 => "help talk",
    14 => "category",
    15 => "category talk"
  }

  ALIAS_NS = {
    "image" => 6,
    "image talk" => 7
  }

  def create_namespaces
    ns = {}
    ins = {}
    # en                          column=namespace:8, timestamp=1294545156043, value=MediaWiki
    
    conf = Hadoop.configuration
    fs = FileSystem.get(conf)
    language_file = Path.new(@root, "languages.seq")
    language_reader = SequenceFile::Reader.new(fs, language_file, conf)
    while(language_reader.next(lang = Text.new, vals = MapWritable.new))
      ns_map = (ns[lang.to_s] ||= Hash.new)
      ins_map = (ins[lang.to_s] ||= Hash.new)
      vals.entry_set.each do |v|
         ns_id = v.key.to_s.to_i
         name = v.value.to_s
         lower = name.to_java_string.to_lower_case

         ns_map[lower] = ns_id

         ins_map[ns_id] = name
      end
      DEFAULT_NS.each do |k,v|
        unless ins_map[k]
          ins_map[k] = v
        end
        ns_map[v] = k
      end
      ALIAS_NS.each do |k,v|
        ns_map[k] = v
      end
    end
    language_reader.close
    @ns = ns
    @ins = ins
  end

  def title_to_string(title)
    array = [title.language]
    array << inverse_namespaces[title.language][title.namespace_id] if title.namespace_id != 0
    array << title.short_title
    array.join(':')
  end

  def close
    @db.disconnect
  end
end
