#['link_extractor', 'rope/lib/rope', 'parser_functions'].each {|x| require File.expand_path(File.join(File.dirname(__FILE__), x)) }
require 'phoenix/extraction/link_extractor'
require 'phoenix/extraction/parser_functions'
class Section < Struct.new(:level, :title, :text); end

class Rope
  attr_accessor :parsed
  attr_accessor :nowiki
  #TaggedEmptyRope = Rope.wrap("", nil)

  def bytes

  end
  def strip
    first = 0
    last = length - 1
    first += 1 while first < last && char_at(first) =~ /\s/
    last -= 1 while last >= first && char_at(last) =~ /\s/
    if first > last
      Rope::TaggedEmptyRope
    else
      # p [first, length - last, self[first..last].to_s]
      self[first..last]
    end
  end

end

class Array
  def rope_sum
    inject { |s,x| s + x} || Rope::TaggedEmptyRope
  end
end

class WikiParser
  attr_accessor :chunk_processors
  attr_accessor :link_extractor
  def initialize(wiki, language)
    @wiki = wiki
    @link_extractor = LinkExtractor.new(@wiki.namespaces)
    @language = language
    init_magic
    init_chunk_processors
    @parser_functions = ParserFunctions.new(@wiki)
  end

  def self.strip_wiki(str)
    str = str.dup
    loop do
      break unless str.gsub!(/(\[http(s?):[^\]]*?(\s([^\]]+))?\])|(<nowiki>.*<\/nowiki>)|(<source(\s*|\s[^>]*)?>.*<\/source>)|(<!--.*?-->)|(\{\{[^{}]*\}\})|(\{\{\{[^{}]*\}\}\})|(<ref(\s*|\s[^>]*)((>[^<]*<\/ref>)|\/\s*>))/m) { $3 ? $3 : "" }
    end
    str.gsub!(/<\/?[^>]*>/, "")
    str
  end
  
  def self.strip_comments(str)
    str.gsub(/<!--.*?-->/, '')
  end

  def convert_links_to_text!(str, article_title, links=[])
    last_input_end = 0
    last_output_end = 0
    str.gsub!(LinkExtractor::LINK_REGEXP) do
      offset = Regexp.last_match.begin(0) - last_input_end
      last_input_end = Regexp.last_match.end(0)

      link_title, link_anchor, link_display, link_colon, link_trail = @link_extractor.link_parts(Regexp.last_match, article_title)
      if (link_title.language != article_title.language) || (link_title.namespace_id == 14) || (link_title.namespace_id == 6)
        unless link_colon
          link_display=""
          hidden = true
        end
      end
      links << [last_output_end + offset, link_title, link_display, hidden]
      last_output_end += offset + link_display.length
      link_display
    end
    str 
  end

  def split_sections(str)
    last_start = 0
    sections = []
    last_title = ""
    last_level = 0
    str.scan(/^=(=+)\s*(.*?)\s*=(=+)\s*$\n*/) do
      sections << Section.new(last_level, last_title, str[last_start...$~.begin(0)])
      last_start = $~.end(0)
      last_level = $1.length
      last_title = $2
    end
    sections << Section.new(last_level, last_title, str[last_start...-1])
    sections
  end

  def extract_links(article_title, text)
    @link_extractor.process_links(article_title, text)
  end

  OPEN_BRACE = '{'[0]
  CLOSE_BRACE = '}'[0]
  OPEN_SQUARE = '['[0]
  CLOSE_SQUARE = ']'[0]
  PIPE = '|'[0]
  EQUALS_SIGN = '='[0]
  MATCH = { OPEN_BRACE => CLOSE_BRACE, CLOSE_BRACE => OPEN_BRACE, OPEN_SQUARE => CLOSE_SQUARE, CLOSE_SQUARE => OPEN_SQUARE }
  MIN_MATCH = { OPEN_BRACE => 2, OPEN_SQUARE => 2, CLOSE_BRACE => 2, CLOSE_SQUARE => 2}
  MAX_MATCH = { OPEN_BRACE => 3, OPEN_SQUARE => 2, CLOSE_BRACE => 3, CLOSE_SQUARE => 2}
  def expand_templates(article_title, text, variables = {}, call_stack = [article_title])
    stack = [[]]

    #ruby does copy on write, so slicing is easy
    i = 0
    last_slide = 0
    loop do
      break if i >= text.length

      c = text[i]
      if c == OPEN_BRACE || c == OPEN_SQUARE
        #pull out enough open braces
        start = i
        while text[i += 1] == c; end
        len = i - start
        if len >= MIN_MATCH[c]
          slide_length = start - last_slide
          stack.last << Rope.new_from_partial_string(text, last_slide, slide_length, article_title) if slide_length > 0
          stack << [Rope.new_from_partial_string(text, start, i - start, article_title)]
          last_slide = i
          next
        end
      elsif (c == CLOSE_BRACE || c == CLOSE_SQUARE) && (stack.length > 1)
        chunk = stack.last
        to_match = chunk.first
        if MATCH[c] == to_match[0] #characters pair up properly
          max_match = [MAX_MATCH[c], to_match.length].min
          start = i
          while text[i += 1] == c && (i - start) < max_match; end
          len = i - start
          if len >= MIN_MATCH[c] #some kind of valid match is needed
            stack.pop #chunk is actually removed from stack, since it is paired up validly
            if len < to_match.length
              left_over_size = to_match.length - len
              if left_over_size < MIN_MATCH[c] #not enough
                chunk[0] = to_match[left_over_size...to_match.length]
              else
                stack.push []
              end

              #stick remaining braces either at end of text or as first part of a chunk
              stack.last << to_match[0...left_over_size]
            end
            slide_length = start - last_slide
            chunk << Rope.new_from_partial_string(text, last_slide, slide_length, article_title) if slide_length > 0
            chunk << Rope.new_from_partial_string(text, start, i - start, article_title)
            last_slide = i
            stack.last << process_chunk(chunk, article_title, variables, call_stack)
          end
          #if not long enough
          #i is already advanced
        else
          i += 1
        end
        #doesn't match the right thing.  continue as if it were a normal character
      elsif (c == PIPE || c == EQUALS_SIGN) && stack.length > 1
        #separate pipe and equal sign if needed
        slide_length = i - last_slide
        stack.last << Rope.new_from_partial_string(text, last_slide, slide_length, article_title) if slide_length > 0
        stack.last << Rope.new_from_partial_string(text, i, 1, article_title)
        i += 1
        last_slide = i
      else
        i += 1
      end
    end
    if last_slide < text.length
      stack.last << Rope.new_from_partial_string(text, last_slide, text.length - last_slide, article_title)
    end
    stack.flatten.inject{ |s,x| s + x }
  end

  def init_magic
    @magic = {
      'PAGENAME' => Proc.new { |call_stack, parts| Rope.wrap(call_stack.first.short_title.dup, call_stack.last) },
      'FULLPAGENAME' => Proc.new { |call_stack, parts| Rope.wrap(@link_extractor.full_name_for(call_stack.first).dup, call_stack.last) },
      'FULLPAGENAMEE' => Proc.new { |call_stack, parts| Rope.wrap(@link_extractor.full_name_for(call_stack.first).gsub(' ', '_'), call_stack.last) },
    }
  end


  def init_chunk_processors
    @chunk_processors = {
      '[[' => lambda { |chunk, *rest| chunk.rope_sum },
      '{{{' => lambda { |chunk, article_title, variables, call_stack|
        #puts variables.map { |k,v| "#{k}:#{v}"}.join(', ')
        #p chunk.inject { |s,x| s + x }.to_s
        parts = [[]]
        split = false
        chunk[1..-2].each do |x|
          next unless x
          if !split && x.length == 1 && !x.parsed &&  x.to_s == '|'
            split = true
            parts << []
          else
            parts.last << x
          end
        end
        #p parts.map { |x| x.to_s }
        pos = parts[0].rope_sum.to_s
        v = variables[pos]
        res = if v
          v
        else
          #puts "VAR '#{pos}' not found in {#{variables.keys.sort.map { |k| "'#{k}':'#{variables[k]}'"}.join(', ')}}"
          if parts[1]
            parts[1].rope_sum
          else
            chunk.rope_sum
          end
        end
        raise "WTF2" unless res
        res
      },
      '{{' => lambda { |chunk, article_title, variables, call_stack|
        template_name = nil
        parts = [[[]]]
        chunk[1..-2].each do |x|
          next unless x
          if !x.parsed && x.size == 1
            case x.to_s
            when '|'
              parts << [[]]
            when '='
              part = parts.last
              if parts.size > 1 && part.size == 1 #an equal sign is only special the first time it is used in a parameter
                part << []
              else
                part.last << x
              end
            else
              parts.last.last << x
            end
          else
            parts.last.last << x
          end
        end
        template_name_string = parts[0].flatten.rope_sum.to_s
        m = @magic[template_name_string]
        if m
          res = m.call(call_stack, parts)
        end

        unless res
          template_variables = {}
          parts[1..-1].each_with_index do |part, i|
            if part.length == 1
              template_variables[(i + 1).to_s] = part[0].rope_sum.strip
            elsif part.length == 2
              name = part[0].to_s
              name.gsub!(/\s+/, ' ')
              name.strip!
              template_variables[name] = part[1].rope_sum.strip
            else
              raise "Why are there three parts to a variable assignment?"
            end
          end

          if template_name_string =~ /\A\s*(#?[^\s:]*?)\s*:\s*(.*?)\s*\Z/
            function = $1.downcase
            conditional = $2
            pf = @parser_functions[function]
            if pf
              res = pf.call(call_stack, conditional, parts.map { |x| x.flatten })
            end
          end
        end

        unless res
          #puts "TRYING '#{template_name_string}'"
          template_title = @link_extractor.title_from_string(article_title, template_name_string, 10)
          #puts "TRYING '#{template_title}'"
          template_id, template_text = @wiki.fetch_page(template_title)
          strip!(template_text) if template_text
          unless template_text
            puts chunk.rope_sum
            puts "<<<MISSING: '#{template_title}'>>>"
            template_text = ":("
          end
          #template_text ||= "<<<MISSING: '#{template_title}'>>>"
          #puts "TEXT2 '#{template_text}'"
          res = expand_templates(template_title, template_text, template_variables, call_stack + [article_title])
        end
        res
      }
    }
  end
  def strip!(text)
    text.gsub!(/(<!--.*?-->)|(<noinclude>.*?<\/noinclude>)|(<ref>.*?<\/ref>)|(<\/?includeonly\s*\/?>)/m, '')
  end
  def process_chunk(chunk, article_title, variables = {}, call_stack = [])
    res = @chunk_processors[chunk[0].to_s].call(chunk, article_title, variables, call_stack)
    res.parsed = true
    res
  end
end
