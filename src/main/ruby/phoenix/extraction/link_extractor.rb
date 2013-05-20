# encoding: utf-8
require 'cgi'
require 'rubygems'
require 'unicase'
require 'phoenix/wiki/links'

class NilClass
  def empty?
    true
  end
end


class LinkExtractor
  def initialize(namespaces_by_language = {}, inverse_namespaces_by_language = nil)
    #map from language name to map from lowercase namespace to id
    @namespaces_by_language = namespaces_by_language
    
    if inverse_namespaces_by_language
      @inverse_namespaces_by_language = inverse_namespaces_by_language
    else
      @inverse_namespaces_by_language = {}
      @namespaces_by_language.each do |lang, namespaces|
        @inverse_namespaces_by_language[lang] = namespaces.invert
      end
    end
  end
 
  def full_name_for(article_title)
    if article_title.namespace_id == 0
      article_title.short_title
    else
      [@inverse_namespaces_by_language[article_title.language][article_title.namespace_id], article_title.short_title].join(':')
    end
  end

  def cannonicalize(article_title)
    res = if article_title.namespace_id == 0
      [article_title.language.downcase, article_title.short_title]
    else
      [article_title.language.downcase, @inverse_namespaces_by_language[article_title.language][article_title.namespace_id],article_title.short_title]
    end
    res.join(':')
  end

  def title_from_string(current_article, link_title, default_namespace_id, capitalize = true)
    link_title = link_title.gsub(/(_|\s)+/, ' ')
    link_title.strip!
    if current_article
      local_namespaces = @namespaces_by_language[current_article.language]
      link_language = current_article.language
    else
      local_namespaces = {}
      link_language = "default"
    end
    link_namespace_id = default_namespace_id
    #handle slashed links in non mainspace
    if current_article && current_article.namespace_id != 0
      if link_title[0..0] == '/'
        article_title = ArticleTitle.new(current_article.language.dup, current_article.namespace_id, link_title.length > 1 ? current_article.short_title + link_title : current_article.short_title.dup)
        article_title.capitalize! if capitalize
        return article_title
      #elsif link_title[0..1] == './'
      #  return nil
      #  #TODO
      #elsif link_title[0..2] == '../'
      #  return nil
      #  link_namespace_id = ns_id
      #  #TODO
      end
    end
    
    #handle languages and prefixes
    link_title =~ /\A([^:]*?)\s*:\s*(.*)\s*\Z/

    unless $1.empty? #the part before first :
      #check if it is a language
      potential_language = $1.downcase
      potential_namespaces = @namespaces_by_language[potential_language]
      if potential_namespaces #if language exists
        link_language = potential_language
        link_title = $2
        link_title =~ /\A([^:]*?)\s*:\s*(.*?)\s*\Z/
      else
        potential_namespaces = local_namespaces
      end
      unless $1.empty? #if $1 could be a namespace
        potential_namespace_id = potential_namespaces[$1.downcase]
        if potential_namespace_id
          link_namespace_id = potential_namespace_id
          link_title = $2 || ""
        end
      end
    end

    article_title = ArticleTitle.new(link_language, link_namespace_id, link_title)
    
    article_title.capitalize! if capitalize
    article_title
  end

  LINK_REGEXP=/\[\[\s*([^\]\[\|]*?)\s*(\|\s*([^\[\]]*?))?\s*\]\]([a-z]*)/
  def process_links(current_article, text)
    result = [] unless block_given?
    text.scan(LINK_REGEXP) do
      raw = $&
      link = Link.new(raw, *link_parts(Regexp.last_match, current_article))
      
      if block_given?
        yield(link)
      else
        result << link
      end
    end
    result unless block_given?
  end

  def link_parts(m, current_article)
    unless m.is_a?(MatchData)
      m =~ LINK_REGEXP
      m = Regexp.last_match
    end
    m = m.to_a
    m.shift
    pre_pipe = m[0].empty? ? m[0] : CGI.unescapeHTML(m[0])
    has_pipe = !(m[1].empty?)
    post_pipe = m[2].empty? ? m[2] : CGI.unescapeHTML(m[2])
    link_trail = m[3]
     
    #convert '_' to space, collapse multiple whitespace, remove from start and end of title
    link_title = m[0]
    link_title.gsub!(/(_|\s)+/, ' ')
    link_title.strip!

    #see if a colon link, pull off link anchor
    link_title =~ /\A(:?)\s*([^#]*?)\s*(#(.*?))?\s*\Z/
    link_colon = !($1.empty?)
    link_title = $2 || ""
    link_anchor = $4
    link_anchor.tr!(' ', '_') if link_anchor

    article_title = title_from_string(current_article, link_title, 0, false)
    unless has_pipe
      link_display = pre_pipe.gsub(/\s+/, ' ')
    else
      unless post_pipe.empty?
        link_display = post_pipe
      else
        if article_title.short_title =~ /\A(.*?)\s*\([^\(\)]*\)\Z/ #remove trailing "(...)"
          link_display = $1
        else
          link_display = article_title.short_title 
        end
      end
    end
    link_display += link_trail if link_trail #changed from unless
    link_display.gsub!(/\s+/, ' ')
    link_display.strip!
    article_title.capitalize!
    return article_title, link_anchor, link_display, link_colon, link_trail
  end
end
