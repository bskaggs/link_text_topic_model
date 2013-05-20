require 'rubygems'
require 'unicase'
class Link < Struct.new(:raw, :title, :anchor, :display, :colon, :trail); end
class ArticleTitle < Struct.new(:language, :namespace_id, :short_title)
  def capitalize!
    self.short_title = self.short_title.first_unicode_capitalize
  end
end
