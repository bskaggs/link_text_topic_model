require 'rubygems'
require 'phoenix/magic_xml'
require 'phoenix/wiki/namespace_resolver'
require 'phoenix/wiki/wiki_database'

class NamespaceExtractor
  def initialize(lang)
    @lang = lang
  end


  def extract(stream)
    begin
      XML.parse_as_twigs(stream) do |node|
        case node.name
          when :namespaces
            nr = NamespaceResolver.new
            nr.parse_node(node)
            return nr
        end
      end
    end
  end
end

