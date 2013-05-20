class NamespaceResolver
  attr_accessor :namespaces, :namespace_ids
  def initialize
    @namespaces = Hash.new
    @namespace_ids = Hash.new
  end

  def extract_namespace(title)
    title =~ /^\s*(([^:]*)\s*:)\s*(.*?)\s*$/
    ns_id = @namespace_ids[$2 && $2.downcase] 
    if ns_id
      title = $3
    else
      ns_id = 0
    end
    [ns_id, title]
  end

  def parse_node(node)
    if node.name == :namespaces
      node.complete!
      node.children(:namespace) do |ns| 
        name = ns.text
        dname = name.downcase
        @namespaces[dname] = name
        @namespace_ids[dname] = ns[:key].to_i
      end
    end
  end
end
