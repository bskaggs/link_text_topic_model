#!/usr/bin/env jruby

require 'rubygems'
require 'java'
import org.jgrapht.util.FibonacciHeap
import org.jgrapht.util.FibonacciHeapNode
class BookmarkColorer
  def self.color(b, alpha, tolerance, possible = nil)
    result = Hash.new(0)
    q = FibonacciHeap.new
    nodes = {}
    start = FibonacciHeapNode.new(b)
    nodes[b] = start
    q.insert(start, -1)
    q_total = 1.0
    while !q.empty?
      node = q.remove_min
      i = node.data
      nodes.delete(i)
      w = -node.key
     
      q_total -= w
      if possible
        result[i] += alpha * w if possible.include?(node.data)
        bm = possible.max_by { |x| result[x] }
        #p [bm, result[bm]]
        break unless bm
        bmm = (possible - [bm]).max_by { |x| result[x] } 
        #p [bmm, result[bmm]]
        break unless bmm
        remaining = q_total + (1 - alpha) * w
        gap = result[bm] - result[bmm]
        #p [w, nodes.size,remaining, gap]
        if remaining < gap || w < tolerance ||  w < gap #* tolerance 
          break # second best can't overtake first, quit
        end
      else
        result[i] += alpha * w
        next if w < tolerance
      end
      links, weights = yield(i)
      wsum = weights.inject(0) { |s,wt| s+wt }
      if wsum > 0
        weight = (1 - alpha) * w / wsum.to_f
        q_total += (1 - alpha) * w
        links.each_with_index do |j, idx|
          if j != b
            n = nodes[j]
            if n
              q.decrease_key(n, n.key - weight * weights[idx])
            else
              n = FibonacciHeapNode.new(j)
              nodes[j] = n
              q.insert(n, -weight * weights[idx])
            end
          end
        end
      end
    end
    result
  end
end

if $0 == __FILE__
  graph = { :a => [:b, :c], :b => [:a], :c => [:b] }
  res = BookmarkColorer.color(:a, 0.1, 0.01) do |n|
    edges = graph[n]
    [edges, Array.new(edges.length, 1)]
  end
  p res
end
