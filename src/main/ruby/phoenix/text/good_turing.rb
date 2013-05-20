require "matrix"
require "set"

class Counter
	attr_reader :trigram_counts
	attr_reader :frequencies	
	#counts the trigrams per file
	def visit_file(name)
		trigram = []
		File.open(name).each { |line|
			trigram.push line.chomp
			if trigram.length > 3
				trigram.shift
			end
			if trigram.length == 3
				t = Array.new(trigram)
				@trigram_counts[t] = 1 + @trigram_counts[t] 
			end
		}
	end

	#counts the trigrams in a file or a directory
	def visit_location(name)
		if File.exists?(name) 
			if (File.directory?(name)) 
				Dir.foreach(name) { |child| visit_location(name + "/" + child) if child != "." && child != ".." }
			else
				visit_file(name)
			end
		else
			STDERR.puts "#{name} doesn't exist."
		end
	end
	
	def initialize(loc) 
		@trigram_counts =  Hash.new { 0 }
		visit_location loc
		@frequencies = Hash.new { 0 }
		@trigram_counts.each { |k,v| @frequencies[v] = @frequencies[v] + 1 }
	end

end

class SimpleGoodTuring
	attr_reader :probs

	#implementation of the Simple Good Turing algorithm
	def initialize(counts)
		@n_r = Hash.new { 0 }
		@n = 0 
		counts.each do |k,v| 
			@n_r[v] += 1
			@n += v
    end
		
		rvals = @n_r.keys.sort
		
		@probs = Hash.new { 0 }
		@probs[0] = (1.0 * @n_r[1]) / @n
		z_r = []
		(0..(rvals.length - 1)).each do |j|
			i = ((j == 0) ? 0 : rvals[j - 1])
			k = ((j == rvals.length - 1) ? (2 * rvals[j] - i) : rvals[j + 1])
			z = 2.0 * @n_r[rvals[j]] / (k - i)
			z_r << z
    end

		log_r = Matrix.rows(rvals.map { |r| [Math.log(r), 1] })
		log_zr = Matrix.rows(z_r.map { |z| [Math.log(z)] })
		@beta = (log_r.transpose * log_r).inverse * log_r.transpose * log_zr
		calc_x = true
		@r_star = Hash.new { 0 }
		n_prime = 0
    rvals.each do |r|
			y = (r + 1) * s(r + 1) / s(r)
			ans = y
			if @n_r[r + 1] == 0
				calc_x = false
			elsif calc_x 
				x = ((r + 1.0) * @n_r[r + 1]) / @n_r[r]
				if (x - y).abs < 1.96 * ((r + 1.0) / @n_r[r]) * Math.sqrt(@n_r[r + 1] * (1 + ((1.0 * @n_r[r+1]) / @n_r[r])))
					ans = x
				else
					calc_x = false
				end
			end
			@r_star[r] = ans
			n_prime += @n_r[r] * ans
    end
		rvals.each { |r| @probs[r] = ((1 - @probs[0]) * @r_star[r]) / n_prime }
		puts [0, 0, @probs[0]].join("\t")
		rvals.each { |r| puts [r, @n_r[r] * (1.0 * r)/@n, @n_r[r] * @probs[r]].join("\t") }
	end
  
	#smoothing function
	def s(r)
		ret = Math.exp((Matrix.rows([[Math.log(r), 1]]) * @beta)[0,0])
		ret
	end 
end


class Scorer
	def initialize(loc) 
		@trigram_counts = Counter.new(loc).trigram_counts
		vocab = Set.new;
		@trigram_counts.each { |t| t.each { |w| vocab.add w} }
		@probs = SimpleGoodTuring.new(@trigram_counts).probs
		@probs[0] /= vocab.size * vocab.size * vocab.size - @trigram_counts.size
	end

	#scores trigram counts based on a reference proabbility
	def score(other_counts)
		score = 0.0
		seen = 0
		other_counts.each { |k, v|
			count = @trigram_counts[k]
			score += v * Math.log(@probs[@trigram_counts[k]])
		}
		score
	end
end
first = Scorer.new(ARGV[0])
second = Scorer.new(ARGV[1])

Dir.foreach(ARGV[2]) { |file|
	if !(file =~ /^\./)
		test = Counter.new(ARGV[2]+"/" + file)
		f = first.score(test.trigram_counts)
		s = second.score(test.trigram_counts)
		if f > s
			puts file + " First " + [f,s].join(" ")
		elsif f < s
			puts file + " Second " + [f,s].join(" ")
		else
			puts file + " same"
		end
	end
}
