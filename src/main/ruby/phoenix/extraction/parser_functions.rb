require 'cgi'
require 'forwardable'

class ParserFunctions 
  extend Forwardable

  
  def_delegator :@functions, :[]
  
  def initialize(wiki)
    @wiki = wiki
    @functions = {
      '#if' => lambda { |call_stack, conditional, parts|
        if !(conditional =~ /\A\s*\Z/)
          res = parts[1].rope_sum
        elsif parts[2]
          res = parts[2].rope_sum
        else
          res = [].rope_sum
        end
        raise "WTF3" unless res
        return res
      },
      '#ifeq' => lambda { |call_stack, conditional, parts|
        if conditional.to_s == parts[1].to_s
          parts[2].rope_sum
        elsif parts.length > 3
          parts[3].rope_sum
        else
          Rope::TaggedEmptyRope
        end
      },
      '#expr' => lambda { |call_stack, conditional, parts|
        puts "trying '#{conditional}'"
        begin
          text =expr(conditional.to_s).to_s
        rescue
          puts $!
          print $!.backtrace.join("\n")
          Rope.wrap($1.to_s, call_stack.last)
        else
          puts text
          Rope.wrap(text, call_stack.last)
        end
      },
      '#ifexpr' => lambda { |call_stack, conditional, parts|
        puts "trying iexpr '#{conditional}'"
        begin
          value = expr(conditional.to_s)
          puts "value: #{value}"
          res = if value != 0
            parts[1].rope_sum
          elsif parts[2]
            parts[2].rope_sum
          else
            Rope::TaggedEmptyRope
          end
          puts "res: #{res}"
          res
        rescue
          puts $!
          print $!.backtrace.join("\n")
          Rope.wrap($1.to_s, call_stack.last)
        end
      },
      'urlencode' => lambda { |call_stack, conditional, parts|
        Rope.wrap(CGI::escape(conditional), call_stack.last)
      },
      'lc' => lambda { |call_stack, conditional, parts|
        result = ""
        conditional.each_char do |c|
          result << (Unicode::Lowercase[c] || c)
        end
        Rope.wrap(result, call_stack.last)
      },
      'formatnum' => lambda { |call_stack, conditional, parts|
        Rope.wrap(conditional, call_stack.last)
      }
    }
  end
  
  NotEqual = lambda { |operands| raise "missing_operand" if operands.length < 2; right = operands.pop; left = operands.pop; operands << ((left != right) ? 1 : 0)}
  Div = lambda { |operands| raise "missing_operand" if operands.length < 2; right = operands.pop; left = operands.pop; operands << (left.to_f / right.to_f)}

  Tokens = { 
    :plus => [:plus, 6, lambda { |operands| raise "missing_operand" if operands.length < 2; operands << (operands.pop + operands.pop)}],
    :minus => [:minus, 6, lambda { |operands| raise "missing_operand" if operands.length < 2; right = operands.pop; left = operands.pop; operands << (left - right)}],
    :unary_plus => [:unary_plus, 10, lambda { |operands| raise "missing_operand" if operands.length < 1}],
    :unary_minus => [:unary_minus, 10, lambda { |operands| raise "missing_operand" if operands.length < 1; operands << (- operands.pop)}],
    '*' => [:times, 8, lambda { |operands| raise "missing_operand" if operands.length < 2; right = operands.pop; left = operands.pop; operands << (left * right)}],
    '/' => [:div, 8, Div],
    'div' => [:div, 8, Div],
    :not => [:not, 9, lambda { |operands| raise "missing_operand" if operands.length < 1; operands << (operands.pop == 0 ? 1 : 0)}],
    'and' => [:and, 3, lambda { |operands| raise "missing_operand" if operands.length < 2; right = operands.pop; left = operands.pop; operands << ((left != 0 && right != 0) ? 1 : 0)}],
    'or' => [:or, 2, lambda { |operands| raise "missing_operand" if operands.length < 2; right = operands.pop; left = operands.pop; operands << ((left != 0 || right != 0) ? 1 : 0)}],
    '=' => [:equal, 4, lambda { |operands| raise "missing_operand" if operands.length < 2; right = operands.pop; left = operands.pop; operands << ((left == right) ? 1 : 0)}],
    '!=' => [:not_equal, 4, NotEqual],
    '<>' => [:not_equal, 4, NotEqual],
    '<' => [:less_than, 4, lambda { |operands| raise "missing_operand" if operands.length < 2; right = operands.pop; left = operands.pop; operands << ((left < right) ? 1 : 0)}],
    '<=' => [:less_than_or_equal, 4, lambda { |operands| raise "missing_operand" if operands.length < 2; right = operands.pop; left = operands.pop; operands << ((left <= right) ? 1 : 0)}],
    '>' => [:greater_than, 4, lambda { |operands| raise "missing_operand" if operands.length < 2; right = operands.pop; left = operands.pop; operands << ((left > right) ? 1 : 0)}],
    '>=' => [:greater_than_or_equal, 4, lambda { |operands| raise "missing_operand" if operands.length < 2; right = operands.pop; left = operands.pop; operands << ((left >= right) ? 1 : 0)}],
    'mod' => [:mod, 8, lambda { |operands| raise "missing_operand" if operands.length < 2; right = operands.pop; left = operands.pop; operands << (left.truncate % right.truncate)}],
    'round' => [:round, 5, lambda { |operands| raise "missing_operand" if operands.length < 2; right = operands.pop.round; left = operands.pop; operands << ((left * (10 ** right)).round / (10 ** right)) }],
    '(' => [:open, -1],
    ')' => [:close, -1]
  }
  def expr(text)
    operands = []
    operators = []
    expecting = :expression
    i = 0
    loop do
      break if i >= text.length
      start = i
      token = nil
      #tokenization
      c = text[i..i]
      #puts "c: #{c}, expecting:#{expecting}"
      case c
        when /\s/
          i += 1
          next
        when /\d|\./
          dot = ('.' == c)
          i += 1
          while (c = text[i..i]) =~ /\d|\./
            dot ||= ('.' == c)
            i += 1
          end
          token = dot ? text[start...i].to_f : text[start...i].to_i
          operands << token
          raise "not expecting number" if expecting != :expression
          expecting = :operator
          next
        when /\A[a-zA-Z]\Z/
          i += 1
          while text[i..i] =~ /[a-zA-Z]/
            i += 1
          end
          token = Tokens[text[start...i].downcase]
        when '+'
          i += 1
          if expecting == :expression
            token = Tokens[:unary_plus]
          else
            token = Tokens[:plus]
          end
          token = Tokens[text[start...i]]
        when '-'
          i += 1
          if expecting == :expression
            token = Tokens[:unary_minus]
          else
            token = Tokens[:minus]
          end
          token = Tokens[text[start...i]]
        when '*', '/', '='
          i += 1
          token = Tokens[text[start...i]]
        when '('
          i += 1
          raise "expecting operator" if expecting == :operator
          token = Tokens[text[start...i]]
          operators << token
          expecting = :expression
          next
        when ')'
          i += 1
          last_op = operators.last
          while last_op && last_op[0] != :open 
            last_op[2].call(operands)
            operators.pop
            last_op = operators.last
          end
          if last_op
            operators.pop
          else
            raise "unexpected closing bracket"
          end
          expecting = :operator
          next
        when '!'
          i += 1
          if text == '='
            i += 1
            raise "unexpected operator" if expecting != :expression
            token = Tokens[text[start...i]]
          else
            raise "what what"
          end
        when '>'
          i += 1
          i += 1 if text[i..i] == '='
          token = Tokens[text[start...i]]
        when '<'
          i += 1
          d = text[i..i]
          case d
            when '>'
              i += 1
              raise "unexpected operator" if expecting != :expression
            when '='
              i += 1
          end
          token = Tokens[text[start...i]]
      end
      #puts "TOkEN: #{token}"
      unless token
        raise "ERROR: i=#{i}"
      end
      if expecting == :expression
        raise "unexpected operator at i=#{i}"
      end
      last_op = operators.last
      while last_op && token[1] <= last_op[1]
        last_op[2].call(operands)
        operators.pop
        last_op = operators.last
      end
      operators << token
      expecting = :expression
    end

    while !operators.empty?
      operator = operators.pop
      raise "unexpected open" if operator[0] == :open
      operator[2].call(operands)
    end
    operands[0]
  end
end
