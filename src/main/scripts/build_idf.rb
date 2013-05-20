require 'rubygems'
require 'magic_xml/magic_xml'
begin
  require 'bz2'
rescue LoadError
  
end
require 'find'
require 'wiki_database'
require 'namespace_extractor'
require 'zlib'
require 'optparse'
require 'logger'
require 'benchmark'

db_name = nil
data_dir = nil
data_list = nil
language_file = nil
$logger ||= Logger.new(STDOUT)
ARGV.options do |o|
  script_name = File.basename($0)
  o.banner = "Usage: #{script_name} [OPTIONS]"
  o.on("-d", "--data [DIR]", String, "Directory containing dump files.", "Default: #{data_dir}") { |d| data_dir = d }
  o.on("-d", "--datalist [x,y,z]", Array, "Explicit list of dump files.") { |d| data_list = d }
  o.on("-b", "--database [FILE]", String, "Database file to use.") { |b| db_name = b }
  o.on("-l", "--languages [FILE]", String, "Language file to use.") { |l| language_file = l }
  o.on_tail("-h", "--help", "Show this help message.") { puts o; exit }
  o.parse!
  unless db_name
    puts o; exit
  end
  unless language_file
    puts "Language file required."
    exit
  end
end

if data_dir
  data_list ||= []
  Find.find(data_dir) do |path|
    data_list << path if path =~ /(^|\/)([a-z]+)wiki-(\d+)-pages-articles.xml/
  end
end

def open_archive(path) 
  if path =~ /\.bz2\Z/
    reader = BZ2::Reader
  elsif path =~ /\.gz\Z/
    reader = Zlib::GzipReader
  elsif path =~ /\.xml\Z/
    reader = File
  elsif
    raise "Archive type not supported"
  end
  reader.open(path) do |b|
    yield b
  end
end

db = WikiDatabase::open_database(db_name)
files = Hash.new
namespace_resolvers = Hash.new
data_list.each do |path|
  if path =~ /(^|\/)([a-z]+)wiki-(\d+)-pages-articles\.xml(\.(gz|bz2))?$/
    lang = $2
    files[lang] = path
    open_archive(path) do |b|
      $logger.info("Getting namespaces for #{path}...")
      ne = NamespaceExtractor.new(lang, db)
      #puts "ne: #{ne.inspect}"
      ne.create_tables
      namespace_resolvers[lang] = ne.extract(b)
    end
  end
end

page_id = 0
revision_id = 0
$logger.info("Creating tables")
db.create_table("languages", [[:language, "varchar(32)"]])
File.open(language_file) do |f|
  insert_language = db.prepare("INSERT INTO languages(language) VALUES (?);")
  while f.gets
    lang = $_.chomp
    insert_language.execute(lang) if lang != ""
  end
end

db.create_table("revisions", [[:id, "integer primary key"], ["page_id" , "integer"], ["text", "text"]])
db.create_table("pages", [[:id, "integer primary key"], ["language", "varchar(32)"],["namespace_id", "integer"], ["title", "varchar(255)"]])
insert_page = db.prepare("INSERT INTO pages(id, language, namespace_id, title) VALUES (?, ?, ?, ?);")
insert_revision = db.prepare("INSERT INTO revisions(id, page_id, text) VALUES (?, ?, ?);")
$logger.info(Benchmark.measure do
  namespace_resolvers.keys.sort.each do |lang|
    $logger.info "Processing #{lang}"
    start_time = Time.now
    count = 0
    nr = namespace_resolvers[lang]
    db.transaction do
      open_archive(files[lang]) do |b|
        XML.parse_as_twigs(b) do |node|
          case node.name
          when :page
            $logger.info([lang, count, count / (Time.now - start_time)].join("\t")) if (count += 1) % 10000 == 0
            node.complete!
            ns_id, article = nr.extract_namespace(node[:@title])
            revision = node.child(:revision)
            text = revision[:@text]
            page_id = node[:@id]
            revision_id = revision[:@id]
            insert_page.execute(page_id, lang, ns_id, article)
            insert_revision.execute(revision_id, page_id, text)
          end
        end
      end
    end
  end
  #insert_page.close
  #insert_revision.close
  db.create_index("languages", ["language"])
  db.create_index("revisions", ["page_id"])
  db.create_index("pages", ["language", "namespace_id", "title"])
  db.disconnect
end)
$logger.info("Done.")
