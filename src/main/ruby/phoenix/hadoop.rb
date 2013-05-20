require 'java'

module Hadoop
  def self.configuration
    @configuration
  end
  @hadoop_dir||='/opt/hadoop'
  Dir.glob(File.join(@hadoop_dir, '{.,build,lib}/*.jar')).each do |jar|
    require jar
  end
  @configuration = org.apache.hadoop.conf.Configuration.new

  Dir.glob(File.join(@hadoop_dir, 'conf/*-site.xml')).each do |xml|
    @configuration.addResource(org.apache.hadoop.fs.Path.new(xml.to_java_string))
  end
  $CLASSPATH << File.join(File.dirname(__FILE__), "../../../../target/disambiguator-1.0-SNAPSHOT-jar-with-dependencies.jar")
  #$CLASSPATH << File.join(File.dirname(__FILE__), "../phoenix.jar")
  #$CLASSPATH << File.join(File.dirname(__FILE__), "../trove-3.0.0rc2.jar")
  #$CLASSPATH << File.join(File.dirname(__FILE__), "../lucene-core-3.0.3.jar")
  #$CLASSPATH << File.join(File.dirname(__FILE__), "../jgrapht-jdk1.6.jar")
end
