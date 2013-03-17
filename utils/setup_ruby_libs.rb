#!/usr/bin/env jruby

# This script reads the pom.xml and determines the dependencies required by neo4j-spatial
# so that they can be included in ruby code. Work on this stopped once we started using:
#   mvn dependency:copy-dependencies
#   java -cp target/classes:target/dependency/* org.neo4j.gis.spatial.osm.OSMImporter osm-db two-street.osm 

require 'rexml/document'

class DepLoader
  attr_reader :properties, :depsets, :verbose

  def initialize(file,verbose=false)
    @file = file
    @verbose = verbose
    @properties = {}
    @depsets = {}
    @depth = 0
    @path = []
  end

  def dep_fields(dep)
    fields={}
    dep.elements.each do |ele|
      fields[ele.name] = expand_props(ele.text)
    end
    puts "Dep: #{fields.inspect}" if(verbose)
    fields
  end

  def expand_props(text)
    text && text.gsub(/\$\{([\w\.]+)\}/){|m| "#{properties[$1]}"}
  end

  def find_version(dep_key)
    depsets[dep_key] || dep_key =~ /jta/ && "1.1.1"
  end

  def extract_dep(dep,depth)
    deps = []
    fields=dep_fields(dep)
    if fields['scope'] =~ /test/
      puts "Ignoring test artifacts: #{fields['artifactId']}" if(verbose)
    else
      if fields['artifactId'] == 'library'
        puts "project.version => #{fields['version']}" if(verbose)
        properties['project.version'] = expand_props(fields['version'])
        fields['artifactId'] = fields['groupId'].split(/\./)[-1]
      end
      dep_key = "#{fields['groupId']}/#{fields['artifactId']}"
      fields['version'] ||= find_version(dep_key)
      depname = "#{fields['groupId'].gsub(/\./,'/')}/#{fields['artifactId']}/#{fields['version']}/#{fields['artifactId']}-#{fields['version']}"
      depname = expand_props(depname)
      jar = "#{ENV['HOME']}/.m2/repository/#{depname}.jar"
      pom = "#{ENV['HOME']}/.m2/repository/#{depname}.pom"
      puts "Testing jar: #{jar}" if(verbose)
      if File.exist?(jar) && (fields['type'].nil? || fields['type'] == 'jar')
        deps << jar
        puts "Added JAR to dependencies: #{jar}" if(verbose)
      end
      puts "Testing pom: #{pom}" if(verbose)
      if File.exist?(pom) && (fields['type'].nil? || fields['type'] == 'pom')
        deps << extract_dependencies(pom,depth+1)
      end
    end
    deps.flatten.uniq
  end

  def extract_dependencies(file,depth)
    return if(file =~ /neo4j/)
    deps = []
    @depth = depth
    @path[@depth] = file
    if(verbose)
      puts "Reading #{file} for dependencies"
      puts "path: #{@path[0..@depth].join(' => ')}"
    end
    File.open(file) do |file|
      doc = REXML::Document.new file.read
      doc.elements.each('*/parent') do |parent|
        deps << extract_dep(parent,depth)
      end
      doc.elements.each('*/dependencyManagement/dependencies/dependency') do |dep|
        fields=dep_fields(dep)
        dep_key = "#{fields['groupId']}/#{fields['artifactId']}"
        depsets[dep_key] = fields['version']
        puts "Dep version: #{dep_key} => #{depsets[dep_key]}"
      end
      doc.elements.each('*/properties/*') do |prop|
        puts "#{prop.expanded_name} => #{prop.text}" if(verbose)
        properties[prop.expanded_name] = expand_props(prop.text)
      end
      doc.elements.each('*/dependencies/dependency') do |dep|
        deps << extract_dep(dep,depth)
      end
    end
    deps.flatten.uniq
  end

  def puts(*args)
    args[0] = (["  "]*@depth).to_s+args[0]
    STDOUT.puts *args
  end

  def dependencies
    extract_dependencies(@file,0)
  end
end

dep_loader = DepLoader.new('pom.xml',true)
pom_dependencies = dep_loader.dependencies

puts "CLASSPATH=\"./target/classes:#{pom_dependencies.join(':')}\""

$test = true

if $test
  pom_dependencies.each do |dep|
    puts "Importing dependency: #{dep}"
    require dep if(dep)
  end

  tt = Java::OrgNeo4jGraphdb::RelationshipType
  puts tt.inspect
  aa = Java::OrgNeo4jGisSpatialOsm::OSMImporter
  puts aa.inspect
end
