#!/usr/bin/env ruby

# This script finds all ruby files in the src/main/resources/sld/ directory and runs them
# They are expected to contain Amanzi::SLD syntax DLS script which will produce XML syntax SLD
# to be used as the default styles for OSM dynamic layers.
# In order for Neo4j-Spatial to not have to depend on Ruby and on Amanzi:SLD, both the ruby
# and the produced SLD (XML) documents are saved to git.

$:.push '../ruby-style/lib'
$:.push '../amanzi-sld/lib'

def process_file file
  if file =~ /^([\w\-\\\/]+)\.rb/
    sld = "#{$1}.sld"
    puts "\t#{file}\t=>\t#{sld}"
    File.open(sld,'w') do |out|
      ans = eval File.open(file).read
      out.puts ans.to_xml(:tab => '  ')
    end
  end
end

def process_directory(dir)
  Dir.new(dir).each do |file|
    next if(file =~ /^\./)
    path = "#{dir}/#{file}"
    #puts "Processing #{path}"
    if File.directory? path
      process_directory path
    else
      process_file path
    end
  end
end

process_directory 'src/main/resources/sld'
