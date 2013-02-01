#!/usr/bin/env ruby

require 'redcarpet'

md = Redcarpet::Markdown.new(Redcarpet::Render::HTML,
  :autolink => true,
  :fenced_code_blocks => true,
  :no_intra_emphasis => true,
  :tables => true
)

puts "Converting 'README.md' --> 'README.html'"

File.open('README.html','w') do |out|
  out.puts md.render(File.read('README.md'))
end

