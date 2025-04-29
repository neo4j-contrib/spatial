#!/usr/bin/env ruby

$data = """
.N
#       #
##      #
# #     #
#  #    #
#   #   #
#    #  #
#     # #
#      ##
#       #
.E
#########
#
#
#
#######
#
#
#
#########
.O
  #####
 #     #
#       #
#       #
#       #
#       #
#       #
 #     #
  #####
.4
      #
     ##
    # #
   #  #
  #   #
 #    #
#########
      #
      #
.J
    ###
      #
      #
      #
      #
      #
      #
 #   #
  ###
.-




  #####  




.S
  #####
 #     #
#       
 ##
   ###
      ##
        #
 #     #
  #####
.P
########
#       #
#       #
#       #
########
#
#
#
#
.A
    #
   # #
  #   #
 #     #
 #######
 #     #
#       #
#       #
#       #
.T
#########
    #
    #
    #
    #
    #
    #
    #
    #
.I
  #####
    #
    #
    #
    #
    #
    #
    #
  #####
.L
#
#
#
#
#
#
#
#
#########
"""

def make_letters(data)
  letter = nil
  letters = {}
  ascii = nil
  data.split(/\n/).each do |line|
    if line =~ /^\.(.)/
      letter = $1
      ascii = []
      letters[letter] = ascii
    elsif ascii && ascii.length < 9
      ascii << line.concat(' '*9)[0...9]
    end
  end
#  letters.each do |k,v|
#    puts "Letter #{k}:"
#    v.each{|l| puts "\t#{l}"}
#  end
  letters
end

def make_text(word)
  word.upcase!
  $letter_map ||= make_letters($data)
  file=word+'.txt'
  File.open(file,'w') do |out|
    (0...9).each do |row|
      #puts "Writing row #{row}:"
      word.split(//).each do |letter|
        #puts "\tLetter: #{letter}"
        ascii = $letter_map[letter]
        text = ascii && ascii[row] || ' '*9
        out.print " #{text}"
        #puts "\t\t[#{text}]"
      end
      out.puts
    end
  end
end

['NEO4J-SPATIAL'].each do |word|
  make_text(word)
end
