require 'rubygems'
require 'zip/zipfilesystem'
require 'fileutils'

class UnZipThemAll
        attr_accessor :zip_file_path, :to_folder_path

        def initialize( zip_file_path, to_folder_path )
                @zip_file_path = zip_file_path
                @to_folder_path = to_folder_path
        end

        def unzip
          puts "unzipping #{self.zip_file_path} to #{self.to_folder_path}"
          if File.exists?( self.zip_file_path ) == false
           puts "Zip file #{self.zip_file_path} does not exist!"
           return
          end

                if File.exists?( self.to_folder_path ) == false
                        FileUtils.mkdir( self.to_folder_path )
                end

                zip_file = Zip::ZipFile.open( self.zip_file_path )
                Zip::ZipFile.foreach( self.zip_file_path ) do | entry |
                        file_path = File.join( self.to_folder_path, entry.to_s )
                        if File.exists?( file_path )
                                FileUtils.rm( file_path )
                        end

                        zip_file.extract( entry, file_path )
                end

        end
end
