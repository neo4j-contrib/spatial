module NetworkHelpers

  require 'socket'
  require 'timeout'

  def is_port_open?(ip, port)
    begin
      Timeout::timeout(1) do
        begin
          s = TCPSocket.new(ip, port)
          s.close
          return true
        rescue Errno::ECONNREFUSED, Errno::EHOSTUNREACH
          return false
        end
      end
    rescue Timeout::Error
    end

    false
  end

  def copy_file(source, target)
    puts "copy '#{source}' -> '#{target}'"
    File.open(source, "r") do |src|
      open(target, "wb") do |file|
        while buf = src.read(2048)
          file.write(buf)
        end
      end
    end
#    File.cp(source, target) 
  end

  def transfer_if_newer(location, target)
    puts "transfer_if_newer(#{location},#{target})"

    if (location.scheme == "http") then
      server = Net::HTTP.new(location.host, 80)
      head = server.head(location.path)
      server_time = Time.httpdate(head['last-modified'])
      if (!File.exists?(target) || server_time != File.mtime(target))
        puts target+" missing or newer version as on #{location} - downloading"
        server.request_get(location.path) do |res|
          open(target, "wb") do |file|
            res.read_body do |segment|
              file.write(segment)
            end
          end
        end
        File.utime(0, server_time, target)
      else
        puts target+" not modified - download skipped"
      end
    else
      puts "transfer_if_newer: normalized file: #{location.to_s} #{getenv("PLUGIN_NAME")}"
      File.open(File.join(location.to_s, getenv("PLUGIN_NAME")), "r") do |src|
        open(target, "wb") do |file|
          while buf = src.read(2048)
            file.write(buf)
          end
        end
      end
    end
  end

  

  def fix_file_sep(file)
    file.tr('/', '\\')
  end

  def unzip(full_archive_name, target)
      unzip_them_all = UnZipThemAll.new(full_archive_name, target) 
      unzip_them_all.unzip
  end

  def exec_wait(cmd)
    puts "exec: '#{cmd}'"
    puts `#{cmd}`
    raise "execution failed(#{$?})" unless $?.to_i == 0
  end

  def untar(archive, target)
    if (current_platform.unix?)
      pushd target
      exec_wait("tar xzf #{archive} --strip-components 1")
      popd
    else
      raise 'platform not supported'
    end
  end
end

