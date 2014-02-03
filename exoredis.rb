#!/usr/bin/env ruby

=begin

Simple Redis server implemented using ruby.
@author: selvam1991@gmail.com
  
=end

require 'socket'
require 'json'
require 'thread'


=begin 
creating seperate hash tables for each redis datastructure is a good option. No need to check for datatype on every call. 
 
#sstore = Hash.new {}
#zstore = Hash.new {}

The tradeoff is same key in different datastructures, for example, name string can be in sstore and name list may be in zsort. 
  
=end

store = Hash.new {}

### Try loading the file
STDOUT.puts 'Trying to populate db with temp/cache.json .....'

if ARGV[1]
  value = File.read ARGV[0]
  #update
  Store = JSON.parse value

elsif File.exist?('temp/cache.json')
  value = File.read 'temp/cache.json'
  #update
  Store = JSON.parse value

else
  STDOUT.puts 'No files to load into database.'
end

# Creating the lock 
# To make atomic operations.
semaphore = Mutex.new

#Creating the server instance

serv = TCPServer.new 15000
puts 'TCP server running @ port 15000'

loop do
  Thread.new(serv.accept) do |connection| # Note : serv.accept is a blocking call.
    STDOUT.puts "Accepting connection from : #{connection.peeraddr[2]}"
      while true
        incomingData = connection.gets "\r\n"
        command = incomingData.split " "
        
        error = false
        
        STDOUT.puts "Incoming: #{incomingData}"
        
        if incomingData != nil
          incomingData = incomingData.chomp
        end
        
        case command[0]

        ### String commands.        
        when "GET","get"
          if (res = store[command[1]]) != nil
            if res[:type] != "string"
              connection.puts "ERR" + command[1] + "is a different datatype. \r\n"
            else
              connection.puts res[:value].to_s + "\r\n"
            end
          else
            connection.puts "(nil) \r\n"
          end
        
        when "SET","set"
          #TODO: atomicity should be taken care of , apply locks.
          if (res = store[command[1]]) != nil
            if res[:type] != "string"
              connection.puts "ERR" + command[1] + "is a different datatype. \r\n"
              error = true
            end
          end
          if !error
            val = command[2].to_s
            semaphore.synchronize{
              store[command[1]] = {:type => "string", :value => {} }
              store[command[1]][:value] = val
              connection.puts "OK \r\n"
            }
          end 

        # GETBIT key offset
        when "GETBIT","getbit"
          #convert the value binary
          if (res = sstore[command[1]]) != nil
            STDOUT.puts res.length*8
            STDOUT.puts (command[2].to_f/8)
            STDOUT.puts command[2].to_f.is_a?(String)
            d = res.length*8
            if command[2].to_i < res.length*8
              STDOUT.puts 'check!'
              #bit value is valid.
              byte = (command[2].to_f/8).floor
              index = command[2]%8
              STDOUT.puts 'byte:' + byte.to_s
              STDOUT.puts 'index:' + index.to_s
              i = 0
              res.each_byte { |b| 
                STDOUT.puts b
                if byte == i
                  #r = b[index]
                  puts '-'
                end
                i = i + 1
              }
              connection.puts r + "\r\n"
            else
              connection.puts 'integer 0'
            end
          else
            connection.puts "0 \r\n"
          end

        when "SETBIT","setbit"
          STDOUT.puts "Received: setbit"

        
        ### Sorted Set commands.
        when "zadd","ZADD"
          # zadd set_name value key
          if store[command[1]] == nil
            store[command[1]] = {:type => "set",:value => {}}
          elsif store[command[1]][:type] != "set"
            connection.puts "ERR different datatype \r\n"
            error = true
          end        
          if !error  
            store[command[1]][:value][command[3]] = command[2].to_f
            connection.puts (store[command[1]][:value].length - 1).to_s + "\r\n"
          end
          
        when "zcard","ZCARD"
          if (res = store[command[1]]) != nil
            if res[:type] == "set" 
              STDOUT.puts res
              connection.puts res[:value].length.to_s + "\r\n"
            elsif 
              connection.puts "ERR different datatype \r\n"
            end
          else
            connection.puts "(nil) \r\n"
          end

        when "ZCOUNT","zcount"
          if (res = store[command[1]]) != nil
            if res[:type] == "set"
              STDOUT.puts res[:value]
              inc = 0
              res[:value].each {|k,v|
                if ( v >= command[2].to_f && v <= command[3].to_f)
                  inc = inc + 1
                end 
              }
              connection.puts inc.to_s + "\r\n"
            else 
              connection.puts "ERR different datatype \r\n"
            end
          else
            connection.puts "(nil) \r\n"
          end

        when "EXIT","exit"
          STDOUT.puts "Received: EXIT, closed connection"
          connection.close
          break
        else 
          connection.puts "#{incomingData} \r\n"
          connection.flush
        end
      end
  end
end
