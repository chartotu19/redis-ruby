#!/usr/bin/env ruby


=begin
  
Primitive Redis client implementation using ruby.
Supported commands: get,set,getbit,setbit,zadd,zcard,zcount,zrange.

@author: selvam1991@gmail.com
  
=end

require 'socket'
require 'readline'

STDOUT.puts 'Welcome to exoredis-cli v1.0'
STDOUT.puts '----------------------------'
STDOUT.puts "initiating connection...."
  if ARGV[0] != nil
    host = ARGV[0]
  else
    host = "localhost"
  end

begin
  s = TCPSocket.new host, 15000
rescue Exception => e
  puts host
  puts e.message
else
end 

### Sends msg to server
def notify(cmd, socket)
  #cmd should be string
  socket.send(cmd + " \r\n", 0)
  line = socket.gets("\r\n")
  STDOUT.puts line
end

while io = Readline.readline('connected> ', true)
  input = io.split " "
  
  case input[0]
  # Get data from the server
  when "GET","get"
    notify io,s

  when "SET","set"
    notify io,s

  when "SETBIT","setbit"
    #check if args are fine.
    notify io,s

  when "GETBIT","getbit"
    #check if args are fine.
    notify io,s

  when "ZADD","zadd"
    #check if args are fine.
    if input.length() != 4
      STDOUT.puts "ERR wrong number of arguments for 'zadd' command \r\n"
    elsif (input[2].is_a?(Float) && input[2].is_a?(Float))
      STDOUT.puts "ERR value is not a valid number. \r\n"
    else
      notify io,s
    end

  when "ZCARD","zcard"
    notify io,s

  when "ZCOUNT","zcount"
    notify io,s


  when "SAVE","save"
   STDOUT.puts "called SAVE"
  #  x = "a".each_byte { |x|
  #  puts x
  # }

  when "EXIT","exit"
    STDOUT.puts "exiting..."
    s.send("EXIT \r\n", 0)
    s.close()
    exit

  else
   STDOUT.puts <<-EOF
  Please provide command name

  Available commands:
    SAVE
    GET
    SET
    ZADD
    ZCARD
    ZCOUNT
    EXIT
  EOF
  end
#STDOUT.print 'connected>'
end



