#!/usr/local/bin/ruby
####################################################################
# user.rb
# This is a partial implementation of the demo program user distributed
# with Spread, rewritten in ruby.
# Copyright(2001) George Schlossnagle
####################################################################
require "./spread"
print "What name would you like to go by? "
name = gets.chomp
print "What group would you like to join? "
group = gets.chomp
conn = Spread::Connection.new("4803", name);
conn.join(group)
fork do
  loop do
    recv_mess = conn.receive
    puts "#{recv_mess.sender} says #{recv_mess.message.chomp}"
  end
end

loop do
  print "User> "
  send_mess = gets
  break if send_mess.nil?
  if not send_mess.empty?
    conn.multicast(send_mess, group, Spread::RELIABLE_MESS)
  end
end
