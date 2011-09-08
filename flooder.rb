require "spread"

GROUP = "flooder"
MESSAGE = "test" * 1000

# Establish new spread connection to a local spread daemon
# listening on port 4803.  We identify ourselves as "bob"
s = Spread::Connection.new("4803", "bob");

s.join(GROUP)
m = s.receive # read the membership message

0.upto(10000) do |i|
  if i.modulo(1000).zero?
    puts "#{i} messages sent!\n"
  end

  # send a message to the group "flooder" with message "test"
  s.multicast(MESSAGE, GROUP, Spread::RELIABLE_MESS)

  # receive the message back again
  m = s.receive
end

s.disconnect

