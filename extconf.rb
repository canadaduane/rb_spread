require "mkmf"

dir_config("spread")

unless have_header("sp.h")
  puts "Unable to find the Spread client headers. Aborting."
  exit(1)
end

unless have_library("spread", "SP_connect")
  puts "Unable to link against the Spread client libraries. Aborting."
  exit(1)
end

create_makefile("spread")

File.open("Makefile", "a") do |f|
  f << "rdoc: $(SRCS)\n\trdoc -m Spread -x _darcs -x .rb\n\n"
  f << "check: $(SRCS)\n\truby tests.rb\n"
end
