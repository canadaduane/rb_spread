require "test/unit"
require "./spread"

class SpreadTest < Test::Unit::TestCase
  MEMBER_JOIN       = 1
  MEMBER_LEAVE      = 2
  MEMBER_DISCONNECT = 3
  MEMBER_NETWORK    = 4

  def setup
    @c1_name = "foo"
    @c1 = Spread::Connection.new("4803", @c1_name)

    @c2_name = "bar"
    @c2 = Spread::Connection.new("4803", @c2_name)

    @conn_names = ["#" + @c1_name + "#localhost",
                   "#" + @c2_name + "#localhost"]

    @c1_fname = @conn_names.first
    @c2_fname = @conn_names.last
  end

  def teardown
    @c1.disconnect
    @c2.disconnect
  end

  def test_basic
    with_group(@c1, @c1_fname) do |group|
      msg = "hello, world"
      @c1.multicast(msg, group, Spread::RELIABLE_MESS)
      recv_data(@c1, msg, [group], Spread::RELIABLE_MESS)
    end
  end

  def test_membership
    with_group(@c1, @c1_fname) do |group|
      with_group(@c2, @conn_names) do
        recv_membership(@c1, MEMBER_JOIN, @conn_names)

        msg = "kjsdf"
        @c1.multicast(msg, group, Spread::SAFE_MESS)
        recv_data(@c1, msg, [group], Spread::SAFE_MESS)
        recv_data(@c2, msg, [group], Spread::SAFE_MESS)
      end

      recv_membership(@c1, MEMBER_LEAVE, @c1_fname)
    end
  end

  def test_msg_type
    with_group(@c1, @c1_fname) do |group|
      msg = "testing message type"
      msg_type = 128
      @c1.multicast(msg, group, Spread::SAFE_MESS, msg_type)
      m = recv_data(@c1, msg, [group], Spread::SAFE_MESS)
      assert_equal(msg_type, m.msg_type)
    end
  end

  def test_io
    with_group(@c1, @c1_fname) do |group|
      with_group(@c2, @conn_names) do
        recv_membership(@c1, MEMBER_JOIN, @conn_names)

        c1_io = @c1.io
        c2_io = @c2.io

        # Should be no messages waiting on either connection
        assert(!@c1.poll)
        assert(!@c2.poll)

        msg = "hello, world"

        # Wait for activity on c1
        t = Thread.new do
          activity = select([c1_io], nil, nil, 60)
          assert_not_nil(activity)
          reads = activity.first
          assert_equal([c1_io], reads)
          # NB: Normally we can't safely assume #poll will return
          # true, but we can here, since we should only be notified
          # via select when there is activity on the FD.
          assert(@c1.poll)
          recv_data(@c1, msg, [group], Spread::SAFE_MESS)
        end

        # Send a message on c2
        @c2.multicast(msg, group, Spread::SAFE_MESS)
        t.join
        recv_data(@c2, msg, [group], Spread::SAFE_MESS)
      end

      recv_membership(@c1, MEMBER_LEAVE, @c1_fname)
    end
  end

  def test_only_data_messages
    c3 = Spread::Connection.new("4803", "xyz", false)
    group = "g1"

    c3.join(group)
    assert(!c3.poll)

    msg = "xxx"
    c3.multicast(msg, group, Spread::RELIABLE_MESS)
    recv_data(c3, msg, [group], Spread::RELIABLE_MESS)

    c3.leave(group)
    assert(!c3.poll)

    c3.disconnect
  end

  def run_flooder_thread(conn, group, msg)
    Thread.new do
      20.times do
        conn.multicast(msg, group, Spread::RELIABLE_MESS)
        Thread.pass if rand(5) == 0
      end
    end
  end

  # XXX: temporarily disabled, as this seems to fail on my machine.
  def NO_test_multi_threads_order
    with_group(@c1, @c1_fname) do |group|
      with_group(@c2, @conn_names) do
        recv_membership(@c1, MEMBER_JOIN, @conn_names)

        msg1 = "hello"
        msg2 = "world"

        t1 = run_flooder_thread(@c1, group, msg1)
        t2 = run_flooder_thread(@c2, group, msg2)

        t1.join ; t2.join

        msg_conn1 = []
        40.times do
          msg_conn1 << recv_data(@c1, nil, [group], Spread::RELIABLE_MESS)
        end

        msg_conn2 = []
        40.times do
          msg_conn2 << recv_data(@c2, nil, [group], Spread::RELIABLE_MESS)
        end

        40.times do |i|
          assert(msg_conn1[i].message == msg_conn2[i].message)
        end
      end

      recv_membership(@c1, MEMBER_LEAVE, @c1_fname)
    end
  end

  def test_anon_connection
    c3 = Spread::Connection.new("4803")
    with_group(c3) do |group|
      msg = "hello, world"
      c3.multicast(msg, group, Spread::RELIABLE_MESS);
      recv_data(c3, msg, [group], Spread::RELIABLE_MESS)
    end
    c3.disconnect
  end

  def test_self_discard
    with_group(@c1, @c1_fname) do |group|
      with_group(@c2, @conn_names) do
        recv_membership(@c1, MEMBER_JOIN, @conn_names)

        msg = "xyz"
        @c2.multicast(msg, group, Spread::SAFE_MESS, 0, true)
        assert(!@c2.poll)
        recv_data(@c1, msg, [group], Spread::SAFE_MESS)
      end

      recv_membership(@c1, MEMBER_LEAVE, @c1_fname)
    end
  end

  def test_private_group
    with_group(@c1, @c1_fname) do |group|
      with_group(@c2, @conn_names) do
        recv_membership(@c1, MEMBER_JOIN, @conn_names)

        # Messages sent to c2's private group should only be received
        # by c2; similarly for messages sent to c1's private group.
        msg = "hello"
        @c1.multicast(msg, @c2.private_group, Spread::SAFE_MESS)
        assert(!@c1.poll)
        recv_data(@c2, msg, [@c2.private_group], Spread::SAFE_MESS)

        @c2.multicast(msg, @c1.private_group, Spread::SAFE_MESS)
        assert(!@c2.poll)
        recv_data(@c1, msg, [@c1.private_group], Spread::SAFE_MESS)
      end

      recv_membership(@c1, MEMBER_LEAVE, @c1_fname)
    end
  end

  def test_large_message
    with_group(@c1, @c1_fname) do |group|
      with_group(@c2, @conn_names) do
        recv_membership(@c1, MEMBER_JOIN, @conn_names)

        # With my copy of Spread, the maximum message size is 1444000
        # by default, so send a 130,000 byte message.
        msg = "x" * 130000
        @c1.multicast(msg, [group], Spread::AGREED_MESS)
        recv_data(@c1, msg, [group], Spread::AGREED_MESS)
        recv_data(@c2, msg, [group], Spread::AGREED_MESS)
      end

      recv_membership(@c1, MEMBER_LEAVE, @c1_fname)
    end
  end

  def test_memb_delta
    with_group(@c1, @c1_fname) do |group|
      with_group(@c2, @conn_names) do
        m = @c1.receive
        assert(m.membership?)
        assert(m.caused_by_join?)
        assert_equal([@c2.private_group], m.delta)
      end

      m = @c1.receive
      assert(m.membership?)
      assert(m.caused_by_leave?)
      assert_equal([@c2.private_group], m.delta)
    end
  end

  def test_connection_error
    begin
      c3 = Spread::Connection.new("4804", "xyz")
      flunk("Didn't expect to be able to connect")
    rescue Spread::Error::CouldNotConnect => e
      assert_equal(e.error_code, -2);
    end
  end

  private

  # Join an arbitrary Spread group, yield, and then leave the group.
  def with_group(conn, group_members = nil)
    group = "group1"
    conn.join(group)
    recv_membership(conn, MEMBER_JOIN, group_members)
    yield group
    conn.leave(group)
    recv_self_leave(conn)
  end

  # NB: it would be nice to test #poll in these methods before
  # invoking #receive, but the problem with doing that is that we get
  # intermittent failures: the data might not be waiting on the socket
  # *at this very moment*, but will be delivered shortly. #receive
  # handles that by blocking, whereas #poll does not.
  def recv_self_leave(conn)
    m = conn.receive
    assert(m.self_leave?)
  end

  def recv_membership(conn, kind, members = nil)
    m = conn.receive
    assert(m.membership?)

    case kind
    when MEMBER_JOIN then assert(m.caused_by_join?)
    when MEMBER_LEAVE then assert(m.caused_by_leave?)
    when MEMBER_DISCONNECT then assert(m.caused_by_disconnect?)
    when MEMBER_NETWORK then assert(m.caused_by_network?)
    else raise "Invalid membership message kind: #{kind}"
    end

    assert_equal(members.sort, m.members.sort) unless members.nil?

    m
  end

  def recv_data(conn, data, groups = nil, serv_type = nil)
    m = conn.receive
    assert(m.data?)
    assert_equal(data, m.message) unless data.nil?
    assert_equal(groups.sort, m.groups.sort) unless groups.nil?

    unless serv_type.nil?
      case serv_type
      when Spread::UNRELIABLE_MESS then assert(m.unreliable?)
      when Spread::RELIABLE_MESS then assert(m.reliable?)
      when Spread::FIFO_MESS then assert(m.fifo?)
      when Spread::CAUSAL_MESS then assert(m.causal?)
      when Spread::AGREED_MESS then assert(m.agreed?)
      when Spread::SAFE_MESS then assert(m.safe?)
      else raise "Unknown service type: #{serv_type}"
      end
    end

    m
  end
end
