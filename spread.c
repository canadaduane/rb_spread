/*
 * Document-class: Spread
 *
 * This module provides Ruby bindings for the client API of the {Spread
 * Group Communication Toolkit}[http://www.spread.org].
 *
 *  Copyright (2001) George Schlossnagle, All Rights Reserved
 *  Copyright (2005) Neil Conway
 *
 *  This software is released under the Perl Artistic License and is
 *  free to use for any purpose.
 *
 *  This product uses software developed by Spread Concepts LLC for
 *  use in the Spread toolkit. For more information about Spread see
 *  http://www.spread.org
 */
#include "ruby.h"
#include "sp.h"
#include <string.h>
#include <stdbool.h>

#define MAX_GROUPS 100

struct SpreadConnection {
    char spread_name[MAX_PROC_NAME];
    int priority;
    mailbox mbox;
    char private_group[MAX_GROUP_NAME];
    bool connected;
    VALUE rb_fd;        /* Lazily-constructed IO object wrapping `mbox' */
};

struct SpreadMessage {
    service service_type;
    char sender[MAX_GROUP_NAME];
    int num_groups;
    char groups[MAX_GROUPS][MAX_GROUP_NAME];
    int16 msg_type;
    int length;
    int endian;
    char *message;
    VALUE rb_groups;    /* Lazily-constructed Array object wrapping `groups' */
    VALUE rb_delta;     /* Lazily-constructed Array object wrapping #delta */
};

struct SpreadError {
	char err_msg[128];
	int err_code;
};

static VALUE rb_mSpread;

static VALUE rb_cSpreadConnection;

static VALUE rb_cSpreadMessage;
static VALUE rb_cSpreadDataMessage;
static VALUE rb_cSpreadMemberMessage;
static VALUE rb_cSpreadTransitionMessage;
static VALUE rb_cSpreadSelfLeaveMessage;

static VALUE rb_eSpread;
static VALUE rb_eSpreadIllegalSession;
static VALUE rb_eSpreadIllegalMessage;
static VALUE rb_eSpreadIllegalService;
static VALUE rb_eSpreadIllegalGroup;
static VALUE rb_eSpreadIllegalSpread;
static VALUE rb_eSpreadCouldNotConnect;
static VALUE rb_eSpreadConnClosed;
static VALUE rb_eSpreadGroupsTooShort;
static VALUE rb_eSpreadBufferTooShort;
static VALUE rb_eSpreadMessageTooLong;
static VALUE rb_eSpreadRejectVersion;
static VALUE rb_eSpreadRejectNoName;
static VALUE rb_eSpreadRejectIllegalName;
static VALUE rb_eSpreadRejectNotUnique;
static VALUE rb_eSpreadRejectQuota;
static VALUE rb_eSpreadRejectAuth;

/*
 * Convert a C `int' or similar to a Ruby boolean value, and the
 * reverse. Any non-zero input is considered true.
 */
#define INT2BOOL(i) ((i) ? Qtrue : Qfalse)
#define BOOL2INT(b) ((b) == Qtrue ? 1 : 0)

static VALUE spconn_disconnect(VALUE obj);

/*
 * Needless to say, this error handling / exception code is a bit
 * laborious. I don't see an obvious way to do this with less code,
 * however, and it is important that users be able to distinguish one
 * error from another.
 */
static void
raise_sp_error(int err_code)
{
    VALUE	exc_class;
	char	err_msg[128];

    switch (err_code)
    {
    case ILLEGAL_SESSION:
        exc_class = rb_eSpreadIllegalSession;
        strcpy(err_msg, "invalid connection provided");
        break;
    case ILLEGAL_MESSAGE:
        exc_class = rb_eSpreadIllegalMessage;
        strcpy(err_msg, "invalid message");
        break;
    case ILLEGAL_SERVICE:
        exc_class = rb_eSpreadIllegalService;
        strcpy(err_msg, "invalid service specified");
        break;
    case ILLEGAL_GROUP:
        exc_class = rb_eSpreadIllegalGroup;
        strcpy(err_msg, "invalid group name");
        break;
    case ILLEGAL_SPREAD:
        exc_class = rb_eSpreadIllegalSpread;
        strcpy(err_msg, "invalid spread host name");
        break;
    case COULD_NOT_CONNECT:
        exc_class = rb_eSpreadCouldNotConnect;
        strcpy(err_msg, "could not connect to spread daemon");
        break;
    case CONNECTION_CLOSED:
        exc_class = rb_eSpreadConnClosed;
        strcpy(err_msg, "connection is closed");
        break;
    case GROUPS_TOO_SHORT:
        exc_class = rb_eSpreadGroupsTooShort;
        strcpy(err_msg, "groups array too short to hold the entire list of groups this message was sent to");
        break;
    case BUFFER_TOO_SHORT:
        exc_class = rb_eSpreadBufferTooShort;
        strcpy(err_msg, "buffer too small to hold message");
        break;
    case MESSAGE_TOO_LONG:
        exc_class = rb_eSpreadMessageTooLong;
        strcpy(err_msg, "message too large");
        break;
    case REJECT_VERSION:
        exc_class = rb_eSpreadRejectVersion;
        strcpy(err_msg, "client library version is different to server");
        break;
    case REJECT_NO_NAME:
        exc_class = rb_eSpreadRejectNoName;
        strcpy(err_msg, "no private name supplied");
        break;
    case REJECT_ILLEGAL_NAME:
        exc_class = rb_eSpreadRejectIllegalName;
        strcpy(err_msg, "name is invalid");
        break;
    case REJECT_NOT_UNIQUE:
        exc_class = rb_eSpreadRejectNotUnique;
        strcpy(err_msg, "a session with this name already exists");
        break;
    case REJECT_QUOTA:
        exc_class = rb_eSpreadRejectQuota;
        strcpy(err_msg, "maximum number of sessions reached");
        break;
    case REJECT_AUTH:
        exc_class = rb_eSpreadRejectAuth;
        strcpy(err_msg, "authorisation failed");
        break;
    default:
        /* XXX: perhaps die less dramatically? */
        rb_bug("unrecognized spread error code: %d", err_code);
    }

	rb_exc_raise(rb_funcall(exc_class, rb_intern("new"),
							2, INT2NUM(err_code), rb_str_new2(err_msg)));
}

static VALUE
sperr_alloc(VALUE klass)
{
	VALUE result;
	struct SpreadError *sp_err;

	result = Data_Make_Struct(klass, struct SpreadError,
							  NULL, NULL, sp_err);
	return result;
}

static VALUE
sperr_initialize(VALUE self, VALUE err_code, VALUE err_msg)
{
	struct SpreadError *sp_err;

	Data_Get_Struct(self, struct SpreadError, sp_err);
	sp_err->err_code = NUM2INT(err_code);
	strcpy(sp_err->err_msg, STR2CSTR(err_msg));

	rb_call_super(1, &err_msg);
	return Qnil;
}

/*
 * call-seq:
 *		error_code -> int
 *
 * The error code associated with this exception, as defined by
 * Spread.
 */
static VALUE
sperr_error_code(VALUE self)
{
	struct SpreadError *sp_err;

	Data_Get_Struct(self, struct SpreadError, sp_err);
	return INT2NUM(sp_err->err_code);
}

/*
 * Apparently, the easiest way to identify a so-called "self leave"
 * message is as a caused-by-leave message that is not a regular group
 * membership message.
 */
static bool
message_is_self_leave(service msg_type)
{
    return (Is_caused_leave_mess(msg_type) &&
            !(Is_reg_memb_mess(msg_type)));
}

static void
mark_spmess(struct SpreadMessage *sp_mess)
{
    if (!NIL_P(sp_mess->rb_groups))
        rb_gc_mark(sp_mess->rb_groups);
    if (!NIL_P(sp_mess->rb_delta))
        rb_gc_mark(sp_mess->rb_delta);
}

static void
free_spmess(void *ptr)
{
    struct SpreadMessage *sp_mess;

    sp_mess = (struct SpreadMessage *) ptr;
    free(sp_mess->message);
    free(sp_mess);
}

static void
mark_spconn(struct SpreadConnection *sp_conn)
{
    if (!NIL_P(sp_conn->rb_fd))
        rb_gc_mark(sp_conn->rb_fd);
}

static void
free_spconn(void *ptr)
{
    struct SpreadConnection *spconn;

    spconn = (struct SpreadConnection *) ptr;
    if (spconn->connected)
    {
        if (SP_disconnect(spconn->mbox) == 0)
            spconn->connected = false;
        /*
         * We ignore errors: it is probably unwise to raise an
         * exception here, and we have no other way of signaling an
         * error.
         */
    }
    free(spconn);
}

static VALUE
spconn_alloc(VALUE klass)
{
    VALUE result;
    struct SpreadConnection *sp_conn;

    result = Data_Make_Struct(klass, struct SpreadConnection,
                              mark_spconn, free_spconn, sp_conn);

    /*
     * XXX: The internal SpreadConnection has been zero'd out, but
     * it's not really `valid' -- #connect needs to be invoked. Is
     * this the right protocol for allocators to follow?
     */
    sp_conn->rb_fd = Qnil;

    return result;
}

/*
 * Document-method: new
 * call-seq:
 *      Spread::Connection.new(target, [private_name], [all_messages]) -> Spread::Connection
 *
 * Construct a new instance of Spread::Connection. Takes the same
 * arguments as #connect.
 */

/*
 * call-seq:
 *      connect(target, [private_name], [all_messages]) -> nil
 *
 * Connect to Spread. +target+ specifies which Spread daemon to
 * connect to. It must be a string in the form "port",
 * "port@hostname", or "port@ip".
 *
 * +private_name+ is the name of this connection. It must be unique
 * among all the connections to a given Spread daemon. If not
 * specified, Spread will assign a randomly-generated unique private
 * name.
 *
 * +all_messages+ indicates whether this connection should receive all
 * Spread messages, or just data messages. The default is +true+ (so
 * all messages will be received).
 *
 * If this Spread::Connection instance is presently connected, it will
 * first be silently disconnected.
 */
static VALUE
spconn_connect(int argc, VALUE *argv, VALUE obj)
{
    VALUE host_str;
    VALUE name_str;
    VALUE all_msgs;
    char *c_name_str;
    struct SpreadConnection *sp_conn;
    int n;

    Data_Get_Struct(obj, struct SpreadConnection, sp_conn);

    rb_scan_args(argc, argv, "12", &host_str, &name_str, &all_msgs);

    if (NIL_P(name_str))
        c_name_str = NULL;
    else
        c_name_str = StringValuePtr(name_str);
    if (NIL_P(all_msgs))
        all_msgs = Qtrue; /* receive all messages by default */

    /* If we're currently connected, silently disconnect first */
    if (sp_conn->connected)
        spconn_disconnect(obj);

    SafeStringValue(host_str);

    if ((n = SP_connect(RSTRING_PTR(host_str),
                        c_name_str,
                        0, /* ignored */
                        BOOL2INT(all_msgs),
                        &sp_conn->mbox,
                        sp_conn->private_group)) < 0)
        raise_sp_error(n);

    snprintf(sp_conn->spread_name, MAX_PROC_NAME, "%s",
             RSTRING_PTR(host_str));
    sp_conn->connected = true;

    return Qnil;
}

/*
 * call-seq:
 *      disconnect -> nil
 *
 * Disconnect from Spread. Invoking methods like #receive and
 * #multicast on a disconnected Spread::Connection will throw
 * exceptions. You can reconnect to Spread (perhaps supplying
 * different connection parameters) using #connect.
 */
static VALUE
spconn_disconnect(VALUE obj)
{
    int n;
    struct SpreadConnection *sp;

    Data_Get_Struct(obj, struct SpreadConnection, sp);
    if ((n = SP_disconnect(sp->mbox)) < 0)
        raise_sp_error(n);

    sp->connected = false;
    /*
     * The mailbox fd is no longer valid, so get rid of the cached IO
     * wrapper for it (if any)
     */
    sp->rb_fd = Qnil;

    return Qnil;
}

/*
 * call-seq:
 *      join(string) -> nil
 *      join(array) -> nil
 *
 * Join the specified group or Array of groups. If an error occurs
 * while attempting to join multiple groups, an exception will
 * immediately be raised, but joins that were processed prior to the
 * error will still take effect.
 */
static VALUE
spconn_join(VALUE obj, VALUE group)
{
    int i, n;
    struct SpreadConnection *sp;

    Data_Get_Struct(obj, struct SpreadConnection, sp);
    if (TYPE(group) == T_ARRAY)
    {
        for (i = 0; i < RARRAY_LEN(group); i++)
        {
            VALUE tmp = RARRAY_PTR(group)[i];
            if ((n = SP_join(sp->mbox, StringValuePtr(tmp))) < 0)
                raise_sp_error(n);
        }
    }
    else
    {
        /* Assume it's a string, raise type error if not */
        if ((n = SP_join(sp->mbox, StringValuePtr(group))) < 0)
            raise_sp_error(n);
    }

    return Qnil;
}

/*
 * call-seq:
 *      leave(string) -> nil
 *      leave(array) -> nil
 *
 * Leave the specified group or Array of groups. This only makes sense
 * to do if you have previously joined the specified groups via
 * #join. However, as of Spread 3.17.3, Spread will _not_ raise an
 * error if you attempt to leave a group you aren't a member of.
 */
static VALUE
spconn_leave(VALUE obj, VALUE group)
{
    struct SpreadConnection *sp;
    int n;

    Data_Get_Struct(obj, struct SpreadConnection, sp);

    if (TYPE(group) == T_ARRAY)
    {
        int i;
        for (i = 0; i < RARRAY_LEN(group); i++)
        {
            VALUE tmp = RARRAY_PTR(group)[i];
            if ((n = SP_leave(sp->mbox, StringValuePtr(tmp))) < 0)
                raise_sp_error(n);
        }
    }
    else
    {
        /* Must be a string or equivalent, raise type error if not */
        if ((n = SP_leave(sp->mbox, StringValuePtr(group))) < 0)
            raise_sp_error(n);
    }

    return Qnil;
}

/*
 * call-seq:
 *      multicast(message, groups, service_type, [msg_type], [self_discard]) -> nil
 *
 * Broadcast +message+ to +groups+, using the service type
 * +service_type+. +groups+ is either a String specifying a single
 * group name, or an Array of Strings specifying the list of group
 * names. +service_type+ specifies the desired ordering guarantee for
 * the message (which are available as constants in the Spread
 * module). For more information on the delivery guarantees provided
 * by Spread, see the Spread documentation.
 *
 * +msg_type+ is the "message type" of the message; this is an
 * arbitrary application-defined value. If not specified, it defaults
 * to 0. The message type of a received data message can be accessed
 * via Spread::DataMessage#msg_type.
 *
 * If +self_discard+ is +true+, the message will not be delivered to
 * the sending node. Otherwise, the message will be delivered if the
 * sending node belongs to the group it is sending the message to.
 *
 * rb_spread imposes no upper limit on message size. By default,
 * Spread itself will reject messages larger than about 144,000 bytes;
 * this limit can be raised by recompiling Spread. If a message is
 * larger than Spread's maximum message size,
 * Spread::Error::MessageTooLong will be raised.
 */
static VALUE
spconn_multicast(int argc, VALUE *argv, VALUE obj)
{
    VALUE message, group, st, mtype, self_discard;
    struct SpreadConnection *sp;
    int n;
    int service_type;

    Data_Get_Struct(obj, struct SpreadConnection, sp);
    rb_scan_args(argc, argv, "32", &message, &group, &st, &mtype, &self_discard);
    if (NIL_P(mtype))
        mtype = INT2FIX(0);

    service_type = NUM2INT(st);
    if (RTEST(self_discard))
        service_type |= SELF_DISCARD;

    SafeStringValue(message);

    if (TYPE(group) == T_ARRAY)
    {
        char groupnames[MAX_GROUPS][MAX_GROUP_NAME];
        int i;

        if (RARRAY_LEN(group) == 0)
            return Qnil;
        if (RARRAY_LEN(group) >= MAX_GROUPS)
            rb_raise(rb_eArgError, "too many groups for multicast");

        for (i = 0; i < RARRAY_LEN(group); i++)
        {
            VALUE tmp = RARRAY_PTR(group)[i];
            snprintf(groupnames[i], MAX_GROUP_NAME, "%s",
                     StringValuePtr(tmp));
        }
        if ((n = SP_multigroup_multicast(sp->mbox, service_type,
                                         RARRAY_LEN(group),
                                         (const char (*)[]) groupnames,
                                         NUM2INT(mtype),
                                         RSTRING_LEN(message),
                                         RSTRING_PTR(message))) < 0)
            raise_sp_error(n);
    }
    else
    {
        /* `group' must be a string or equivalent, raise type error if not */
        if ((n = SP_multicast(sp->mbox,
                              service_type,
                              StringValuePtr(group),
                              NUM2INT(mtype),
                              RSTRING_LEN(message),
                              RSTRING_PTR(message))) < 0)
            raise_sp_error(n);
    }

    return Qnil;
}

/*
 * call-seq:
 *      poll -> bool
 *
 * Returns +true+ if there are any messages waiting to be read, and
 * +false+ otherwise. If #poll returns +true+, #receive can be called
 * without blocking.
 */
static VALUE
spconn_poll(VALUE obj)
{
    int n;
    struct SpreadConnection *sp;
    Data_Get_Struct(obj, struct SpreadConnection, sp);

    if ((n = SP_poll(sp->mbox)) < 0)
        raise_sp_error(n);

    return INT2BOOL(n);
}

/*
 * call-seq:
 *      receive -> Spread::Message
 *
 * Receive a message from Spread. This method will block indefinitely
 * until a message is available. To avoid blocking, clients can call
 * #poll, or use Kernel#select on the +IO+ object returned by #io.
 *
 * The return value is an instance of a subclass of Spread::Message,
 * depending on the type of message that was received.
 */
static VALUE
spconn_receive(VALUE obj)
{
    struct SpreadConnection *sp;
    struct SpreadMessage *sp_mess;
    int n;
    VALUE message;
    VALUE msgKlass;

    Data_Get_Struct(obj, struct SpreadConnection, sp);

    if ((n = SP_poll(sp->mbox)) < 0)
        raise_sp_error(n);

    if (n == 0)
    {
        /*
         * There is no data available, so we need to block. If we were
         * to call SP_receive() here, we would block the entire Ruby
         * process, which is undesirable. Therefore, just block the
         * current thread using rb_thread_select()
         */
        fd_set read_set;

        for (;;)
        {
            FD_ZERO(&read_set);
            FD_SET(sp->mbox, &read_set);

            /* XXX: add mbox to error set as well? */
            n = rb_thread_select(sp->mbox + 1, &read_set, 0, 0, 0);
            if (n < 0)
                rb_sys_fail("select failed");

            if (FD_ISSET(sp->mbox, &read_set))
                break;
        }
    }

    /*
     * We could receive any type of message off the wire, so delay
     * creating a Ruby instance for it until we know what type it is
     * (and therefore, the type of Ruby class we need to wrap it in).
     */
    sp_mess = ALLOC(struct SpreadMessage);

    /*
     * Zero out the structure. This is probably wise due to padding
     * considerations, but is also necessary for at least one reason:
     * the `service_type' pointer to SP_receive() must point to either
     * zero or the constant DROP_RECV.
     */
    memset(sp_mess, 0, sizeof(struct SpreadMessage));
    sp_mess->rb_groups = Qnil;
    sp_mess->rb_delta = Qnil;

    /*
     * We don't know the size of the buffer that is required in
     * advance.  So we first invoke SP_receive() with an empty (NULL)
     * buffer, and then use the error returned by Spread to figure out
     * the size of the buffer we need to allocate.
     */
    n = SP_receive(sp->mbox,
                   &sp_mess->service_type,
                   sp_mess->sender,
                   MAX_GROUPS,
                   &sp_mess->num_groups,
                   sp_mess->groups,
                   &sp_mess->msg_type,
                   &sp_mess->endian,
                   0, NULL);

    if (n < 0)
    {
        if (n != BUFFER_TOO_SHORT)
            raise_sp_error(n);

        sp_mess->length = -sp_mess->endian; /* message length */
        sp_mess->message = ALLOC_N(char, sp_mess->length);

        if ((n = SP_receive(sp->mbox,
                            &sp_mess->service_type,
                            sp_mess->sender,
                            MAX_GROUPS,
                            &sp_mess->num_groups,
                            sp_mess->groups,
                            &sp_mess->msg_type,
                            &sp_mess->endian,
                            sp_mess->length, sp_mess->message)) < 0)
            raise_sp_error(n);
    }

    if (Is_regular_mess(sp_mess->service_type))
        msgKlass = rb_cSpreadDataMessage;
    else if (Is_reg_memb_mess(sp_mess->service_type))
        msgKlass = rb_cSpreadMemberMessage;
    else if (Is_transition_mess(sp_mess->service_type))
        msgKlass = rb_cSpreadTransitionMessage;
    else if (message_is_self_leave(sp_mess->service_type))
        msgKlass = rb_cSpreadSelfLeaveMessage;
    else
        rb_raise(rb_eRuntimeError, "unrecognized message type: %d",
                 sp_mess->service_type);

    message = Data_Wrap_Struct(msgKlass, mark_spmess, free_spmess, sp_mess);
    return message;
}

/*
 * call-seq:
 *      io -> IO
 *
 * Returns an +IO+ handle representing the connection to Spread. The
 * Spread "mailbox" is actually a file descriptor; I'm not sure if
 * this is documented anywhere, but it seems to be relied upon by a
 * number of things.
 *
 * Note that it is *not* safe to do much with the IO object; in
 * particular, reading or writing to it is certainly not wise. This
 * method is intended primarily to allow the use of Kernel#select to
 * wait for Spread activity.
 */
static VALUE
spconn_io(VALUE obj)
{
    struct SpreadConnection *sp_conn;

    Data_Get_Struct(obj, struct SpreadConnection, sp_conn);
    if (NIL_P(sp_conn->rb_fd))
    {
        /* Construct a new wrapper */
        VALUE fd = INT2NUM(sp_conn->mbox);
        sp_conn->rb_fd =  rb_class_new_instance(1, &fd, rb_cIO);
    }

    return sp_conn->rb_fd;
}

/*
 * call-seq:
 *      private_group -> string
 *
 * Returns this connection's "private group name". This is a group
 * automatically created by Spread that allows private messages to be
 * sent to this connection.
 */
static VALUE
spconn_private_group(VALUE obj)
{
    struct SpreadConnection *sp_conn;

    Data_Get_Struct(obj, struct SpreadConnection, sp_conn);
    return rb_str_new2(sp_conn->private_group);
}

/*
 * call-seq:
 *      Spread::version -> string
 *
 * The version of the Spread client libraries being used.
 */
static VALUE
spread_version(VALUE obj)
{
    int n;
    int len;
    int maj_ver, min_ver, patch_ver;
    char buf[64];

    if ((n = SP_version(&maj_ver, &min_ver, &patch_ver)) < 0)
        raise_sp_error(n);

    len = snprintf(buf, sizeof(buf), "%d.%d.%d",
                   maj_ver, min_ver, patch_ver);
    if (len < 0 || (size_t) len >= sizeof(buf))
        rb_raise(rb_eRuntimeError, "buffer overflow");

    return rb_str_new(buf, len);
}

/*
 * Document-class: Spread::Message
 *
 * This abstract class represents a message received from Spread. All
 * messages will be a subclass of Spread::Message, depending on the
 * type of message they are (Spread::DataMessage,
 * Spread::MembershipMessage, Spread::TransitionMessage, or
 * Spread::SelfLeaveMessage). For more information on when the
 * different types of messages will be received, see the Spread User
 * Guide.
 */

/*
 * call-seq:
 *      data? -> bool
 *
 * +true+ iff the message is a data message.
 */
static VALUE
sm_is_data_msg(VALUE obj)
{
    struct SpreadMessage *sp_mess;

    Data_Get_Struct(obj, struct SpreadMessage, sp_mess);
    return INT2BOOL(Is_regular_mess(sp_mess->service_type));
}

/*
 * call-seq:
 *      membership? -> bool
 *
 * +true+ iff the message is a membership message.
 */
static VALUE
sm_is_membership_msg(VALUE obj)
{
    struct SpreadMessage *sp_mess;

    Data_Get_Struct(obj, struct SpreadMessage, sp_mess);
    return INT2BOOL(Is_reg_memb_mess(sp_mess->service_type));
}

/*
 * call-seq:
 *      transition? -> bool
 *
 * +true+ iff the message is a transition message.
 */
static VALUE
sm_is_transition_msg(VALUE obj)
{
    struct SpreadMessage *sp_mess;

    Data_Get_Struct(obj, struct SpreadMessage, sp_mess);
    return INT2BOOL(Is_transition_mess(sp_mess->service_type));
}

/*
 * call-seq:
 *      self_leave? -> bool
 *
 * +true+ iff the message is a self-leave message.
 */
static VALUE
sm_is_self_leave_msg(VALUE obj)
{
    struct SpreadMessage *sp_mess;

    Data_Get_Struct(obj, struct SpreadMessage, sp_mess);

    return INT2BOOL(message_is_self_leave(sp_mess->service_type));
}

/*
 * call-seq:
 *      sender -> string
 *
 * The private group name of the Spread node that sent the message.
 */
static VALUE
sm_sender(VALUE obj)
{
    /*
     * The various message types store different stuff in the `sender'
     * field, but since it's always a NUL-terminated string, the
     * implementation remains the same.
     */
    struct SpreadMessage *sp_mess;

    Data_Get_Struct(obj, struct SpreadMessage, sp_mess);
    /* XXX: is it worth memo-izing this? */
    return rb_str_new2(sp_mess->sender);
}

/*
 * call-seq:
 *      groups -> array
 *
 * Returns the names of all the groups who received this message.
 *
 * XXX: document Spread::MembershipMessage#members as well
 */
static VALUE
sm_groups(VALUE obj)
{
    struct SpreadMessage *sp_mess;

    Data_Get_Struct(obj, struct SpreadMessage, sp_mess);

    if (NIL_P(sp_mess->rb_groups))
    {
        /* First call, so initialize and cache the array */
        int i;

        sp_mess->rb_groups = rb_ary_new2(sp_mess->num_groups);

        for (i = 0; i < sp_mess->num_groups; i++)
        {
            VALUE group_str = rb_str_new2(sp_mess->groups[i]);
            rb_ary_store(sp_mess->rb_groups, i, group_str);
        }
    }

    return sp_mess->rb_groups;
}

/*
 * call-seq:
 *      unreliable? -> bool
 *
 * +true+ iff the message's service type is Spread::UNRELIABLE_MESS.
 */
static VALUE
data_msg_is_unreliable(VALUE obj)
{
    struct SpreadMessage *sp_mess;

    Data_Get_Struct(obj, struct SpreadMessage, sp_mess);
    return INT2BOOL(Is_unreliable_mess(sp_mess->service_type));
}

/*
 * call-seq:
 *      reliable? -> bool
 *
 * +true+ iff the message's service type is Spread::RELIABLE_MESS.
 */
static VALUE
data_msg_is_reliable(VALUE obj)
{
    struct SpreadMessage *sp_mess;

    Data_Get_Struct(obj, struct SpreadMessage, sp_mess);
    return INT2BOOL(Is_reliable_mess(sp_mess->service_type));
}

/*
 * call-seq:
 *      fifo? -> bool
 *
 * +true+ iff the message's service type is Spread::FIFO_MESS.
 */
static VALUE
data_msg_is_fifo(VALUE obj)
{
    struct SpreadMessage *sp_mess;

    Data_Get_Struct(obj, struct SpreadMessage, sp_mess);
    return INT2BOOL(Is_fifo_mess(sp_mess->service_type));
}

/*
 * call-seq:
 *      causal? -> bool
 *
 * +true+ iff the message's service type is Spread::FIFO_MESS.
 */
static VALUE
data_msg_is_causal(VALUE obj)
{
    struct SpreadMessage *sp_mess;

    Data_Get_Struct(obj, struct SpreadMessage, sp_mess);
    return INT2BOOL(Is_causal_mess(sp_mess->service_type));
}

/*
 * call-seq:
 *      agreed? -> bool
 *
 * +true+ iff the message's service type is Spread::AGREED_MESS.
 */
static VALUE
data_msg_is_agreed(VALUE obj)
{
    struct SpreadMessage *sp_mess;

    Data_Get_Struct(obj, struct SpreadMessage, sp_mess);
    return INT2BOOL(Is_agreed_mess(sp_mess->service_type));
}

/*
 * call-seq:
 *      safe? -> bool
 *
 * +true+ iff the message's service type is Spread::SAFE_MESS.
 */
static VALUE
data_msg_is_safe(VALUE obj)
{
    struct SpreadMessage *sp_mess;

    Data_Get_Struct(obj, struct SpreadMessage, sp_mess);
    return INT2BOOL(Is_safe_mess(sp_mess->service_type));
}

/*
 * call-seq:
 *      message -> string
 *
 * The content of the data message.
 */
static VALUE
data_msg_contents(VALUE obj)
{
    struct SpreadMessage *sp_mess;

    Data_Get_Struct(obj, struct SpreadMessage, sp_mess);
    return rb_str_new(sp_mess->message, sp_mess->length);
}

/*
 * call-seq:
 *      msg_type -> integer
 *
 * The user-defined "type" of the message. This can be set when the
 * message is broadcast via Spread::Connection#multicast; if not
 * defined, it defaults to 0.
 */
static VALUE
data_msg_type(VALUE obj)
{
    struct SpreadMessage *sp_mess;

    Data_Get_Struct(obj, struct SpreadMessage, sp_mess);
    return INT2FIX(sp_mess->msg_type);
}

/*
 * call-seq:
 *      caused_by_join? -> bool
 *
 * +true+ iff this membership message was caused by a new node joining
 * a group.
 */
static VALUE
memb_caused_by_join(VALUE obj)
{
    struct SpreadMessage *sp_mess;

    Data_Get_Struct(obj, struct SpreadMessage, sp_mess);
    return INT2BOOL(Is_caused_join_mess(sp_mess->service_type));
}

/*
 * call-seq:
 *      caused_by_leave? -> bool
 *
 * +true+ iff this membership message was caused by a node leaving a
 * group.
 */
static VALUE
memb_caused_by_leave(VALUE obj)
{
    struct SpreadMessage *sp_mess;

    Data_Get_Struct(obj, struct SpreadMessage, sp_mess);
    return INT2BOOL(Is_caused_leave_mess(sp_mess->service_type));
}

/*
 * call-seq:
 *      caused_by_disconnect? -> bool
 *
 * +true+ iff this membership message was caused by a node abruptly
 * disconnecting from a group.
 */
static VALUE
memb_caused_by_disconnect(VALUE obj)
{
    struct SpreadMessage *sp_mess;

    Data_Get_Struct(obj, struct SpreadMessage, sp_mess);
    return INT2BOOL(Is_caused_disconnect_mess(sp_mess->service_type));
}

static VALUE
memb_caused_by_network(VALUE obj)
{
    struct SpreadMessage *sp_mess;

    Data_Get_Struct(obj, struct SpreadMessage, sp_mess);
    return INT2BOOL(Is_caused_network_mess(sp_mess->service_type));
}

static VALUE
memb_delta(VALUE obj)
{
    struct SpreadMessage *sp_mess;

    Data_Get_Struct(obj, struct SpreadMessage, sp_mess);

    if (NIL_P(sp_mess->rb_delta))
    {
        /* First call, so initialize and cache the array */
        int i;
        int num_members;
        char *members;

        num_members = (int) sp_mess->message[SP_get_num_vs_offset_memb_mess()];
        members = sp_mess->message + SP_get_vs_set_offset_memb_mess();
        sp_mess->rb_delta = rb_ary_new2(num_members);

        for (i = 0; i < num_members; i++)
        {
            char *member = members + (i * MAX_GROUP_NAME);
            rb_ary_store(sp_mess->rb_delta, i, rb_str_new2(member));
        }
    }

    return sp_mess->rb_delta;
}

void
Init_spread(void)
{
#define DEFINE_ERROR_CLASS(obj, name) \
    obj = rb_define_class_under(rb_eSpread, name, rb_eSpread);

    /* Everything lives in the Spread module */
    rb_mSpread = rb_define_module("Spread");
    rb_define_module_function(rb_mSpread, "version", spread_version, 0);

    rb_define_const(rb_mSpread, "UNRELIABLE_MESS", INT2FIX(UNRELIABLE_MESS));
    rb_define_const(rb_mSpread, "RELIABLE_MESS", INT2FIX(RELIABLE_MESS));
    rb_define_const(rb_mSpread, "FIFO_MESS", INT2FIX(FIFO_MESS));
    rb_define_const(rb_mSpread, "CAUSAL_MESS", INT2FIX(CAUSAL_MESS));
    rb_define_const(rb_mSpread, "AGREED_MESS", INT2FIX(AGREED_MESS));
    rb_define_const(rb_mSpread, "SAFE_MESS", INT2FIX(SAFE_MESS));
    rb_define_const(rb_mSpread, "REGULAR_MESS", INT2FIX(REGULAR_MESS));

    rb_eSpread = rb_define_class_under(rb_mSpread, "Error",
                                       rb_eStandardError);
	rb_define_alloc_func(rb_eSpread, sperr_alloc);
	rb_define_method(rb_eSpread, "initialize", sperr_initialize, 2);
	rb_define_method(rb_eSpread, "error_code", sperr_error_code, 0);

    DEFINE_ERROR_CLASS(rb_eSpreadIllegalSession, "IllegalSession");
    DEFINE_ERROR_CLASS(rb_eSpreadIllegalMessage, "IllegalMessage");
    DEFINE_ERROR_CLASS(rb_eSpreadIllegalService, "IllegalService");
    DEFINE_ERROR_CLASS(rb_eSpreadIllegalGroup, "IllegalGroup");
    DEFINE_ERROR_CLASS(rb_eSpreadIllegalSpread, "IllegalSpread");
    DEFINE_ERROR_CLASS(rb_eSpreadCouldNotConnect, "CouldNotConnect");
    DEFINE_ERROR_CLASS(rb_eSpreadConnClosed, "ConnectionClosed");
    DEFINE_ERROR_CLASS(rb_eSpreadGroupsTooShort, "GroupsTooShort");
    DEFINE_ERROR_CLASS(rb_eSpreadBufferTooShort, "BufferTooShort");
    DEFINE_ERROR_CLASS(rb_eSpreadMessageTooLong, "MessageTooLong");
    DEFINE_ERROR_CLASS(rb_eSpreadRejectVersion, "RejectVersion");
    DEFINE_ERROR_CLASS(rb_eSpreadRejectNoName, "RejectNoName");
    DEFINE_ERROR_CLASS(rb_eSpreadRejectIllegalName, "RejectIllegalName");
    DEFINE_ERROR_CLASS(rb_eSpreadRejectNotUnique, "RejectNotUnique");
    DEFINE_ERROR_CLASS(rb_eSpreadRejectQuota, "RejectQuota");
    DEFINE_ERROR_CLASS(rb_eSpreadRejectAuth, "RejectAuth");

    rb_cSpreadConnection = rb_define_class_under(rb_mSpread, "Connection",
                                                 rb_cObject);
    rb_define_alloc_func(rb_cSpreadConnection, spconn_alloc);
    rb_define_method(rb_cSpreadConnection, "initialize", spconn_connect, -1);
    rb_define_method(rb_cSpreadConnection, "connect", spconn_connect, -1);
    rb_define_method(rb_cSpreadConnection, "disconnect", spconn_disconnect, 0);
    rb_define_method(rb_cSpreadConnection, "join", spconn_join, 1);
    rb_define_method(rb_cSpreadConnection, "leave", spconn_leave, 1);
    rb_define_method(rb_cSpreadConnection, "multicast", spconn_multicast, -1);
    rb_define_method(rb_cSpreadConnection, "poll", spconn_poll, 0);
    rb_define_method(rb_cSpreadConnection, "receive", spconn_receive, 0);
    rb_define_method(rb_cSpreadConnection, "io", spconn_io, 0);
    rb_define_method(rb_cSpreadConnection, "private_group",
                     spconn_private_group, 0);

    /* Generic message type */
    rb_cSpreadMessage = rb_define_class_under(rb_mSpread, "Message", rb_cObject);
    rb_define_method(rb_cSpreadMessage, "data?", sm_is_data_msg, 0);
    rb_define_method(rb_cSpreadMessage, "membership?", sm_is_membership_msg, 0);
    rb_define_method(rb_cSpreadMessage, "transition?", sm_is_transition_msg, 0);
    rb_define_method(rb_cSpreadMessage, "self_leave?", sm_is_self_leave_msg, 0);

    /* Data messages */
    rb_cSpreadDataMessage = rb_define_class_under(rb_mSpread, "DataMessage",
                                                  rb_cSpreadMessage);
    rb_define_method(rb_cSpreadDataMessage, "message", data_msg_contents, 0);
    rb_define_method(rb_cSpreadDataMessage, "sender", sm_sender, 0);
    rb_define_method(rb_cSpreadDataMessage, "groups", sm_groups, 0);
    rb_define_method(rb_cSpreadDataMessage, "msg_type", data_msg_type, 0);
    rb_define_method(rb_cSpreadDataMessage, "unreliable?", data_msg_is_unreliable, 0);
    rb_define_method(rb_cSpreadDataMessage, "reliable?", data_msg_is_reliable, 0);
    rb_define_method(rb_cSpreadDataMessage, "fifo?", data_msg_is_fifo, 0);
    rb_define_method(rb_cSpreadDataMessage, "causal?", data_msg_is_causal, 0);
    rb_define_method(rb_cSpreadDataMessage, "agreed?", data_msg_is_agreed, 0);
    rb_define_method(rb_cSpreadDataMessage, "safe?", data_msg_is_safe, 0);

    /* Normal membership messages */
    rb_cSpreadMemberMessage = rb_define_class_under(rb_mSpread,
                                                    "MembershipMessage",
                                                    rb_cSpreadMessage);
    rb_define_method(rb_cSpreadMemberMessage, "group", sm_sender, 0);
    rb_define_method(rb_cSpreadMemberMessage, "members", sm_groups, 0);
    rb_define_method(rb_cSpreadMemberMessage, "caused_by_leave?",
                     memb_caused_by_leave, 0);
    rb_define_method(rb_cSpreadMemberMessage, "caused_by_join?",
                     memb_caused_by_join, 0);
    rb_define_method(rb_cSpreadMemberMessage, "caused_by_disconnect?",
                     memb_caused_by_disconnect, 0);
    rb_define_method(rb_cSpreadMemberMessage, "caused_by_network?",
                     memb_caused_by_network, 0);
    /* XXX: need better name */
    rb_define_method(rb_cSpreadMemberMessage, "delta", memb_delta, 0);

    /* Transition membership messages */
    rb_cSpreadTransitionMessage = rb_define_class_under(rb_mSpread,
                                                        "TransitionMessage",
                                                        rb_cSpreadMessage);
    rb_define_method(rb_cSpreadTransitionMessage, "group", sm_sender, 0);

    /* Self-leave membership messages */
    rb_cSpreadSelfLeaveMessage = rb_define_class_under(rb_mSpread,
                                                       "SelfLeaveMessage",
                                                       rb_cSpreadMessage);
}
