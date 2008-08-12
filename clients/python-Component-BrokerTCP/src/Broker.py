# vim: set fileencoding=utf-8 :

###########################
# Implementation details:
# 
#  * XML is generated using regular string concatenation and escaping.
#    No XML module is used for XML generation
#
#  * All socket operations are made at the lowest level possible allowed by python
#    In particular no buffering at the python IO level
#
#  * All IO is blocking with no timeout
#
#  * XML parsing is made using several backends:
#   * If available lxml is used since it's by far the fastest implementation
#   * As an alternative cElementTree is used (both the version shipped with python and external versions)
#   * If everything fails, xml.sax is used and this should be OK for any python with version > 2.3
#
#  * Thread safety:
#   * All IO should be thread safe. 2 mutexes are used, one for reading and another for writing.
#   * Reading in one thread while writing in another is possible.
#   * All other operations done in python are assumed to be thread safe.
#   * Thread safety status of XML parsing is unknown to me since several backends can be used. This could be a problem.
#
###########################

"""
Module to encapsulate access to a Broker server via TCP binary protocol.

Example of minimal typical usage:

from Broker import Client, Message

#first connect to the server
broker = Client(host='some.broker.server.pt', port=2222)

#send some events to the broker

#first construct the message
message = Message(payload='this is the payload', destination='/some/path')

#then send it to a TOPIC
broker.produce(message, 'TOPIC')

#or you can also send it to a QUEUE
broker.produce(message, 'QUEUE')

#subscribe to some messages
broker.subscribe('/some/path', 'TOPIC')

#you can subscribe to several topics or queues
broker.subscribe('/some/path', 'QUEUE')

#main loop to process messages

for message in broker:
    print message.id
    print message.payload
"""

import socket

#for pack/unpack
import struct

#for locking
import threading

#for workaround in date parsing from broker
import re

from datetime import datetime
import types

import xml.sax.saxutils

import iso8601

import os, os.path, tempfile, shutil

import logging
log = logging.getLogger("Broker")

__all__ = ['Client', 'DropBox', 'Message']

TRACE = False

#namespaces for XML
NS = {
    'soap'   : 'http://www.w3.org/2003/05/soap-envelope',
    'wsa'    : 'http://www.w3.org/2005/08/addressing',
    'broker' : 'http://services.sapo.pt/broker'
    }

try:
    packer = struct.Struct("!L")
    def pack(s):
        return packer.pack(len(s))
    def unpack(s):
        return packer.unpack(s)[0]

    log.info("using new pack/unpack interface (compile format only once)")
except:
    log.info("using old pack/unpack interface")
    def pack(s):
        return struct.pack("!L", len(s))

    def unpack(s):
        return struct.unpack("!L", s)[0]

def escape_xml(data):
    if type(data)==types.UnicodeType:
        data = data.encode('utf-8')

    if not type(data)==types.StringType:
        log.warn("serializing non string type %s", type(data))
        data = str(data)
    
    return xml.sax.saxutils.escape(data)

DEFAULT_KIND  = 'TOPIC'
DEFAULT_PORT  = 3322
ALLOWED_KINDS = ('TOPIC', 'QUEUE')

def check_kind(kind):
    """
    Checks whether kind is a valid destination kind.
    """
    if kind not in ALLOWED_KINDS:
        raise AttributeError("Unknown kind '%s'" % kind)

def check_msg(msg):
    """
    Checks whether its argument is a subclass of a broker message.
    """
    if not isinstance(msg, Message):
        raise TypeError("%r is not a subclass of %s.%s" % (msg, Message.__module__, Message.__name__))

soap_open       = """<soap:Envelope xmlns:soap="%s"><soap:Body>""" % (escape_xml(NS['soap']))
soap_close      = """</soap:Body></soap:Envelope>"""
broker_ns       = NS['broker']
soap_ns         = NS['soap']

#aux function to pre-build open/close xml tags
def prod_tags(tagname, ns='broker'):
    otag = """%s<%s xmlns="%s">""" % (soap_open, tagname, escape_xml(NS[ns]))
    ctag = """</%s>%s""" % (tagname, soap_close)
    return (otag, ctag)

taglist = (
    ('publish',     'Publish'),
    ('enqueue',     'Enqueue'),
    ('subscribe',   'Notify'),
    ('unsubscribe', 'Unsubscribe'),
    ('acknowledge', 'Acknowledge'),
    ('request',     'Poll')
)

#pre-built tags
tags = dict( map( lambda (name, tag): (name, prod_tags(tag)) , taglist) )

def build_msg(name, payload):
    (otag, ctag) = tags[name]
    return otag+'\n'+payload+'\n'+ctag

def subscribe_msg(destination, kind):
    if 'TOPIC_AS_QUEUE' == kind:
        #check whether the destination has the correct syntax
        if '@' not in destination:
            raise AttributeError("Invalid destination %r syntax for TOPIC_AS_QUEUE. Format is handle@topic." % destination)
    else:
        check_kind(kind)

    return """<DestinationName>%s</DestinationName>\n<DestinationType>%s</DestinationType>""" % (
        escape_xml(destination),
        escape_xml(kind)
    )

def request_msg(destination):
    return """<DestinationName>%s</DestinationName>""" % (escape_xml(destination),)

#aux function for debugging
def str2hex(raw):
    """
    Given raw binary data outputs a string with all octets in hexadecimal notation.
    """
    return ':'.join(["%02X" % ord(c) for c in raw ])

def safe_cast(function, value):
    """
    Returns function(value) or None in case some error occurred.
    """
    try:
        return function(value)
    except:
        return None

def date_cast(date):
    if isinstance( date, datetime ):
        return date
    elif isinstance( date, basestring ):
        return iso8601.parse_date(date)
    elif type(date) in (types.IntType, types.FloatType):
        return datetime.utcfromtimestamp(date)

date_clean_rx = re.compile(r'\.\d+\D')
def date2iso(date):
    ret = date.isoformat()
    #check whether there is time information
    if date.tzinfo is None:
        ret += 'Z'

    #workaround for bug in broker
    return date_clean_rx.sub('', ret)

try:
    set()
except NameError:
    log.info("No set defined using Set from sets")
    from sets import Set as set

try:
    try:
        from lxml import etree as ElementTree
        log.info("Using lxml as XML backend")
    except ImportError:
        try:
            import xml.etree.cElementTree as ElementTree
            log.info("Using built-in cElementTree as XML backend")
        except ImportError:
            import cElementTree as ElementTree
            log.info("Using cElementTree as XML backend")

    #so simple it almost hurts
    #just as lax as the SAX parser
    def fromXML(raw):
        """Constructs a broker message from its XML representation. ((c)ElementTree backend)"""
        tree = ElementTree.fromstring(raw)
        broker = tree.find('.//{%s}BrokerMessage' % broker_ns)

        if broker is None:
            #must have been an error. Try and find SOAP Fault
            fault = tree.find('.//{%s}Fault' % soap_ns)
            if fault is None:
                #This is trully bad. I don't know what to do with this
                log.error("Received unknown error message %r", raw)
                raise Client.Error()
            else:
                code   = fault.find('.//{%s}Code/{%s}Value' % (soap_ns, soap_ns) )
                reason = fault.find('.//{%s}Reason/{%s}Text' % (soap_ns, soap_ns) )
                detail = fault.find('.//{%s}Detail' % (soap_ns,) )

                if code is not None:
                    code = code.text
                if reason is not None:
                    reason = reason.text
                if detail is not None:
                    detail = detail.text

                e = Client.SOAPError(code, reason, detail)
                log.exception(e)
                raise e
        else:
            fields = {}
            for node in broker:
                if len(node) > 0:
                    pass
                else:
                    (_, tag) = node.tag.split('}')[0:2]
                    fields[tag] = node.text

            return msgfromFields(fields)

except ImportError:
    #since 2.3 an XML SAX parser is shipped with python so sax is both faster than DOM and always available in all reasonable versions
    import xml.sax

    log.info("Using fallback xml.sax as XML backend")

    class SaxHandler(xml.sax.ContentHandler):
        """
        Handler for sax events while parsing a Broker notification.
        Very lax since as it stands there could be extra tags between the tags we are expecting until reaching the actual broker message and it would still give meaningful results.
        """
        def startDocument(self):
            self.__fields  = {}
            self.__txt     = u""
            self.__count   = 0
            self.__consume = False

            #setup default handlers
            self.startElementNS = self.top_start
            self.endElementNS   = self.def_end

        def fields(self):
            return self.__fields

        #For some unknown reason, this can't be changed at run time by the parser.
        #The original method is the one that is always called. (pretty dumb and sloppy)
        def characters(self, data):
            if self.__consume:
                self.__txt += data

        def startElementNS(self, name, qname, attrs):
            pass

        def endElementNS(self, name, qname):
            pass

        def endDocument(self):
            pass

        def top_start(self, name, qname, attrs):
            if (soap_ns, u'Envelope') == name:
                self.startElementNS = self.envelope_start

        def envelope_start(self, name, qname, attrs):
            if (soap_ns, u'Body') == name:
                self.startElementNS = self.body_start

        def body_start(self, name, qname, attrs):
            if (broker_ns, u'Notification') == name:
                self.startElementNS = self.notification_start

        def notification_start(self, name, qname, attrs):
            if (broker_ns, u'BrokerMessage') == name:
                self.startElementNS = self.broker_start
                self.endElementNS   = self.broker_end
                self.__consume      = True

        def broker_start(self, name, qname, attrs):
            self.__txt = u""
            if broker_ns == name[0] and name[1] is not None:
                self.__count += 1
            else:
                pass

        def broker_end(self, name, qname):
            self.__count -= 1
            if 0 == self.__count:
                if broker_ns == name[0] and name[1] is not None:
                    self.__fields[name[1]] = self.__txt
                else:
                    pass
            else:
                self.startElementNS = self.def_start
                self.endElementNS   = self.def_end
                self.__consume      = False

        def def_end(self, name, qname):
            pass

        def def_start(self, name, qname, attrs):
            pass

    def fromXML(raw):
        """Constructs a broker message from its XML representation. (xml.sax backend)"""
        parser = xml.sax.make_parser()
        #we want to parse using namespaces
        parser.setFeature(xml.sax.handler.feature_namespaces, 1)
        sax_handler = SaxHandler()
        parser.setContentHandler(sax_handler)
        parser.feed(raw)
        parser.close()
        return msgfromFields(sax_handler.fields())
    

def msgfromFields(fields):
    return Message(
        payload       = fields['TextPayload'],
        destination   = fields['DestinationName'],
        id            = fields['MessageId'],
        priority      = safe_cast(int, fields.get('Priority')),
        expiration    = fields.get('Expiration'),
        timestamp     = fields.get('Timestamp'),
        correlationId = fields.get('CorrelationId')
    )


class Client:
    """
    Abstracts access to a broker server using TCP.
    """

    class Error(Exception):
        """
        Empty class for error in the broker.
        """

    class DisconnectedError(EOFError, Error):
        """
        Class to indicate that the Server disconnected while the client was waiting for a response.
        """

        def __init__(self, message):
            EOFError.__init__(self, message)

    class SOAPError(Error):
        """
        Class to represent SOAP Faults.
        """
        def __init__(self, code, reason=None, detail=None):
            self.code   = code
            self.reason = reason
            self.detail = detail

        def __repr__(self):
            return """<%s{ code : %r, reason: %r }>""" % (self.__class__, self.code, self.reason)


    def __init__ (self, host, port=DEFAULT_PORT):
        """
        Constructs a client object to connect to a broker at host:port using the binary TCP protocol.
        """

        log.info("Client for %s:%s", host, port)
        self.__mutex_r     = threading.RLock()
        self.__mutex_w     = threading.RLock()
        self.__mutex_ack   = threading.RLock()
        self.host          = host
        self.port          = port
        self.endpoint      = "%s:%s" % (host, port)
        self.subscribed    = set()
        self.__auto_ack    = set()
        self.__closed      = False
        self.__request_ack = {}

        #first create the socket
        self.__socket = socket.socket( socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.__socket.setsockopt(socket.SOL_TCP,socket.SO_KEEPALIVE,True)
            #self.__socket.setsockopt(socket.SOL_SOCKET,socket.SO_KEEPALIVE,True)
            #10 seconds of idle connection time
            self.__socket.setsockopt(socket.SOL_TCP,socket.TCP_KEEPIDLE, 10)
            #see also  SOL_TCP integer parameters TCP_KEEPIDLE, TCP_KEEPINTVL,and TCP_KEEPCNT
        except Exception, e:
            log.exception(e)
        log.debug("Socket timeout  %s s", self.__socket.gettimeout())
        #connect to host:port
        self.__socket.connect((host, port))

    def __add_request_ack(self, destination):
        """
        A message for destination has been requested by poll.
        """
        self.__lock_ack()
        try:
            self.__request_ack[destination] = self.__request_ack.get(destination, 0)+1
        finally:
            self.__unlock_ack()

        return self

    def __remove_request_ack(self, message):
        """
        A message for destination requested by poll has been acknowledged.
        """
        self.__lock_ack()
        try:
            destination = message.destination
            count       = self.__request_ack[destination]
            if count<0:
                log.error("Internal error. I should acknowledged %d messages for %s.", count, destination)
            elif 1==count:
                del self.__request_ack[destination]
            else:
                self.__request_ack[destination] = count-1
        finally:
            self.__unlock_ack()

        return self

    def __should_request_ack(self, message):
        """
        Returns whether a message should be auto-acknowledged due to a previous poll.
        """
        self.__lock_ack()
        try:
            return message.destination in self.__request_ack
        finally:
            self.__unlock_ack()

    def __add_auto_ack(self, destination):
        """
        Adds destination to the list of destinations for which auto_acknowledge for be made.
        """
        self.__lock_ack()
        try:
            self.__auto_ack.add(destination)
        finally:
            self.__unlock_ack()

        return self

    def __in_auto_ack(self, message):
        """
        Returns whether a message has a destination that should be auto_acknowledged.
        """
        self.__lock_ack()
        try:
            return message.destination in self.__auto_ack
        finally:
            self.__unlock_ack()

    def __lock_ack(self):
        """
        Locks the object's internal mutex.
        """
        if TRACE:
            log.debug("Ack locking")
        self.__mutex_ack.acquire()
        if TRACE:
            log.debug("Ack locked")

        return self

    def __unlock_ack(self):
        """
        Unlocks the object's internal mutex.
        """
        if TRACE:
            log.debug("Ack unlocking")
        self.__mutex_ack.release()
        if TRACE:
            log.debug("Ack unlocked")

        return self

    def __lock_w(self):
        """
        Locks the object's write mutex.
        """
        if TRACE:
            log.debug("Thread write locking")
        self.__mutex_w.acquire()
        if TRACE:
            log.debug("Thread write locked")

        return self

    def __unlock_w(self):
        """
        Unlocks the object's write mutex.
        """
        if TRACE:
            log.debug("Thread write unlocking")
        self.__mutex_w.release()
        if TRACE:
            log.debug("Thread write unlocked")

        return self

    def __lock_r(self):
        """
        Locks the object's read mutex.
        """
        if TRACE:
            log.debug("Thread read locking")
        self.__mutex_r.acquire()
        if TRACE:
            log.debug("Thread read locked")

        return self

    def __unlock_r(self):
        """
        Unlocks the object's read mutex.
        """
        if TRACE:
            log.debug("Thread read unlocking")
        self.__mutex_r.release()
        if TRACE:
            log.debug("Thread read unlocked")

        return self

    def __write_raw(self, msg):
        """
        Sends a raw message to the broker.
        """
        if TRACE:
            log.debug("Sending raw message [%r]", msg)
        
        l = pack(msg)

        if TRACE:
            log.debug("hexlen [%s]", str2hex(l))

        try:
            self.__lock_w()
            try:
                #send message length
                self.__socket.sendall(l)
                #send actual payload
                self.__socket.sendall(msg)
            finally:
                self.__unlock_w()
        except socket.error:
            raise Client.DisconnectedError("""Broker server at %s is dead. Can't write message data""" % self.endpoint)

        return self

    #XXX this function doesn't handle EINTR gracefully, but neither do python's own IO functions so not sure whether it's coherent to do it here
    def __read_len(self, msglen):
        """
        Reads msglen bytes from the server.
        Thread safe but not EINTR safe (like all python IO?)
        """
        read = ''
        while msglen:
            if TRACE:
                log.debug("Trying to read %d bytes", msglen)
            ret = self.__socket.recv(msglen)
            if '' == ret:
                raise Client.DisconnectedError("""Broker server at %s is dead. Can't read message data.""" % self.endpoint)
            else:
                l = len(ret)
                if TRACE:
                    log.debug('Read %d bytes.', l)
                read = read + ret
                msglen = msglen - l

        return read

    def __read_raw(self):
        """
        Reads and returns the raw message broker notification. (without the length header)
        """
        if TRACE:
            log.debug("Reading raw message")
        self.__lock_r()
        try:
            msg_len = unpack(self.__read_len(4))
            if TRACE:
                log.debug("len = %d", msg_len)
            msg = self.__read_len(msg_len)
            if TRACE:
                log.debug("Message read = [%s]", msg)
        finally:
            self.__unlock_r()

        return msg

    def close(self):
        """
        Closes current client object. No other operation should be possible with this object afterwards.
        """
        log.debug("Close")
        if self.__closed:
            log.warn("Trying to close an already closed socket.")
        else:
            #try to cleanup as nicely as possible
            try:
                #signal end of reading and writing to socket
                self.__socket.shutdown(socket.SHUT_RDWR)
                self.__socket.close()
            except Exception, e:
                log.exception(e)

            self.__closed = True

        return self

    def __del__(self):
        """
        "Destructor". Tries to do fallback cleanup.
        """
        log.debug("Client.__del__")
        if self.__closed:
            log.info("Destruction on an already closed socket")
        else:
            self.close()

    def subscribe(self, destination, kind=DEFAULT_KIND, auto_acknowledge=True):
        """
        Subscribes notifications for events of destination with kind either TOPIC or QUEUE.

        auto_acknowledge determines whether the client needs to acknowledge the received messages for QUEUE and TOPIC_AS_QUEUE.
        By default auto_acknowledge is True meaning acknowledge is done automatically each time a message is consumed.
        A False value requires the user to call acknowledge for the received message explicitly when he sees fit.
        
        ** For kind == 'TOPIC' auto_acknowledge is always ignored and considered to be False **
        """
        log.info("Client.subscribe (%s, %s)", destination, kind)

        if (destination, kind) in self.subscribed:
            log.warn("destination (%s, %s) already subscribed" %(destination, kind))
        else:
            #send the message 
            self.__write_raw(build_msg('subscribe', subscribe_msg(destination, kind)))
            #append into subscribed endpoints
            self.subscribed.add( (destination, kind) )
            log.debug('Currently subscribed to %s', self.subscribed)

        if auto_acknowledge and kind not in ('TOPIC'):
            log.debug('Using client auto-acknowledgement on consume')
            self.__add_auto_ack(destination)

        return self

    def unsubscribe(self, destination, kind=DEFAULT_KIND):
        """
        Unsubscribes notifications for destination and kind
        """
        log.info("Client.unsubscribe (%s, %s)", destination, kind)
        if (destination, kind) in self.subscribed:
            self.__write_raw(build_msg('unsubscribe', subscribe_msg(destination, kind)))
            self.subscribed.remove( (destination, kind) )
        else:
            log.warn("destination (%s, %s) not subscribed. Can't unsubscribe." %(destination, kind))

        return self

    def request(self, destination):
        """
        Requests that a notification for the destination QUEUE be delivered.
        Doesn't guarantee that the very next notification is from destination when multiple destinations have been subscribed or polled.
        """
        log.info("Client.request (%s)", destination)
        self.__lock_w()
        try:
            self.__write_raw(build_msg('request', request_msg(destination)))
            self.__add_request_ack(destination)
        finally:
            self.__unlock_w()

        return self

    def consume(self):
        """
        Wait for a notification and return it as a Message object.
        Blocking call (no timeout).
        """
        log.info("Client.consume")
        self.__lock_r()
        try:
            content = self.__read_raw()
            msg = Message.fromXML(content)

            if self.__in_auto_ack(msg):
                #XXX I can't tell whether this is from a TOPIC or QUEUE which is a pain
                log.info("Auto acknowledging received message (subscribe)")
                self.acknowledge(msg)
            elif self.__should_request_ack(msg):
                self.__remove_request_ack(msg)
                log.info("Auto acknowledging received message (poll)")
                self.acknowledge(msg)
            
            return msg
        finally:
            self.__unlock_r()

    def produce(self, message, kind=DEFAULT_KIND):
        """
        Send a notification to the broker.
        message must be a Message object.
        Blocking call (no timeout).
        """
        log.info("Client.produce(%r, %s)", message, kind)
        check_kind(kind)
        check_msg(message)
        msg_xml = message.toXML()
        name = {'TOPIC': 'publish', 'QUEUE': 'enqueue'}[kind]
        self.__write_raw(build_msg(name, msg_xml))
        return self

    def acknowledge(self, message):
        """
        Acknowledge that the client did receive/process a message.
        message must be a Message object.
        Blocking call (no timeout).
        """
        log.info("Client.acknowledge(%r)", message)
        check_msg(message)

        msg_xml = """<MessageId>%s</MessageId><DestinationName>%s</DestinationName>""" % (escape_xml(message.id), escape_xml(message.destination))
        self.__write_raw(build_msg('acknowledge', msg_xml))
        return self

    def __iter__(self):
        """
        Syntax sugar to allow iterating over a Broker Client and getting the received messages.

        This allows for construct such as:

        for message in broker:
            process(message)
        """
        while True:
            yield self.consume()

class DropBox:
    """
    Abstracts access to a broker server using file system.
    """

    def __init__(self, directory, good=".good", tmp="_py"):
        """
        Constructs a client object to send messages to a broker using the file system.
        good is the extension messages should have to signal they should be sent.
        tmp is the extension messages have while they are being written to disk.
        """

        log.info("DropBox for %s", directory)

        if good == tmp:
            raise ValueError ("good and tmp must be different (%r == %r)" % good, tmp)
        else:
            self.__directory = directory
            self.__good      = good
            self.__tmp       = tmp

            if os.path.isdir( directory ):
                log.debug("directory %r already exists.")
            else:
                log.info("directory %r doesn't exist. Creating it")
                #What should the access mode be?
                os.makedirs(directory) 

    def produce(self, message, kind=DEFAULT_KIND):
        """
        Send a notification to the broker.
        message must be a Message object.
        """

        log.info("DropBox.produce(%r, %s)", message, kind)
        check_kind(kind)
        check_msg(message)
        name = {'TOPIC': 'publish', 'QUEUE': 'enqueue'}[kind]
        raw_msg = (build_msg(name, message.toXML()))

        #create temporary file
        (tmpfd, tmpname) = tempfile.mkstemp(prefix='brk_', suffix=self.__tmp, dir=self.__directory)
        tmpfile = os.fdopen(tmpfd, 'w')
        tmpfile.write(raw_msg)
        tmpfile.close()

        #move to destination
        destname = tmpname + self.__good
        shutil.move(tmpname, destname)

        return self

class Message:
    __all__ = ['__init__', 'toXML', 'fromXML']
    def __init__(self, payload, destination, id=None, correlationId=None, timestamp=None, expiration=None, priority=None):
        """
        Creates a Broker message given the mandatory payload and destination.
        All other fields are optional.

        This object has as fields all the parameters used in this construtor.
        
        timestamp and expiration are supposed to be datetime objects and default to None and are thus optional.
        If these fields don't have timezone information, they are assumed to be in UTC.
        You can also pass seconds since the epoch or a string in ISO8601 (use at your own risk).

        id is supposed to be a unique id of the message and defaults to None meaning that the Broker server will generate one automatically.

        correlationId is an identifier supposed to provide logical aggregation of messages of different ids.

        This object should be constructed to send an event notification to the Server and is returned by the Client object when a new event is received.

        Notice regarding Unicode and all text fields (payload, destination, id, and correlationId):
            All text fields may either be Unicode strings (preferably) or regular strings (byte arrays).
            If these fields are Unicode strings, then their content is xml escaped, encoded into utf-8 bytes and sent through the network.
            If the fields are regular strings they are only XML-escaped and apart from that are sent "ipis verbis". This can be problematic in case one wishes to sent raw binary information (no character semantics) because this stream might no be valid utf-8 and a decent XML browser will throw an error.

        Bottom line:
            If you don't use Unicode strings as input make sure you know what you are doing (utf-8 encode everything)
            If you want to send binary data consider first encoding it to an ASCII string (base64 or uuencode) and then send these characters.
        """

        self.payload       = payload
        self.destination   = destination
        self.id            = id
        self.__timestamp   = timestamp
        self.__expiration  = expiration
        self.priority      = priority
        self.correlationId = correlationId

    #generate a static method
    fromXML = staticmethod(fromXML)

    def toXML(self):
        """
        Serializes the message to XML
        """
        ret = '<BrokerMessage>\n'

        for(tname, attr, fun) in ( 
            ('DestinationName', 'destination',   None),
            ('MessageId',       'id',            None),
            ('TextPayload',     'payload',       None),
            ('Priority',        'priority',      str),
            ('CorrelationId',   'correlationId', None),
            ('Timestamp',       'timestamp',     date2iso),
            ('Expiration',      'expiration',    date2iso),
        ):
            content = getattr(self, attr, None)

            if content is None:
                #do not output the tag
                #could just place it empty
                pass
            else:
                if fun is not None:
                    content = safe_cast(fun, content)
                if content is not None:
                    ret += "\t<%(tname)s>%(content)s</%(tname)s>\n" % {'tname':tname, 'content':escape_xml(content)}

        ret += '</BrokerMessage>'
        return ret

    def __repr__(self):
        """
        Just returns its id.
        Subclasses should probably add more relevant data for their usage.
        """
        return """<%s{ id : %r }>""" % (self.__class__, self.id)

    #just return the actual payload
    def __unicode__(self):
        """
        Syntax sugar that returns the payload of the message
        """
        return self.payload

    def __str__(self):
        return unicode(self).encode('utf-8')

    def __get_expiration(self):
        self.__expiration = date_cast(self.__expiration)
        return self.__expiration

    def __get_timestamp(self):
        self.__timestamp = date_cast(self.__timestamp)
        return self.__timestamp

    def __set_expiration(self, value):
        self.__expiration = value
        return self

    def __set_timestamp(self, value):
        self.__timeout = value
        return self

    timestamp  = property(fget=__get_timestamp, fset=__set_timestamp)
    expiration = property(fget=__get_expiration, fset=__set_expiration)

#log handler code

import cPickle as pickle
fqdn = socket.getfqdn()

class LogHandler(logging.Handler):
    def __init__(self, host, port, kind, topic):
        logging.Handler.__init__(self)
        self.__broker = Client(host, port)
        self.__kind   = kind
        self.__topic  = topic

    def handleError(self, record):
        #for now just propagate through the inheritance chain
        logging.Handler.handleError(self, record)

    def get_obj(self, record):
        obj = {}
        obj['fqdn'] = fqdn
        obj['message'] = record.getMessage()
        obj['logline'] = self.format(record)
        obj.update( dict( map( lambda x: (x, getattr(record, x, None)), self.fields()) ) )
        return obj

    def serialize(self, record):
        return pickle.dumps( self.get_obj(record))

    def emit(self, record):
        msg = Message(payload=self.serialize(record), destination=self.__topic)
        self.__broker.produce(msg, self.__kind)

    def fields(self):
        return ['asctime', 'created' ,'exc_text', 'filename', 'levelname', 'levelno', 'lineno', 'message', 'module', 'name', 'pathname', 'process', 'relativeCreated', 'thread', 'threadName']

    def close(self):
        self.__broker.close()
        logging.Handler.close(self)

class LogListener:
    #TODO
    #for now just have a look at the examples
    pass
