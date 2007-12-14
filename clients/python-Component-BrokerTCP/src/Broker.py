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
import struct
from xml.dom.minidom import parseString
import string
import threading


import logging
log = logging.getLogger("Broker")

__all__ = ['Client', 'Message']

#namespaces for xml
NS = {
    'soap'   : 'http://www.w3.org/2003/05/soap-envelope',
    'wsa'    : 'http://www.w3.org/2005/08/addressing',
    'broker' : 'http://services.sapo.pt/broker'
    }

xml_entities = [('&', 'amp'), ('"', 'quot'), ('\'', 'apos'), ('<', 'lt'), ('>', 'gt')]
def escape_xml(input):
    s=input 
    for (look, rep) in xml_entities:
        s=s.replace(look, '&'+rep+';')

    if(isinstance(s, unicode)):
        #coerce everything into utf-8 bytes
        s=s.encode('utf-8')
    return s

DEFAULT_KIND  = 'TOPIC'
#XXX no support for TOPIC_AS_QUEUE (yet)
ALLOWED_KINDS = ('TOPIC', 'QUEUE')

def check_kind(kind):
    if kind not in ALLOWED_KINDS:
        raise AttributeError("Unknown kind '%s'" % kind)

def check_msg(msg):
    if not isinstance(msg, Message):
        raise TypeError("%s is not a subclass of %s.%s" % (repr(msg), Message.__module__, Message.__name__))

soap_open       = """<soap:Envelope xmlns:soap="%s"><soap:Body>""" % (escape_xml(NS['soap']))
soap_close      = """</soap:Body></soap:Envelope>"""

#aux function to pre-build open/close xml tags
def prod_tags(tagname, ns='broker'):
    otag = """%s<%s xmlns="%s">""" % (soap_open, tagname, escape_xml(NS[ns]))
    ctag = """</%s>%s""" % (tagname, soap_close)
    return (otag, ctag)

taglist = (
    ('publish', 'Publish'),
    ('enqueue', 'Enqueue'),
    ('subscribe', 'Notify'),
    #('unsubscribe', 'Unsubscribe'),
    ('acknowledge', 'Acknowledge'))
#pre-built tags
tags = dict( map( lambda (name, tag): (name, prod_tags(tag)) , taglist) )

def build_msg(name, payload):
    (otag, ctag) = tags[name]
    return otag+'\n'+payload+'\n'+ctag

def subscribe_msg(destination, kind, ack=False):
    check_kind(kind)
    return """<DestinationName>%s</DestinationName>\n<DestinationType>%s</DestinationType>\n<AcknowledgeMode>%s</AcknowledgeMode>""" % (
    escape_xml(destination),
    escape_xml(kind),
    ack and 'CLIENT' or 'AUTO'
    )

#aux function for debugging
def str2hex(raw):
    return string.join( ["%02X" % ord(c) for c in raw ], ':')

class Client:
    """
    Abstracts access to a broker server.
    """

    class DisconnectedError(EOFError):
        """
        Class to indicate that the Server disconnected while the client was waiting for a response
        """

        def __init__(self, message):
            EOFError.__init__(self, message)

    def __init__ (self, host, port):
        """
        Constructs a server object to connect to a broker at host:port using the binary TCP protocol
        """

        log.info("Client for %s:%s", host, port)
        self.__mutex_r  = threading.Lock()
        self.__mutex_w  = threading.Lock()
        self.host       = host
        self.port       = port
        self.endpoint   = "%s:%s" % (host, port)
        self.subscribed = set()
        self.__closed   = False

        #first create the socket
        self.__socket = socket.socket( socket.AF_INET, socket.SOCK_STREAM)
        log.debug("Socket timeout  %s s", str(self.__socket.gettimeout()))
        #connect to host:port
        self.__socket.connect((host, port))

    def __lock_w(self):
        """
        Locks the object's read mutex
        """
        log.debug("Thread write locking")
        self.__mutex_w.acquire()
        log.debug("Thread write locked")

    def __unlock_w(self):
        """
        Unlocks the object's read mutex
        """
        log.debug("Thread write unlocking")
        self.__mutex_w.release()
        log.debug("Thread write unlocked")

    def __lock_r(self):
        """
        Locks the object's read mutex
        """
        log.debug("Thread read locking")
        self.__mutex_r.acquire()
        log.debug("Thread read locked")

    def __unlock_r(self):
        """
        Unlocks the object's read mutex
        """
        log.debug("Thread read unlocking")
        self.__mutex_r.release()
        log.debug("Thread read unlocked")

    def __write_raw(self, msg):
        """
        Sends a raw message to the broker.
        """
        log.debug("Sending raw message [%s]", msg)
        
        l = struct.pack("!L", len(msg))
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

    #XXX this function doesn't handle EINTR gracefully, but neither do python's own IO functions so not sure whether it's coherent to do it here
    def __read_len(self, msglen):
        """
        Reads msglen bytes from the server.
        Threadsafe but not EINTR safe (like all python IO?)
        """
        read = ''
        self.__lock_r()
        try:
            while msglen:
                log.debug("Trying to read %d bytes", msglen)
                ret = self.__socket.recv(msglen)
                if '' == ret:
                    raise Client.DisconnectedError("""Broker server at %s is dead. Can't read message data.""" % self.endpoint)
                else:
                    l = len(ret)
                    log.debug('Read %d bytes.', l)
                    read = read + ret
                    msglen = msglen - l
        finally:
            self.__unlock_r()
        return read

    def __read_raw(self):
        """
        Reads and returns the raw message broker notification. (without the length header)
        """
        log.debug("Reading raw message")
        msg_len = struct.unpack("!L", self.__read_len(4))[0]
        log.debug("len = %d", msg_len)
        msg = self.__read_len(msg_len)
        return msg

    def close(self):
        """
        Closes current server object. No other operation should be possible with this object afterwards.
        """
        log.debug("Close")
        if self.__closed:
            log.warn("Trying to close an already closed socket.")
        else:
            #try to cleanup as nicely as possible
            try:
                #signal end of reading and writting to socket
                self.__socket.shutdown(socket.SHUT_RDWR)
                self.__socket.close()
            except Exception, e:
                log.exception(e)

            self.__closed = True

    def __del__(self):
        """
        "Destructor". Tries to do fallback cleanup.
        """
        log.debug("Client.__del__")
        if self.__closed:
            log.info("Destruction on an already closed socket")
        else:
            self.close()

    def subscribe(self, destination, kind=DEFAULT_KIND, acknowledge=False):
        """
        Subscribes for notification for destination with kind either TOPIC or QUEUE.

        acknowledge determines whether the client need to acknowledge the messages it receives.
        By default acknowledgement is automatic so no client action is needed.
        """
        log.info("Client.subscribe (%s, %s)", destination, kind)

        if (destination, kind) in self.subscribed:
            log.warn("destination (%s, %s) already subscribed" %(destination, kind))
        else:
            #send the message 
            self.__write_raw(build_msg('subscribe', subscribe_msg(destination, kind, acknowledge)))
            #append into subscribed endpoints
            self.subscribed.add( (destination, kind) )
            log.debug('Currently subscribed to %s', self.subscribed)

#    def unsubscribe(self, destination, kind=DEFAULT_KIND):
#        """
#        Unsubscribes for notification for destination with kind either TOPIC or QUEUE.
#        """
#        log.info("Client.unsubscribe (%s, %s)", destination, kind)
#
#        if( (destination, kind) in self.subscribed ):
#            self.__write_raw(build_msg('unsubscribe', subscribe_msg(destination, kind)))
#            self.subscribed.remove( (destination, kind) )
#            log.debug('Currently subscribed to %s', self.subscribed)
#        else:
#            log.warn("destination (%s, %s) not subscribed so can't unsubscribe" %(destination, kind))

    def consume(self):
        """
        Wait for a notification and return it as a Message object.
        Blocking call (no timeout).
        """
        log.info("Client.consume")
        content = self.__read_raw()
        return Message.fromXML(content)

    def produce(self, message, kind=DEFAULT_KIND):
        """
        Send a notification to the broker.
        message must be a Message object.
        """
        log.info("Client.produce(%s, %s)", repr(message), kind)
        check_msg(message)
        msg_xml = message.toXML()
        name = {'TOPIC': 'publish', 'QUEUE': 'enqueue'}[kind]
        self.__write_raw(build_msg(name, msg_xml))

    def acknowledge(self, message):
        """
        Acknowledge that the client did receive/process a message.
        message must either be a Message object or an id that one wishes to acknowledge.
        """
        log.info("Client.acknowledge(%s)", repr(message))
        id = None
        if isinstance(message, basestring):
            id = message
        else:
            check_msg(message)
            id = message.id

        msg_xml = """<MessageId>%s</MessageId>""" % escape_xml(id)
        self.__write_raw(build_msg('acknowledge', msg_xml))

    def __iter__(self):
        """
        Syntax sugar to allow iterating over a Consumer and get the received messages.

        This allows for construct such as:

        for message in broker:
            process(message)
        """
        while True:
            yield self.consume()

class Message:
    def __init__(self, payload, destination, id=None, correlationId=None, timestamp=None, expiration=None, priority=None, deliveryMode=None):
        """
        Creates a Broker message given the manadatory payload and destination.
        All other fields are optional.
        
        deliveryMode can either be PERSISTENT or TRANSIENT

        timestamp and expiration are supposed to be datetime objects and default to None and are thus optional.

        id is supposed to be a unique id of the message and defaults to None meaning that the Broker server will generate one automatically.

        correlationId is an identifier supposed to provide logical aggreagtion of messages of differente ids.

        This object should be constructed to send an event notification to the Server and is returned by the Client object when a new event is received.

        Notice regarding Unicode and all text fields (payload, destination, id, and correlationId):
            All text fields can wither be unicode strings (preferably) or regular strings (byte arrays).
            If these fields are unicode strings, then its content is encoded into utf-8 bytes, xml escaped and sent through the network.
            If the filds are regular strings they are only xml-escaped and apart from that are sent "ipis verbis". This can be problematica in case one wishes to sent raw binary information (no character semantics) because this strem might no be valid utf-8 and a decent XML browser will throw an error.

        Bottom line:
            If you don't use unicode strings as input make sure you know what you are doing (utf-8 encode everything)
            If you want to send binary data consider first encoding it to an ASCII string (base64 or uuencode) and then send these characters.
        """

        self.payload      = payload
        self.destination  = destination
        self.deliveryMode = deliveryMode
        self.id           = id
        self.timestamp    = timestamp
        self.expiration   = expiration
        self.priority     = priority

    def fromXML_minidom(raw):
        """
        Given an xml representing the message returns an object describing it
        """
        #now try and parse the actual parameters
        #XXX no need to worry about date data for the time being
        dom = parseString(raw)
        id          = getMessageData(dom, 'MessageId', 'broker')
        priority    = getMessageData(dom, 'Priority', 'broker', int)
        destination = getMessageData(dom, 'DestinationName', 'broker')
        payload     = getMessageData(dom, 'TextPayload', 'broker')

        #XXX what to do with action?
        #XXX process all date time fields into nice python objects
        return Message(payload=payload, destination=destination, id=id, priority=priority)

    #generate a static method
    fromXML = staticmethod(fromXML_minidom)

    def toXML(self):
        """
        Serializes the message to XML
        """
        ret = '<BrokerMessage>\n'

        for(tname, attr, fun) in ( 
            ('DestinationName', 'destination', None),
            ('MessageId', 'id', None),
            ('TextPayload', 'payload', None),
            ('Priority', 'priority', lambda x : str(x))
        ):
            fun = {None: lambda x:x}.get(fun, fun)

            content = getattr(self, attr, None)

            if content is None:
                #do not output the tag
                #could just place it empry
                pass
            else:
                content = fun(content)
                ret+= "\t<%(tname)s>%(content)s</%(tname)s>\n" % {'tname':tname, 'content':escape_xml(content)}

        ret += '</BrokerMessage>'
        return ret

    def __repr__(self):
        """
        Just returns its id.
        Subclasses should probably add more relevant data for their usage.
        """
        return """<%s{ id : %s }>""" % (self.__class__, repr(self.id))

    #just return the actual payload
    def __unicode__(self):
        """
        Syntax sugar that returns the payload of the message
        """
        return self.payload

    def __str__(self):
        return unicode(self).encode('utf-8')

def getMessageData(dom, key, ns=None, fun = lambda x : x):
    """
    Auxiliary function for parsing XML.
    Given a dom tree, a tag name, a namespace and a transformation function, returns the result of aplying it to the text content of the node or None in case of error.
    """
    try:
        ret = None
        if ns:
            ret = dom.getElementsByTagNameNS(NS[ns], key).item(0).childNodes[0].nodeValue
        else:
            ret = dom.getElementsByTagName(key).item(0).childNodes[0].nodeValue
        return fun(ret)
    except:
        return
