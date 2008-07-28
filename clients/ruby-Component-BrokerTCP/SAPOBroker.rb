# -*- coding: iso-8859-1 -*-
# Ruby module to interact with the SAPO Broker
# Author: André Cruz <andre.cruz@co.sapo.pt>

# WARNING: This API is NOT threadsafe!
# PRODUCER EXAMPLE:
# require 'SAPOBroker'

# client = SAPOBroker::Client.new(["10.135.5.110:2222"])

# 10000.times do |i|
#   msg = SAPOBroker::Message.new
#   msg.destination = '/blogoscopio/test'
#   msg.payload = "Message N. %i" % i
#   client.enqueue(msg)
# end

# CONSUMER EXAMPLE:
# require 'SAPOBroker'

# client = SAPOBroker::Client.new(["10.135.5.110:2222"])
# client.subscribe('/blogoscopio/test', 'QUEUE')

# 10000.times do |i|
#   msg = client.receive
# end

# ASYNC CONSUMER EXAMPLE:
# require 'SAPOBroker'

# client = SAPOBroker::Client.new(["10.135.5.110:2222"])
# client.subscribe('/blogoscopio/test', 'QUEUE')

# thr = client.receive do |msg|
#     puts msg.payload
#   end
# # Go on with your life
# thr.join

module SAPOBroker

  require 'socket'
  require 'rubygems'

  class Message
    attr_accessor(:destination, :payload, :id, :correlation_id, :timestamp, :expiration, :priority)
    
    if RUBY_PLATFORM =~ /java/
      include Java
      import java.io.ByteArrayInputStream
      import javax.xml.stream.XMLInputFactory
      import javax.xml.stream.XMLStreamConstants
      import javax.xml.stream.XMLStreamException
      import javax.xml.stream.XMLStreamReader

      @@x_factory ||= XMLInputFactory.newInstance

      def from_xml(xml)

        begin
          stax = @@x_factory.createXMLStreamReader(ByteArrayInputStream.new(xml.to_java_bytes), 'UTF-8')
          evt_type = stax.getEventType()

          begin
            if evt_type == XMLStreamConstants::START_ELEMENT
              name = stax.getLocalName()
              case name
              when 'Priority' then self.priority = stax.getElementText()
              when 'MessageId' then self.id = stax.getElementText()
              when 'Timestamp' then self.timestamp = stax.getElementText()
              when 'Expiration' then self.expiration = stax.getElementText()
              when 'DestinationName' then self.destination = stax.getElementText()
              when 'TextPayload' then self.payload = stax.getElementText()
              when 'CorrelationId' then self.correlation_id = stax.getElementText()
              end
            end
            evt_type = stax.next
          end while evt_type != XMLStreamConstants::END_DOCUMENT
            
        rescue Exception => ex
          raise ArgumentError, ex.message
        end

        raise ArgumentError, "ID and destination not present" unless self.id && self.destination
        self
      end

    else
      require 'xml/libxml'

      @@x_parser ||= XML::Parser.new

      def from_xml(xml)

        begin
          @@x_parser.string = xml
          doc = @@x_parser.parse

          doc.find('//mq:BrokerMessage/*', 'mq:http://services.sapo.pt/broker').each do |elem|
            case elem.name
            when 'Priority' then self.priority = elem.content
            when 'MessageId' then self.id = elem.content
            when 'Timestamp' then self.timestamp = elem.content
            when 'Expiration' then self.expiration = elem.content
            when 'DestinationName' then self.destination = elem.content
            when 'TextPayload' then self.payload = elem.content
            when 'CorrelationId' then self.correlation_id = elem.content
            end
          end          
        rescue Exception => ex
          raise ArgumentError, ex.message
        end

        raise ArgumentError, "ID and destination not present" unless self.id && self.destination
        self
      end
      
    end

    def xml_escape(input)

      specials = [ /&(?!#?[\w-]+;)/u, /</u, />/u, /"/u, /'/u, /\r/u ]
      substitutes = ['&amp;', '&lt;', '&gt;', '&quot;', '&apos;', '&#13;']
      copy = input.clone
      # Doing it like this rather than in a loop improves the speed
      copy.gsub!( specials[0], substitutes[0] )
      copy.gsub!( specials[1], substitutes[1] )
      copy.gsub!( specials[2], substitutes[2] )
      copy.gsub!( specials[3], substitutes[3] )
      copy.gsub!( specials[4], substitutes[4] )
      copy.gsub!( specials[5], substitutes[5] )
      copy
    end

    def to_xml()
      # return xml representation of broker message
      message = ""
      message << '<BrokerMessage>'
      message << "<DestinationName>#{self.destination}</DestinationName>"
      message << "<TextPayload>#{xml_escape(self.payload)}</TextPayload>"
      message << "<Priority>#{self.priority}</Priority>" if self.priority
      message << "<MessageId>#{self.id}</MessageId>" if self.id
      message << "<Timestamp>#{self.timestamp}</Timestamp>" if self.timestamp
      message << "<Expiration>#{self.expiration}</Expiration>" if self.expiration
      message << "<CorrelationId>#{self.correlation_id}</CorrelationId>" if self.correlation_id
      message << '</BrokerMessage>'
    end

  end
  
  class Client
    
    def disconnect
      @sock.close
    end

    def unsubscribe(dest_name)
      return unless @sub_map.has_key?(dest_name)

      sub_msg = <<END_SUB
<soapenv:Envelope xmlns:soapenv='http://www.w3.org/2003/05/soap-envelope'><soapenv:Body>
<Unsubscribe xmlns='http://services.sapo.pt/broker'>
<DestinationName>#{dest_name}</DestinationName>
<DestinationType>#{@sub_map[dest_name][:type]}</DestinationType>
</Unsubscribe>
</soapenv:Body></soapenv:Envelope>
END_SUB

    sub_msg = [sub_msg.length].pack('N') + sub_msg
    @sock.write(sub_msg)
    @sub_map.delete(dest_name)
    end

    def poll(queues)
      queues.each do |queue|

        @logger.debug("Polling #{queue}")
        poll_msg = <<END_POLL
<?xml version='1.0' encoding='UTF-8'?>
<soap:Envelope xmlns:soap='http://www.w3.org/2003/05/soap-envelope' xmlns:mq='http://services.sapo.pt/broker'>
<soap:Body>
<mq:Poll>
<mq:DestinationName>#{queue}</mq:DestinationName>
</mq:Poll>
</soap:Body>
</soap:Envelope>
END_POLL

        poll_msg = [poll_msg.length].pack('N') + poll_msg
        @sock.write(poll_msg)
        @poll_map[queue] = 1
        @logger.debug("Polled #{queue}")
      end
    end

    def subscribe(dest_name, type = 'QUEUE', ack_mode = 'AUTO')
      sub_msg = <<END_SUB
<soapenv:Envelope xmlns:soapenv='http://www.w3.org/2003/05/soap-envelope'><soapenv:Body>
<Notify xmlns='http://services.sapo.pt/broker'>
<DestinationName>#{dest_name}</DestinationName>
<DestinationType>#{type}</DestinationType>
</Notify>
</soapenv:Body></soapenv:Envelope>
END_SUB

      sub_msg = [sub_msg.length].pack('N') + sub_msg
      @sock.write(sub_msg)

      @sub_map[dest_name] = {:type => type, :ack_mode => ack_mode} unless @sub_map.has_key?(dest_name)
    end

    def ack(message)
      ack_msg = <<END_ACK
<soapenv:Envelope xmlns:soapenv="http://www.w3.org/2003/05/soap-envelope"><soapenv:Body>
<Acknowledge xmlns="http://services.sapo.pt/broker">
<MessageId>#{message.id}</MessageId>
<DestinationName>#{message.destination}</DestinationName>
</Acknowledge>
</soapenv:Body></soapenv:Envelope>
END_ACK
      @logger.debug("Will send ACK %s" % ack_msg) if @logger.debug?
      ack_msg = [ack_msg.length].pack('N') + ack_msg
      @sock.write(ack_msg)
    end

    def enqueue(message)
      send_event('Enqueue', message)
    end

    def publish(message)
      send_event('Publish', message)
    end

    def receive
      if block_given?
        # we want assynchronous behaviour
        Thread.new do
          loop do
            yield _receive
          end
        end
      else
        _receive
      end
    end

    private
    def initialize(server_list, logger = nil)
      
      if logger
        @logger = logger
      else
        require 'logger'
        @logger = Logger.new(STDOUT)
      end

      @sub_map = {}
      @poll_map = {}

      @server_list = server_list
      @sock = nil
      reconnect()

    end

    def _receive
      begin
        msg_len = @sock.recv(4).unpack('N')[0]
        sick_socket?(msg_len)
        @logger.debug("Will receive message of %i bytes" % msg_len) if @logger.debug?

        xml = ""
        while msg_len > 0
          block = @sock.recv(msg_len)
          msg_len -= block.length
          xml << block
        end
        
        unless msg_len.zero?
          msg = "Could get the correct number of message bytes. DIFF = #{msg_len}. Reconnecting..."
          @logger.error(msg)
          raise Errno::EAGAIN, msg
        end
        
        @logger.debug("Got message %s" % xml) if @logger.debug?
        message = Message.new.from_xml(xml)
        ack(message) unless @sub_map.has_key?(message.destination) && 
          @sub_map[message.destination][:type] != 'TOPIC' &&
          @sub_map[message.destination][:ack_mode] != 'AUTO'
        @poll_map.delete(message.destination) if @poll_map.has_key?(message.destination)
        message
      rescue SystemCallError => ex
        @logger.error("Problems receiving event: #{ex.message}")
        reconnect
        retry
      rescue ArgumentError => ex
        @logger.error("Problems parsing message: #{ex.message}")
        reconnect
        retry
      end
    end

    def reconnect
      @sock.close unless @sock.nil? || @sock.closed?
      @server_list.sort_by {rand}.each do |server|
        host, port = server.split(/:/)
        port = 3322 if port.nil?

        begin
          @logger.debug("Trying #{host} on port #{port}")
          @sock = TCPSocket.new(host, port)
          @sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_KEEPALIVE, true)
          @logger.debug("Connected to #{host} on port #{port}")
          @sub_map.each_pair do |destination, params|
            @logger.debug("Re-subscribing to #{destination}")
            subscribe(destination, params[:type], params[:ack_mode])
            @logger.debug("Subscribed to to #{destination}")
          end
          @poll_map.each_key {|queue| poll(queue)}
          return
        rescue StandardError => ex
          @logger.warn("Problems (#{server}): #{ex.message}")
        end
      end

      raise IOError, "All servers are down"
    end

    def send_event(msg_type, message)
      evt_msg = <<END_EVT
<soapenv:Envelope xmlns:soapenv="http://www.w3.org/2003/05/soap-envelope"><soapenv:Body>
<#{msg_type} xmlns="http://services.sapo.pt/broker">
#{message.to_xml}
</#{msg_type}>
</soapenv:Body></soapenv:Envelope>
END_EVT

      @logger.debug("Will send message %s" % evt_msg) if @logger.debug?
      evt_msg = [evt_msg.length].pack('N') + evt_msg
      begin
        @sock.write(evt_msg)
      rescue StandardError => ex
        @logger.error("Problems sending event: #{ex.message}")
        reconnect
        retry
      end
    end

    def sick_socket?(len)
      if len.nil? || (len == 0 && (@sock.closed? || @sock.eof?)) then
        raise Errno::EAGAIN, "Problems with the socket. Reconnect and try again."
      end
    end
    
  end

end
