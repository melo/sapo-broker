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
  require 'rexml/document'

  class Message
    attr_accessor(:destination, :payload, :id, :correlation_id, :timestamp, :expiration, :priority)

    def xml_escape(text)
      t = REXML::Text.new('')
      str = ''
      t.write_with_substitution(str, text)
      str
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
    
    def from_xml(xml)
      begin
        doc = REXML::Document.new(xml)
      rescue REXML::ParseException
        raise ArgumentError
      end
      
      REXML::XPath.each(doc, "//mq:BrokerMessage/*", {'mq' => 'http://services.sapo.pt/broker'}) do |elem|
        case elem.name
        when 'Priority' then self.priority = elem.text
        when 'MessageId' then self.id = elem.text
        when 'Timestamp' then self.timestamp = elem.text
        when 'Expiration' then self.expiration = elem.text
        when 'DestinationName' then self.destination = elem.text
        when 'TextPayload' then self.payload = elem.text
        when 'CorrelationId' then self.correlation_id = elem.text
        end
      end
      self
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

      @server_list = server_list
      @sock = nil
      reconnect()

    end

    def _receive
      begin
        msg_len = @sock.recv(4).unpack('N')[0]
        sick_socket?(msg_len)
        @logger.debug("Will receive message of %i bytes" % msg_len)

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
        
        @logger.debug("Got message %s" % xml)
        message = Message.new.from_xml(xml)
        ack(message) if @sub_map.has_key?(message.destination) && 
          @sub_map[message.destination][:type] != 'TOPIC' &&
          @sub_map[message.destination][:ack_mode] == 'AUTO'
        message
      rescue SystemCallError => ex
        @logger.error("Problems receiving event: #{ex.message}")
        reconnect
        retry
      rescue ArgumentError
        @logger.error('Problems parsing message')
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
          @logger.debug("Connected to #{host} on port #{port}")
          @sub_map.each_pair do |destination, params|
            @logger.debug("Re-subscribing to #{destination}")
            subscribe(destination, params[:type], params[:ack_mode])
            @logger.debug("Subscribed to to #{destination}")
          end
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
      
      evt_msg = [evt_msg.length].pack('N') + evt_msg
      begin
        @sock.write(evt_msg)
      rescue SystemCallError => ex
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
