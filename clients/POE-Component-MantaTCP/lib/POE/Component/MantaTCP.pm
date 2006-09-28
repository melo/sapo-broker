package POE::Component::MantaTCP;

use version; $VERSION = qv('0.9');

use warnings;
use strict;
use bytes;
use Carp qw( croak );
use Sys::Hostname qw( hostname );

use XML::LibXML;
use XML::LibXML::XPathContext;
use URI::Escape;

use POE;
use POE::Component::Client::TCP;
use POE::Filter::Block;


sub spawn {
  my ($class, %args) = @_;

  my $app   = delete $args{AppName}   || delete $args{app_name};
  my $port  = delete $args{Port}      || delete $args{port}        || $ENV{POE_MANTATCP_PORT}     || 2222;
  my $host  = delete $args{Hostname}  || delete $args{hostname}    || $ENV{POE_MANTATCP_HOSTNAME} || '127.0.0.1';
  my $alias = delete $args{Alias}     || delete $args{alias}       || 'MantaTCP';
  my $callb = delete $args{OnConnect} || delete $args{on_connect}; # TODO: we need a OnError
  my $stats = delete $args{Stats}     || delete $args{stats}       || 0;
  my $vrbos = delete $args{Verbose}   || delete $args{verbose}     || $ENV{POE_MANTATCP_VERBOSE}  || 0;

  if (%args) {
    croak "FATAL: the following parameters where not understood: '"
        . join("', '", keys %args) . "'. Check you code, ";
  }

  POE::Component::Client::TCP->new(
      RemoteAddress => $host,
      RemotePort    => $port,

      Alias         => $alias,

      ConnectTimeout => 10,

      Filter       => [ 'POE::Filter::Block', LengthCodec => [ \&_bus_encoder, \&_bus_decoder ] ],

      Started      => \&_started,
      Args         => [ $host, $port, $app, $callb, $vrbos, $stats ],
      
      Connected    => \&_connected,
      Disconnected => \&_disconnected,

      ServerInput  => \&_message_from_manta_tcp,

      ConnectError => \&_connect_error,
      ServerError  => \&_server_error,


      InlineStates => {
          subscribe => \&_subscribe,
          publish   => \&_publish,
          stats     => \&_stats,
          
          monitoring_start  => \&_monitoring_start,
          monitoring_action => \&_monitoring_action,
          monitoring_ping   => \&_monitoring_ping,
          monitoring_check  => \&_monitoring_check,
      },
  );
  
  return bless \$alias, $class;
}


#####################
# Object-oriented API

sub subscribe {
  my $self = shift;
  
  $poe_kernel->post($$self, 'subscribe', @_);
}


sub publish {
  my $self = shift;
  
  $poe_kernel->post($$self, 'publish', @_);
}


sub notify {
  my ($self, %args) = @_;

  # Create notification header and timestamp it
  my @ts = reverse((gmtime())[0..5]);
  $ts[1]++; $ts[0] += 1900;
  my $notification = "<notification xmlns='http://uri.sapo.pt/schemas/sapo/notification.xsd'>";
  $notification .= '<timestamp>'.sprintf("%0.4d%0.2d%0.2d%0.2d%0.2d%0.2d", @ts).'</timestamp>';

  # Add involved entities
  my $has_entities = 0;
  foreach my $role (qw( source actor victim )) {
    next unless exists $args{$role};
    $notification .= '<entities>' if ! $has_entities;
    $has_entities++;
    $notification .= "<entity role='$role'>";
    my $ids = $args{$role};
    $ids = [ $ids ] unless ref $ids eq 'ARRAY';
    foreach my $id (@$ids) {
      $notification .= "<$id->{prefix}:$id->{tag} xmlns:$id->{prefix}='$id->{ns}'>"
          . _xml_escape($id->{value}) . "</$id->{prefix}:$id->{tag}>";
    }
    $notification .= "</entity>";
  }
  $notification .= "</entities>" if $has_entities;

  # Add the main event
  my $event_payload = $args{xml_payload} || $args{event}; # XML payload
  $event_payload ||= _xml_escape($args{text_payload});
  $event_payload ||= _build_simple_xml_doc($args{perl_payload});
  $notification .= "<event>".$event_payload."</event>" if $event_payload;

  # If we have a plaintext version, use it
  if ($args{text} || $args{title} || $args{uri}) {
    $args{uri} = _expand_uri($args{uri});
    $notification .= "<summary>";
    foreach my $field (qw( title text uri )) {
      $notification .= "<$field>"._xml_escape($args{$field})."</$field>" if $args{$field};      
    }
    $notification .= "</summary>";    
  }

  # and we are done
  $notification .= '</notification>';

  $poe_kernel->post($$self, 'publish', topic => $args{topic}, payload => $notification);
}


sub start {
  $poe_kernel->run;
}


sub xml_escape {
  return _xml_escape($_[1]);
}


######################################
# Connection and reconnection handlers

sub _started {
  my ($kernel, $heap, $session, $host, $port, $app_name, $on_connect, $verbose, $stats) = 
  @_[ KERNEL,  HEAP,  SESSION,  ARG0,  ARG1,  ARG2,      ARG3,        ARG4,     ARG5 ];
  
  $heap->{on_connect} = $on_connect if $on_connect;
  $heap->{verbose}    = $verbose;
  $heap->{stats}      = $stats;
  $heap->{connected}  = 0;
  $heap->{host} = $host;
  $heap->{port} = $port;
  $heap->{subs} = [];
  $heap->{pubs} = [];

  $kernel->yield('monitoring_start');
}


sub _connected {
  my ($kernel, $heap, $host, $port) = @_[KERNEL, HEAP, ARG1, ARG2];
  
  $heap->{connected} = 1;
  
  _log($heap, "Connected to '$heap->{host}' port $heap->{port}");
  _monitoring_counter($heap, 'connect');
  
  if ($heap->{on_connect}) {
    $heap->{on_connect}->($host, $port);
  }
  
  foreach my $queue (['subs', 'subscribe'], ['pubs', 'publish']) {
    my $pending = $heap->{$queue->[0]};
    next unless @$pending;
    my $event = $queue->[1];
  
    while (@$pending) {
      $kernel->yield($event, %{ shift(@$pending) });
    }
  }
}


sub _connect_error {  
  my ($kernel, $heap, $syscall, $errno, $errstr) = @_[KERNEL, HEAP, ARG0, ARG1, ARG2];
  
  _log($heap, "Connect error, will reconnect: $syscall/$errno/$errstr");
  _monitoring_counter($heap, 'connect_error', "$syscall/$errno/$errstr");
  
  $heap->{connected} = 0;
  # TODO: trigger a callback
  # TODO: reconnect should be an option, probably.

  $kernel->delay( reconnect => 1 );
}


sub _disconnected {
  my ($kernel, $heap) = @_[KERNEL, HEAP];
  
  _log($heap, "Disconnected from MantaTCP agent");
  _monitoring_counter($heap, 'disconnect');

  $heap->{connected} = 0;
  # TODO: como lidar com um shutdown limpo?
  # TODO: trigger a callback
  # TODO: reconnect should be an option, probably.
  
  $kernel->delay( reconnect => 1 );
}


sub _server_error {
  my ($kernel, $heap, $syscall, $errno, $errstr) = @_[KERNEL, HEAP, ARG0, ARG1, ARG2];
  
  _log($heap, "Server error, will reconnect: $syscall/$errno/$errstr");
  _monitoring_counter($heap, 'server_error', "$syscall/$errno/$errstr");

  $heap->{connected} = 0;
  # TODO: trigger a callback
  # TODO: reconnect should be an option, probably.

  $kernel->delay( reconnect => 1 );
}


########################################
# Receive and notify messages from Manta

sub _message_from_manta_tcp {
  my ($heap, $data) = @_[ HEAP, ARG0 ];
  
  _monitoring_counter($heap, 'incoming');
  
  my $event = _parse_soap_message(
      $data, 'http://services.sapo.pt/broker', 'BrokerMessage',
      qw( DeliveryMode Priority MessageId Timestamp Expiration DestinationName TextPayload )
  );
  return unless $event;

  # Map to perl names
  my $topic = $event->{topic} = delete $event->{DestinationName};
  $event->{priority}          = delete $event->{Priority};
  $event->{message_id}        = delete $event->{MessageId};
  $event->{timestamp}         = delete $event->{Timestamp};
  $event->{expiration}        = delete $event->{Expiration};
  $event->{payload}           = delete $event->{TextPayload};
  $event->{raw_soap_message} = $data;
  if (delete $event->{DeliveryMode} eq 'PERSISTENT') {
    $event->{persistent} = 1;
    $event->{transient}  = 0;
  }
  else {
    $event->{persistent} = 0;
    $event->{transient}  = 1;
  }
  
  # Find callback for this event and send it
  
  my $event_as_xml;
  foreach my $rule (@{$heap->{rules}}) {
    next unless $topic =~ /$rule->[0]/;
    my $info = $rule->[1];
    if ($info->{is_xml}) {
      $event->{xml} ||= eval { _parse_with_libxml($event->{payload}) };
      $event->{xml_errors} = $@ || $!;
    }
    $info->{callback}->($event);      
  }

  _monitoring_counter($heap, 'incoming_accepted', $topic);
}


####################
# Subscribe a topic

sub _subscribe {
  my ($heap, %args) = @_[ HEAP, ARG0..$#_ ];
  my $topic = $args{topic};

  # Allow future re-subscribe in future reconnects
  push @{$heap->{subs}}, \%args;

  if (! $heap->{connected}) {
    _monitoring_counter($heap, 'subscribe_delayed', $topic);  
    return;
  }

  my $regexp = $topic;
  $regexp =~ s/\*/[^\/]+?/g;
  $regexp =~ s/\#/.+?/g;
  
  push @{$heap->{rules}}, [ qr/^$regexp$/, \%args ];
  
  $args{type} ||= 'TOPIC';
  
  my $mesg = qq{<soapenv:Envelope xmlns:soapenv="http://www.w3.org/2003/05/soap-envelope"><soapenv:Body>};
  $mesg .= qq{<Notify xmlns="http://services.sapo.pt/broker">};
  $mesg .= "<DestinationName>"._xml_escape($topic)."</DestinationName>";
  $mesg .= "<DestinationType>"._xml_escape($args{type})."</DestinationType>";
  
#  if ($args{durable_name}) {
#    $mesg .= qq{<def:durableSubscriptionName>$args{durable_name}</def:durableSubscriptionName>};
#  }
 
  if ($args{manual_ack}) {
    $mesg .= q{<AcknowledgeMode>CLIENT</AcknowledgeMode>};
  }
  
  $mesg .= q{</Notify></soapenv:Body></soapenv:Envelope>};

  $heap->{server}->put($mesg);
  
  _log($heap, "Subscribed topic '$topic'");
  _monitoring_counter($heap, 'subscribed', $topic);
}


sub _publish {
  my ($heap, %args) = @_[ HEAP, ARG0..$#_ ];
  my $topic = $args{topic};
  
  # TODO: we should validate our argments better

  if (! $heap->{connected}) {
    # TODO: deal with lot's of pending pubs
    push @{$heap->{pubs}}, \%args;
    _monitoring_counter($heap, 'publish_delayed', $topic);
    return;
  }

  my $mesg = qq{<soapenv:Envelope xmlns:soapenv="http://www.w3.org/2003/05/soap-envelope"><soapenv:Body>};
  $mesg   .= qq{<Publish xmlns="http://services.sapo.pt/broker"><BrokerMessage>};
  
  if ($args{persistent}) {
    $mesg .= qq{<DeliveryMode>PERSISTENT</DeliveryMode>};
  }
  elsif ($args{transient}) {
    $mesg .= qq{<DeliveryMode>TRANSIENT</DeliveryMode>};
  }
  
  if ($args{priority}) {
    $mesg .= "<Priority>"._xml_escpae($args{priority})."</Priority>";
  }
  
  if ($args{message_id}) {
    $mesg .= "<MessageId>"._xml_escape($args{message_id})."</MessageId>";
  }
  
  $mesg .= "<DestinationName>"._xml_escape($topic)."</DestinationName>";
  $mesg .= "<TextPayload>"._xml_escape($args{payload})."</TextPayload>";
  $mesg .= q{</BrokerMessage></Publish></soapenv:Body></soapenv:Envelope>};

  $heap->{server}->put($mesg);
  _monitoring_counter($heap, 'published', $topic);
}


###################################
# Active monitoring of Cloud Health

sub _monitoring_start {
  my ($heap, $session, $kernel) = @_[ HEAP, SESSION, KERNEL ];
  
  $heap->{mon_check_intv} = 15;
  $heap->{mon_cntrs} = {};
  
  if (!$heap->{app_name}) {
    my $hostname = eval { hostname() };
    $hostname ||= 'NO_HOST_FOUND';
    my ($basename) = $0 =~ m{([^/]+)$};
    $heap->{app_name} = join('/', $hostname, $basename, $$);
  }
  
  my $callback = $session->postback('monitoring_action');
  $kernel->yield('subscribe', topic => "/system/action/$heap->{app_name}", callback => $callback);
  $kernel->yield('subscribe', topic => "/system/action",           callback => $callback);
  
  $kernel->yield('subscribe', topic => "/system/ping", callback => $session->postback('monitoring_ping'));
  
  $kernel->delay_set('monitoring_check', $heap->{mon_check_intv});
}


sub _monitoring_action {
  my ($event, $session, $kernel) = @_[ARG1, SESSION, KERNEL];
  my $payload = $event->[0]{payload};
  
  my $sx = eval { _parse_with_libxml($payload) };
  if (! $sx) {
    print STDERR "ERROR: payload '$payload' is invalid XML: ".($@ || $!)."\n";
    return;
  }
  $sx->registerNs(a => 'http://uri.sapo.pt/schemas/broker/action.xsd');
  
  # we only know how to deal with stats_refresh for now
  my $action = $sx->findnode('//a:stats_refresh');
  return unless $action;
  
  my $request_id = $sx->findvalue('//a:request_id', $action);
  $kernel->yield('monitoring_check', $request_id);
}


sub _monitoring_ping {
  my ($event, $session, $kernel, $heap) = @_[ARG1, SESSION, KERNEL, HEAP];
  my $payload = $event->[0]{payload};
  
  my $sx = eval { _parse_with_libxml($payload) };
  if (! $sx) {
    print STDERR "ERROR: payload '$payload' is invalid XML: ".($@ || $!)."\n";
    return;
  }
  $sx->registerNs(a => 'http://uri.sapo.pt/schemas/broker/ping.xsd');
  
  my $tracker = $sx->findvalue('//a:ping/a:tracker');
  return unless $tracker;
  my $timestamp = $sx->findvalue('//a:ping/a:timestamp');

  $heap->{mon_trackers}{$tracker}{remote} = $timestamp;
  $heap->{mon_trackers}{$tracker}{local}  = time();
}


sub _monitoring_check {
  my ($heap, $kernel, $request_id) = @_[ HEAP, KERNEL, ARG0 ];
  my $stats = $heap->{stats};
  my $app_name = $heap->{app_name};
  my $ctrs = $heap->{mon_ctrs};
  
  my $now = time();
  my $interval = $now - ($heap->{mon_last_check} || $^T);
   
  my $xml = q{<stats xmlns='http://uri.sapo.pt/schemas/broker/stats.xsd'>};
  print STDERR "STATS REPORT BEGIN for '$app_name' at ".localtime($now)."\n" if $stats;
  
  $xml .= "<app_name>"._xml_escape($app_name)."</app_name>";
  $xml .= "<timestamp>$now</timestamp>";
  $xml .= '<uptime>'.($now-$^T).'</uptime>';
  $xml .= "<interval>$interval</interval>";
  
  if (%$ctrs) {
    $xml .= '<internal_counters>';
    foreach my $event (sort keys %$ctrs) {
      my $data = $ctrs->{$event};
      my $delta = $data->{delta} || 0;
      my $total = $data->{total} || 0;
      my $delay = $now - ($data->{stamp} || $^T);

      $xml .= "<counter>";
      $xml .= "<name>"._xml_escape($event)."</name>";
      $xml .= "<total>$total</total>";
      $xml .= "<delta>$delta</delta>";
      $xml .= "<delay>$delay</delay>";
      $xml .= "</counter>";

      if ($stats) {
        my $hit_per_sec = $delta? sprintf('%0.2f', $delta/$interval) : 0;
        print STDERR "STATS: event '$event' - total $total"
          . ", delta $delta last $interval seconds ($hit_per_sec/sec)\n";
      }    
    }
    $xml .= '</internal_counters>';
  }

  # -- Per topic counters
  # First collect the topics
  my %topics_reported;
  my @events = qw( incoming_accepted subscribe_delayed subscribed publish_delayed published );
  foreach my $event (@events) {
    my $topics = $ctrs->{$event}{sub} || {};
    foreach my $topic (keys %$topics) {
      $topics_reported{$topic}++;
    }
  }
  # Then, if we have them, report them
  if (%topics_reported) {
    $xml .= '<topics>';
    foreach my $topic (keys %topics_reported) {
      $xml .= '<topic><name>'._xml_escape($topic).'</name><counters>';
      foreach my $event (@events) {
        next unless exists $ctrs->{$event}{sub}{$topic};
        my $data = $ctrs->{$event}{sub}{$topic};
        my $delta = $data->{delta} || 0;
        my $total = $data->{total} || 0;
        my $delay = $now - ($data->{stamp} || $^T);

        $xml .= "<counter>";
        $xml .= "<name>"._xml_escape($event)."</name>";
        $xml .= "<total>$total</total>";
        $xml .= "<delta>$delta</delta>";
        $xml .= "<delay>$delay</delay>";
        $xml .= "</counter>";
      }
      $xml .= '</counters></topic>';
    }
    $xml .= '</topics>';
  }
    
  # if we use ping trackers, report them
  my $trackers = $heap->{mon_trackers} || {};
  if (%$trackers) {
    $xml .= '<trackers>';
    foreach my $tracker (keys %$trackers) {
      $xml .= '<tracker><name>'._xml_escape($tracker).'</name>';
      $xml .= '<remote>'._xml_escape($trackers->{$tracker}{remote}).'</remote>';
      $xml .= '<delay>'.($now-$trackers->{$tracker}{local}).'</delay>';
      $xml .= '</tracker>';
    }
    $xml .= '</trackers>';
  }
  $xml .= '</stats>';
  print STDERR "STATS REPORT END\n" if $stats;
  
  # Send out our stats
  $kernel->yield('publish', topic => '/system/stats', payload => $xml);
  
  # reset delta's and reset alarm for timer-based requests
  if (!$request_id) {
    $heap->{mon_last_check} = $now;
    foreach my $event_data (values %{$heap->{mon_ctrs}}) {
      $event_data->{delta} = 0;
      foreach my $sub_event_data (values %{$event_data->{sub}}) {
        $sub_event_data->{delta} = 0;
      }
    }
    print STDERR "STATS: Next update at ".localtime($now+$heap->{mon_check_intv})."\n" if $stats;
    $kernel->delay_set('monitoring_check', $heap->{mon_check_intv});
  }
}


sub _monitoring_counter {
  my ($heap, $event, $sub_key) = @_;
  my $now = time();
  
  my $ctrs = $heap->{mon_ctrs}{$event} ||= {};

  $ctrs->{total}++;
  $ctrs->{delta}++;
  $ctrs->{stamp} = $now;
  $ctrs->{sub} ||= {};

  if ($sub_key) {
    $ctrs->{sub}{$sub_key}{total}++;
    $ctrs->{sub}{$sub_key}{delta}++;    
    $ctrs->{sub}{$sub_key}{stamp} = $now;
  }
}


#######################
# Log stuff, optionally

sub _log {
  my ($cfg, $mesg) = @_;
  
  print STDERR "$mesg\n" if $cfg->{verbose};
}


#######################################
# Expand a hash ref to a full blown URI

sub _expand_uri {
  my ($uri) = @_;
  
  return $uri unless ref $uri;
  
  my $text = delete $uri->{_base} || '';
  return $text unless %$uri;
  
  return $text . '?' . join('&', map { "$_=".uri_escape($uri->{$_}) } grep { defined $uri->{$_} } keys %$uri);
}


########################################################
# Build a simple XML snippet based on a hash ref of data

sub _build_simple_xml_doc {
  my ($data) = @_;
  
  return undef unless $data;
  
  my $tag = delete $data->{_tag};
  my $ns  = delete $data->{_xmlns};
  
  my $xml = "<$tag xmlns='$ns'";
  return "$xml />" unless %$data;
  $xml .= '>';

  while (my ($field, $value) = each %$data) {  
    next unless defined $value;
    next if $field =~ /^_/;
    
    if (length($value)) {
      $xml .= "<$field>"._xml_escape($value)."</$field>";      
    }
    else {
      $xml .= "<$field />";
    }
  }
  $xml .= "</$tag>";
  
  return $xml;
}


######################################
# YAXEC: Yet Another XML Escaping Code

sub _xml_escape {
  # fast path for the commmon case:
  return $_[0] unless $_[0] =~ /[&\"\'<>\x00-\x08\x0B\x0C\x0E-\x1F]/;
  # what are those character ranges? XML 1.0 allows:
  # #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF]
  
  my $xml = shift;
  $xml =~ s/\&/&amp;/g;
  $xml =~ s/\"/&quot;/g;
  $xml =~ s/\'/&apos;/g;
  $xml =~ s/</&lt;/g;
  $xml =~ s/>/&gt;/g;
  $xml =~ s/[\x00-\x08\x0B\x0C\x0E-\x1F]//g;
  return $xml;
}


##############################################
# Encoding rules when talking to the Manta Bus
# 
# encoding is 4 byte lenght, network order, followed by lenght-data bytes
# 

sub _bus_encoder {
  my $buffer = shift;
  
  # Not black magic: substr as a lvalue can be used to prepend the length of the buffer :)
  substr($$buffer, 0, 0) = pack('N', length($$buffer));
  return;
}

sub _bus_decoder {
  my $buffer = shift;
  
  # and now the other way around: remove 4 bytes from buffer and convert from network order
  return if length($$buffer) < 4;
  return unpack('N', substr($$buffer, 0, 4, ''));
}


#############################
# SOAP parsing, using raw LibXML2

sub _parse_soap_message {
  my ($xml, $ns, $tag, @fields) = @_;
  my %fields;
      
  # Parse the SOAP Message
  my $message = _parse_with_libxml($xml);
  $message->registerNs( def => $ns );
  $message->registerNs( wsa => 'http://www.w3.org/2005/08/addressing' );
  
  return unless $message->findnodes("//def:$tag");
  
  # Fetch all the fields we want
  foreach my $elem (@fields) {
    my $value = $message->findvalue("//def:$tag/def:$elem");
    $fields{$elem} = $value if defined $value;
  }
  
  $fields{source} = $message->findvalue('//wsa:From/wsa:Address');

  return \%fields;
}


#########################################
# Generic LibXML parser for XML documents

our $libxml_parser;

sub _parse_with_libxml {
  my ($xml) = @_;
      
  $libxml_parser ||= XML::LibXML->new;
  return XML::LibXML::XPathContext->new($libxml_parser->parse_string($xml));
}


1; # Magic true value required at end of module

__END__

=head1 NAME

POE::Component::MantaTCP - POE Interface to MantaTCP


=head1 VERSION

This document describes POE::Component::MantaTCP version 0.0.1


=head1 SYNOPSIS

    use POE::Component::MantaTCP;

    ### POE-style API, see below for more OO approach
    # Start a POE session that connects to the MantaTCP agent
    
    # defaults Alias = 'MantaTCP', host 127.0.0.1, and port 2222
    POE::Component::MantaTCP->spawn();

    # to use, send messages to the session

    # subscribe a topic
    $kernel->post( 'MantaTCP', 'subscribe', 
         topic => 'some_topic', callback => \&function );

    # publish a item
    $kernel->post( 'MantaTCP', 'publish', 
         topic => 'some_topic', payload => "something important here" );
    
    
    #### OO Approach

    my $proxy = POE::Component::MantaTCP->spawn();
    $proxy->subscribe( topic => 'some_topic', callback => \&function );
    $proxy->publish( 
        topic => 'some_topic',  payload => "something important here" );


=head1 DESCRIPTION

MantaBus is an agent of MantaRay that supports some of the operations over a TCP connection using
SOAP messages. MantaTCP is the name of that TCP-based interface.

This module allows a POE-based application to send and receive requests or messages using MantaTCP.

The API only support the basic publish/subscribe methods. The queue interface is not implemented yet.


=head1 INTERFACE 

B<The API is considered in alpha stage. It might change yet, as experience is gained with this module>.

The module can be used in two ways: using POE events, or using the OO API. Both take the exact same arguments,
therefore we describe them together.

When you use the POE-style programming, you'll C<post()> or C<call()> the MantaTCP session, using the alias you defined with the C<spawn()> method. The event name is the method name, and the parameters should be passed as an hash. For example, to send the message "i'm here" to the topic "ego-centrics", you would do something like this:

    POE::Component::MantaTCP->spawn( Alias => 'MantaTCP' ); # MantaTCP is the default Alias, btw
    $poe_kernel->post('MantaTCP', 'publish', topic => 'ego-centrics', payload => "i'm here");

With a object oriented aproach, you create an MantaTCP object with the C<spawn()> class method, and then, you would call methods on that object, like this:

   my $proxy = POE::Component::MantaTCP->spawn;
   $proxy->publish(topic => 'ego-centrics', payload => "i'm here");

Both methods of interacting with POE::Component::MantaTCP are valid and supported. Pick your favourite.

The following methods are available:


=over 4

=item spawn( OPTIONS )

The class method spawn() creates a new session to a MantaTCP agent running on a specific host and port. It returns an proxy instance that can be used to call methods afterwards, in a object oriented fashion. Alternatively, you can use L<POE::Kernel> C<post()> or C<call()>  APIs to send the MantaTCP session the same commands.

The following parameters are all optional (with sane defaults).

=over 8

=item Alias or alias

The name used to identify the POE session. By default, 'MantaTCP'. You would later use this alias to send C<subscribe()> and C<publish()> messages.

=item Hostname or hostname

IP or hostname of the server where MantaTCP is running. By default, 127.0.0.1.

=item Port or port

TCP port number where MantaTCP is running at I<Hostname>. By default, 2222.

=item OnConnect or on_connect

A callback (coderef or postback) that will be called after a sucessfull connection to the MantaTCP agent.

The callback, or the POE event triggered by the postback, will receive two parameters: the hostname and port of the MantaTCP agent.

The code should not assume a sucessfull connection until the I<OnConnect> callback is triggered. Calling other methods until the I<OnConnect> callback is triggered will be ignored.

=item Verbose or verbose

If true, will print to STDERR the major events happening inside the module. See also CONFIGURATION AND ENVIRONMENT below.i

=item Stats or stats

If true, the module will keep track of several counters for all the major events, and will print them to STDERR each 30 seconds.

=back

You can have multiple sessions simultaneously inside POE, as long as you use different Alias for each one.


=item subcribe( PARAMETERS )

Subscribes to a topic. As messages arrive for this topic, a callback will be invoked with the relevant data.

You can invoke C<subscribe()> as a method, but you can also post the 'subscribe' message to the alias you set for your L<POE::Component::MantaTCP> session, with the same arguments.

The following parameters are required:

=over 8

=item topic

The topic you wish to subscribe.

=item callback

The callback that will be called when a message arrives for this topic.

This parameter should be a code reference. You can also use a L<POE> postback.

The first argument of the callback, or the ARG0 argument of the L<POE> event triggered by the postback, is a reference to an hash with all the message information. The following keys are present in the hash:

=over 12

=item topic

The name of the topic where this message was published.

=item payload

The payload that was published.

=item message_id

The ID assigned to this message.

=item timestamp

The date/time when the message was published. This key is optional, it might not exist.

=item expires

The expiration date of this message.

=item priority

The priority that was assigned to this message.

=item persistent

If true, this message was a persistent message.

=item transient

If true, this message was a transient message.

=back

The return value of the callback is ignored.

=back


The following parameters can also be used with the subscribe() method:

=over 8

=item durable_name

The name of a durable (persistent) mailbox. If you subscribe to a topic once with a durable name, MantaTCP will create a mailbox with that name and will store all messages that arrive for this topic, even if you are offline. When you connect later on, with the same durable name, you'll receive all the messages. (TODO: check details with luis neves)

=item manual_ack

Don't use this parameter yet, it doesn't work, see BUGS AND LIMITATIONS.

If true, all messages received must be explicitly acknwoledged. If you don't acknoweledge a message, it will be resent later. (TODO: check with luis, when will it be resent? after reconnect?)

=back


=item publish( PARAMETERS )

Send a message to a topic. 

The following parameters are required:

=over 8

=item topic

The topic where to send the message.

=item payload

The message content. You can send anything text oriented. If you want to send binary objects, please base64-encode them first.

=back

The following parameters are optional:

=over 8

=item persistent

If true, this message will be marked as persistent. 

=item transient

If true, this message will be marked as transient.

=item priority

A integer value between 1 and 10.

=item message_id

A ID for this message. MantaBus will generate a random ID if none is set.

=back

The current defaults are dictated by the MantaBus proxy, currently marking every message as persistent, with a priority 4.


=item start

Starts the L<POE> system. This is a shortcut to a classic C<< $poe_kernel->run >>, because some scripts don't even need to know they are running under L<POE>.


=back


=head1 DIAGNOSTICS

This module does not generate any messages.


=head1 CONFIGURATION AND ENVIRONMENT

POE::Component::MantaTCP requires no configuration files.

You can use the environment variable POE_MANTATCP_PORT and POE_MANTATCP_HOSTNAME to set the default port and hostname.

To enable verbose logging, you can set POE_MANTATCP_VERBOSE to 1. The output will go to STDERR.

POE::Component::MantaTCP requires a MantaTCP instance running on a reacheable network address.


=head1 DEPENDENCIES

=for author to fill in:
    A list of all the other modules that this module relies upon,
    including any restrictions on versions, and an indication whether
    the module is part of the standard Perl distribution, part of the
    module's distribution, or must be installed separately. ]

None.


=head1 INCOMPATIBILITIES

None reported.


=head1 BUGS AND LIMITATIONS

Please report any bugs or feature requests using the web interface at
L<http://trac.intra.sapo.pt/projects/broker/newticket?component=POE::Component::MantaTCP>.

The following issues are known:

=over 4

=item spawn() is slow

After calling spawn() you should wait a couple of seconds before calling subscribe() or publish(). It takes a little bit to have the connection with the MantaBus open and settled. Until that happens, all subscribe()'s and publish()'s are ignored.

=item subscribe() does not have a success/fail callback

There is no way to know if a subscription was sucessful or not. Part of this problem lays with MantaTCP itself, because he doesn't notify us either.

=item SOAP Faults are ignored

If a SOAP fault is sent by MantaTCP, we just ignore it for now.

=item All subscribes are done with auto-ack

You cannot subscribe to a topic with manual_ack as true (hidden parameter to the subscribe() method) because we still lack the acknowledge method that needs to be called for each message delivered. TODO: check with Luis Neves, this method is missing


=back


=head1 AUTHOR

Pedro Melo  C<< <melo@co.sapo.pt> >>


=head1 LICENCE AND COPYRIGHT

Copyright (c) 2006, PT.COM. All rights reserved.

This module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself. See L<perlartistic>.


=head1 DISCLAIMER OF WARRANTY

BECAUSE THIS SOFTWARE IS LICENSED FREE OF CHARGE, THERE IS NO WARRANTY
FOR THE SOFTWARE, TO THE EXTENT PERMITTED BY APPLICABLE LAW. EXCEPT WHEN
OTHERWISE STATED IN WRITING THE COPYRIGHT HOLDERS AND/OR OTHER PARTIES
PROVIDE THE SOFTWARE "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER
EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THE SOFTWARE IS WITH
YOU. SHOULD THE SOFTWARE PROVE DEFECTIVE, YOU ASSUME THE COST OF ALL
NECESSARY SERVICING, REPAIR, OR CORRECTION.

IN NO EVENT UNLESS REQUIRED BY APPLICABLE LAW OR AGREED TO IN WRITING
WILL ANY COPYRIGHT HOLDER, OR ANY OTHER PARTY WHO MAY MODIFY AND/OR
REDISTRIBUTE THE SOFTWARE AS PERMITTED BY THE ABOVE LICENCE, BE
LIABLE TO YOU FOR DAMAGES, INCLUDING ANY GENERAL, SPECIAL, INCIDENTAL,
OR CONSEQUENTIAL DAMAGES ARISING OUT OF THE USE OR INABILITY TO USE
THE SOFTWARE (INCLUDING BUT NOT LIMITED TO LOSS OF DATA OR DATA BEING
RENDERED INACCURATE OR LOSSES SUSTAINED BY YOU OR THIRD PARTIES OR A
FAILURE OF THE SOFTWARE TO OPERATE WITH ANY OTHER SOFTWARE), EVEN IF
SUCH HOLDER OR OTHER PARTY HAS BEEN ADVISED OF THE POSSIBILITY OF
SUCH DAMAGES.

=cut

