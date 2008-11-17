package SAPO::Broker::AnyEvent;

use strict;
use warnings;
use base 'SAPO::Broker';
use AnyEvent;
use AnyEvent::Socket;
use AnyEvent::Handle;


sub receive {
  my ($self, $cb) = @_;
  
  croak("FATAL: SAPO::Broker::AnyEvent::receive() requires a coderef for callback, ")
    unless ref($cb) && ref($cb) eq 'CODE';
  
  $self->{_CORE_}{on_message_cb} = $cb;
}

sub on_connect {
  my ($self, $cb) = @_;
  
  croak("FATAL: SAPO::Broker::AnyEvent::on_connect() requires a coderef for callback, ")
    unless ref($cb) && ref($cb) eq 'CODE';
  
  $self->{_CORE_}{on_connect_cb} = $cb;
}

sub _connect {
    my $self = shift;

    my @hosts;
    
    @hosts = @{$self->{hosts}} if $self->{hosts};
    push @hosts, { host => $self->{host}, port => $self->{port}, timeout => $self->{timeout} };
    
    $self->_async_connect(@hosts);
    
    return -1; # $sock->connected;
}

sub _async_connect {
  my $self = shift;
  my $peer = shift;
  
  tcp_connect $peer->{host}, $peer->{port}, sub {
    my ($sock) = @_;
    
    if (!$sock) {
       $self->_debug("Peer down, trying next...");
       return $self->_async_connect(@_);
    }

    $self->{_CORE_}{handle} = AnyEvent::Handle->new(
      fh => $sock,
      on_eof     => sub { $self->_on_reconnect },
      on_error   => sub { $self->_on_reconnect },
    );
    
    $self->_on_connected($sock);
    $self->_start_reading;
  };
  
  return;
}

sub _connected {
  my $self = shift;
  
  return $self->{_CORE_}{sock};
}

sub _on_connected {
  my ($self, $sock) = @_;
  
  $self->{_CORE_}{sock} = $sock;
  $self->_debug("CONNECTED!");
  
  my $cb = $self->{_CORE_}{on_connect_cb};
  $cb->($self) if $cb;
  
  return;
}

sub _reconnect {} # Disable parent class _reconnect() handling
sub _on_reconnect {
  my $self = shift;
  
  delete $self->{_CORE_}{sock};
  delete $self->{_CORE_}{handle};
  
  my $t; $t = AnyEvent->timer(
    after => 0.1,
    cb    => sub {
      $self->_debug('RECONNECT!');
      $self->_connect;
      
      undef $t;
    },
  );
  
  return;
}

sub _start_reading {
  my ($self) = @_;
  my $handle = $self->{_CORE_}{handle};
  
  $handle->on_read(sub {
    $handle->push_read(
      packstring => 'N',
      sub {
        my (undef, $data) = @_;
        my $cb = $self->{_CORE_}{on_message_cb};

        return unless $cb;

        my $event = SAPO::Broker::_parse_soap_message(
      		$data,'http://services.sapo.pt/broker',
      		'BrokerMessage', qw(TextPayload MessageId DestinationName)
      	);

        my $as_struct = $self->{retstruct} || ( $self->{msg_type} && $self->{msg_type} eq 'TOPIC_AS_QUEUE' );

      	$cb->($as_struct? $event : $event->{TextPayload});

      	return; # Keep reading
      },
    );
  });
}


1;
