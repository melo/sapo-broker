#!/usr/bin/perl -w
# 
# Sample POE::Component::MantaTCP client
# 

use strict;
use FindBin;
use lib "$FindBin::Bin/../blib/lib";
use POE;
use POE::Component::MantaTCP;

my $role = shift || '';
my $proxy = POE::Component::MantaTCP->spawn( Port => 2222 );

POE::Session->create(
    inline_states => {
        _start          => \&start,
        subscribe_topic => \&subscribe_topic,
        send_message    => \&send_message,
    },
);

sub start {
  my $kernel = $_[KERNEL];
  
  if ($role eq 'listener') {
    $kernel->delay('subscribe_topic', 2, 'tropical', 'polar');
  }
  else {
    $kernel->delay('send_message', 2, 'tropical');
  }
}


sub subscribe_topic {
  my @topics = @_[ARG0..$#_];

  foreach my $topic (@topics) {
    print "Subscribe topic $topic\n";  
    $proxy->subscribe( topic => $topic, callback => \&receive_event );
  }
}


sub send_message {
  my ($kernel, $topic) = @_[KERNEL, ARG0];
  
  my $payload = "in $topic, something red, something blue, for I <<<&&&>>> you... @ " . localtime();
  print "SEND: $payload\n";
  $proxy->publish(
      topic   => $topic,
      payload => $payload,
  );
  $topic = $topic eq 'polar'? 'tropical' : 'polar';
  $kernel->delay('send_message', 1, $topic);
}


sub receive_event {
  my ($event) = @_;

  print "GOT: $event->{payload}\n";
}


$poe_kernel->run;