#!/usr/bin/perl 

use lib qw(lib);
use warnings;
use strict;
use SAPO::Broker;
use Data::Dumper;
use English qw( -no_match_vars );
use Time::HiRes qw(time);

my $topic = '/sapo/broker/machomac'; 

my $sleep = 1;

#---

$OUTPUT_AUTOFLUSH++;  # $|++;

&usage unless @ARGV;
my $stype = $ARGV[0] eq 'p' ? undef : 1;

my $events;
my $time = time;
$SIG{ALRM} = sub {
	my $diff_time = time-$time;
	my $events_per_sec = $events && $diff_time>0 ? sprintf("%02.2f",$events/$diff_time) : 0;
	print "Events per second: $events_per_sec\n";
	$events=0;
	$time=time;
	alarm 5;
};

alarm 5;


my $broker = SAPO::Broker->new(
	timeout		=> 60, 
	hosts       => [
	    { host    =>  '127.0.0.1' },
	    { host    =>  'devbroker' }
	],
	DEBUG		=> 0,
	retstruct   => 1,
);

die "No Broker?\n" unless $broker;


if ($stype) { # CONSUMER
	print "Starting as CONSUMER\n";
	die "Can't subscribe\n"  unless $broker->subscribe(
		topic => $topic,
	);
	while (1) {
		#print Dumper($broker->receive), "\n";		
		my $event = $broker->receive;
		print "Received: ",Dumper($event), "\n";
		
		# send the acknowledge when you subscribed the topic as queue
		if ($broker->msg_type($topic) eq 'TOPIC_AS_QUEUE') {
			$broker->ack($event);
		}
		
		$events++;
		#sleep $sleep;
	}
}
else { # PRODUCER
	print "Starting as PRODUCER\n";
	while (1) {
	    my $data = time;
	    #print "Sending: $data\n";
		warn "Can't Publish\n" unless $broker->publish(
			topic   => $topic,
			payload => $data,
		);
		$events++;
		#sleep $sleep;
	}
}

sub usage {
  print <<"USAGE";
$0 [p|c]
  p = producer
  c = consumer
USAGE

exit;
}
