#!/usr/bin/perl 

use lib qw(lib);
use warnings;
use strict;
use SAPO::Broker;
use Data::Dumper;
use English qw( -no_match_vars );
use Time::HiRes qw(time);

# hugly hack to just run once...
my $oneshot = shift;

# I hate buffers.com.br
$|++;

# stats struct
my %stats;

# stats cycle in seconds
my $stat_cycle = 60;

# start stats
$SIG{ALRM} = sub {_proc_stats() };
alarm($stat_cycle);

# topic to follow
my $topic = '/.*';

# connect to broker
my $broker = SAPO::Broker->new(
	timeout		=> 60,
	hosts       => [
	    {
	        host    => '127.0.0.1'
	    },
	    {
	        host    =>  'devbroker'
	    }
	],
	port        => 3322,
	DEBUG		=> 0,
	retstruct   => 1
);

die "No broker? $@\n" unless $broker;

die "Can't subscribe\n" 
    unless $broker->subscribe( 
        topic       => $topic, 
    );


print "Starting collecting data...\n";
while (1) {
	my $event = $broker->receive;
	
	$stats{$event->{topic}}++;
}

sub _proc_source {
    my $source = shift;
    
    my ($host, $topic) = $source =~ m!topic\@broker-([^-]+)-\d+://(.*)!;
    
    return $host, $topic;
}


sub _proc_stats {
    
    print "\nStats for $stat_cycle seconds\n";
    
    for my $topic (sort {$stats{$b} <=> $stats{$a}} keys %stats) {
        print " $topic $stats{$topic}\n";
    }
    
    %stats = ();
    
    alarm($stat_cycle);
    
    exit if $oneshot;
}