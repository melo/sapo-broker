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
	msg_type	=> 'FF',
	host		=> '127.0.0.1',
	port		=> 3322,
	DEBUG		=> 0,
	retstruct   => 1
);

die "Can't subscribe\n" unless $broker->subscribe( topic => $topic );

while (1) {
	my $xml = $broker->receive;
	
	my $source = $xml->{source};
	
	my ($host, $topic) = _proc_source($source);
	
	unless ($topic) {
	    $topic = $source;
    	print "WARINIG: voodoo wsa:Address\n"; 
	}
	
	$stats{$topic}++;
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