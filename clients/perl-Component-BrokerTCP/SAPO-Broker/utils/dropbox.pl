#!/usr/bin/perl

use lib qw(lib);

use SAPO::Broker;

use strict;
use warnings;
use utf8;

my ($dir, $count) = (@ARGV, 100);

my $topic = '/perl/tests'; 

my $broker = SAPO::Broker->new(
    drop     => 1,
    dropbox  => $dir,
);

while($count--){
    $broker->drop(
        msg_type => 'TOPIC',
        topic   => $topic,
        payload => "O servi\x{e7}o custa ".rand(100)." \x{20ac}"
    );
}
