#!/usr/bin/perl

use lib qw(lib);

use SAPO::Broker;

use strict;
use warnings;
use utf8;

my ($dir, $count) = @ARGV;

my $topic = '/perl/tests'; 

if( defined($dir) ){
    my $broker = SAPO::Broker->new(
            drop     => 1,
            dropbox  => $dir,
            );

    $count ||= 100;
    while($count--){
        $broker->drop(
                topic   => $topic,
                payload => sprintf("O servi\x{e7}o custa %.2f \x{20ac}", rand(100*100)/100)
                );
    }
}else{
    warn "\n$0 directory count\n\n";
    exit(-1);
}
