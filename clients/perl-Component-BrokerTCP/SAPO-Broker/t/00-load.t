#!perl -T

use Test::More tests => 1;

BEGIN {
	use_ok( 'SAPO::Broker' );
}

diag( "Testing SAPO::Broker $SAPO::Broker::VERSION, Perl $], $^X" );
