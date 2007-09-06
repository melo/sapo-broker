# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl SAPO-BrokerXS.t'

#########################

# change 'tests => 2' to 'tests => last_test_to_print';

use Test::More tests => 2;
BEGIN { use_ok('SAPO::BrokerXS') };


my $fail = 0;
foreach my $constname (qw(
	 DEFAULT_PORT EQUEUE_PUBLISH EQUEUE_QUEUE EQUEUE_TOPIC MAX_BUFFER
	SAPO_BROKER_IO_BLOCKING SAPO_BROKER_IO_UNBLOCKING SAPO_BROKER_MAX_RETRY
	SB_ERROR SB_HAS_MESSAGE SB_NO_ERROR SB_NO_MESSAGE)) {
  next if (eval "my \$a = $constname; 1");
  if ($@ =~ /^Your vendor has not defined SAPO::BrokerXS macro $constname/) {
    print "# pass: $@";
  } else {
    print "# fail: $@";
    $fail = 1;
  }

}

ok( $fail == 0 , 'Constants' );
#########################

# Insert your test code below, the Test::More module is use()ed here so read
# its man page ( perldoc Test::More ) for help writing this test script.

