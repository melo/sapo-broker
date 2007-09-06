package SAPO::BrokerXS;

use 5.008008;
use strict;
use warnings;
use Carp;

our $VERSION = '0.0.2';

require XSLoader;
XSLoader::load('SAPO::BrokerXS', $VERSION);

1;

__END__

=head1 NAME

SAPO::BrokerXS - Perl extension for the SAPO Broker C library

=head1 SYNOPSIS

  use SAPO::BrokerXS;
  my $broker = SAPO::BrokerXS->new('127.0.0.1',2222);
  $broker->connect() || die $broker->error();
  $broker->subscribe('/sapo/weather') || die $broker->error();
  my $message = $broker->receive();
  $broker->disconnect();
  $broker->reconnect();

=head1 DESCRIPTION

This is the binding module to the libsapo-broker C implementation.
It implements all the communication with the broker and also implements
TOPIC AS A QUEUE.

=head1 METHODS

All methods return a positive value on success. In case of failure, the
error string can be obtained using the $broker->error() method.

=over

=item SAPO::BrokerXS->new($host, $port)

This method creates a Broker instance, it doesn't connect it.

=item $broker->connect()

Connect an already created instance.

=item $broker->disconnect()

Disconnect from the agent.

=item $broker->reconnect()

Reconnect to the agent.

=item $broker->publish($topic, $payload)

Publish the payload to the topic in question.

=item $broker->publish_queue($topic, $payload)

Publish the payload to the topic in question using the
TOPIC AS A QUEUE pattern.

=item $broker->subscribe($topic)

Subscribe to the given topic

=item $broker->subscribe_queue($topic)

Subscribe to the given topic using the TOPIC AS A QUEUE
pattern.

=item $broker->error()

Returns the last error message registered by the library.

=item $broker->wait()

Do a blocking wait on the broker for 1 second.

=item $broker->wait_time($seconds)

Do a blocking wait on the broker for the given amount of
time. The seconds can be a double value, with precision to
microseconds.

=item $broker->receive()

Receive any message that is ready to be receive.

=back

=head1 SEE ALSO

See the Broker documentation.

=head1 AUTHOR

Daniel Ruoso, E<lt>daniel.ruoso@segula.ptE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007 by PT.COM

=cut
