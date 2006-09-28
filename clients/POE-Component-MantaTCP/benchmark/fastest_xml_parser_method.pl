#!/usr/bin/perl -w

use strict;
use Benchmark qw(:all :hireswallclock);
use FindBin;
use lib "$FindBin::Bin/../blib/lib";
use POE::Component::MantaTCP;
use XML::LibXML;
use XML::LibXML::XPathContext;
use XML::XPath;


my $soap_message = <<EOM;
<?xml version="1.0" encoding="UTF-8"?><Envelope xmlns="http://schemas.xmlsoap.org/soap/envelope/"><Body><Notification xmlns="http://services.sapo.pt/broker"><brokerMessage><deliveryMode>2</deliveryMode><priority>4</priority><messageId>ID:741560951</messageId><timestamp/><expiration>292278994-08-17T07:12:55Z</expiration><destinationName>/sapo/messenger/presence/sapo</destinationName><textPayload>
        &lt;notification xmlns='http://uri.sapo.pt/schemas/sapo/notification.xsd'>
          &lt;timestamp>20060805151430&lt;/timestamp>
          &lt;entities>
            &lt;entity role='actor'>
               &lt;j:jid xmlns:j='http://uri.sapo.pt/schemas/messenger/jid.xsd'>carloset\@sapo.pt&lt;/j:jid>
            &lt;/entity>
          &lt;/entities>
          &lt;event>
            &lt;m:presence xmlns:m='http://uri.sapo.pt/schemas/messenger/presence.xsd'>
              &lt;m:jid>carloset\@sapo.pt/Casa&lt;/m:jid>
              &lt;m:show>away&lt;/m:show>
              &lt;m:status>&lt;/m:status>
              &lt;m:priority>0&lt;/m:priority>
                  &lt;m:mood>smile&lt;/m:mood>
              &lt;m:caps_ver>4.0.1.4594a&lt;/m:caps_ver>

            &lt;/m:presence>
          &lt;/event>
        &lt;/notification>
    </textPayload></brokerMessage></Notification></Body></Envelope>
EOM

my $count = $ARGV[0] || 5000;

my $results = timethese( $count, {
  'libxml' => sub {
     POE::Component::MantaTCP::_parse_soap_message_with_libxml(
         $soap_message,
         'http://services.sapo.pt/broker',
         'brokerMessage',
         qw( deliveryMode priority messageId timestamp expiration destinationName textPayload ));
  },
  'xpath' => sub {
     POE::Component::MantaTCP::_parse_soap_message_with_xpath(
         $soap_message,
         'http://services.sapo.pt/broker',
         'brokerMessage',
         qw( deliveryMode priority messageId timestamp expiration destinationName textPayload ));
  },
  'regexp' => sub {
     POE::Component::MantaTCP::_parse_soap_message_with_regexp(
         $soap_message,
         'http://services.sapo.pt/broker',
         'brokerMessage',
         qw( deliveryMode priority messageId timestamp expiration destinationName textPayload ));
  },
});

print "\n";

cmpthese($results);
