#!/usr/bin/php -q
<?php
include('../classes/broker.php');
set_time_limit(0);
error_reporting(1);

$count=0;

#$broker=new SAPO_Broker(array('debug'=>TRUE));
$broker=new SAPO_Broker(array('server'=>'10.135.64.41','port'=>3322));

// consumer example
echo "Subscribing topics\n";
$broker->subscribe('/test/b',NULL,"processEvent");
echo "Entering consumer() loop now\n";
$broker->consumer();

echo "Consumer exited (last err: ".$broker->net->last_err.")\n";

function processEvent($payload) {
  GLOBAL $count,$broker;
  echo "processEvent() just got ".$payload."\n";
  $count++;
  if($count==10) {
    echo "Unsubsribing topic now\n";
    $broker->unsubscribe('/test/b');
    }
  }

?>
