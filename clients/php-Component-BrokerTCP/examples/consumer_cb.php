#!/usr/bin/php -q
<?php
include('../classes/broker.php');
set_time_limit(0);
error_reporting(1);

#$broker=new SAPO_Broker(array('debug'=>TRUE,'force_streams'=>TRUE));
$broker=new SAPO_Broker;

// consumer example with periodic callbacks in main loop
echo "Subscribing topics\n";
$broker->subscribe('/sapo/blogs/activity/post',NULL,"processTopic1");
echo "Adding periodic callbacks to the main loop\n";
$broker->add_callback(array("sec"=>5),"periodicCalls");
$broker->add_callback(array("sec"=>10),"periodicCalls2");
echo "Starting consumer\n";
$broker->consumer();

echo "Consumer exited (last err: ".$broker->net->last_err.")\n";

function periodicCalls() {
  echo "Look mom, here I am\n";
  }

function periodicCalls2() {
  echo "Look mom, I'm here as well\n";
  }

function processTopic1($payload,$topic,$net) {
  echo "latest_status_timestamp_sent: ".$net->latest_status_timestamp_sent."\n";
  echo "latest_status_timestamp_received: ".$net->latest_status_timestamp_received."\n";
  echo "latest_status_message: ".$net->latest_status_message."\n";
  echo "processTopic1() just got ".$payload."\n";
  }

?>
