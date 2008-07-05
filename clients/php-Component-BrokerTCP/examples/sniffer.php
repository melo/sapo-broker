#!/usr/bin/php -q
<?php
include('../classes/broker.php');
set_time_limit(0);
error_reporting(1);

if(!$argv[1]) {
  echo "Usage $argv[0] topic [QUEUE|TOPIC|TOPIC_AS_QUEUE]\n";
  exit;
  }

#$broker=new SAPO_Broker(array('debug'=>TRUE));
$broker=new SAPO_Broker;

// consumer example
if($argv[2]) {
  $broker->subscribe($argv[1],array('destination_type'=>$argv[2]),"processExceptions");
  }
  else
  {
  $broker->subscribe($argv[1],NULL,"processExceptions");
  }

$broker->consumer();

function processExceptions($payload) {
  echo $payload."\n";
  }

?>
