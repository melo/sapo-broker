#!/usr/bin/php -q
<?php
include('../classes/broker.php');
set_time_limit(0);
error_reporting(1);

if(!$argv[1]) {
  echo "Usage $argv[0] path [QUEUE|TOPIC]\n";
  exit;
  }

#$broker=new SAPO_Broker(array('debug'=>TRUE));
$broker=new SAPO_Broker;

// consumer example
if(strtoupper(strtoupper($argv[2]))=='QUEUE') {
  $broker->subscribe($argv[1],array('destination_type'=>'QUEUE'),"processExceptions");
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
