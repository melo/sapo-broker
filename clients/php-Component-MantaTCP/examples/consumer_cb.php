#!/usr/bin/php -q
<?php
include('../classes/manta.php');
set_time_limit(0);
error_reporting(1);

#$broker=new SAPO_Manta(array('server'=>'10.135.64.41','debug'=>TRUE,'force_streams'=>TRUE));
$broker=new SAPO_Manta(array('server'=>'10.135.64.41'));

// consumer example with periodic callbacks in main loop
$broker->subscribe('/sapo/messenger/presence/isapo',NULL,"processUsers");
$broker->add_callback(array("sec"=>5),"periodicCalls");
$broker->add_callback(array("sec"=>10),"periodicCalls2");
$broker->consumer();

echo "Consumer exited (last err: ".$broker->net->last_err.")\n";

function periodicCalls() {
  echo "Look mom, here I am\n";
  }

function periodicCalls2() {
  echo "Look mom, I'm here as well\n";
  }

function processUsers($payload) {
  echo "processUsers() just got ".$payload."\n";
  }

?>
