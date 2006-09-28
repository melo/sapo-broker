#!/usr/bin/php -q
<?php
include('../classes/manta.php');
set_time_limit(0);
error_reporting(1);

#$broker=new SAPO_Manta(array('debug'=>TRUE,'server'=>'10.135.64.42','port'=>2222,'force_streams'=>TRUE));
$broker=new SAPO_Manta(array('server'=>'10.135.64.42'));

#$broker->debug=TRUE;
#$broker->uselibxml=FALSE;

// consumer example
$broker->subscribe('/sapo/tags/feeds/urls',NULL,"processUrls");
$broker->subscribe('/sapo/messenger/presence/sapo',NULL,"processUsers");
$broker->subscribe('/sapo/developer/tests',NULL,"processTests");
$broker->consumer();

echo "Consumer exited (last err: ".$broker->net->last_err.")\n";

function processUrls($payload) {
  echo "processUrls() just got ".$payload."\n";
  }

function processUsers($payload) {
  echo "processUsers() just got ".$payload."\n";
  }

function processTests($payload) {
  echo "processTests() just got ".$payload."\n";
  }

?>
