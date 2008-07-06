#!/usr/bin/php -q
<?php
include('../classes/broker.php');
set_time_limit(0);
error_reporting(1);

#$broker=new SAPO_Broker(array('debug'=>TRUE));
$broker=new SAPO_Broker;

// consumer example
echo "Subscribing topics\n";
$broker->subscribe('/sapo/tags/feeds/urls',NULL,"processUrls");
$broker->subscribe('/sapo/pesquisa/queries',NULL,"processSearch");
$broker->subscribe('/sapo/web/homepage/>',NULL,array("Test_Class","processTests"));
echo "Entering consumer() loop now\n";
$broker->consumer();

echo "Consumer exited (last err: ".$broker->net->last_err.")\n";

function processUrls($payload) {
  echo "processUrls() just got ".$payload."\n";
  }

function processSearch($payload) {
  echo "processSearch() just got ".$payload."\n";
  }

class Test_Class {

  function processTests($payload) {
    echo "processTests() just got ".$payload."\n";
    }

  }

?>
