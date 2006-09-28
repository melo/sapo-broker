#!/usr/bin/php -q
<?php
include('../classes/manta.php');
include('../classes/manta_tail.php');

set_time_limit(0);
error_reporting(1);

$broker=new SAPO_Manta;
$tailer=new SAPO_Manta_Tail;

$broker->init('10.135.64.42',2222);
$tailer->init('/var/log/system.log',10000);

$args['topic']='/sapo/developer/tests';

$tailer->tail("line_processer");
echo "Exited\n";

function line_processer($line) {
  GLOBAL $broker,$args;
  echo "Publishing $line\n";
  $r=$broker->publish('<rawline>'.$broker->xmlentities($line).'</rawline>',$args);
  if($r==FALSE) echo "Publish exited (last err: ".$broker->net->last_err.")\n";
  }

?>
