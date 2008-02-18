#!/usr/bin/php -q
<?php
include('../classes/broker.php');
include('../classes/broker_tail.php');

set_time_limit(0);
error_reporting(1);

$broker=new SAPO_Broker(array('debug'=>TRUE));

$tailer=new SAPO_Broker_Tail(array('debug'=>TRUE,'file'=>'/var/log/system.log','max_lines_before_quit'=>10000));

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
