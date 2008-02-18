#!/usr/bin/php -q
<?php
include('../classes/broker.php');
set_time_limit(0);
error_reporting(1);

$broker=new SAPO_Broker(array('debug'=>TRUE));
#$broker=new SAPO_Broker;

// publish example

$r=$broker->publish('<xml>'.$broker->xmlentities('<<<ESCAPE-ME>>>').'</xml>',array('topic'=>'/testes/myqueue1','destination_type'=>'QUEUE'));

if($r==FALSE) echo "Publish exited (last err: ".$broker->net->last_err.")\n";

echo "Done\n";
?>
