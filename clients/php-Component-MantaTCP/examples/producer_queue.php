#!/usr/bin/php -q
<?php
include('../classes/manta.php');
set_time_limit(0);
error_reporting(1);

$broker=new SAPO_Manta(array('debug'=>TRUE));
#$broker=new SAPO_Manta;

// publish example

$r=$broker->publish('<xml>'.$broker->xmlentities('<<<ESCAPE-ME>>>').'</xml>',array('topic'=>'/testes/myqueue1','destination_type'=>'QUEUE'));

if($r==FALSE) echo "Publish exited (last err: ".$broker->net->last_err.")\n";

echo "Done\n";
?>
