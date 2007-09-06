#!/usr/bin/php -q
<?php
include('../classes/manta.php');
set_time_limit(0);
error_reporting(1);

#$broker=new SAPO_Manta(array('debug'=>TRUE));
$broker=new SAPO_Manta;

// publish example

$args['priority']=1;
$args['message_id']=1;
$args['persistent']=1;

$args['topic']='/sapo/developer/tests';
$r=$broker->publish('<xml>'.$broker->xmlentities('<<<ESCAPE-ME>>>').'</xml>',$args);
if($r==FALSE) echo "Publish exited (last err: ".$broker->net->last_err.")\n";


$args['topic']='/sapo/tags/sessions/users';
$r=$broker->publish('<xml>user has landed</xml>',$args);
if($r==FALSE) echo "Publish exited (last err: ".$broker->net->last_err.")\n";

echo "Done\n";
?>
