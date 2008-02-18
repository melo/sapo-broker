#!/usr/bin/php -q
<?php
include('../classes/broker.php');
set_time_limit(0);
error_reporting(1);

#$broker=new SAPO_Broker(array('debug'=>TRUE));
$broker=new SAPO_Broker; // will use localhost

// this will be publish on the disks's dropbox because it's localhost
$r=$broker->publish('<xml>'.$broker->xmlentities('<<<ESCAPE-ME>>>').'</xml>',array('topic'=>'/sapo/developer/tests'));
if($r==FALSE) echo "Publish exited (last err: ".$broker->net->last_err.")\n";

echo "Done\n";
?>
