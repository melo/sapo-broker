#!/bin/sh

cd `dirname $0`

. functions.sh

check_user root

pid=`get_pid`
if [ "$pid" != "0" ]
then
  kill $pid
  echo "Broker stopped"
else
  echo "Broker is not running"
fi

echo "Removing Startup script"
rm -fr /Library/StartupItems/SAPOBroker/

echo "Removing Broker directories"
rm -fr /opt/local/broker/

echo "Removing PrefPane"
rm -fr ~/Library/PreferencePanes/SAPO\ Broker.prefPane/
rm -fr /Library/PreferencePanes/SAPO\ Broker.prefPane/

echo "Removing Install Receipts"
rm -fr /Library/Receipts/SAPO\ Broker.pkg/

echo "SAPO Broker removed from system"
cd ~
