#!/bin/sh

exec 2>&1

cd `dirname $0`

. functions.sh

# defaults
export PEER_PORT="3315"
export BROKER_PORT="3322"
export HTTP_PORT="3380"
export UDP_PORT="3366"
export DROPBOX="/opt/local/var/broker/dropbox"
export ETC_DIR="/opt/local/etc/broker"
export PATH_TO_PERSISTENT_STORAGE="/opt/local/var/broker/dropbox/persistent"
export DROP_BOX_POLLING_INTERVAL="60"
export LC_ALL=en_US

case "$1" in
  start)
    if [ "`get_pid`" != "0" ]
      then
        echo "Broker is already running. Did nothing."
        exit;
      fi
    init
    growl_notify "SAPO Broker has started in background"
    start_server bg
    retry=0
    while [ "`get_pid`" != "0" ] && [ "$retry" -lt 5 ]
      do
        retry=`echo $retry+1|bc`
        sleep 1
      done
    if [ "`get_pid`" == "0" ]
      then
        echo "Broker had troubles starting"
        exit;
      fi
    echo "Broker started with pid `get_pid`"
    ;;
  foreground)
    init
    growl_notify "SAPO Broker has started in foreground"
    start_server
    ;;
  pid)
    get_pid
    ;;
  stop)
    check_user root
    pid=`get_pid`
    if [ "$pid" != "0" ]
    then
      kill $pid
      sleep 2
      pid=`get_pid`
      if [ "$pid" != "0" ]
      then
        kill -9 $pid
      fi
      echo "Broker stopped"
    else
      echo "Broker is not running"
    fi
    ;;
  ping)
    pid=`get_pid`
    if [ "$pid" != "0" ]
    then
      echo "Broker is running with pid $pid"
    else
      echo "Broker is stopped"
    fi
    ;;
  autostarton)
    check_user root
    touch /opt/local/broker/.startup
    ;;
  autostartoff)
    check_user root
    rm -fr /opt/local/broker/.startup
    ;;
  status)
    pid=`get_pid`
    if [ "$pid" != "0" ]
    then
      isRunning="1"
    else
      isRunning="0"
    fi
    conns=`netstat -an -p tcp|grep ESTABLISHED|cut -b-44|grep ".3322"|wc -l|bc`
    echo "$isRunning:$conns"
    ;;
  *)
    echo "Usage: $0 [start|foreground|stop|ping|pid|stop|status|autostarton|autostartoff]"
    ;;
esac
