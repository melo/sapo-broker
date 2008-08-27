#!/bin/sh

growl_notify () {
./growlnotify -n "SAPO Broker" -m "$1" --image sapo.png >/dev/null 2>/dev/null
}

init () {
  check_user root
  make_dirs
  get_worldmap
  get_ip_address
  get_config
}

get_pid () {
  pid=`ps axww|grep java|grep -v grep|grep pt.com.broker.Start|cut -b1-6|bc`
  if [ "$pid" != "" ]
  then
    echo $pid
  else
    echo "0"
  fi
}

get_config () {
  WORLD_MAP="\\/tmp\\/world_map_tmp.xml"
  WORLD_MAP=`echo $WORLD_MAP_FILE| sed "s/\\//\\\\\\\\\\//g"`
  if [ -f /opt/local/etc/broker/broker.xml ]
  then
    BROKER_CONFIG="/opt/local/broker/etc/broker.xml"
  else
    EDROPBOX=`echo $DROPBOX| sed "s/\\//\\\\\\\\\\//g"`
    EPATH_TO_PERSISTENT_STORAGE=`echo $PATH_TO_PERSISTENT_STORAGE| sed "s/\\//\\\\\\\\\\//g"`
    cat /opt/local/broker/conf/main.xml \
    | sed s/@HOSTNAME@/broker/g \
    | sed s/@PEER_IP@/$PEER_IP/g \
    | sed s/@PEER_PORT@/$PEER_PORT/g \
    | sed s/@BROKER_PORT@/$BROKER_PORT/g \
    | sed s/@PATH_TO_WORLDMAP@/$WORLD_MAP/g \
    | sed s/@HTTP_PORT@/$HTTP_PORT/g \
    | sed s/@UDP_PORT@/$UDP_PORT/g \
    | sed s/@PATH_TO_PERSISTENT_STORAGE@/$EPATH_TO_PERSISTENT_STORAGE/g \
    | sed s/@PATH_TO_DROP_BOX@/$EDROPBOX/g \
    | sed s/@DROP_BOX_POLLING_INTERVAL@/$DROP_BOX_POLLING_INTERVAL/g \
    >/tmp/broker.xml
    BROKER_CONFIG="/tmp/broker.xml"
  fi
  export BROKER_CONFIG=$BROKER_CONFIG
}

get_worldmap () {
  if [ -f /opt/local/etc/broker/world_map.xml ]
  then
    WORLD_MAP_FILE="/opt/local/etc/broker/world_map.xml"
  else
    cat >  /tmp/local_world.xml << EOF
<?xml version="1.0"?>

<world>
 <domain>
  <peer>
   <name>broker-127.0.0.1-3315</name>
   <transport>
    <type>TCP</type>
    <ip>127.0.0.1</ip>
    <port>3315</port>
   </transport>
  </peer>
 </domain>
</world>
EOF
    WORLD_MAP_FILE="/tmp/local_world.xml"
  fi
  if [ ! -f $WORLD_MAP_FILE ]
  then
    echo "Couldn't fint any working world_map.xml file. Exiting..."
    exit;
  fi
  export WORLD_MAP_FILE=$WORLD_MAP_FILE
}

start_server () {
  # check version
  java -version 2>&1 | grep 1.5 > /dev/null
  if [ $? = 0 ] ; then # Yup, 1.5 still
    classpath="/opt/local/broker/conf"
    for i in /opt/local/broker/jvm15/lib/*.jar; do
      classpath=$classpath:$i
    done
    for i in /opt/local/broker/lib/*.jar; do
      classpath=$classpath:$i
    done
  else # we assume 1.6 here
    classpath="/opt/local/broker/conf:/opt/local/broker/lib/*"
  fi
  cd /opt/local/broker/
  if [ "$1" == "bg" ]
  then
   exec sudo -u nobody /usr/bin/java \
      -server \
      -Xverify:none -Xms256M -Xmx256M \
      -Djava.awt.headless=true \
      -Djava.net.preferIPv4Stack=true \
      -Djava.net.preferIPv6Addresses=false \
      -Dfile.encoding=UTF-8 \
      -Dconfig-path=$BROKER_CONFIG \
      -cp "$classpath" \
      pt.com.broker.Start >/dev/null 2>/dev/null &
  else
   exec sudo -u nobody /usr/bin/java \
      -server \
      -Xverify:none -Xms256M -Xmx256M \
      -Djava.awt.headless=true \
      -Djava.net.preferIPv4Stack=true \
      -Djava.net.preferIPv6Addresses=false \
      -Dfile.encoding=UTF-8 \
      -Dconfig-path=$BROKER_CONFIG \
      -cp "$classpath" \
      pt.com.broker.Start
  fi
}

make_dirs () {
  mkdir -p $ETC_DIR
  mkdir -p $DROPBOX
  mkdir -p $PATH_TO_PERSISTENT_STORAGE
  chown nobody:nogroup `echo $DROPBOX`
  chown nobody `echo $DROPBOX`
  chmod 1777 `echo $DROPBOX`
  chown nobody:nogroup `echo $PATH_TO_PERSISTENT_STORAGE`
  chown nobody `echo $PATH_TO_PERSISTENT_STORAGE`
  chmod 1777 `echo $PATH_TO_PERSISTENT_STORAGE`
}

get_ip_address () {
  PEER_IP=""
  for pi in `/sbin/ifconfig |grep -i "inet "|grep -i " netmask "|cut -d" " -f2`
  do
    for wip in `cat $WORLD_MAP_FILE|grep "<ip>"|cut -d">" -f2|cut -d"<" -f1`
    do
      if [ "$pi" == "$wip" ]
      then
        PEER_IP=$wip
      fi
    done
  done
  if [ "$PEER_IP" == "" ]
  then
    PEER_IP="127.0.0.1"
  fi
  export PEER_IP=$PEER_IP
}

check_user () {
  RUNNING_AS=`id -un`
  if [ "$RUNNING_AS" != "$1" ] ; then
    echo "FATAL: script '`basename $0`' must be run under the '$1' account. You're '$RUNNING_AS'"
    exit 1
  fi
}

