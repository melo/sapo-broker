#!/bin/sh

CFG=~/.manta-bus.config
if [ -n "$1" ] ; then
  CFG=$1
fi
echo using config file $CFG

JAVA2_MAC_OS_X=/System/Library/Frameworks/JavaVM.framework/Versions/1.5/Commands

if [ -d $JAVA2_MAC_OS_X ] ; then
  PATH=$JAVA2_MAC_OS_X:$PATH
  export PATH
else
  echo "hmms... Duvido que funcione, nao tens a versao 1.5 do Java instalado"
  exit 1
fi

if [ ! -e $CFG ] ; then
  echo "ERROR: o ficheiro de config '$CFG' nao foi encontrado"
  echo "  Sugestao: cp manta-proxy.config $CFG"
  echo "            e edita a gosto"
  exit 1
fi


java -Dbus_port=2222 -Dhttp_port=8000     \
     -DmantaConfig=$CFG                   \
     -Xverify:none -Xms64M -Xmx64M        \
     -Djava.awt.headless=true             \
     -Djava.net.preferIPv4Stack=true      \
     -Djava.net.preferIPv6Addresses=false \
     -classpath "./lib/manta-bus.jar" \
     pt.com.manta.Start 
