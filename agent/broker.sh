#!/bin/sh

cd `dirname $0`

java -server \
-Xverify:none -Xms256M -Xmx256M \
-Djava.awt.headless=true \
-Djava.net.preferIPv4Stack=true \
-Djava.net.preferIPv6Addresses=false \
-Dfile.encoding=UTF-8 \
-Dconfig-path=./conf/main.xml \
-cp "./conf:./lib/*" \
pt.com.broker.Start