#!/bin/sh

cd `dirname $0`

java -server \
-Xverify:none -Xms16M -Xmx16M \
-Djava.awt.headless=true \
-Djava.net.preferIPv4Stack=true \
-Djava.net.preferIPv6Addresses=false \
-Dfile.encoding=UTF-8 \
-cp "./conf:./lib/*" \
pt.com.broker.sample.Consumer -n /test/foo -d TOPIC
