#!/bin/sh

cd `dirname $0`

cd ..

classpath="./conf"

for i in ./lib/*.jar
do
classpath=$classpath:$i
done

for i in ./jvm15/lib/*.jar
do
classpath=$classpath:$i
done

bin/java -server \
-Xverify:none -Xms16M -Xmx16M \
-Djava.awt.headless=true \
-Djava.net.preferIPv4Stack=true \
-Djava.net.preferIPv6Addresses=false \
-Dfile.encoding=UTF-8 \
-cp $classpath \
pt.com.broker.client.sample.Producer -n /test/foo -d TOPIC
