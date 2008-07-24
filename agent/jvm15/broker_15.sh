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

java -server \
-Xverify:none -Xms256M -Xmx256M \
-Djava.awt.headless=true \
-Djava.net.preferIPv4Stack=true \
-Djava.net.preferIPv6Addresses=false \
-Dfile.encoding=UTF-8 \
-Dconfig-path=./conf/main_example.xml \
-cp $classpath \
pt.com.broker.Start
