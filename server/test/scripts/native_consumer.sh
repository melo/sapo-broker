#!/bin/sh
/home/lneves/jvm/bin/java -Xverify:none -Xms16M -Xmx16M -server -DmantaConfig=test-proxy_n1.config -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -Djava.net.preferIPv6Addresses=false -cp "./lib/manta-bus.jar:./lib/manta-bus-test.jar" pt.com.test.NativeConsumer lixo 40000
