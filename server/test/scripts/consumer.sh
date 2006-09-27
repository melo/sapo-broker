#!/bin/sh
/home/lneves/jvm/bin/java -Xverify:none -Xms16M -Xmx16M -server -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -Djava.net.preferIPv6Addresses=false -cp "./lib/manta-bus.jar:./lib/manta-bus-test.jar" pt.com.test.TestConsumer localhost 2222 lixo TOPIC 10000
