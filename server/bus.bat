@echo off

set JAVA=C:\Java\ibm_sdk50\jre\bin\java

set CLASSPATH=./lib/manta-bus.jar

set JAVA_OPTS=%JAVA_OPTS% -Xverify:none -Xms256M -Xmx256M

set JAVA_OPTS=%JAVA_OPTS% -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -Djava.net.preferIPv6Addresses=false -Dfile.encoding=UTF-8

set JAVA_OPTS=%JAVA_OPTS% -Dbus_port=2222 -Dhttp_port=8000 -DmantaConfig=manta-proxy.config

rem ********* run manta-bus ***********

@echo on

"%JAVA%" %JAVA_OPTS% -cp "%CLASSPATH%" pt.com.manta.Start

pause