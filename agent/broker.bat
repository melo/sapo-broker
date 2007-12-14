@echo off

title broker1

set JAVA=C:\Java\jdk1.6.0\bin\java

set CLASSPATH=./conf
set CLASSPATH=%CLASSPATH%;./lib/*


set JAVA_OPTS=%JAVA_OPTS% -Xverify:none -Xms256M -Xmx256M

set JAVA_OPTS=%JAVA_OPTS% -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -Djava.net.preferIPv6Addresses=false -Dfile.encoding=UTF-8

set JAVA_OPTS=%JAVA_OPTS% -Dconfig-path=./conf/main.xml

 


rem ********* run broker ***********

@echo on

"%JAVA%" %JAVA_OPTS% -cp "%CLASSPATH%" pt.com.broker.Start

pause