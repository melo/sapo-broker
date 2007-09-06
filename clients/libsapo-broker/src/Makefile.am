lib_LTLIBRARIES = libsapo-broker.la
libsapo_broker_la_SOURCES = sapo_broker.c sapo_broker.h
libsapo_broker_la_LDFLAGS = -no-undefined -version-info @SOVERSION@ `xml2-config --libs`
libsapo_broker_la_includedir = $(includedir)
libsapo_broker_la_include_HEADERS = sapo_broker.h
CFLAGS = `xml2-config --cflags`
# set the include path found by configure
INCLUDES= $(all_includes) 
