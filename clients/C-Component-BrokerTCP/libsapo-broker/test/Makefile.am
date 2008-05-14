check_PROGRAMS = tsapo_broker
AM_CFLAGS = -Wall -O0 `pkg-config --cflags gthread-2.0 glib-2.0`
INCLUDES = -I$(top_srcdir)/src

tsapo_broker_SOURCES = main.c
tsapo_broker_LDADD = -lsapo-broker 
tsapo_broker_LDFLAGS = -no-undefined -L$(top_builddir)/src/.libs/  `pkg-config --libs gthread-2.0 glib-2.0`

