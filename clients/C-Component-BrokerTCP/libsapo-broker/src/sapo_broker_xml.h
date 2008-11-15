#ifndef _sb_xml_
#define _sb_xml_

#include <time.h>
#include "expat.h"

#define BUFFSIZE	8192
char Buff[BUFFSIZE];

enum {
    XML_NOP = 0,
    XML_MSGID,
    XML_PRIORITY,
    XML_EXPIRATION,
    XML_DESTINATION,
    XML_PAYLOAD
};

#define MAX_SIZE_BUFFER  256*1024+1
#define MSG_ID_SIZE      256
#define DESTINATION_SIZE 256
#define MAX_XML_LEVELS    20

typedef struct _ud_ {
    char text[MAX_SIZE_BUFFER];
    int len;
    int stack[MAX_XML_LEVELS];
    int nstack;
} BrokerBuffer;


typedef struct _brokermessage_ {
    char payload[MAX_SIZE_BUFFER];
    int priority;
    struct tm expiration;
    char message_id[MSG_ID_SIZE];
    char destination[DESTINATION_SIZE];
    short ack;
} BrokerMessage;

typedef struct {
    BrokerBuffer buffer;
    BrokerMessage *msg;
} BrokerParserData;


BrokerMessage *sb_parser_process(XML_Parser p, char *buffer, int len);
XML_Parser sb_parser_new();
void sb_parser_destroy(XML_Parser p);

#endif
