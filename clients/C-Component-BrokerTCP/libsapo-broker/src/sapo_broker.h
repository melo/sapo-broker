#ifndef _SAPO_BROKER_H_
#define _SAPO_BROKER_H_
/*
  Sapo broker C interface lib.  

*/

#include <sys/time.h>
#include <sys/socket.h>
#include <expat.h>

#include "sapo_broker_xml.h"


#define DEFAULT_HOST "127.0.0.1"
#define DEFAULT_PORT 2222

enum {
    EQUEUE_QUEUE = 1,
    EQUEUE_TOPIC = 2,
    EQUEUE_PUBLISH = 3,
};

#define SB_SEND_QUEUE "Enqueue"
#define SB_SEND_TOPIC "Publish"
#define SB_RECV_QUEUE "QUEUE"
#define SB_RECV_TOPIC "TOPIC"


// Template for publishing messages (Topics or queues)
// This will be filled by the correct params 
// Not pretty, but fast

#define TPUBLISH "<soapenv:Envelope xmlns:soapenv=\"http://www.w3.org/2003/05/soap-envelope\"><soapenv:Body>\
<%s xmlns=\"http://services.sapo.pt/broker\"><BrokerMessage>\
<DestinationName>%s</DestinationName>\
<TextPayload>\
<![CDATA[%s]]>\
</TextPayload>\
</BrokerMessage></%s>\
</soapenv:Body></soapenv:Envelope>"


#define TPUBLISH_TIME "<soapenv:Envelope xmlns:soapenv=\"http://www.w3.org/2003/05/soap-envelope\"><soapenv:Body>\
<%s xmlns=\"http://services.sapo.pt/broker\"><BrokerMessage>\
<Expiration>%s</Expiration>\
<DestinationName>%s</DestinationName>\
<TextPayload>\
<![CDATA[%s]]>\
</TextPayload>\
</BrokerMessage></%s>\
</soapenv:Body></soapenv:Envelope>"


#define TACK "<soapenv:Envelope xmlns:soapenv=\"http://www.w3.org/2003/05/soap-envelope\"><soapenv:Body>\
<Acknowledge xmlns=\"http://services.sapo.pt/broker\">\
<MessageId>%s</MessageId>\
<DestinationName>%s</DestinationName>\
</Acknowledge>\
</soapenv:Body></soapenv:Envelope>"

// Template for subscribing topics/queues
// This will be filled by the correct params 
// Not pretty, but fast

#define TSUBSCRIBE "<soapenv:Envelope xmlns:soapenv='http://www.w3.org/2003/05/soap-envelope'><soapenv:Body>\
<Notify xmlns='http://services.sapo.pt/broker'>\
<DestinationName>%s</DestinationName>\
<DestinationType>%s</DestinationType>\
</Notify>\
</soapenv:Body></soapenv:Envelope>"


// not in use ... yet
#define SAPO_BROKER_MAX_RETRY 5
#define SAPO_BROKER_IO_UNBLOCKING 0
#define SAPO_BROKER_IO_BLOCKING   1

// error codes
#define SB_OK                  0
#define SB_NO_MESSAGE       -100
#define SB_HAS_MESSAGE      -101
#define SB_ERROR            -102
#define SB_NO_ERROR         -103
#define SB_BAD_MESSAGE_TYPE -110
#define SB_NOT_CONNECTED    -120
#define SB_ERROR_UNKNOWN    -121
#define SB_NOT_INITIALIZED  -122

// max static buffer
#define MAX_BUFFER 2048

#define MAX_HOSTNAME 256
#define MAX_TOPIC    256
#define MAX_MSG_TYPE 256

#define MAX_BODY_LEN_RECV 1024*1024
// var types
#define CHANGE_TOPIC  1
#define CHANGE_MSG_ID 2


// sb_new types
#define SB_TYPE_TCP	SOCK_STREAM
#define SB_TYPE_UDP	SOCK_DGRAM

typedef struct _sapo_broker_connection {
    char hostname[MAX_HOSTNAME];
    int port;
    int type;
    int max_retry;
    int blocking_io;
    int socket;
    int connected;
    int last_status;
    XML_Parser parser;
} SAPO_BROKER_T;





SAPO_BROKER_T *sb_new(char *hostname, int port, int type);
void sb_destroy(SAPO_BROKER_T * conn);
char *sb_error();

// tcp socket operations
int sb_connect(SAPO_BROKER_T * conn);
int sb_reconnect(SAPO_BROKER_T * conn);
int sb_disconnect(SAPO_BROKER_T * conn);
static int _sb_write(int socket, char *msg, int size);
static int _sb_read(int socket, char *msg, int size);


// mantaray operations
int sb_publish(SAPO_BROKER_T * conn, int type, char *topic, char *payload);
int sb_publish_time(SAPO_BROKER_T * conn, int type, char *topic, char *payload,
                    int expiration);
BrokerMessage *sb_receive(SAPO_BROKER_T * conn);
int sb_subscribe(SAPO_BROKER_T * conn, int type, char *topic);
int sb_wait(SAPO_BROKER_T * conn);
int sb_wait_time(SAPO_BROKER_T * conn, struct timeval *tv);

int sb_send_ack(SAPO_BROKER_T * conn, BrokerMessage * msg);


// xml operations 
void sb_free_message(BrokerMessage * message);





#endif                          // _SAPO_BROKER_H_
