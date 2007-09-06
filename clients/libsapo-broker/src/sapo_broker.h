#ifndef _SAPO_BROKER_H_
#define _SAPO_BROKER_H_

#define DEFAULT_HOST "127.0.0.1"
#define DEFAULT_PORT 2222

enum{ 
	EQUEUE_QUEUE = 1,
	EQUEUE_TOPIC = 2,
	EQUEUE_PUBLISH = 3,
};

#define TPUBLISH "<soapenv:Envelope xmlns:soapenv=\"http://www.w3.org/2003/05/soap-envelope\"><soapenv:Body>\
<%s xmlns=\"http://services.sapo.pt/broker\"><BrokerMessage>\
<DestinationName>%s</DestinationName>\
<TextPayload>\
  <![CDATA[%s]]>\
</TextPayload>\
</BrokerMessage></%s>\
</soapenv:Body></soapenv:Envelope>"


#define TSUBSCRIBE "<soapenv:Envelope xmlns:soapenv='http://www.w3.org/2003/05/soap-envelope'><soapenv:Body>\
<Notify xmlns='http://services.sapo.pt/broker'>\
<DestinationName>%s</DestinationName>\
<DestinationType>%s</DestinationType>\
</Notify>\
</soapenv:Body></soapenv:Envelope>"



#define SAPO_BROKER_MAX_RETRY 5
#define SAPO_BROKER_IO_UNBLOCKING 0
#define SAPO_BROKER_IO_BLOCKING   1

#define SB_NO_MESSAGE   100
#define SB_HAS_MESSAGE  101
#define SB_ERROR        102
#define SB_NO_ERROR     103

#define MAX_BUFFER 2048


typedef struct _sapo_broker_connection {
	char hostname[255];
	int port;
	int max_retry;
	int blocking_io;
	int socket;
	int connected;
	int last_status;
} SAPO_BROKER_T;



SAPO_BROKER_T * sapo_broker_new(char * hostname, int port);
void sapo_broker_destroy(SAPO_BROKER_T * conn);
char * sapo_broker_error();

// tcp socket operations
int sapo_broker_connect(SAPO_BROKER_T * conn);
int sapo_broker_reconnect(SAPO_BROKER_T * conn);
int sapo_broker_disconnect(SAPO_BROKER_T * conn);
int _sapo_broker_write(int socket, char*msg, int size);
int _sapo_broker_read(int socket, char*msg, int size);


// mantaray operations
int sapo_broker_publish(SAPO_BROKER_T * conn, int type, char * topic, char * payload);
char * sapo_broker_receive(SAPO_BROKER_T * conn);
int sapo_broker_subscribe(SAPO_BROKER_T * conn, int type, char * topic);
int sapo_broker_wait(SAPO_BROKER_T * conn);
int sapo_broker_wait_time(SAPO_BROKER_T * conn, struct timeval * tv);



// xml operations 
char * sapo_broker_parse_soap_message(char * message, int size, char *ns, char * tag, char *field );
void sapo_broker_free_message(char * message);



#endif // _SAPO_BROKER_H_
