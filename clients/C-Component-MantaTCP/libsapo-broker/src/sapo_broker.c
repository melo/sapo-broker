#include <stdio.h>      // s/printf , perror,
#include <netdb.h>      // gethostbyname
#include <errno.h>      // error numbers
#include <sys/socket.h> // socket
#include <arpa/inet.h>  // for sockaddr_in and inet_ntoa()
#include <stdlib.h>     // for atoi() and exit() 
#include <string.h>     // memset
#include <unistd.h>     // sleep, close, read , write
#include <getopt.h>
#include <sys/time.h>

#include <libxml/tree.h>
#include <libxml/parser.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>



#include "sapo_broker.h"

/********************

  TCP Socket operations

*********************/

char g_sapo_broker_error[4096]="";


void _sapo_broker_set_error(char *str){
	strncpy(g_sapo_broker_error, str, 4096 );
}

void _sapo_broker_set_errorf(char * str, ...){	
        va_list ap;
        char strLog[4096];
        va_start(ap,str);
        vsnprintf(strLog,4096,str,ap);
        va_end(ap);
        return _sapo_broker_set_error(strLog);	
}


char * sapo_broker_error(){
	return g_sapo_broker_error;
}



SAPO_BROKER_T * sapo_broker_new(char * hostname, int port)
{	
	SAPO_BROKER_T * conn = calloc(1,sizeof(SAPO_BROKER_T));
	_sapo_broker_set_error("");
	if (conn==0){
		_sapo_broker_set_error("[sapo_broker]Out of memory!Cannot allocate memory for SAPO_BROKER_T");
		return NULL;
	}
	
	strcpy(conn->hostname, hostname);
	conn->port = port;
	
	conn->max_retry = SAPO_BROKER_MAX_RETRY;
	conn->blocking_io = SAPO_BROKER_IO_BLOCKING;
	conn->socket = 0;
	conn->connected = 0;

	return conn;
}



void sapo_broker_destroy(SAPO_BROKER_T * conn){
	_sapo_broker_set_error("");
	if (conn){
		if (conn->connected){
			sapo_broker_disconnect(conn);			
		}
		free(conn);
		conn = 0;
	}
}

/*! \fn  int sapo_broker_connect (char * hostname, int port)
  \brief Creates a tcp connection to hostname:port
  \param hostname - name of the host
  \param port - port used by the host
  \returns -1 in case of error otherwise returns the socket created
*/

int sapo_broker_connect (SAPO_BROKER_T * conn){
	struct hostent     *he=NULL;
	struct sockaddr_in server_addr; 
	
	_sapo_broker_set_error("");
	//log_msgf("Connecting to %s:%d\n",INFO, conn->hostname , conn->port);
	
	// change if going for non_blocking
	
	// create client endpoint
	if ((conn->socket = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0){
		_sapo_broker_set_errorf ("[sapo_broker]Could not create a socket!%s",strerror(errno));
		return -1;
	}
	
	if ((he = gethostbyname(conn->hostname)) == NULL) {
		_sapo_broker_set_errorf ("[sapo_broker]Error resolving hostname!%s",strerror(errno));		
		return -1;
	}
	
	memset(&server_addr, 0, sizeof(server_addr));    
	memcpy(&server_addr.sin_addr, he->h_addr_list[0], he->h_length);
	server_addr.sin_family      = AF_INET;             // Internet address family
	server_addr.sin_port        = htons(conn->port); // Server port
	
	/* Establish the connection to the echo server */
	if ( ( connect(conn->socket, (struct sockaddr *) &server_addr, sizeof(server_addr))) < 0){
		conn->last_status = errno;
		_sapo_broker_set_errorf ("[sapo_broker]Error connecting to server!%s",strerror(errno));
		close(conn->socket);
		return -1;
	}
	
	conn->connected=1;
	return 0;
}



/*! \fn  int sapo_broker_disconnect(int socket)
  \brief Disconnects the socket
  \param socket - The socket descriptor of the connection 
  \returns 0 in case of success otherwise returns an error code
*/

int sapo_broker_disconnect(SAPO_BROKER_T * conn){
	if (conn->connected){
		close(conn->socket);
		conn->last_status= errno;
		conn->connected = 0;
	}else {
		conn->last_status = 0;
	}
	
	return conn->last_status;
}



int sapo_broker_reconnect(SAPO_BROKER_T * conn){
	int ret ;
	sapo_broker_disconnect(conn);
	ret = sapo_broker_connect(conn);
	return ret;
}

/*! \fn int _sapo_broker_read(int socket, (char*)msg , int size);
  \brief reads from the socket given until "size" bytes have arrived
  \param socket - connection socket 
  \param msg - a data buffer to receive
  \param size - the size of the expected data
  \returns -1 in case of error otherwise returns the number of bytes read
*/
int _sapo_broker_read(int socket, char*msg , int size){
	int remain = size;
	int ret=0;
	do {		
		ret = read (socket, msg, remain);
		
		if (errno == EINTR)			
			continue;	       
 		if (ret <0){ // got an error reading . Aborting
			//_sapo_broker_set_errorf ("[Sapo_Broker]Error reading :",strerror(errno));
			return -1;
		}
		
		if (ret == 0) { // end of file encontered 
			//_sapo_broker_set_error("[sapo_Broker]Error Reading : EOF found");
			return -1;
		}
		remain -=ret;
		msg += ret;
	}while (remain>0 );

	return size - remain;
}

/*! \fn int _sapo_broker_write(int socket, (char*)msg , int size);
  \brief writes to the socket given until "size" bytes are sent
  \param socket - connection socket 
  \param msg - a data buffer to send
  \param size - the size of the expected data to send
  \returns -1 in case of error otherwise returns the number of bytes writen
*/

int _sapo_broker_write(int socket, char*msg , int size){
	int remain = size;
	int ret=0;
	//printf ("socket[%d]",socket);
	do {		
		
		ret = write(socket, msg, remain);
		if (errno == EINTR){
			continue;
		}
		if (ret <0){ // got an error writing . Aborting
			//_sapo_broker_set_errorf ("[Sapo_Broker]Error writing:",strerror(errno));
			return -1;
		}
		
		remain -=ret;
		msg += ret;
	}while (remain>0 );

	return size - remain;
}


// send size + message
// size is in network mode

int _sapo_broker_send(int socket, char * body, int body_len ){
	uint32_t nbody_len = htonl(body_len);

	_sapo_broker_set_error("");
	
	if (_sapo_broker_write(socket, (char*)&nbody_len , 4) < 0 ){
		_sapo_broker_set_errorf("[sapo_broker]Error writing message size:%s",strerror(errno));
		return -1;
	}

	//printf ("writing total body *%s*\n", body);
	if ( _sapo_broker_write(socket, body, body_len) < 0){
		_sapo_broker_set_errorf("[sapo_broker]Error writing message:%s",strerror(errno));
		return -1;
	}

	// listen for error...
	return 0;
}


/* ******************
   mantaray operations

**********************/



/*
  socket   : connection to mantaray
  type     : type of message - "Enqueue" or "Publish"
  topic    : 
  payload  :

*/

int sapo_broker_publish(SAPO_BROKER_T * conn, int type, char * topic, char * payload){
	char msg_type[10];
	uint32_t body_len;
	char static_body[MAX_BUFFER];
	char *body;
	short allocated=0;	     	

	_sapo_broker_set_error("");

	if (type == EQUEUE_QUEUE) {
		strcpy(msg_type, "Enqueue");
	}
	else if (type == EQUEUE_PUBLISH){
		strcpy(msg_type, "Publish");
	} else {
		_sapo_broker_set_errorf("[sapo_broker]Publish:Unknown message type %d. Not sending message.",type);
		return -1;
	}

	body_len = strlen(TPUBLISH) - 8
		+ 2*strlen(msg_type) 
		+ strlen(topic) 
		+ strlen(payload);
	
	if (body_len > MAX_BUFFER){
		body = calloc (body_len+1, sizeof(char));
		allocated = 1;
	}else {
		body = static_body;
	}	
	sprintf(body, TPUBLISH, msg_type, topic, payload, msg_type );
	if (_sapo_broker_send(conn->socket, body, body_len) < 0 ){
		//_sapo_broker_set_error("[sapo_broker]Error publishing!");
		if (allocated){
			free(body);
		}

		sapo_broker_disconnect(conn);
		return -1;
	}
	if (allocated){
		free (body);
	}
	return 0;
}




/*
  Subscribe to a topic

*/

int sapo_broker_subscribe(SAPO_BROKER_T * conn, int type, char * topic){
	char  msg_type[10];
	uint32_t body_len;
	char static_body[MAX_BUFFER];
	char *body;
	short allocated=0;	     	
	
	_sapo_broker_set_error("");

	if (type == EQUEUE_QUEUE) {
		strcpy(msg_type, "QUEUE");
	}
	else if (type == EQUEUE_TOPIC){
		strcpy(msg_type, "TOPIC");
	} else {
		_sapo_broker_set_errorf("[sapo_broker]Subscribe:Unknown message type %d. Not sending message.",type);
		return -1;
	}

	body_len = strlen(TSUBSCRIBE) - 4
		+ strlen(msg_type) 
		+ strlen(topic);
	

	if (body_len > MAX_BUFFER){
		body = calloc (body_len+1, sizeof(char));
		allocated = 1;
	}else {
		body = static_body;
	}	
	sprintf(body, TSUBSCRIBE, topic, msg_type );
	//printf ("body_len  %d = %d\n ", body_len, strlen(body));
	if (_sapo_broker_send(conn->socket, body, body_len) < 0 ){
		//_sapo_broker_set_error("[sapo_broker]Error sending subscription!\n");
		if (allocated){
			free(body);
		}
		sapo_broker_disconnect(conn);
		return -1;
	}

       
	struct timeval tv={0,1};
	
	if ( sapo_broker_wait_time(conn, &tv) == SB_HAS_MESSAGE){
		_sapo_broker_read(conn->socket, body, 10);
		//printf("body= %s\n");
	}
	
	if (allocated){
		free (body);
	}
	return 0;
}

/*
  socket : connection socket  
  returns : message received
*/

char * sapo_broker_receive(SAPO_BROKER_T * conn){
	uint32_t nbody_len;  // body_len in network format
	uint32_t body_len;   // body_len in local format
	char *body;    // body contents (xml)
	char static_body[MAX_BUFFER];
	short allocated=0;
	struct timeval start, end, diff;
		
	_sapo_broker_set_error("");
	
	gettimeofday(&start, 0);	
	
	if (_sapo_broker_read(conn->socket, (char*)&nbody_len, sizeof(uint32_t))<=0 ) {
		_sapo_broker_set_errorf("[sapo_broker]Error reading size of the message: %s", strerror(errno));
		sapo_broker_disconnect(conn);
		return 0;
	}		
	gettimeofday(&end, 0);	
	timersub(&end,&start,&diff);
	//printf("waiting for read: %i.%06i\n",(unsigned int)diff.tv_sec,(unsigned int)diff.tv_usec);


	body_len = ntohl(nbody_len);

	if (body_len > 1048576){
		_sapo_broker_set_errorf("[sapo_broker]body_len is bigger than 1mb [%u]!!",body_len);
		sapo_broker_disconnect(conn);
		return NULL;
	}
	
	if (body_len > MAX_BUFFER){
		body = calloc (body_len+1, sizeof(char));
		allocated = 1;
	}else {
		body = static_body;
	}	

	memset(body,0, body_len);
       
	if (_sapo_broker_read(conn->socket, body, body_len )<0 ) {
		_sapo_broker_set_errorf("[sapo_broker]Error reading the body of the message:%s",strerror(errno));
		if (allocated){
			free(body);
			body = NULL;
		}
		sapo_broker_disconnect(conn);
		return 0;
	}

	//printf("received this *%s*\n", body);
	
	char *parsed_message = sapo_broker_parse_soap_message(body,
							  body_len,
							  "http://services.sapo.pt/broker", 
							  "BrokerMessage",
							  "TextPayload");
	if (allocated){
		free(body);
		body = NULL;
	}
	
	return parsed_message;
}



int sapo_broker_wait(SAPO_BROKER_T * conn){
	struct timeval tv={1,0};
	
	return sapo_broker_wait_time(conn, &tv);
}


int sapo_broker_wait_time(SAPO_BROKER_T * conn, struct timeval * tv){	
	fd_set set;
	int ret = 0;
	       
	// Initialize the file descriptor set. 
	FD_ZERO (&set);
	FD_SET (conn->socket, &set);
	
	// select returns 0 if timeout, 1 if input available, -1 if error. *
	ret = select (FD_SETSIZE, &set, NULL, NULL, tv);
	
	switch(ret){
	case 0:
		return SB_NO_MESSAGE;
	case 1:
		return SB_HAS_MESSAGE;
	default:
		return SB_ERROR;
	}
}





/***********************
 
  XML Operations

************************/




/*
  xml    : xml string
  ns     : namespace
  tag    : tag to find
  field  : field to find
*/

char * sapo_broker_parse_soap_message(char * message, int size, char *ns, char * tag, char *field ){	
	xmlDocPtr doc;
	xmlXPathContextPtr xpathCtx; 
	xmlXPathObjectPtr xpathObj; 
	unsigned char tag2find[255];

	char * payload = NULL;
	int payload_len;
	struct timeval start, end, diff;

	_sapo_broker_set_error("");
	
	gettimeofday(&start, 0);	
	
	// moved to caller program
	//xmlInitParser();
	//LIBXML_TEST_VERSION ;
	
	// Load XML document
	doc = xmlParseMemory(message, size);
	if (doc == NULL) {
		_sapo_broker_set_errorf ("[sapo_broker] unable to parse \"%s\"", message);
		//xmlCleanupParser();
		return 0;
	}
	
	// Create xpath evaluation context     
	xpathCtx = xmlXPathNewContext(doc);
	if (xpathCtx == NULL) {
		_sapo_broker_set_errorf( "[sapo_broker]unable to parse \"%s\"",message);
		xmlFreeDoc(doc); 
		//xmlCleanupParser();
		return 0;
	}
	
	// register namespace		
	xmlXPathRegisterNs(xpathCtx, (unsigned char*)"def", (unsigned char*)ns);		
	//xmlXPathRegisterNs(xpathCtx, "wsa", "http://www.w3.org/2005/08/addressing");

	/* Evaluate xpath expression */
	sprintf((char*)tag2find,"//def:%s/def:%s/text()",tag, field);		
	xpathObj = xmlXPathEvalExpression(tag2find, xpathCtx);
	if(xpathObj == NULL) {
		
		_sapo_broker_set_errorf ("[sapo_broker]unable to evaluate xpath expression \"%s\"",tag2find);
		xmlXPathFreeContext(xpathCtx); 
		xmlFreeDoc(doc); 
		//xmlCleanupParser();
		return 0;
	}
	
	if (xpathObj == NULL) {
		_sapo_broker_set_error("[sapo_broker]Error in xmlXPathEvalExpression");
		return NULL;
	}
	
	if(xmlXPathNodeSetIsEmpty(xpathObj->nodesetval)){
		xmlXPathFreeObject(xpathObj);
                _sapo_broker_set_error("[sapo_broker]Message with no contents!");		
       	}else {
		if (xpathObj->nodesetval->nodeTab[0]->content){
			//printf ("%s\n", xpathObj->nodesetval->nodeTab[0]->content);			
			payload_len = strlen((char*)xpathObj->nodesetval->nodeTab[0]->content);
			payload = calloc(1,payload_len+1);			
			strcpy(payload, (char*)xpathObj->nodesetval->nodeTab[0]->content);
		}
		xmlXPathFreeObject(xpathObj);
	}
				
	xmlXPathFreeContext(xpathCtx); 
	/* free the document */
	xmlFreeDoc(doc);
	
	/* Shutdown libxml */
	// moved to caller program
	//xmlCleanupParser();
	
	gettimeofday(&end, 0);
	
	timersub(&end,&start,&diff);
	//printf("with took: %i.%06i\n",(unsigned int)diff.tv_sec,(unsigned int)diff.tv_usec);

	return payload;
}



void sapo_broker_free_message(char * message){
	if (message!=NULL)
		free (message);
	message = NULL;
}




