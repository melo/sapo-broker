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
#include <assert.h>
#include <time.h>
#include <stdarg.h> // va_arg

#include "sapo_broker.h"
#include "sapo_broker_xml.h"
/********************

  TCP Socket operations

*********************/

char g_sapo_broker_error[4096]="";

#define SB_CONNECTED(conn) (conn->connected)

void _sb_set_error(char *str){
	strncpy(g_sapo_broker_error, str, 4096 );
}

void _sb_set_errorf(char * str, ...){	
        va_list ap;
        char strLog[4096];
        va_start(ap,str);
        vsnprintf(strLog,4096,str,ap);
        va_end(ap);
        _sb_set_error(strLog);	
}


char * sb_error(){
	return g_sapo_broker_error;
}


SAPO_BROKER_T * sb_new(char * hostname, int port){
	SAPO_BROKER_T * conn = calloc(1,sizeof(SAPO_BROKER_T));
	_sb_set_error("");
	if (conn==NULL){
		_sb_set_error("[SB]Out of memory!Cannot allocate memory for SAPO_BROKER_T");
		return NULL;
	}
	
	strncpy(conn->hostname, hostname, MAX_HOSTNAME);
	conn->port = port;
	        
	conn->max_retry = SAPO_BROKER_MAX_RETRY;
	conn->blocking_io = SAPO_BROKER_IO_BLOCKING;
	conn->socket = 0;
	conn->connected = 0;
        
        conn->parser = sb_parser_new();
	return conn;
}

void sb_destroy(SAPO_BROKER_T * conn){
	_sb_set_error("");
	if (conn){
		if (conn->connected){
			sb_disconnect(conn);			
		}
                sb_parser_destroy(conn->parser);
		free(conn);
		conn = 0;
	}
}

/*! \fn  int sb_connect (char * hostname, int port)
  \brief Creates a tcp connection to hostname:port
  \param hostname - name of the host
  \param port - port used by the host
  \returns <0 in case of error otherwise returns the socket created
*/

int sb_connect (SAPO_BROKER_T * conn){
	struct hostent     *he=NULL;
	struct sockaddr_in server_addr; 
		
	if (conn && conn->connected){
		_sb_set_error("[sapo_broker] Already connected so sapo_broker");
		return SB_ERROR;
	}	

	_sb_set_error("");
	//log_msgf("Connecting to %s:%d\n",INFO, conn->hostname , conn->port);
	
	// create client endpoint
	if ((conn->socket = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0){
		_sb_set_errorf ("[SB]Could not create a socket: %s",strerror(errno));
		return SB_ERROR;
	}
	
	if ((he = gethostbyname(conn->hostname)) == NULL) {
		_sb_set_errorf ("[SB]Error resolving hostname: %s",strerror(errno));		
		return SB_ERROR;
	}
	
	memset(&server_addr, 0, sizeof(server_addr));
	memcpy(&server_addr.sin_addr, he->h_addr_list[0], he->h_length);
	server_addr.sin_family      = AF_INET;             // Internet address family
	server_addr.sin_port        = htons(conn->port); // Server port
		
	if ( ( connect(conn->socket, (struct sockaddr *) &server_addr, sizeof(server_addr))) < 0){
		conn->last_status = errno;
		_sb_set_errorf ("[SB]Error connecting to server: %s",strerror(errno));
		close(conn->socket);
		return SB_ERROR;
	}
	
	conn->connected=1;

	return SB_OK;
}



/*! \fn  int sb_disconnect(int socket)
  \brief Disconnects the socket
  \param socket - The socket descriptor of the connection 
  \returns 0 in case of success otherwise returns an error code
*/

int sb_disconnect(SAPO_BROKER_T * conn){
	if (conn && conn->connected){
		// wait for broker to receive
		sleep(1);
		close(conn->socket);
		conn->last_status= errno;
		conn->connected = 0;
	}else {
		conn->last_status = 0;
	}
	
	return conn->last_status;
}



int sb_reconnect(SAPO_BROKER_T * conn){
	int ret ;
	sb_disconnect(conn);
	ret = sb_connect(conn);
	return ret;
}

/*! \fn int _sb_read(int socket, (char*)msg , int size);
  \brief reads from the socket given until "size" bytes have arrived
  \param socket - connection socket 
  \param msg - a data buffer to receive
  \param size - the size of the expected data
  \returns -1 in case of error otherwise returns the number of bytes read
*/
int _sb_read(int socket, char*msg , int size){
	int remain = size;
	int ret=0;
	do {		
                // wait all information 
		ret = recv (socket, msg, remain, MSG_WAITALL);
		
		if (errno == EINTR)			
			continue;	       
 		if (ret <0){ // got an error reading . Aborting
			_sb_set_errorf ("[SB]Error reading (Unknown) :",strerror(errno));
			return SB_ERROR_UNKNOWN;
		}
		
		if (ret == 0) { // end of file encontered 
			_sb_set_error("[SB]Error Reading : EOF found");
			return SB_ERROR;
		}

		remain -=ret;
		msg += ret;
	}while (remain>0 );

	return size - remain;
}

/*! \fn int _sb_write(int socket, (char*)msg , int size);
  \brief writes to the socket given until "size" bytes are sent
  \param socket - connection socket 
  \param msg - a data buffer to send
  \param size - the size of the expected data to send
  \returns -1 in case of error otherwise returns the number of bytes writen
*/

int _sb_write(int socket, char*msg , int size){
	int remain = size;
	int ret=0;
        int err=0;
	//printf ("socket[%d]",socket);
	do {
                // MSG_NOSIGNAL -> returns EPIPE ?
                // ENOTCONN
		ret = send(socket, msg, remain, MSG_NOSIGNAL);
                err = errno;
		if (err == EINTR){
			continue;
		}

                if (err == ENOTCONN){
                        _sb_set_errorf("[SB]Error writing (Not connected):",strerror(errno));
                        return SB_NOT_CONNECTED;                        
                }
		if (ret <0){ // got an error writing . Aborting
			_sb_set_errorf ("[SB]Erro writing (Unknown):",strerror(errno));
			return SB_ERROR_UNKNOWN;
		}		
		remain -=ret;
		msg += ret;
	}while (remain>0 );

	return size - remain;
}


// send size + message
// size is in network mode

int _sb_send(int socket, char * body, int body_len ){
	uint32_t nbody_len = htonl(body_len);
        int err=0;

	_sb_set_error("");
	
	if ((err = _sb_write(socket, (char*)&nbody_len , 4)) < 0 ){
		_sb_set_errorf("[SB]Error writing message size:%s",strerror(errno));
		return err;
	}

	//printf ("writing total body *%s*\n", body);
	if ((err = _sb_write(socket, body, body_len)) < 0){
		_sb_set_errorf("[SB]Error writing message:%s",strerror(errno));
		return err;
	}

	return SB_OK;
}


/* ******************
   SAPO BROKER Operations

**********************/
/*
  socket   : connection to Sapo Broker
  type     : type of message - "Enqueue" or "Publish"
  topic    : 
  payload  :
  timetolive: time to live in seconds the broker

*/


int sb_publish_time(SAPO_BROKER_T * conn, int type, char * topic, char * payload, int timetolive){
	char msg_type[10];
	uint32_t body_len;
	char static_body[MAX_BUFFER];
	char *body;
	short allocated=0;
	char ttl_str[30];
	
        if (!conn){
                _sb_set_error("[SB]Not initialized!");
                return SB_NOT_INITIALIZED;
        }
        
	if (!conn->connected){
		_sb_set_error("[SB]No connected!");
		return SB_NOT_CONNECTED;
	}	

	_sb_set_error("");

	if (type == EQUEUE_QUEUE) {
		strcpy(msg_type, SB_SEND_QUEUE);
	}
	else if (type == EQUEUE_PUBLISH){
		strcpy(msg_type, SB_SEND_TOPIC);
	} else {
		_sb_set_errorf("[SB]Publish:Unknown message type %d. Not sending message.",type);
		return SB_BAD_MESSAGE_TYPE;
	}
		
	if (timetolive>0){
		time_t tnow, tmax;
		struct tm *tm_max;
				
		tnow = time(NULL);
		tmax = tnow + timetolive;
		
		tm_max = gmtime(&tmax);
		
		if (strftime(ttl_str, sizeof(ttl_str), "%FT%TZ", tm_max) == 0) {
			_sb_set_errorf("[SB]Publish:Cannot set timetolive in message!"
						"Sending without ttl");
			timetolive = 0;
			
		}
	} else {  // either zero or invalid
		timetolive = 0;
	}
	
	// TPUBLISH is static, can use sizeof
	if (timetolive){
		body_len = sizeof(TPUBLISH_TIME) - 10 -1  // minus \0
			+ 2*strlen(msg_type) 
			+ strlen(topic) 
			+ strlen(payload) 
			+ strlen(ttl_str) ;
	}else {
		body_len = sizeof(TPUBLISH) - 8 -1  // minus \0
			+ 2*strlen(msg_type) 
			+ strlen(topic) 
			+ strlen(payload) ;
	}
	
	// allocate memory if necessary
	if (body_len > MAX_BUFFER){
		body = calloc (body_len+1, sizeof(char));
		allocated = 1;
	}else {
		body = static_body;
	}	

	// fill in the blanks
	if (timetolive){
		sprintf(body, TPUBLISH_TIME, msg_type, ttl_str, topic, payload, msg_type );
	}else {
		sprintf(body, TPUBLISH, msg_type, topic, payload, msg_type );
	}
        
	//printf ("%s\n", body);
        int err;
	if ((err =_sb_send(conn->socket, body, body_len) ) < 0 ){
		_sb_set_error("[SB]Error publishing message!");
		if (allocated){
			free(body);
		}
		
		sb_disconnect(conn);
		return SB_ERROR;
	}
	if (allocated){
		free (body);
	}
	
	return SB_OK;
}




/*
  socket   : connection to mantaray
  type     : type of message - "Enqueue" or "Publish"
  topic    : 
  payload  :

*/

int sb_publish(SAPO_BROKER_T * conn, int type, char * topic, char * payload){
        return sb_publish_time(conn, type, topic, payload,0);
}




/*
  Subscribe to a topic

*/

int sb_subscribe(SAPO_BROKER_T * conn, int type, char * topic){
	char  msg_type[10];
	uint32_t body_len;
	char static_body[MAX_BUFFER];
	char *body;
	short allocated=0;	     	
	
	if (conn && !conn->connected){
		_sb_set_error("[SB] Not connected");
		return SB_NOT_CONNECTED;
	}	
	
	_sb_set_error("");

	if (type == EQUEUE_QUEUE) {
		strcpy(msg_type, SB_RECV_QUEUE);
	}
	else if (type == EQUEUE_TOPIC){
		strcpy(msg_type, SB_RECV_TOPIC);
	} else {
		_sb_set_errorf("[SB]Subscribe:Unknown message type %d. Not sending message.",type);
		return SB_BAD_MESSAGE_TYPE;
	}

	body_len = sizeof(TSUBSCRIBE) - 4 -1 // minus \0
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
	if (_sb_send(conn->socket, body, body_len) < 0 ){
		_sb_set_error("[SB]Error subscribing!");
		if (allocated){
			free(body);
		}
		sb_disconnect(conn);
		return SB_ERROR;
	}             
	
	// wait one second
	struct timeval tv={0,1};
	
	if ( sb_wait_time(conn, &tv) == SB_HAS_MESSAGE){
		_sb_read(conn->socket, body, 10);
		//printf("body= %s\n");
	}
	
	if (allocated){
		free (body);
	}
	
	return SB_OK;
}

/*
  socket : connection socket  
  returns : message received
*/

BrokerMessage * sb_receive(SAPO_BROKER_T * conn){
	uint32_t nbody_len;  // body_len in network format
	uint32_t body_len;   // body_len in local format
	char *body;    // body contents (xml)
	char static_body[MAX_BUFFER];
	short allocated=0;

        if (!conn){
                _sb_set_error("[SB]Not initialized!");
                return NULL;
        }
        
	if (!conn->connected){
		_sb_set_error("[SB]No connected!");
		return NULL;
	}	
	
	_sb_set_error("");
	
	if (_sb_read(conn->socket, (char*)&nbody_len, sizeof(uint32_t))<=0 ) {
		_sb_set_errorf("[SB]Error reading size of the message: %s", strerror(errno));
		sb_disconnect(conn);
		return 0;
	}
	body_len = ntohl(nbody_len);

	if (body_len > MAX_BODY_LEN_RECV ){
		_sb_set_errorf("[SB]body_len is bigger than 1mb [%u]!!",body_len);
		sb_disconnect(conn);
		return NULL;
	}
	
	if (body_len > MAX_BUFFER){
		body = calloc (body_len+1, sizeof(char));
		allocated = 1;
	}else {
		body = static_body;
	}	

	memset(body,0, body_len);
       
	if (_sb_read(conn->socket, body, body_len )<0 ) {
		_sb_set_errorf("[SB]Error reading the body of the message:%s",strerror(errno));
		if (allocated){
			free(body);
			body = NULL;
		}
		sb_disconnect(conn);
		return NULL;
	}

	//printf("received this *%s*\n", body);
	
	/*char *parsed_message = sb_parse_soap_message(body,
							  body_len,
							  "http://services.sapo.pt/broker", 
							  "BrokerMessage",
                                                          "TextPayload");
        */

        BrokerMessage *payload = NULL;
        if (body ){
                payload =  sb_parser_process(conn->parser, body, body_len);
        }
        
	if (allocated){
		free(body);
		body = NULL;
	}
	
	return payload;
}



int sb_send_ack(SAPO_BROKER_T * conn, BrokerMessage * msg){
        char body[MAX_BUFFER];
        int body_len;
        
        if (!conn){
                _sb_set_error("[SB]Not initialized!");
                return SB_NOT_INITIALIZED;
        }
        
	if (!conn->connected){
		_sb_set_error("[SB]No connected!");
		return SB_NOT_CONNECTED;
	}

	if (msg && msg->ack){                
                _sb_set_error("[SB]Already acked!");
		return SB_OK;

        }
        
        
        // TACK is static , you can use sizeof
        body_len = sizeof(TACK) - 4  -1 // minus \0
                + strlen(msg->message_id) 
                + strlen(msg->destination);

        sprintf(body, TACK, msg->message_id, msg->destination);

	if (_sb_send(conn->socket, body, body_len) < 0 ){
		_sb_set_error("[sapo_broker]Error publishing message!");
		sb_disconnect(conn);
		return SB_ERROR;
	}
        
        msg->ack=0;
        
        return 0;
}


int sb_wait(SAPO_BROKER_T * conn){
	struct timeval tv={1,0};
	
	return sb_wait_time(conn, &tv);
}


int sb_wait_time(SAPO_BROKER_T * conn, struct timeval * tv){	
	fd_set set;
	int ret = 0;
	
	if (conn && !conn->connected){
		_sb_set_error("Sapo broker not connected");
		return SB_ERROR;
	}
 
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
		// disconnect from broker
		conn->connected = 0;
		return SB_ERROR;
	}
}



void sb_free_message(BrokerMessage * message){
	if (message!=NULL)
		free (message);
	message = NULL;
}
