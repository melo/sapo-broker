#include <stdio.h>      // s/printf , perror,
#include <getopt.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <glib.h>
#include "sapo_broker.h"
#include "sapo_broker_xml.h"


/*
#define ADW_QUEUE_STRING "\
<adw-queue>\
  <operation>\
    <operation_id> 1 </operation_id>\
    <creation_time> %d </creation_time>\
    <aged> 0 </aged>\
    <click>\
      <word_id> 30184 </word_id>\
      <bid_id> 436 </bid_id>\
      <advertise_id> 16 </advertise_id>\
      <campaign_id> 70 </campaign_id>\
      <user_id> 70 </user_id>\
      <value> 0.05 </value>\
      <ip>123.123.123.123</ip>\
      <channel> 1 </channel>\
    </click>\
    <impression>\
      <word_id> 111 </word_id>\
      <bid_id> 222 </bid_id>\
      <advertise_id> 333 </advertise_id>\
      <user_id> 555 </user_id>\
      <channel> 666 </channel>\
      <position> 777 </position>\
    </impression>\
    <balance>\
      <user_id> 555 </user_id>\
      <value> 111.111 </value>\
    </balance>\
    <table>\
       <table_id> 9999 </table_id>\
       <record_id> 888 </record_id>\
       <group>\
          <group_id> 999 </group_id>\
          <user_id> 666 </user_id>\
          <campaign_id> 444 </campaign_id>>\
          <group_description> char </group_description>\
          <max_cpc> 222.222 </max_cpc>\
          <status> 1 </status>\
          <deleted> 1111 </deleted>\
          <created> 2222 </created>\
          <modified> 3333 </modified>\
          <default_url> char </default_url>\
       </group>\
       <bid>\
          <bid_id> 222 </bid_id>\
          <group_id> 999 </group_id>\
          <word_id> 111 </word_id>\
          <word_impressions> 1111 </word_impressions>\
          <word_clicks> 2222 </word_clicks>\
          <bid_cpc> 333.333 </bid_cpc>\
          <created> 1 </created>\
          <modified> 0 </modified>\
          <deleted> 0 </deleted>\
          <user_id> 555 </user_id>\
          <bid_url> char </bid_url>\
          <district_id> 1 </district_id>\
       </bid>\
       <advertise>\
          <advertise_id> 333 </advertise_id>\
          <user_id> 555 </user_id>\
          <group_id> 999 </group_id>\
          <title> char </title>\
          <line1> char </line1>\
          <line2> char  </line2>\
          <url_redirect> char </url_redirect>\
          <url_display> char </url_display>\
          <deleted> 1 </deleted>\
          <created> 0 </created>\
          <modified> 0 </modified>\
          <img_path> char </img_path>\
          <img_title> char </img_title>\
          <impressions> 1 </impressions>\
          <clicks> 2 </clicks>\
       </advertise>\
       <campaign>\
          <campaign_id> 444 </campaign_id>\
          <user_id> 555 </user_id>\
          <description> char </description>\
          <max_budget> 444.444 </max_budget>\
          <start_date> 123455 </start_date>\
          <end_date> 23456 </end_date>\
          <status> 1 </status>\
          <deleted> 1 </deleted>\
          <created> 0 </created>\
          <modified> 0 </modified>\
          <daily_costs> 555.555 </daily_costs>\
       </campaign>\
       <user>\
          <user_id> 555 </user_id>\
          <netbi> char </netbi>\
          <credit> 1 </credit>\
          <status> 1 </status>\
          <deleted> 1 </deleted>\
          <created> 1 </created>\
          <modified> 1 </modified>\
          <balance> 666.666 </balance>\
          <daily_costs> 777.777 </daily_costs>\
       </user>\
       <word>\
          <word_id> 111 </word_id>\
          <word> char </word>\
          <impressions> 1 </impressions>\
          <clicks> 2 </clicks>\
       </word>\
       <channel>\
          <channel_id> 1666 </channel_id>\
          <name>  char </name>\
          <max_ads> 2 </max_ads>\
          <css_url> char </css_url>\
          <words> char </words>\
          <title> char </title>\
          <subtitle> char </subtitle>\
          <valid_data> 1 </valid_data>\
       </channel>\
    </table>\
  </operation>\
</adw-queue>"
*/

/*! \struct option
  \brief This structure defines the application parameters.
*/
const static struct option long_options[] = {
	{"hostname", 1, NULL, 'h'},
	{"port", 1, NULL, 'p'},
	{"topic",1,NULL, 't'},
	{"message",1,NULL,'m'},
	{"receiver",no_argument,NULL,'r'},	
	{"interative",no_argument,NULL,'i'},
	{"sender",no_argument,NULL,'s'},
	{"help",no_argument,NULL,'?'},
	{"queue",1,NULL,'q'},
	{0,0,0,0}
};

enum{ ROLE_NONE = 0,
	      ROLE_RECEIVER,
	      ROLE_SENDER,
	      ROLE_INTERACTIVE
	      };

typedef struct _sapo_broker_options {
	int role;
	int interactive;
	int receiver;	
	int sender;
} SAPO_BROKER_OPTIONS;



int receiver( SAPO_BROKER_T *conn, int msg_type, char * topic);
int sender( SAPO_BROKER_T *conn, int msg_type, char * topic, char * message);
int interactive( SAPO_BROKER_T *conn, int msg_type, char * topic, char * message);

void usage(char *name);


unsigned char map_bit[]={0x1, 0x2, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80};
void showbits( unsigned char* str, int len){  
  int i;
  int j;

  for(j=0; j<len; j++){
    for(i=0;i<8; i++){
      printf("%c",(str[j]&map_bit[7-i])?'1':'0');
    }
    printf(" ");
  }
  printf("\n");
}


void usage(char *name){
	printf("Usage: %s [options]\n", name);
	printf("  -?\tShows this help\n");
	printf("  -h=hostname\tHostname of the mantaray[default 127.0.0.1]\n");
	printf("  -p=port\tPort number of the mantaray[default 2222]\n");
	printf("  -t=topic\tTopic\n");
	printf("  -m=message\tMessage to send\n");
	printf("  -r\tRole is receiver\n");
        printf("  -c\tcount messages per second\n");
	printf("  -i\tRole is interactive\n");
	printf("  -s\tRole is sender\n");
	printf("  -q=msg_type\tType of message to send [1=QUEUE,2=TOPIC,3=PUBLISH]\t");
        
}

int show_menu(int socket, int type, char * topic, char * body){	
	printf("1 - Connect\n");
	printf("2 - Disconnect\n");
	printf("3 - Set message\n");
	printf("4 - Publish\n");
	printf("5 - Subscribe\n");
	printf("6 - Receive\n");
	printf("\n0 - quit\n");
	printf("Current status : %s\n", (socket==0)?"Disconnected":"Connected");
	printf("Message type : %d\n", type);
	printf("Message topic: %s\n", topic);
	printf("Message body : %s\n", body);
		
	char bla[255];
	fgets(bla,255, stdin);
	return bla[0];
}



int count = 0;

int main (int argc, char *argv[]){	
	char hostname[20];
	int port ;
	int lastopt=0;
	int longindex=0;	
	

	int msg_type=EQUEUE_QUEUE;
	char topic[255];
	char message[255];
	SAPO_BROKER_T * conn=0;

	int role = ROLE_NONE;
	strcpy(topic, "/sapo/adword\0");
	strcpy (message, "Hello world\0");
	strcpy(hostname,DEFAULT_HOST);
	port = DEFAULT_PORT;
	
	while (1) {
                lastopt = getopt_long(argc,argv,"q:h:p:t:m:ris",long_options,&longindex);
                if (lastopt == -1)
                        break;
                switch (lastopt) {
                case 0:
                        break;
		case 'h':
			printf ("hostname = %s\n", optarg);
			strncpy(hostname, optarg, 20);
			break;
		case 'p':
			printf ("port = %s\n", optarg);
			port = atoi(optarg);
			break;
		case 't':
			printf ("Using topic : %s\n",optarg);
			strcpy(topic, optarg);
			break;
		case 'm': 
			printf ("Using message: %s\n", optarg);
			strcpy(message, optarg);
			break;
		case 'r': 
			if (role){
				printf("Cannot be receiver and sender/interactive at the same time\n");
				exit(1);
			}
			printf ("Will subscribe the topic and receive only\n");			
			role = ROLE_RECEIVER;
			break;

                case 'c':
                        count = 0;
                        break;
		case 'i':
			if (role){
				printf("Cannot be interactive and sender/receiver at the same time\n");
				exit(1);
			}
			printf("Showing interactive menu\n");
			role = ROLE_INTERACTIVE;
			break;
		case 's': 
			if (role){
				printf("Cannot be sender  and interactive/receiver at the same time\n");
				exit(1);
			}
			printf("Will send to the topic only\n");
			role = ROLE_SENDER;
			break;
		case 'q':
			msg_type = atoi(optarg);
			if ((msg_type< 1) || (msg_type>3)){
				printf("Invalid message type %d. Only valid 1,2 or 3",msg_type);
				usage(argv[0]);
				exit(0);
			}
			break;
		case '?': 
			usage(argv[0]);
			exit(0);
                default:
			printf ("Bad param %c \n", lastopt);
			usage(argv[0]);
                        exit(1);
                }
        }

	conn = sb_new(hostname , port, SB_TYPE_TCP);
	
	
	switch(role){
	case ROLE_INTERACTIVE:
		interactive(conn, msg_type, topic, message);
		break;
	case ROLE_SENDER:
		sender(conn, msg_type, topic, message);
		break;
	case ROLE_RECEIVER :
		receiver(conn, msg_type, topic);
		break;
	default:
		printf("No role provided. Defaulting to interactive\n");
		interactive(conn, msg_type, topic, message);
	}
	
	if (conn)
		sb_destroy(conn);

	return 0;
}




int receiver( SAPO_BROKER_T *conn, int msg_type, char * topic){
	BrokerMessage * payload;
	int i=1;
	
        GTimer * gtime_iter;
        gtime_iter = g_timer_new();        
        
        g_timer_start(gtime_iter);

	while(1){
		if (!conn->connected){
			if (sb_reconnect(conn) ){
				printf("Problems connecting: %s\n", strerror(conn->last_status));
				sleep(1);
				continue;
			}
			if (sb_subscribe(conn,msg_type, topic)<0){
				printf("Problems subscribing: %s\n ", strerror(conn->last_status));
				sleep(1);
				continue;
			}
		}
		if ( (payload = sb_receive(conn))==NULL){
			printf("Connection was broken when receiving. Reconnecting\n");
			usleep(10000);
			sb_reconnect(conn);
			continue;
		}
		payload->payload[120] = 0;
		printf("%s\n",payload->payload);
		//if (i%100 ==0)
		//	printf (">>%s\n",payload);
		
                sb_send_ack(conn, payload);
		i++;		
		sb_free_message(payload);

                if (i%1000==0){
                        float a= g_timer_elapsed(gtime_iter,NULL) ;
                        printf("Received %d in %.2f secs. %.2f messages per second\n", i, a , (float)i/a );
                        g_timer_start(gtime_iter);
                        i = 0;
                }
                
	}
}

int sender( SAPO_BROKER_T *conn, int msg_type, char * topic, char * message){
	int i=0 , c=0;
	char str[1024*8];
	
        GTimer * gtime_iter;
        gtime_iter = g_timer_new();        
        
        g_timer_start(gtime_iter);
	while(i < 10){
                
		i++;
                c++;
		if (!conn->connected){
			if (sb_reconnect(conn) ){
				printf("Problems connecting: %s\n", strerror(conn->last_status));
				sleep(1);
				continue;
			}
		}
		
		//sprintf(str,ADW_QUEUE_STRING,i++, time(NULL));
		//printf ("word_id = %d\n", i);
		//sprintf(str,ADW_QUEUE_STRING, (unsigned int)time(NULL) );
		
                sprintf(str,"%d",i);
		//sprintf(str,"AAAAAAA generating %d AAAAAAA",i);		
		//if (i%100 ==0)
		//	printf("%d\n",i);
			//i++;

		if (sb_publish_time(conn, msg_type, topic, str,10)<0){
			printf("Connection was broken when sending. Reconnecting\n");
			sleep(1);
			sb_reconnect(conn);
			continue;
		}
                
                if (c%1000==0){
                        float a= g_timer_elapsed(gtime_iter,NULL);
                        printf("Sending %d msg in %.2f : %.2f messages per second\n", c, a, (float)c/a );
                        g_timer_start(gtime_iter);
                        c= 0;
                }
                
		//usleep(1);
	}
	printf("going away\n");
	return 0;
}

int interactive( SAPO_BROKER_T *conn, int msg_type, char * topic, char * message){	
	int quit=0;

	do {
		printf ("\n\n\n");
		int option = show_menu(conn->socket, msg_type, topic, message);
		switch(option){
		case '1': // 
			if (conn && conn->socket>0){
				printf("Already connected\n");				
				continue;
			}
			//socket = sb_connect(hostname, port);
			sb_connect(conn);
			if (conn && conn->socket>0){
				printf("Connected successfuly\n");
			}
			else {
				
				printf("Error connecting\n");
				sb_disconnect(conn);
				sleep(1);			       
			}
			break;
			
		case '2':
			if (conn && conn->socket==0){
				printf("Already disconnected\n");				
				continue;
			}
			if (sb_disconnect(conn) < 0){
				printf("Disconnected successfuly\n");
			}		
			else {
				printf("Got problems disconnecting\n");
			}
			break;
		case '3':
			do{
				printf("Message type <%d-%d>:", 1 , 3);
				scanf("%d", &msg_type);
			}while(msg_type<= 1 && msg_type >= 3);
			printf("Topic: ");
			scanf("%s", topic);
			printf("Message: ");
			scanf("%s", message);
			break;
			
		case '4': // publish
			if (conn && conn->socket>0)
				sb_publish(conn, msg_type, topic, message);
			break;
		case '5': // subscribe
			if (conn && conn->socket>0)
				sb_subscribe(conn, msg_type, topic);
			break;
		case '6': // receive
			if (conn && conn->socket>0)
				sb_receive(conn);
			break;			
		case '0':
			quit = 1;
			break;
		default:
			continue;			
		}
	}while (!quit);
	return 0;
}
