#include <stdio.h>
#include <string.h>
//#include <glib.h>
#include "expat.h"
#include "sapo_broker_xml.h"



void sbx_start(void *data, const char *el, const char **attr)
{
    BrokerParserData *bp = (BrokerParserData *) data;

    ++bp->buffer.nstack;
    bp->buffer.len = 0;

    if (strcmp(el, "http://services.sapo.pt/broker MessageId") == 0) {
        bp->buffer.stack[bp->buffer.nstack] = XML_MSGID;
    } else if (strcmp(el, "http://services.sapo.pt/broker Priority") == 0) {
        bp->buffer.stack[bp->buffer.nstack] = XML_PRIORITY;
    } else if (strcmp(el, "http://services.sapo.pt/broker Expiration") == 0) {
        bp->buffer.stack[bp->buffer.nstack] = XML_EXPIRATION;
    } else if (strcmp(el, "http://services.sapo.pt/broker DestinationName") ==
               0) {
        bp->buffer.stack[bp->buffer.nstack] = XML_DESTINATION;
    } else if (strcmp(el, "http://services.sapo.pt/broker TextPayload") == 0) {
        bp->buffer.stack[bp->buffer.nstack] = XML_PAYLOAD;
    } else {
        bp->buffer.stack[bp->buffer.nstack] = XML_NOP;
    }
}

void sbx_end(void *data, const char *el)
{
    BrokerParserData *bp = (BrokerParserData *) data;

    // zero the last position
    bp->buffer.text[bp->buffer.len] = 0;

    if (bp->buffer.len > 0) {
        switch (bp->buffer.stack[bp->buffer.nstack]) {
        case XML_MSGID:
            strcpy(bp->msg->message_id, bp->buffer.text);
            break;
        case XML_PRIORITY:
            bp->msg->priority = atoi(bp->buffer.text);
            break;
        case XML_EXPIRATION:
            //expiration not in use ..yet
            memset(&bp->msg->expiration, 0, sizeof(bp->msg->expiration));
            strptime(bp->buffer.text, "%Y-%m-%dT%T.%z", &bp->msg->expiration);
            break;
        case XML_DESTINATION:
            strcpy(bp->msg->destination, bp->buffer.text);
            break;
        case XML_PAYLOAD:
            strcpy(bp->msg->payload, bp->buffer.text);
            break;
        default:
            break;
        }
    }
    // cleanup buffer
    bp->buffer.len = 0;
    bp->buffer.nstack--;
}

void getText(void *data, const XML_Char * s, int len)
{
    BrokerParserData *bp = (BrokerParserData *) data;
    if (bp->buffer.stack[bp->buffer.nstack] != XML_NOP) {
        memcpy(bp->buffer.text + bp->buffer.len, s, len);
        bp->buffer.len += len;
    }
}


BrokerMessage *sb_parser_process(XML_Parser p, char *buffer, int len)
{
    BrokerParserData *bp = (BrokerParserData *) XML_GetUserData(p);
    bp->msg = calloc(1, sizeof(BrokerMessage));
    XML_ParserReset(p, NULL);
    XML_SetUserData(p, bp);
    XML_SetElementHandler(p, sbx_start, sbx_end);
    XML_SetCharacterDataHandler(p, getText);

    if (!XML_Parse(p, buffer, len, 1)) {
        fprintf(stderr, "Parse error at line %d:\n%s\n",
                XML_GetCurrentLineNumber(p),
                XML_ErrorString(XML_GetErrorCode(p)));
        exit(-1);
    }

    return bp->msg;
}


int showResult(BrokerMessage * bm)
{
    printf("destination : %s\n", bm->destination);
    printf("priority : %d\n", bm->priority);
    //printf ("expiration : %u\n", (unsigned int)bm->expiration);
    printf("message_id : %s\n", bm->message_id);
    printf("payload  : %s\n", bm->payload);
    return 0;
}


XML_Parser sb_parser_new()
{
    XML_Parser p;
    BrokerParserData *bp = calloc(1, sizeof(BrokerParserData));
    p = XML_ParserCreateNS(NULL, ' ');

    XML_SetUserData(p, bp);
    XML_SetElementHandler(p, sbx_start, sbx_end);
    XML_SetCharacterDataHandler(p, getText);

    return p;
}

void sb_parser_destroy(XML_Parser p)
{
    void *x = XML_GetUserData(p);
    if (x)
        free(x);
    XML_ParserFree(p);
}

/*
int main(int argc, char **argv) {
  FILE *f ;
  char buffer[MAX_SIZE_BUFFER];
  
  if (argc !=3){
          printf("Provide file\n");
          return -1;
  }
  
  int a = atoi(argv[2]);
  f = fopen(argv[1], "r");
  if (!f){
          perror("Cannot open file\n");
          return -1;
  }

  int total=0;
  int len;

  for (;;) {    
          len = fread(buffer+total, 1, BUFFSIZE, f);
          total +=len;
          if (len==0){
                  fclose(f);
                  break;            
          }
  }
  int i ;
  XML_Parser p;
  p = newParser();

  GTimer * gtime_iter;
  gtime_iter = g_timer_new();
  g_timer_start(gtime_iter);
  
  for (i=0;i<a;i++){
          void * x = parseBrokerMessage(p, buffer, total);
          free(x);
  }
  float tt = g_timer_elapsed(gtime_iter, NULL); // time receive
  printf("took me %f seconds to do it; %f per second\n", tt, a/tt);
  
  g_timer_destroy(gtime_iter);
  // show stuff
  BrokerMessage * x = parseBrokerMessage(p,buffer, total);
  showResult(x);
  free(x);

  destroyParser(p);
  //printf()

  return 0;
} 

*/
