#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include "ppport.h"

#include <sys/time.h>
#include <math.h>
#include <string.h>
#include <sapo_broker.h>

typedef struct SAPOBrokerXSStruct {
        SAPO_BROKER_T* handle;
}* SAPO__BrokerXS;

MODULE = SAPO::BrokerXS		PACKAGE = SAPO::BrokerXS		
PROTOTYPES: DISABLE

SV *
new(SV * class, char * hostname, int port)
CODE:
{
        /** This code is heavily inspired in the
            Digest::Whirlpool XS code. **/

        // my $object = SAPO_BROKER_T->new($host,$port)
        SAPO__BrokerXS self = malloc(sizeof(SAPO__BrokerXS));
        SV * self_ref;
        const char * pkg;
        
        /* Figure out what class we're supposed to bless into, handle
           $obj->new (for completeness) and Class->new  */
        if (SvROK(class)) {
                /* An object, get is type */
                pkg = sv_reftype(SvRV(class), TRUE);
        } else {
                /* If this function gets called as Pkg->new the value being passed is
                 * a READONLY SV so we'll need a copy
                 */
                pkg = SvPV(class, PL_na);
        }
        
        /* Initialize SAPO Broker and create an IV ref
           containing its memory location */
        self->handle = sb_new(hostname, port);
        self_ref = newRV_noinc((SV *) self);
        RETVAL = newSV(0); /* This gets mortalized automagically */

        sv_setref_pv(RETVAL, pkg, (void*)self);
}
OUTPUT:
    RETVAL

void
DESTROY(self)
         SAPO::BrokerXS self
CODE:
{
        sb_destroy(self->handle);
        self->handle = NULL;
}

int
connect(self)
        SAPO::BrokerXS self
        CODE:
{
        // $object->connect();
        // this returns a true value in success
        // opposed to the C implementation
        int r = sb_connect(self->handle);
        if (r == 0) {
                RETVAL = 1;
        } else {
                RETVAL = 0;
        }
}
OUTPUT:
    RETVAL

int
disconnect(self)
SAPO::BrokerXS self
CODE:
{
        // $object->disconnect();
        // this returns a true value in success
        // opposed to the C implementation
        int r = sb_disconnect(self->handle);
        if (r == 0) {
                RETVAL = 1;
        } else {
                RETVAL = 0;
        }
}
OUTPUT:
    RETVAL

int
reconnect(self)
SAPO::BrokerXS self
CODE:
{
        // $object->reconnect();
        // this returns a true value in success
        // opposed to the C implementation
        int r = sb_reconnect(self->handle);
        if (r == 0) {
                RETVAL = 1;
        } else {
                RETVAL = 0;
        }
}
OUTPUT:
    RETVAL

int
publish(self, char * topic, char * payload)
            SAPO::BrokerXS self
CODE:
{
        // The API here will be rather different for
        // simplification. We'll have four methods with
        // one parameter less instead of two methods with
        // two possible constant values.
        // As the most common case is the Simple TOPIC, 
        // the methods "publish" and "subscribe" will refer
        // to that. For "Topic as a queue", the suffix "_queue"
        // will be used in both methods.
        int r = sb_publish(self->handle, EQUEUE_TOPIC, topic, payload);
        if (r == 0) {
                RETVAL = 1;
        } else {
                RETVAL = 0;
        }
}
OUTPUT:
    RETVAL

int
publish_queue(self, char * topic, char * payload)
SAPO::BrokerXS self
CODE:
{
        int r = sb_publish(self->handle, EQUEUE_QUEUE, topic, payload);
        if (r == 0) {
                RETVAL = 1;
        } else {
                RETVAL = 0;
        }
}
OUTPUT:
    RETVAL


int publish_queue_time(self, char* topic, char* payload, int expiration)
SAPO::BrokerXS self
CODE:
{
       int r = sb_publish_time(self->handle, EQUEUE_QUEUE, topic, payload, expiration);
       RETVAL = (r==0);
}
OUTPUT:
    RETVAL

int
subscribe(self, char * topic)
SAPO::BrokerXS self
CODE:
{
        int r = sb_subscribe(self->handle, EQUEUE_TOPIC, topic);
        if (r == 0) {
                RETVAL = 1;
        } else {
                RETVAL = 0;
        }
}
OUTPUT:
    RETVAL

int
send_ack(self, SV * msg_p)
SAPO::BrokerXS self
CODE:
{
        BrokerMessage message;
	HV * msg = (HV*)SvRV(msg_p);
	SV** message_id =  hv_fetch(msg, (const char*)"msg_id", 6,0);
	SV** destination = hv_fetch(msg, (const char*)"destination", 11,0);
	
	if (message_id && destination){
	  message.ack = 0;
	  strcpy(message.message_id,(char*)SvRV(*message_id));
	  strcpy(message.destination,(char*)SvRV(*destination));	  
	  int r = sb_send_ack(self->handle,&message);
	  RETVAL = (r == 0);
	}else {
	  RETVAL = 0;
	}
}
OUTPUT:
    RETVAL


int
subscribe_queue(self, char * topic)
SAPO::BrokerXS self
CODE:
{
        int r = sb_subscribe(self->handle, EQUEUE_QUEUE, topic);
        if (r == 0) {
                RETVAL = 1;
        } else {
                RETVAL = 0;
        }
}
OUTPUT:
    RETVAL


char *
error(self)
SAPO::BrokerXS self
CODE:
{
        RETVAL = sb_error(self->handle);
}
OUTPUT:
    RETVAL

int
wait(self)
SAPO::BrokerXS self
CODE:
{
        int r = sb_wait(self->handle);
        if (r == 0) {
                RETVAL = 1;
        } else {
                RETVAL = 0;
        }
}
OUTPUT:
    RETVAL

int
wait_time(self, double sec)
SAPO::BrokerXS self
CODE:
{
        int sec_t, usec_t;
        // 1.000001 becomes
        // 1 second and
        // 1 microsecond
        sec_t = trunc(sec);
        usec_t = round((sec - trunc(sec)) * 1000000);
        int r = sb_wait(self->handle);
        if (r == 0) {
                RETVAL = 1;
        } else {
                RETVAL = 0;
        }
}
OUTPUT:
    RETVAL

SV *
receive(self)
SAPO::BrokerXS self
CODE:
{
	HV * message = (HV*)sv_2mortal((SV*)newHV());
        BrokerMessage * bm = sb_receive(self->handle);
	if (bm){
	  hv_store(message, "valid", 5, newSVuv(1),0);
	  hv_store(message, "payload",7,newSVpv(bm->payload, strlen(bm->payload)),0);
	  hv_store(message, "msg_id",6,newSVpv(bm->message_id, strlen(bm->message_id)),0);	
	  hv_store(message, "destination",11,newSVpv(bm->destination, strlen(bm->destination)),0);	
	  sb_free_message(bm);
	}else{
	  hv_store(message, "valid", 5, newSVuv(0),0);
	}

	RETVAL = newRV_inc((SV*)message);
}
OUTPUT:
    RETVAL
