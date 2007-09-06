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
        self->handle = sapo_broker_new(hostname, port);
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
        sapo_broker_destroy(self->handle);
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
        int r = sapo_broker_connect(self->handle);
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
        int r = sapo_broker_disconnect(self->handle);
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
        int r = sapo_broker_reconnect(self->handle);
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
        int r = sapo_broker_publish(self->handle, EQUEUE_TOPIC, topic, payload);
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
        int r = sapo_broker_publish(self->handle, EQUEUE_QUEUE, topic, payload);
        if (r == 0) {
                RETVAL = 1;
        } else {
                RETVAL = 0;
        }
}
OUTPUT:
    RETVAL

int
subscribe(self, char * topic)
SAPO::BrokerXS self
CODE:
{
        int r = sapo_broker_subscribe(self->handle, EQUEUE_TOPIC, topic);
        if (r == 0) {
                RETVAL = 1;
        } else {
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
        int r = sapo_broker_subscribe(self->handle, EQUEUE_QUEUE, topic);
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
        RETVAL = sapo_broker_error(self->handle);
}
OUTPUT:
    RETVAL

int
wait(self)
SAPO::BrokerXS self
CODE:
{
        int r = sapo_broker_wait(self->handle);
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
        int r = sapo_broker_wait(self->handle);
        if (r == 0) {
                RETVAL = 1;
        } else {
                RETVAL = 0;
        }
}
OUTPUT:
    RETVAL

char *
receive(self)
SAPO::BrokerXS self
CODE:
{
        RETVAL = sapo_broker_receive(self->handle);
}
OUTPUT:
    RETVAL
