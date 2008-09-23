#define _XOPEN_SOURCE 600
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <stdint.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <errno.h>

#include "sapo_broker.h"

#define TCP_PORT    3322
#define UDP_PORT    3366

#define HOST        "127.0.0.1"
#define PORT        TCP_PORT
#define TYPE        SB_TYPE_TCP // or SB_TYPE_UDP

#define TOPIC       "/my/test/topic"
#define PAYLOAD     "ping"

int main(int argc, char *argv[])
{
    SAPO_BROKER_T *sb;

    sb = sb_new(HOST, PORT, TYPE);

    if (!sb) {
        printf("%s", sb_error());
        exit(-1);
    }

    if (sb_connect(sb) != SB_OK) {
        printf("%s", sb_error());
        exit(-1);
    }

    if (sb_publish(sb, EQUEUE_PUBLISH, TOPIC, PAYLOAD) != SB_OK) {
        printf("%s", sb_error());
        exit(-1);
    }

    exit(0);
}
