/*
 *  brokerctl.h
 *  SAPO Broker
 *
 *  Created by Celso Martinho on 8/14/08.
 *  Copyright 2008 __MyCompanyName__. All rights reserved.
 *
 */

#ifndef __BROKERCTL_H__
#define __BROKERCTL_H__

#include <Security/Authorization.h>
#include <sys/param.h>

typedef enum
{
  BrokerCtlStart = 1,
  BrokerCtlStop = 2,
  BrokerCtlToggleAutoStart = 3
} BrokerCtlCommandType;

typedef struct
{
  BrokerCtlCommandType authorizedCommandId;
  
  char argumentPath[1024];
  bool enable;  
} BrokerCtlCommand;

enum
{
  BrokerCtlCommandInternalError = -1,
  BrokerCtlCommandSuccess = 0,
  BrokerCtlCommandExecFailed,
  BrokerCtlCommandChildError,
  BrokerCtlCommandAuthFailed,
  BrokerCtlCommandOperationFailed,
  BrokerCtlCommandCancelled,
  BrokerCtlCommandCtlNotFound
};

#endif