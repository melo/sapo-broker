/*
 *  brokerctl.c
 *  SAPO Broker
 *
 *  Created by Celso Martinho on 8/14/08.
 *  Copyright 2008 __MyCompanyName__. All rights reserved.
 *
 */

#include "brokerctl.h"

#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/fcntl.h>
#include <sys/errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <mach-o/dyld.h>

//#define DEBUG(fmt, ...) fprintf(stderr, "brokerctl: "fmt"\n", ##__VA_ARGS__)
#define DEBUG(fmt, ...) printf( "brokerctl: "fmt"\n", ##__VA_ARGS__)

static const char *
rightNameForCommand(const BrokerCtlCommand *cmd)
{
  switch (cmd->authorizedCommandId)
  {
    case BrokerCtlStart:
    case BrokerCtlStop:
    case BrokerCtlToggleAutoStart:
      return "pt.sapo.sapoBroker.com.mysql.helper";
  }
  return "system.unknown";
}


static bool performExternalCommand(const BrokerCtlCommand *cmd)
{
  char *args[4];
  int argc;
  pid_t pid;
  int status;
  char *const envp[] = { "PATH=/bin:/usr/bin:/sbin:/usr/sbin", NULL };
  
  switch (cmd->authorizedCommandId)
  {
      /*
    case MAHelperStartMySQL:
      args[0]= MYSQL_COMMAND;
      args[1]= "start";
      argc= 2;
      break;
    case MAHelperStopMySQL:
      args[0]= MYSQL_COMMAND;
      args[1]= "stop";
      argc= 2;
      break; */
    default:
      return false;
  }
  
  DEBUG("will execute %s %s", args[0], args[1]);
  
  args[argc]= NULL;
  if ((pid= fork()) == 0)
  {
    execve(args[0], args, envp);
    exit(-1);
  }
  else if (pid < 0)
    return false;
  
  wait(&status);
  if (pid == -1 || ! WIFEXITED(status))
    return false;
  
  return true;
}

static bool performCommand(const BrokerCtlCommand *cmd)
{
  switch (cmd->authorizedCommandId)
  {
    case BrokerCtlStart:
    case BrokerCtlStop:
      return performExternalCommand(cmd);
    case BrokerCtlToggleAutoStart:
//      return toggleAutoStart(cmd->enable);
    default:
      return false;
  }
}



int main(int argc, char **argv)
{
  char mypath[MAXPATHLEN];
  unsigned long mypath_size= sizeof(mypath);
  OSStatus status;
  AuthorizationRef auth;
  int bytesRead;
  BrokerCtlCommand command;
  
  if (_NSGetExecutablePath(mypath, &mypath_size) < 0)
  {
    DEBUG("could not get my path");
    exit(BrokerCtlCommandInternalError);
  }
  
  AuthorizationExternalForm extAuth;
    
  // read bytestream with Auth data
  if (read(0, &extAuth, sizeof(extAuth)) != sizeof(extAuth))
    exit(BrokerCtlCommandInternalError);
    
  // bytestream --*poof*--> auth
  if (AuthorizationCreateFromExternalForm(&extAuth, &auth))
    exit(BrokerCtlCommandInternalError);
    
  // if we're not being ran as root, spawn a copy that will make us suid root
  if (geteuid() != 0)
  {
    printf("I'm not suided\n");
    exit(-1);
  }
  
  /* Read a command object from stdin. */
  bytesRead = read(0, &command, sizeof(BrokerCtlCommand));
  
  if (bytesRead == sizeof(BrokerCtlCommand))
  {
    const char *rightName = rightNameForCommand(&command);
    AuthorizationItem right = { rightName, 0, NULL, 0 } ;
    AuthorizationRights rights = { 1, &right };
    AuthorizationFlags flags = kAuthorizationFlagDefaults | kAuthorizationFlagInteractionAllowed
    | kAuthorizationFlagExtendRights;
    
    if (status = AuthorizationCopyRights(auth, &rights, kAuthorizationEmptyEnvironment, flags, NULL))
    {
      DEBUG("failed authorization in helper: %ld.\n", status);
      exit(BrokerCtlCommandAuthFailed);
    }
    
    /* Peform the requested command */
    if (!performCommand(&command))
      exit(BrokerCtlCommandOperationFailed);
  }
  else
  {
    exit(BrokerCtlCommandChildError);
  }
  
  return BrokerCtlCommandSuccess;
}