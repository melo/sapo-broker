//
//  SAPO_BrokerPref.m
//  SAPO Broker
//
//  Created by Celso Martinho on 8/14/08.
//  Copyright (c) 2008 __MyCompanyName__. All rights reserved.
//

#import "SAPO_BrokerPref.h"
#import "brokerctl.h"
#include <Security/Authorization.h>
#include <Security/AuthorizationTags.h>

#undef NSLocalizedString
#define NSLocalizedString(a,b) a

@implementation SAPO_BrokerPref

- (void) mainViewDidLoad
{
  curState=Stopped;
  firstTime=YES;
  autoStart=YES;
  [self prefStats];

}

- (NSString *)execBrokerCtl:(NSString *)command
{
  NSTask *task = [[NSTask alloc] init];  
  NSString *tmpctl = @BROKER_TMP_CTL_FILE;
  NSString *fileContents = @"";

  if ( [[NSFileManager defaultManager] fileExistsAtPath: tmpctl] )
  {
    [[NSFileManager defaultManager] removeFileAtPath: tmpctl handler: nil];
  }

  BOOL fileCreated = [[NSFileManager defaultManager] createFileAtPath: tmpctl contents: @"" attributes: nil];
  if (fileCreated)
  {

    NSFileHandle *fileHandle = [NSFileHandle fileHandleForWritingAtPath: tmpctl];
    [task setLaunchPath:[NSString stringWithUTF8String:BROKER_COMMAND]];
    [task setArguments:[NSArray arrayWithObject:command]];
    [task setStandardOutput:fileHandle];
    [task setStandardError:[task standardOutput]];
    @try {

      [task launch];
      [task waitUntilExit];
      fileContents = [NSString stringWithContentsOfFile: tmpctl encoding: NSUTF8StringEncoding error: nil];
      unlink(BROKER_TMP_CTL_FILE);
    }
    @catch (NSException *exc) {
      NSRunAlertPanel(NSLocalizedString(@"Error",nil), 
                      (NSString *)@"brokerctl execution falied", 
                      nil, nil, nil);
    }
  }
//  NSRunAlertPanel(NSLocalizedString(@"Alert",nil),(NSString *)fileContents, nil, nil, nil);

  return fileContents;
}

- (NSString *)execPrivBrokerCtl:(NSString *)command
{
  char buf[MAX_STATUS_LEN];
  AuthorizationRef authorizationRef = NULL;
  AuthorizationRights rights;
  AuthorizationFlags flags;
  rights.count=0;
  rights.items = NULL;
  unsigned long ticks;
  NSString *fileContents=@"";
  FILE* f;
  int len=0,l;

  flags = kAuthorizationFlagDefaults;
  OSStatus err = AuthorizationCreate(&rights, kAuthorizationEmptyEnvironment, flags, &authorizationRef);

  AuthorizationItem items[1];
  items[0].name = kAuthorizationRightExecute;
  items[0].value = BROKER_COMMAND;
  items[0].valueLength = strlen(BROKER_COMMAND);
  items[0].flags = 0;
  
  rights.count=1;
  rights.items = items;
  
  flags = kAuthorizationFlagExtendRights;
  err = AuthorizationCopyRights(authorizationRef,&rights,kAuthorizationEmptyEnvironment,flags, NULL);
  
  char* args[2]={(char *)[command UTF8String],NULL};
  if(err = AuthorizationExecuteWithPrivileges(authorizationRef,BROKER_COMMAND,0, args, &f))
  {
    NSRunAlertPanel(NSLocalizedString(@"Error",nil), (NSString *)@"Authorization failed", nil, nil, nil);  
  }
  else
  {
    int child;
    wait(&child); 
  }
  
  if (f)
  {
    while(!feof(f))
    {
      l=fread(buf, 1, MAX_STATUS_LEN, f);
      len+=l;
      if (l<= 0)
      {
        if (feof(f)) break;
        Delay(1, &ticks);
      }
    }
    buf[len-1]=0;
    fileContents=[NSString stringWithFormat:@"%s", buf];
    fflush(f);
    fclose(f);
  }
//  NSRunAlertPanel(NSLocalizedString(@"Error",nil),fileContents, nil, nil, nil);
  
  AuthorizationFree(authorizationRef,kAuthorizationFlagDestroyRights);
  return fileContents;
}

- (void)prefStats
{
  NSString *txStatus = @"";
  FILE*fp;
  
  NSString *output= [self execBrokerCtl:(NSString *)@"status"];
  NSArray *components = [output componentsSeparatedByString:@":"];
  txStatus=[components objectAtIndex:0];

  if(fp=fopen("/opt/local/broker/.startup","r"))
  {
    [autoStartCheck setState:NSOnState];
    autoStart=YES;
    fclose(fp);
  }
  else
  {
    [autoStartCheck setState:NSOffState];
    autoStart=NO;
  }

  if([txStatus isEqualToString:(NSString *)@"1"]) {
    [button setTitle:NSLocalizedString(@"Stop SAPO Broker", nil)];
    [stateText setStringValue:@"running"];
    [descrText setStringValue:@"SAPO Broker is started and ready for client connections.\n"
     "To shut down the daemon, use the \"Stop SAPO Broker\" button."];
    [[stateText cell] setTextColor: [NSColor greenColor]];
    [stateImage setImage:[[[NSImage alloc] initWithContentsOfFile:[[NSBundle bundleForClass:[self class]] pathForResource:@"instance_started" ofType:@"png"]] autorelease]];
    curState=Running;
  }
  else
  {
    [button setTitle:NSLocalizedString(@"Start SAPO Broker", nil)];
    [stateText setStringValue:@"stopped"];
    [descrText setStringValue:@"SAPO Broker is not running.\n"
     "To start SAPO Broker, use the \"Start SAPO Broker\" button."];
    [[stateText cell] setTextColor: [NSColor redColor]];
    [stateImage setImage:[[[NSImage alloc] initWithContentsOfFile:[[NSBundle bundleForClass:[self class]] pathForResource:@"instance_stopped" ofType:@"png"]] autorelease]];
    curState=Stopped;
  }
}

static NSString *errorMessage(int rc)
{
  switch (rc)
  {
    case 1:
      return [NSString stringWithFormat:@"%i", rc];//@"";
    default:
      return [NSString stringWithFormat:@"%i", rc];//@"";
  }
}


- (IBAction)toggleServer:(id)sender
{
  if(curState==Running) {
    [self execPrivBrokerCtl:(NSString *)@"stop"];
  }
  else
  {
    [self execPrivBrokerCtl:(NSString *)@"start"];
    //   NSRunAlertPanel(NSLocalizedString(@"Alert",nil),(NSString *)@"starting", nil, nil, nil);
    
  }
  [self prefStats];
}


- (IBAction)changeAutoStart:(id)sender
{
  if(autoStart==YES) {
    [self execPrivBrokerCtl:(NSString *)@"autostartoff"];
  }
  else
  {
    [self execPrivBrokerCtl:(NSString *)@"autostarton"];    
  }
  [self prefStats];
}

@end
