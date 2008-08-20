//
//  SAPO_BrokerPref.h
//  SAPO Broker
//
//  Created by Celso Martinho on 8/14/08.
//  Copyright (c) 2008 __MyCompanyName__. All rights reserved.
//

#import <PreferencePanes/PreferencePanes.h>
#include <Security/Authorization.h>

#define BROKER_COMMAND "/opt/local/broker/scripts/brokerctl.sh"
#define BROKER_TMP_CTL_FILE "/tmp/brokerctl.log"
#define MAX_STATUS_LEN 40

typedef enum {
	Stopped,
	Running,
	NotFound
} ServerState;

@interface SAPO_BrokerPref : NSPreferencePane 
{
  IBOutlet NSButton *autoStartCheck;
  IBOutlet NSButton *button;
  IBOutlet NSTextField *descrText;
  IBOutlet NSTextField *stateText;
  IBOutlet NSTextField *warningText;
  IBOutlet NSImageView *stateImage;
  NSString *originalWarningText;
  ServerState curState;
  BOOL autoStart;
  BOOL firstTime;
}


- (IBAction)toggleServer:(id)sender;
- (IBAction)changeAutoStart:(id)sender;

- (void)prefStats;

- (void) mainViewDidLoad;

@end
