<?php
/**
 * SAPO Broker access abstraction class.
 *
 * The Broker class abstracts all the low-level complexity and
 * Mataray agents and gives the developers a simple to use high-level API
 * to build consumers and producers.
 *
 * @package Broker
 * @version 0.2  
 * @author Celso Martinho <celso@co.sapo.pt>
 * @author Bruno Pedro <bpedro@co.sapo.pt>
 **/
class SAPO_Broker {
    
    var $parser;
    var $net;
    var $debug;

    function SAPO_Broker ($args=array())
    {
        // args defaults 
        $args=array_merge(array('port'=>3322,
                                'debug'=>FALSE,
                                'force_sockets'=>FALSE,
                                'force_streams'=>FALSE,
				                        'timeout'=>60*5, // in seconds
                                'force_expat'=>FALSE,
                                'locale'=>'pt_PT',
                                'force_dom'=>FALSE),$args);

        setlocale(LC_TIME, $args['locale']);

        // pre init()
        $this->debug = $args['debug'];
        //
        // Check for Multibyte functions
        //
        if(extension_loaded('mbstring')==FALSE) {
          die ("SAPO_Broker requires Multibyte String Functions support.\nPlease upgrade...\n\n");
        }

        //
        // Check for supported PHP version.
        //
        if (version_compare(phpversion(), '4.3.0', '<')) {
            die ("SAPO_Broker needs at least PHP 4.3.0 to run properly.\nPlease upgrade...\n\n");
        }

        $this->net =& new SAPO_Broker_Net($this->debug);
        
        // setting default timeouts - usefull for low traffic topics (higher these to avoid disconnects)
	      $this->net->rcv_to=$args['timeout']*1000000;
	      $this->net->snd_to=$args['timeout']*1000000;

        // Health checking
        $this->add_callback(array("sec"=>5),array('SAPO_Broker_Net','sendKeepalive'));
          
        //
        // Look for DOM support and use appropriate Parser.
        //
        if ((!extension_loaded('dom') || $args['force_expat']) && $args['force_dom']==FALSE) {
            SAPO_Broker::dodebug("Using SAPO_Broker_Parser() aka expat");
            $this->parser =& new SAPO_Broker_Parser($this->debug,$this->net);
        } else {
            SAPO_Broker::dodebug("Using SAPO_Broker_Parser_DOM() aka native DOM support");
            $this->parser =& new SAPO_Broker_Parser_DOM($this->debug,$this->net);
        }

        // post init()

        if(!$args['server']) {
          SAPO_Broker::dodebug("No server defined. Doing auto-discovery.");
          if(getenv('SAPO_BROKER_SERVER')) {
            SAPO_Broker::dodebug("Trying to use SAPO_BROKER_SERVER");
            if($this->net->tryConnect(getenv('SAPO_BROKER_SERVER'),$args['port'])) {
              $args['server']=getenv('SAPO_BROKER_SERVER');
              }
              else
              {
              SAPO_Broker::dodebug("Couldn't connect to SAPO_BROKER_SERVER: ".getenv('SAPO_BROKER_SERVER'));
              }
            }
          if(!$args['server'] && $this->net->tryConnect('127.0.0.1',$args['port'])) {
            SAPO_Broker::dodebug("Using 127.0.0.1. Local agent seems present.");
            $args['server']='127.0.0.1';
            }
          if(!$args['server'] && @file_exists('/etc/world_map.xml')) {
            SAPO_Broker::dodebug("Picking random agent from /etc/world_map.xml.");
            $i=0;
            while($i<3) {
              $server=$this->parser->worldmapPush('/etc/world_map.xml');
              SAPO_Broker::dodebug("Picked ".$server." (".($i+1)."). Testing connection.");
              if($this->net->tryConnect($server,$args['port'])) {
                $args['server']=$server;
                break;
                }
              $i++;
              }
            if($args['server']) SAPO_Broker::dodebug("Will use ".$args['server']);
            }
          if(!$args['server']) {
            SAPO_Broker::dodebug("Usign last resort round-robin DNS broker.bk.sapo.pt");
            $args['server']='broker.bk.sapo.pt';
            }
          }

        SAPO_Broker::dodebug("Initializing network.");
        $this->net->init($args['server'], $args['port']);
        $this->net->latest_status_message='';
        $this->net->latest_status_timestamp_received=0;
        $this->net->latest_status_timestamp_sent=0;
    }

    function dodebug($msg) {
      if($this->debug) {
        echo $msg."\n";
        }
      }

    /**
     * This is a facade to SAPO_Broker_Tools::xmlentities()
     *
     * @return string
     * @author Bruno Pedro
     **/
    function xmlentities ($string, $quote_style = ENT_QUOTES, $charset = 'UTF-8')
    {
        return SAPO_Broker_Tools::xmlentities($string, $quote_style, $charset);

    }

    /**
     * This is a facade to SAPO_Broker_Net::init()
     *
     * @return void
     * @author Bruno Pedro
     **/
    function init ($server = '127.0.0.1', $port = 3322)
    {
        //
        // Initialize network access.
        //
        $this->net->init($server, $port);
    }

    function debug ($debug)
    {
        //
        // Set this object's debug property.
        //
        $this->debug = $debug;
        
        //
        // Propagate through all used objects.
        //
        $this->net->debug = $debug;
        $this->parser->debug = $debug;
    }

    function publish($payload = '', $args = array())
    {
        $args=array_merge(array('destination_type'=>'TOPIC'),$args);

        $msg='<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:mq="http://services.sapo.pt/broker" xmlns:wsa="http://www.w3.org/2005/08/addressing">';
        $msg.='<soap:Body>';

        switch(strtoupper($args['destination_type'])) {
	        case 'QUEUE':
            $msg.='<mq:Enqueue>';
	          break;
	        case 'TOPIC':
	        default:
            $msg.='<mq:Publish>';
	          break;
          }

        $msg.='<mq:BrokerMessage>';

        if ($args['persistent']) {
            $msg .= '<mq:DeliveryMode>PERSISTENT</mq:DeliveryMode>';
        } elseif ($args['transient']) {
            $msg .= '<mq:DeliveryMode>TRANSIENT</mq:DeliveryMode>';
        }
        if ($args['priority']) {
            $msg .= '<mq:Priority>'.$args['priority'].'</mq:Priority>';
        }
        if ($args['message_id']) {
            $msg .= '<mq:MessageId>'.$args['message_id'].'</mq:MessageId>';
        }
        if($args['correlation_id']) {
            $msg .= '<mq:CorrelationId>'.$args['correlation_id'].'</mq:CorrelationId>';
        }
        if($args['timestamp']) {
            $msg .= '<mq:Timestamp>'.$args['timestamp'].'</mq:Timestamp>';
        }
        if($args['expiration']) {
            $msg .= '<mq:Expiration>'.$args['expiration'].'</mq:Expiration>';
        }

        $msg .= '<mq:DestinationName>'.$args['topic'].'</mq:DestinationName>';
        $msg .= '<mq:TextPayload>'.htmlspecialchars($payload).'</mq:TextPayload>';

        $msg .= '</mq:BrokerMessage>';

        switch(strtoupper($args['destination_type'])) {
	        case 'QUEUE':
            $msg.='</mq:Enqueue>';
	          break;
	        case 'TOPIC':
	        default:
            $msg.='</mq:Publish>';
	          break;
          }

        $msg .= '</soap:Body>';
        $msg .= '</soap:Envelope>';
        SAPO_Broker::dodebug("Publishing $msg");
        if($this->net->server=='127.0.0.1') {
          SAPO_Broker::dodebug("Using local dropbox");
          umask(0);
          $filename = '/servers/broker/dropbox/' . md5(microtime() . mt_rand() . getmypid());
          $fd = fopen($filename, 'x');
          if($fd) {
            fwrite($fd, $msg);
            fclose($fd);
            rename($filename, $filename . '.good');
            } else { return false; }
          return true;
	  }
        return $this->net->put($msg);
    }

    function subscribe($topic, $args, $callback)
    {
        array_push($this->net->subscriptions,array('topic'=>$topic,'args'=>$args,'callback'=>$callback));
    }

    function unsubscribe($topic)
    {
      $unsub_item=false;
      $subscriptions=array();
      foreach($this->net->subscriptions as $subscription) {
        if($subscription['topic']!=$topic) {
          array_push($subscriptions,$subscription);
          }
          else
          {
          $unsub_item=$subscription;
          }
        }
      $this->net->subscriptions=$subscriptions;
      if($unsub_item) {  
        SAPO_Broker::dodebug("unsubscribe() unsubscribing ".$subscription['topic']);
        $msg = '<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:wsa="http://www.w3.org/2005/08/addressing" xmlns:mq="http://services.sapo.pt/broker">';
        $msg .= '<soap:Body>';
        $msg .= '<mq:Unsubscribe>';
        $msg .= '<mq:DestinationName>'.$unsub_item['topic'].'</mq:DestinationName>';    
        if($subscription['args']['destination_type']) {
          $msg.='<mq:DestinationType>'.strtoupper($unsub_item['args']['destination_type']).'</mq:DestinationType>';
          } else {
          $msg.='<mq:DestinationType>TOPIC</mq:DestinationType>';
          }
        $msg .= '</mq:Unsubscribe>';
        $msg .= '</soap:Body>';
        $msg .= '</soap:Envelope>';
        return $this->net->put($msg);
        }
        else
        {
        SAPO_Broker::dodebug("unsubscribe() no such topic is subscribed ".$topic);
        }
    return(false);
    }

    function add_callback($args,$callback)
    {
        $period_float=0;
        $period=0;
        if($args['sec']) {
            $period_float+=$args['sec'];
            $period+=$args['sec']*1000000;
        }
        if($args['usec']) {
            $period+=$args['usec'];
            $period_float+=(float)($args['usec']/1000000); 
        }

        $this->net->callbacks_count++;

        SAPO_Broker::dodebug("Adding Callback function #".$this->net->callbacks_count." '".$callback."' periodicity ".$period);

        array_push($this->net->callbacks,array('id'=>$this->net->callbacks_count,'period'=>(float)$period/1000000,'name'=>$callback));
        if(($period<$this->net->rcv_to || $period<$this->net->snd_to) && $period>0) {
            $this->net->rcv_to=$period;
            $this->net->snd_to=$period;
            $this->net->timeouts();
        }
        $this->net->callbacks_ts[$this->net->callbacks_count]=SAPO_Broker_Tools::utime();
    }

    function consumer()
    {
        do {
            $tmp=$this->net->netread(4);
            $len=(double)(ord($tmp[3])+(ord($tmp[2])<<8)+(ord($tmp[1])<<16)+(ord($tmp[0])<<24)); // unpack("N");
            if($len==0) {
              SAPO_Broker::dodebug("consumer() WARNING: packet length is 0!");
	      }
	      else
	      {
              SAPO_Broker::dodebug("consumer() I'm about to read ".$len." bytes");
              $tmp=$this->net->netread($len);
              SAPO_Broker::dodebug("consumer() got this xml: ".$tmp."");
              list($dtype,$dname,$mid)=$this->parser->handlePackets($tmp, $this->net->subscriptions);
              if($dtype=='QUEUE') {
                SAPO_Broker::dodebug("consumer() Got QUEUE message. Acknowledging $dname with id $mid");
                $msg="<soap:Envelope xmlns:soap='http://www.w3.org/2003/05/soap-envelope' xmlns:mq='http://services.sapo.pt/broker'><soap:Body><mq:Acknowledge><mq:DestinationName>".$dname."</mq:DestinationName><mq:MessageId>".$mid."</mq:MessageId></mq:Acknowledge></soap:Body></soap:Envelope>";
                $this->net->put($msg);
                }
	      }
        } while ($this->net->con_retry_count<10);
    }

}

class SAPO_Broker_Net {
    var $server = '127.0.0.1';
    var $port = 3322;
    var $connected = false;
    var $socket;
    var $sokbuf = '';
    var $sokbuflen = 0;
    var $rcv_to = 0; // time in microseconds to timeout on receiving data
    var $snd_to = 0; // time in microseconds to timeout on sending data
    var $snd_to_sec;
    var $rcv_to_sec;
    var $snd_to_usec;
    var $rcv_to_usec;
    var $php_utime_bug_add=0.01; // php bug with microtime. see http://www.rohitab.com/discuss/lofiversion/index.php/t25344.html
    var $con_retry_count = 0;
    var $initted = false;
    var $debug = false;
    var $last_err = "none";
    var $callbacks_ts = array();
    var $callbacks = array();
    var $callbacks_count = 0;
    var $subscriptions = array();

    function SAPO_Broker_Net ($debug = false)
    {
        $this->debug = $debug;
    }
    
    function init($server = '127.0.0.1', $port = 3322)
    {
        $this->server = $server;
        $this->port = $port;
        $this->connected = false;
        $this->timeouts();
        $this->initted = true;
    }

    function timeouts()
    {
        list($this->rcv_to_sec, $this->rcv_to_usec, $this->rcv_to_float) = $this->timesplit($this->rcv_to);
        list($this->snd_to_sec, $this->snd_to_usec, $this->snd_to_float) = $this->timesplit($this->snd_to);
        SAPO_Broker::dodebug("SAPO_Broker_Net::Adjusting timmers because of lower periodic Callback. New timers:");
        SAPO_Broker::dodebug("  rcv_to_sec: ".$this->rcv_to_sec."");
        SAPO_Broker::dodebug("  rcv_to_usec: ".$this->rcv_to_usec."");
        SAPO_Broker::dodebug("  rcv_to_float: ".$this->rcv_to_float."");
        SAPO_Broker::dodebug("  snd_to_sec: ".$this->snd_to_sec."");
        SAPO_Broker::dodebug("  snd_to_usec: ".$this->snd_to_usec."");
        SAPO_Broker::dodebug("  snd_to_float: ".$this->snd_to_float."");
    }

    function timesplit($microseconds)
    {
        $secs = floor($microseconds / 1000000);
        $usecs = $microseconds % 1000000;
        return array($secs, $usecs, (float) ($microseconds / 1000000));
    }

    function sendSubscriptions() {
        SAPO_Broker::dodebug("SAPO_Broker_Net::entering sendSubscriptions()");
        foreach($this->subscriptions as $subscription)
        {
            SAPO_Broker::dodebug("SAPO_Broker_Net::sendSubscriptions() subscribing ".$subscription['topic']);
            $msg = '<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:wsa="http://www.w3.org/2005/08/addressing" xmlns:mq="http://services.sapo.pt/broker">';
            $msg .= '<soap:Body>';
            $msg .= '<mq:Notify>';
            if($subscription['topic']) {
                $msg .= '<mq:DestinationName>'.$subscription['topic'].'</mq:DestinationName>';    
            }
            if($subscription['args']['destination_type']) {
                $msg.='<mq:DestinationType>'.strtoupper($subscription['args']['destination_type']).'</mq:DestinationType>';
            } else {
                $msg.='<mq:DestinationType>TOPIC</mq:DestinationType>';
            }
            if ($subscription['args']['acknowledge_mode']) {
                $msg .= '<mq:AcknowledgeMode>'.$subscription['args']['acknowledge_mode'].'</mq:AcknowledgeMode>';
            }
            $msg .= '</mq:Notify>';
            $msg .= '</soap:Body>';
            $msg .= '</soap:Envelope>';
            $ret = $this->put($msg);
            if ($ret==false) {
                return(false);
            }
        }
      }

    function tryConnect($server,$port,$timeout=5) {
      $address = gethostbyname($server);
      $socket = fsockopen($address, $port, $errno, $errstr, $timeout);
      if(!$socket) return(FALSE);
      fclose($socket);
      return(TRUE);
      }

    function connect()
    {
        if(!$this->initted) {
            $this->init();
        }
        $this->con_retry_count++;
        SAPO_Broker::dodebug("SAPO_Broker_Net::Entering connect(".$this->server.") ".$this->con_retry_count."");
        
        $address = gethostbyname($this->server);
        $this->socket = fsockopen($address, $this->port, $errno, $errstr);
        
        
        if (!$this->socket) {
            SAPO_Broker::dodebug($errstr);
            $this->last_err = $errstr;
            $this->connected = false;
            
        } else {
            
            stream_set_timeout($this->socket, $this->snd_to_sec, $this->snd_to_sec);
            
            //
            // Set stream to blocking mode.
            //
            stream_set_blocking($this->socket, 1);
            
            SAPO_Broker::dodebug("SAPO_Broker_Net::Connected to server");
            $this->connected=true;
            $this->con_retry_count=0;
            $this->sendSubscriptions();
        }
        return $this->connected;
    }
    
    function put($msg)
    {
        if($this->connected==false) {
            SAPO_Broker::dodebug("SAPO_Broker_Net::put() ups, we're not connected, let's go for ir");

            if($this->connect()==false) {
                return(false);
            }
        }
        SAPO_Broker::dodebug("SAPO_Broker_Net::put() socket_writing: ".$msg."\n");
        if(fwrite($this->socket, pack('N',mb_strlen($msg,'latin1')).$msg, mb_strlen($msg,'latin1') + 4)===false) {
            $this->connected = false;
            return(false);
        }
        return(true);
    }

    function sendKeepalive() {
      $m="<?xml version='1.0' encoding='UTF-8'?>\n<soap:Envelope xmlns:soap='http://www.w3.org/2003/05/soap-envelope' xmlns:mq='http://services.sapo.pt/broker'><soap:Body>
<mq:CheckStatus /></soap:Body></soap:Envelope>";
      $this->put($m);
      $this->latest_status_timestamp_sent=date("Y-m-d\TH:m:s\Z");
    }

    function netread($len) {
        SAPO_Broker::dodebug("netread(".$len.") entering sokbuflen is ".$this->sokbuflen."");
        if($this->connected==false) {
            SAPO_Broker::dodebug("SAPO_Broker_Net::netread() ups, we're not connected, let's go for ir");
            if($this->connect()==false) {
              SAPO_Broker::dodebug("SAPO_Broker_Net::netread() couldn't reconnect");
              return('');
            }
        }
        $i=$this->sokbuflen;
        if($this->debug) {
            if(function_exists('memory_get_usage')) {
                echo "SAPO_Broker_Net::PHP process memory: ".memory_get_usage()." bytes\n";
            } else {
                switch(php_uname('s')) {
                    case "Darwin":
                    $pid=getmypid();
                    echo 'USER       PID %CPU %MEM      VSZ    RSS  TT  STAT STARTED      TIME COMMAND'."\n";
                    ob_start();
                    passthru('ps axu|grep '.$pid.'|grep -v grep');
                    $var = ob_get_contents();
                    ob_end_clean(); 
                    echo $var;
                    break;
                }
            }
        } // end this->debug
        while($i<$len) { // read just about enough. do i hate sockets...
            $start=SAPO_Broker_Tools::utime();
            $tmp=fread($this->socket, 1024); // block read with timeout
            $end=SAPO_Broker_Tools::utime()+$this->php_utime_bug_add; // there's a problem with php's microtime function and the tcp timeout. This 0.1 offset fixes this.
            // Execute callbacks on subscribed topics
            foreach($this->callbacks as $callback) { // periodic callbacks here, if any
                if(($this->callbacks_ts[$callback['id']]+$callback['period'])<=SAPO_Broker_Tools::utime()) {
		                SAPO_Broker::dodebug("SAPO_Broker_Net::Callbacking #".$callback['id']." ".$callback['name'].". Next in ".$callback['period']." seconds");
                    $this->callbacks_ts[$callback['id']]=SAPO_Broker_Tools::utime();
                    call_user_func($callback['name'],$this);
                }
            } // end callbacks
            $l=mb_strlen($tmp,'latin1');
            SAPO_Broker::dodebug("SAPO_Broker_Net::Doing socket_read() inside netread()");
            SAPO_Broker::dodebug("end-start: ".($end-$start)."\nthis->rcv_to_float: ".$this->rcv_to_float."\nl: ".$l."\n");
            if((($end-$start)<((float)($this->rcv_to_float)))&&$l==0) {
                $this->connected=false; return('');
            }
            $this->sokbuf.=$tmp;
            $this->sokbuflen+=$l;
            $i+=$l;
        }
        $this->sokbuflen-=$len;
        $r=substr($this->sokbuf,0,$len);
        $this->sokbuf=substr($this->sokbuf,$len); // cut
        SAPO_Broker::dodebug("SAPO_Broker_Net::netread(".$len.") leaving sokbuflen is ".$this->sokbuflen."");
        return($r);
    }

    function disconnect() {
        fclose($this->socket);
    }
    
}

class SAPO_Broker_Parser {
    
    var $debug;
    var $pelements=array('DestinationName','TextPayload','Message','Status','Timestamp','MessageId');
    
    function SAPO_Broker_Parser ($debug = false,$instance)
    {
        $this->debug = $debug;
        $this->instance = $instance;
    }
   
    function worldmapPush($file) {
      $parser = xml_parser_create();
      if (!($fp = fopen($file, "r"))) return(FALSE);
      $data = fread($fp, filesize($file));
      fclose($fp);
      xml_parse_into_struct($parser, $data, $vals, $index);
      xml_parser_free($parser);
      $ips=array();
      foreach($index['IP'] as $i) {
        array_push($ips,$vals[$i]['value']);
        }
      return($ips[rand(0,count($ips)-1)]);
      }
   
    function getElements ($msg, $namespace = null)
    {
        //
        // Create a parser and set the namespace identifier.
        //
        if (!empty($namespace)) {
            $nsIdentifier = $namespace . ':';
            $xml = xml_parser_create_ns();
        } else {
            $nsIdentifier = null;
            $xml = xml_parser_create();
        }

        //
        // Set parser options.
        //
        xml_parser_set_option($xml, XML_OPTION_CASE_FOLDING, 0);
        xml_parser_set_option($xml, XML_OPTION_SKIP_WHITE, 1);

        //
        // Get elements from the XML document.
        //
        xml_parse_into_struct($xml, $msg, $values, $tags);
        foreach($this->pelements as $eln) {
          $elements[$eln] = $values[$tags[$nsIdentifier . $eln][0]]['value'];
        }

        xml_parser_free($xml);
        
        return $elements;
    }

    function handlePackets ($msg, $subscriptions = array())
    {
        //
        // Get XML elements needed to handle subscriptions.
        //
        $elements = $this->getElements($msg, 'http://services.sapo.pt/broker');


        // It's a status message
        if(!empty($elements['Status'])) {
          $this->instance->latest_status_message=$elements['Message'];
          $this->instance->latest_status_timestamp_received=$elements['Timestamp'];          
        }
        else // It's a payload
        {
          //
          // If the destination name wasn't found try to find it
          // without using a namespace.
          //
          if (empty($elements['DestinationName'])) {
              $elements = $this->getElements($msg);
          }
          //
          // If a destination name was found, handle its associated callback.
          //
          if (!empty($elements['DestinationName'])) {
            foreach ($subscriptions as $subscription) {
              if ($subscription['topic'] == $elements['DestinationName']) {
                call_user_func($subscription['callback'], $elements['TextPayload'],$elements['DestinationName'],$this->instance);
              // only one callback per subscribed topic, only one subscription per topic
              return(array($subscription['args']['destination_type'],$elements['DestinationName'],$elements['MessageId']));
              }
            }
          }
        }
      return(array(null,null,null));
      } // end handlePackets()
}

class SAPO_Broker_Parser_DOM extends SAPO_Broker_Parser {

    var $debug;
    
    function SAPO_Broker_Parser_DOM ($debug = false,$instance)
    {
        $this->debug = $debug;
        $this->instance = $instance;
    }

    function worldmapPush($file) {
      $parser = xml_parser_create();
      if (!($fp = fopen($file, "r"))) return(FALSE);
      $data = fread($fp, filesize($file));
      fclose($fp);
      $xml= new DOMDocument();
      $xml->preserveWhiteSpace=true;
      $xml->loadXML($data);
      $xpath = new DOMXpath($xml);
      $ips=array();
      foreach($xpath->query('/world/domain/peer/transport/ip') as $nc) {
        array_push($ips,$nc->nodeValue);
        }
      return($ips[rand(0,count($ips)-1)]);
      }

    function getElements ($msg, $namespace = null)
    {
        //
        // Create a new DOM document.
        //
        $dom =& new DOMDocument;
        $dom->loadXML($msg);
        
        //
        // Obtain the node lists, with or without namespaces.
        //
        if (!empty($namespace)) {
            foreach($this->pelements as $eln) {
              $nodeLists[$eln] = $dom->getElementsByTagNameNS($namespace, $eln);
            }
        } else {
            foreach($this->pelements as $eln) {
              $nodeLists[$eln] = $dom->getElementsByTagName($eln);
            }
        }

        //
        // Obtain the elements.
        //
        foreach ($nodeLists as $tagName => $nodeList) {
            $node = $nodeList->item(0);
            $elements[$tagName] = $node->nodeValue;
        }
        
        return $elements;        
    }
}

class SAPO_Broker_Tools {

    function utime()
    {
        list($usec, $sec) = explode(" ", microtime());
        return ((float)$usec + (float)$sec);
    }


    function xmlentities ($string, $quote_style = ENT_QUOTES, $charset = 'UTF-8')
    {
        static $trans;
        if (!isset($trans)) {
            $trans = get_html_translation_table(HTML_SPECIALCHARS, $quote_style);
            foreach ($trans as $key => $value)
            $trans[$key] = '&#'.ord($key).';';
            // dont translate the '&' in case it is part of &xxx;
            $trans[chr(38)] = '&';
        }
        // after the initial translation, _do_ map standalone '&' into '&#38;'
        return preg_replace("/&(?![A-Za-z]{0,4}\w{2,3};|#[0-9]{2,3};)/","&#38;" , strtr(html_entity_decode($string), $trans));
    }

}

?>
