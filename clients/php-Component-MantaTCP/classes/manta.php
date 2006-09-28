<?php
/**
 * Mantaray access abstraction class.
 *
 * The Manta class abstracts all the low-level complexity of the MantaTCP and
 * Mataray agents and gives the developers a simple to use high-level API
 * to build consumers and producers.
 *
 * @package Manta
 * @version 0.1.0
 * @author Celso Martinho <celso@co.sapo.pt>
 * @author Bruno Pedro <bpedro@co.sapo.pt>
 **/
class SAPO_Manta {
    
    var $subscriptions = array();

    var $parser;
    var $net;

    var $debug;

    function SAPO_Manta ($args=array())
    {
        // args defaults 
        $args=array_merge(array('server'=>'127.0.0.1','port'=>2222,'debug'=>FALSE,'force_sockets'=>FALSE,'force_streams'=>FALSE),$args);

        // pre init()
	$this->debug = $args['debug'];

        //
        // Check for supported PHP version.
        //
        if (version_compare(phpversion(), '4.3.0', '<')) {
            die ("SAPO_Manta needs at least PHP 4.3.0 to run properly.\nPlease upgrade...\n\n");
        }
        
        //
        // Look for sockets support.
        //
        if ( (extension_loaded('sockets') || $args['force_sockets']) && $args['force_streams']==FALSE) {
	    if($this->debug) {
                echo "Using SAPO_Manta_Net_Sockets()\n";
            }
            $this->net =& new SAPO_Manta_Net_Sockets($this->debug);
        } else {
	    if($this->debug) {
                echo "Using SAPO_Manta_Net()\n";
            }
            $this->net =& new SAPO_Manta_Net($this->debug);
        }

        //
        // Look for DOM support and use appropriate Parser.
        //
        if (!extension_loaded('dom')) {
            $this->parser =& new SAPO_Manta_Parser($this->debug);
        } else {
             $this->parser =& new SAPO_Manta_Parser_DOM($this->debug);
        }

        // post init()
        $this->net->init($args['server'], $args['port']);

    }


    /**
     * This is a facade to SAPO_Manta_Tools::xmlentities()
     *
     * @return string
     * @author Bruno Pedro
     **/
    function xmlentities ($string, $quote_style = ENT_QUOTES, $charset = 'UTF-8')
    {
        return SAPO_Manta_Tools::xmlentities($string, $quote_style, $charset);

    }

    /**
     * This is a facade to SAPO_Manta_Net::init()
     *
     * @return void
     * @author Bruno Pedro
     **/
    function init ($server = '127.0.0.1', $port = 2222)
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
        $msg='<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:mq="http://services.sapo.pt/broker" xmlns:wsa="http://www.w3.org/2005/08/addressing">';
        $msg.='<soap:Body>';
        $msg.='<mq:Publish>';

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
        $msg .= '</mq:Publish>';
        $msg .= '</soap:Body>';
        $msg .= '</soap:Envelope>';

        return $this->net->put($msg);
    }

    function subscribe($topic, $args, $callback)
    {
        array_push($this->subscriptions,array('topic'=>$topic,'args'=>$args,'callback'=>$callback));
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

        if($this->debug) {
            echo "Adding Callback function #".$this->net->callbacks_count." '".$callback."' periodicity ".$period."\n";
        }

        array_push($this->net->callbacks,array('id'=>$this->net->callbacks_count,'period'=>(float)$period/1000000,'name'=>$callback));
        if(($period<$this->rcv_to || $period<$this->snd_to) && $period>0) {
            $this->rcv_to=$period;
            $this->snd_to=$period;
            $this->timeouts();
        }
        $this->net->callbacks_ts[$this->net->callbacks_count]=SAPO_Manta_Tools::utime();
    }

    function consumer()
    {
        foreach($this->subscriptions as $subscription)
        {
            $msg = '<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:wsa="http://www.w3.org/2005/08/addressing" xmlns:mq="http://services.sapo.pt/broker">';
            $msg .= '<soap:Body>';
            $msg .= '<mq:Notify>';
            if($subscription['topic']) {
                $msg .= '<mq:DestinationName>'.$subscription['topic'].'</mq:DestinationName>';    
            }
            if($subscription['args']['destination_type']) {
                $msg.='<mq:DestinationType>'.$subscription['args']['destination_type'].'</mq:DestinationType>';
            } else {
                $msg.='<mq:DestinationType>TOPIC</mq:DestinationType>';
            }
            if ($subscription['args']['acknowledge_mode']) {
                $msg .= '<mq:AcknowledgeMode>'.$subscription['args']['acknowledge_mode'].'</mq:AcknowledgeMode>';
            }
            $msg .= '</mq:Notify>';
            $msg .= '</soap:Body>';
            $msg .= '</soap:Envelope>';
            $ret = $this->net->put($msg);
            if ($ret==false) {
                return(false);
            }
        }

        do {
            $tmp=$this->net->netread(4);
            $arr=unpack('N',$tmp);
            $len=$arr[1];
            if($this->debug) {
                echo "consumer() I'm about to read ".$len." bytes\n";
            }
            $tmp=$this->net->netread($len);
            if($this->debug) {
                echo "consumer() got this xml: ".$tmp."\n";
            }
            $this->parser->handleSubscriptions($tmp, $this->subscriptions);
        } while ($this->net->con_retry_count<10);
    }

}

class SAPO_Manta_Net {

    var $server = '127.0.0.1';
    var $port = 2222;
    var $connected = false;
    var $socket;
    var $sokbuf = '';
    var $sokbuflen = 0;
    var $rcv_to = 5000000; // time in microseconds to timeout on receiving data - xuning here
    var $snd_to = 5000000; // time in microseconds to timeout on sending data
    var $snd_to_sec;
    var $rcv_to_sec;
    var $snd_to_usec;
    var $rcv_to_usec;
    var $con_retry_count = 0;
    var $initted = false;
    var $debug = false;
    var $last_err = "none";
    var $callbacks_ts = array();
    var $callbacks = array();
    var $callbacks_count = 0;

    function SAPO_Manta_Net ($debug = false)
    {
        $this->debug = $debug;
    }
    
    function init($server = '127.0.0.1', $port = 2222)
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
        if($this->debug) {
            echo "Adjusting timmers because of lower periodic Callback. New timers:\n";
            echo "  rcv_to_sec: ".$this->rcv_to_sec."\n";
            echo "  rcv_to_usec: ".$this->rcv_to_usec."\n";
            echo "  rcv_to_float: ".$this->rcv_to_float."\n";
            echo "  snd_to_sec: ".$this->snd_to_sec."\n";
            echo "  snd_to_usec: ".$this->snd_to_usec."\n";
            echo "  snd_to_float: ".$this->snd_to_float."\n";
        }
    }

    function timesplit($microseconds)
    {
        $secs = floor($microseconds / 1000000);
        $usecs = $microseconds % 1000000;
        return array($secs, $usecs, (float) ($microseconds / 1000000));
    }


    function connect()
    {
        if(!$this->initted) {
            $this->init();
        }
        $this->con_retry_count++;
        if ($this->debug) {
            echo "Entering connect() ".$this->con_retry_count."\n";
        }
        
        $address = gethostbyname($this->server);
        $this->socket = fsockopen($address, $this->port, $errno, $errstr);
        
        
        if (!$this->socket) {
            if($this->debug) {
                echo $errstr . "\n";
            }
            $this->last_err = $errstr;
            $this->connected = false;
            
        } else {
            
            stream_set_timeout($this->socket, $this->snd_to_sec, $this->snd_to_sec);
            
            //
            // Set stream to blocking mode.
            //
            stream_set_blocking($this->socket, 1);
            
            if ($this->debug) {
                echo "Connected to server\n";
            }
            $this->connected=true;
            $this->con_retry_count=0;
        }
        return $this->connected;
    }
    
    function put($msg)
    {
        if($this->connected==false) {
            if($this->connect()==false) {
                return(false);
            }
        }
        if($this->debug) {
            echo "put() socket_writing: ".$msg."\n\n";
        }
        if(fwrite($this->socket, pack('N',strlen($msg)).$msg, strlen($msg) + 4)===false) {
            $this->connected = false;
            return(false);
        }
        return(true);
    }

    function netread($len) {
        if($this->debug) {
            echo "netread(".$len.") entering sokbuflen is ".$this->sokbuflen."\n";
        }
        if($this->connected==false) {
            if($this->connect()==false) {
                return('');
            }
        }
        $i=$this->sokbuflen;
        if($this->debug) {
            if(function_exists('memory_get_usage')) {
                echo "PHP process memory: ".memory_get_usage()." bytes\n";
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
            $start=SAPO_Manta_Tools::utime();
            $tmp=fread($this->socket, 1024); // block read with timeout
            foreach($this->callbacks as $callback) { // periodic callbacks here, if any
                if(($this->callbacks_ts[$callback['id']]+$callback['period'])<=SAPO_Manta_Tools::utime()) {
                    if($this->debug) echo "Callbacking #".$callback['id']." ".$callback['name'].". Next in ".$callback['period']." seconds\n";
                    $this->callbacks_ts[$callback['id']]=SAPO_Manta_Tools::utime();
                    call_user_func($callback['name']);
                }
            } // end callbacks
            if($this->debug) {
                echo "Doing socket_read() inside netread()\n";
            }
            $end=SAPO_Manta_Tools::utime();
            $l=strlen($tmp);
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
        if($this->debug) {
            echo "netread(".$len.") leaving sokbuflen is ".$this->sokbuflen."\n";
        }
        return($r);
    }

    function disconnect() {
        fclose($this->socket);
    }
    
}

class SAPO_Manta_Net_Sockets extends SAPO_Manta_Net {

    function SAPO_Manta_Net_Sockets ($debug = false)
    {
        $this->debug = $debug;
    }

    function connect()
    {
        if(!$this->initted) {
            $this->init();
        }
        $this->con_retry_count++;
        if ($this->debug) {
            echo "Entering connect() ".$this->con_retry_count."\n";
        }
        $address = gethostbyname($this->server);
        $this->socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
        if ($this->socket < 0) {
            if($this->debug) {
                echo "socket_create() failed\n";
            }
            $this->last_err = 'socket_create() failed';
            $this->connected = false;
        } else {
            socket_set_option($this->socket,SOL_SOCKET, SO_RCVTIMEO, array("usec"=>$this->rcv_to_usec,"sec"=>$this->rcv_to_sec));
            socket_set_option($this->socket,SOL_SOCKET, SO_SNDTIMEO, array("usec"=>$this->snd_to_usec,"sec"=>$this->snd_to_sec));
            socket_set_option($this->socket,SOL_SOCKET, SO_KEEPALIVE, 1);
            socket_set_block($this->socket);
            if (socket_connect($this->socket, $address, $this->port)) {
                if ($this->debug) {
                    echo "Connected to server\n";
                }
                $this->connected=true;
                $this->con_retry_count=0;
            } else {
                if($this->debug) {
                    echo "socket_connect() failed.\n";
                }
                $this->last_err = 'socket_connect() failed';
                $this->connected = false;
            }
        }
        return $this->connected;
    }

    function put($msg)
    {
        if($this->connected==false) {
            if($this->connect()==false) {
                return(false);
            }
        }
        if($this->debug) {
            echo "put() socket_writing: ".$msg."\n\n";
        }
        if(socket_write($this->socket, pack('N',strlen($msg)).$msg, strlen($msg) + 4)===false) {
            $this->connected = false;
            return(false);
        }
        return(true);
    }
    
    function netread($len) {
        if($this->debug) {
            echo "netread(".$len.") entering sokbuflen is ".$this->sokbuflen."\n";
        }
        if($this->connected==false) {
            if($this->connect()==false) {
                return('');
            }
        }
        $i=$this->sokbuflen;
        if($this->debug) {
            if(function_exists('memory_get_usage')) {
                echo "PHP process memory: ".memory_get_usage()." bytes\n";
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
            $start=SAPO_Manta_Tools::utime();
            $tmp=socket_read($this->socket, 1024,PHP_BINARY_READ); // block read with timeout
            foreach($this->callbacks as $callback) { // periodic callbacks here, if any
                if(($this->callbacks_ts[$callback['id']]+$callback['period'])<=SAPO_Manta_Tools::utime()) {
                    if($this->debug) echo "Callbacking #".$callback['id']." ".$callback['name'].". Next in ".$callback['period']." seconds\n";
                    $this->callbacks_ts[$callback['id']]=SAPO_Manta_Tools::utime();
                    call_user_func($callback['name']);
                }
            } // end callbacks
            if($this->debug) {
                echo "Doing socket_read() inside netread()\n";
            }
            $end=SAPO_Manta_Tools::utime();
            $l=strlen($tmp);
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
        if($this->debug) {
            echo "netread(".$len.") leaving sokbuflen is ".$this->sokbuflen."\n";
        }
        return($r);
    }
        
    function disconnect() {
        socket_close($this->socket);
    }    
}

class SAPO_Manta_Parser {
    
    var $debug;
    
    function SAPO_Manta_Parser ($debug = false)
    {
        $this->debug = $debug;
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
        $elements['DestinationName'] = $values[$tags[$nsIdentifier . 'DestinationName'][0]]['value'];
        $elements['TextPayload'] = $values[$tags[$nsIdentifier . 'TextPayload'][0]]['value'];
        xml_parser_free($xml);
        
        return $elements;
    }

    function handleSubscriptions ($msg, $subscriptions = array())
    {
        //
        // Get XML elements needed to handle subscriptions.
        //
        $elements = $this->getElements($msg, 'http://services.sapo.pt/broker');
        
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
                    call_user_func($subscription['callback'], $elements['TextPayload']);
                }
            }
        }
    }
}

class SAPO_Manta_Parser_DOM extends SAPO_Manta_Parser {

    var $debug;
    
    function SAPO_Manta_Parser_DOM ($debug = false)
    {
        $this->debug = $debug;
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
            $nodeLists['DestinationName'] = $dom->getElementsByTagNameNS($namespace, 'DestinationName');
            $nodeLists['TextPayload'] = $dom->getElementsByTagNameNS($namespace, 'TextPayload');
        } else {
            $nodeLists['DestinationName'] = $dom->getElementsByTagName('DestinationName');
            $nodeLists['TextPayload'] = $dom->getElementsByTagName('TextPayload');
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

class SAPO_Manta_Tools {

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
