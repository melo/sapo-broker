<?php


class SAPO_Broker_Tail extends SAPO_Broker {
  var $tailfile;
  var $max_lines_before_quit;
  var $publish_count;
  var $initted=FALSE;

  function SAPO_Broker_Tail ($args=array()) {
    $args=array_merge(array('max_lines_before_quit'=>0),$args);
    $this->debug=$args['debug'];
    $this->tailfile=$args['file'];
    SAPO_Broker::dodebug("SAPO_Broker_Tail innited with file ".$args['file']);
    $this->max_lines_before_quit=$args['max_lines_before_quit'];
    $this->publish_count=0;
    $this->initted=TRUE;
    }

  function tail($callback) {
    if($fp=fopen($this->tailfile,"rb")) {
      fseek($fp,0,SEEK_END);
      $fs=fstat($fp);
      $ino=$fs['ino'];
      while(($this->publish_count<$this->max_lines_before_quit) || $this->max_lines_before_quit==0) {
        do {
          do {
            $ino=$this->rotate($ino);
            if($ino==FALSE) return(FALSE);
            $c=fgetc($fp);
            $line.=$c;
            } while ($c!="\n" && $c!="\r" && $c);
          if($c===FALSE) sleep(1);
          } while ($c!="\n" && $c!="\r");

        $this->publish_count++;
        call_user_func($callback,$line);

        $line='';
        $c='';
        }
      fclose($fp);
      } else echo "Can't open file\n";
    }

  function rotate($ino) {
    $fs=stat($this->tailfile);
    if($fs['ino']!=$ino) {
      echo "File rotated.\n";
      return(FALSE);
      }
    clearstatcache();
    return($fs['ino']);
    }

  } // end class

?>
