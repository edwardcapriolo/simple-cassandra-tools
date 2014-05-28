package io.teknek.cassandra.simple;  

import io.teknek.cassandra.simple.FramedConnWrapper;
import io.teknek.farsandra.Farsandra;
import io.teknek.farsandra.LineHandler;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public abstract class AbstractFarsandra {

  private Farsandra fs;
  
  @Before
  public void setup() throws InterruptedException{
    fs = new Farsandra();
    fs.withVersion("2.0.4");
    fs.withCleanInstanceOnStart(true);
    fs.withInstanceName("3_1");
    fs.withCreateConfigurationFiles(true);
    fs.withHost("127.0.0.1");
    fs.withSeeds(Arrays.asList("127.0.0.1"));
    fs.withJmxPort(9999);   
    fs.appendLineToYaml("#this means nothing");
    fs.appendLinesToEnv("#this also does nothing");
    fs.withEnvReplacement("#MALLOC_ARENA_MAX=4", "#MALLOC_ARENA_MAX=wombat");
    fs.withYamlReplacement("# NOTE:", "# deNOTE:");
    final CountDownLatch started = new CountDownLatch(1);
    fs.getManager().addOutLineHandler( new LineHandler(){
        @Override
        public void handleLine(String line) {
          System.out.println("out "+line);
          if (line.contains("Listening for thrift clients...")){
            started.countDown();
          }
        }
      } 
    );
    fs.start();
    started.await(10, TimeUnit.SECONDS);
  }
    
  @After
  public void close(){
    if (fs != null){
      try {
        fs.getManager().destroyAndWaitForShutdown(6);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
    
  @After
  public void after(){
    try {
      fs.getManager().destroyAndWaitForShutdown(5);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
