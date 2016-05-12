package org.cstamas.vertx.content.examples;

import java.util.concurrent.CountDownLatch;

import io.vertx.core.Vertx;
import io.vertx.core.logging.SLF4JLogDelegateFactory;
import org.junit.Test;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * Junit test.
 */
public class SimpleTest
{
  static {
    System.setProperty("vertx.logger-delegate-factory-class-name", SLF4JLogDelegateFactory.class.getName());
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
  }

  @Test
  public void sendFile() throws Exception {
    CountDownLatch latch = new CountDownLatch(2);
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(
        ContentSenderVerticle.class.getName(),
        v -> {
          latch.countDown();
        }
    );
    vertx.deployVerticle(
        ContentReceiverVerticle.class.getName(),
        v -> {
          latch.countDown();
        }
    );

    latch.await();
    vertx.eventBus().send("sendFile", "/Users/cstamas/tmp/testfile.json");
    Thread.sleep(10000);
  }
}
