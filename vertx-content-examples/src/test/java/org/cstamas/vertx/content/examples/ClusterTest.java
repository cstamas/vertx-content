package org.cstamas.vertx.content.examples;

import java.util.concurrent.CountDownLatch;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.logging.SLF4JLogDelegateFactory;
import org.junit.Test;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * Junit test.
 */
public class ClusterTest
{
  static {
    System.setProperty("vertx.logger-delegate-factory-class-name", SLF4JLogDelegateFactory.class.getName());
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
  }

  @Test
  public void sendFile() throws Exception {
    CountDownLatch latch = new CountDownLatch(2);
    Vertx.clusteredVertx(new VertxOptions(), v -> {
      v.result().deployVerticle(ContentSenderVerticle.class.getName());
      latch.countDown();
    });

    Vertx.clusteredVertx(new VertxOptions(), v -> {
      v.result().deployVerticle(ContentReceiverVerticle.class.getName());
      latch.countDown();
    });

    latch.await();

    Vertx.clusteredVertx(new VertxOptions(), v -> {
      LoggerFactory.getLogger("TEST").info("Firing event");
      v.result().eventBus().send("sendFile", "/Users/cstamas/tmp/testfile.json");
    });

    Thread.sleep(10000);
  }
}
