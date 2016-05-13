package org.cstamas.vertx.content.examples;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.logging.SLF4JLogDelegateFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * Junit test utilities.
 */
public class TestUtil
{
  private TestUtil() {
    // nop
  }

  public static Logger log() {
    return LoggerFactory.getLogger("TEST");
  }

  public static void initLogging() {
    System.setProperty("vertx.logger-delegate-factory-class-name", SLF4JLogDelegateFactory.class.getName());
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
  }

  public static void blockingCloseAll(List<Vertx> vertxInstances) throws InterruptedException {
    CountDownLatch closeLatch = new CountDownLatch(vertxInstances.size());
    vertxInstances.forEach(
        vertx -> {
          vertx.close(v -> closeLatch.countDown());
        }
    );
    closeLatch.await();
  }
}
