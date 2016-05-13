package org.cstamas.vertx.content.examples;

import java.util.concurrent.CountDownLatch;

import com.google.common.collect.ImmutableList;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.cstamas.vertx.content.examples.TestUtil.blockingCloseAll;
import static org.cstamas.vertx.content.examples.TestUtil.initLogging;
import static org.cstamas.vertx.content.examples.TestUtil.log;

/**
 * Junit test.
 */
public class SimpleTest
{
  @BeforeClass
  public static void before() {
    initLogging();
  }

  @Test
  public void sendFileHttp() throws Exception {
    sendFile(new JsonObject().put("type", "http"));
  }

  @Test
  public void sendFileEb() throws Exception {
    sendFile(new JsonObject().put("type", "eventbus"));
  }

  private void sendFile(final JsonObject config) throws Exception {
    CountDownLatch deployment = new CountDownLatch(2);
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(
        ContentSenderVerticle.class.getName(),
        new DeploymentOptions().setConfig(config.copy().put("host", "localhost").put("port", 8081)),
        v -> {
          deployment.countDown();
        }
    );
    vertx.deployVerticle(
        ContentReceiverVerticle.class.getName(),
        new DeploymentOptions().setConfig(config.copy().put("host", "localhost").put("port", 8082)),
        v -> {
          deployment.countDown();
        }
    );
    deployment.await(); // wait for all get deployed

    CountDownLatch operationLatch = new CountDownLatch(1);
    vertx.eventBus().send(
        ContentSenderVerticle.ADDRESS,
        new JsonObject().put("path", "/Users/cstamas/tmp/testfile.json"),
        reply -> {
          if (reply.succeeded()) {
            log().info("Succeeded " + reply.result().body().toString());
          }
          else {
            log().info("Failed ", reply.cause());
          }
          operationLatch.countDown();
        }
    );
    operationLatch.await();

    blockingCloseAll(ImmutableList.of(vertx));
  }
}
