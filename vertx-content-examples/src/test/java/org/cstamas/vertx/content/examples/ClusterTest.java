package org.cstamas.vertx.content.examples;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.cstamas.vertx.content.examples.TestUtil.blockingCloseAll;
import static org.cstamas.vertx.content.examples.TestUtil.initLogging;
import static org.cstamas.vertx.content.examples.TestUtil.log;

/**
 * Junit test.
 */
public class ClusterTest
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
    ArrayList<Vertx> vertxInstances = new ArrayList<>();
    CountDownLatch deploymentLatch = new CountDownLatch(2);
    Vertx.clusteredVertx(new VertxOptions(), v -> {
      if (v.succeeded()) {
        Vertx instance = v.result();
        vertxInstances.add(instance);
        instance.deployVerticle(
            ContentSenderVerticle.class.getName(),
            new DeploymentOptions().setConfig(config.copy().put("host", "localhost").put("port", 8081)),
            deploymentHandler -> {
              deploymentLatch.countDown();
            }
        );
      }
      else {
        throw new AssertionError("Could not deploy: " + v.cause());
      }
    });
    Vertx.clusteredVertx(new VertxOptions(), v -> {
      if (v.succeeded()) {
        Vertx instance = v.result();
        vertxInstances.add(instance);
        instance.deployVerticle(
            ContentReceiverVerticle.class.getName(),
            new DeploymentOptions().setConfig(config.copy().put("host", "localhost").put("port", 8082)),
            deploymentHandler -> {
              deploymentLatch.countDown();
            }
        );
      }
      else {
        throw new AssertionError("Could not deploy: " + v.cause());
      }
    });
    deploymentLatch.await();

    CountDownLatch operationLatch = new CountDownLatch(1);
    Vertx.clusteredVertx(new VertxOptions(), v -> {
      if (v.succeeded()) {
        Vertx instance = v.result();
        vertxInstances.add(instance);
        log().info("Firing event");
        instance.eventBus().send(
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
      }
      else {
        throw new AssertionError("Could not deploy: " + v.cause());
      }
    });
    operationLatch.await();

    blockingCloseAll(vertxInstances);
  }
}
