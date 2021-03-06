package org.cstamas.vertx.content.examples;

import java.util.ArrayList;

import com.jayway.awaitility.Awaitility;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.cstamas.vertx.content.examples.TestUtil.createDummyFile;
import static org.cstamas.vertx.content.examples.TestUtil.initLogging;
import static org.cstamas.vertx.content.examples.TestUtil.log;
import static org.cstamas.vertx.content.examples.TestUtil.verifyFilesEqual;

/**
 * Junit test.
 */
@RunWith(VertxUnitRunner.class)
public class ClusterTest
{
  @BeforeClass
  public static void before() {
    initLogging();
  }

  @Test
  public void sendFileHttpSmall(final TestContext testContext) throws Exception {
    sendFile(testContext, new JsonObject().put("type", "http"), 15000);
  }

  @Test
  public void sendFileHttpBig(final TestContext testContext) throws Exception {
    sendFile(testContext, new JsonObject().put("type", "http"), 8000000);
  }

  @Test
  public void sendFileSharedFileSmall(final TestContext testContext) throws Exception {
    sendFile(testContext, new JsonObject().put("type", "sharedFile"), 15000);
  }

  @Test
  public void sendFileSharedFileBig(final TestContext testContext) throws Exception {
    sendFile(testContext, new JsonObject().put("type", "sharedFile"), 8000000);
  }

  private void sendFile(final TestContext testContext, final JsonObject config, final long sourceSize)
      throws Exception
  {
    ArrayList<Vertx> vertxInstances = new ArrayList<>();
    Vertx.clusteredVertx(new VertxOptions().setClusterManager(new HazelcastClusterManager()), v -> {
      if (v.succeeded()) {
        Vertx instance = v.result();
        vertxInstances.add(instance);
        instance.deployVerticle(
            ContentSenderVerticle.class.getName(),
            new DeploymentOptions().setConfig(config.copy().put("host", "localhost").put("port", 8081))
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
            new DeploymentOptions().setConfig(config.copy().put("host", "localhost").put("port", 8082))
        );
      }
      else {
        throw new AssertionError("Could not deploy: " + v.cause());
      }
    });

    String sourcePath = createDummyFile(sourceSize);

    HazelcastClusterManager clusterManager = new HazelcastClusterManager();
    Vertx.clusteredVertx(new VertxOptions().setClusterManager(clusterManager), v -> {
      if (v.succeeded()) {
        Vertx instance = v.result();
        vertxInstances.add(instance);

        Awaitility.await().until(() -> clusterManager.getNodes().size() == 3);

        log().info("Firing event");
        instance.eventBus().send(
            ContentSenderVerticle.ADDRESS,
            new JsonObject().put("path", sourcePath),
            reply -> {
              try {
                if (reply.succeeded()) {
                  JsonObject result = (JsonObject) reply.result().body();
                  log().info("Succeeded " + result.getInteger("status"));
                  testContext.assertTrue(
                      verifyFilesEqual(sourcePath, result.getString("path")),
                      "Transport corrupted files"
                  );
                }
                else {
                  log().info("Failed ", reply.cause());
                  testContext.assertTrue(false, reply.cause().getMessage());
                }
              }
              finally {
                vertxInstances.forEach(vertx -> vertx.close());
              }
            }
        );
      }
      else {
        throw new AssertionError("Could not deploy: " + v.cause());
      }
    });
  }
}
