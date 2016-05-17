package org.cstamas.vertx.content.examples;

import com.google.common.collect.ImmutableList;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.cstamas.vertx.content.examples.TestUtil.blockingCloseAll;
import static org.cstamas.vertx.content.examples.TestUtil.createDummyFile;
import static org.cstamas.vertx.content.examples.TestUtil.initLogging;
import static org.cstamas.vertx.content.examples.TestUtil.log;
import static org.cstamas.vertx.content.examples.TestUtil.verifyFilesEqual;

/**
 * Junit test.
 */
@RunWith(VertxUnitRunner.class)
public class SimpleTest
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
  public void sendFileEbSmall(final TestContext testContext) throws Exception {
    sendFile(testContext, new JsonObject().put("type", "eventbus"), 15000);
  }

  @Test
  public void sendFileEbBig(final TestContext testContext) throws Exception {
    sendFile(testContext, new JsonObject().put("type", "eventbus"), 8000000);
  }

  private void sendFile(final TestContext testContext, final JsonObject config, final long sourceFileSize)
      throws Exception
  {
    Vertx vertx = Vertx.vertx();
    Async deploy = testContext.async(2);
    vertx.deployVerticle(
        ContentSenderVerticle.class.getName(),
        new DeploymentOptions().setConfig(config.copy().put("host", "localhost").put("port", 8081)),
        v -> {
          deploy.countDown();
        }
    );
    vertx.deployVerticle(
        ContentReceiverVerticle.class.getName(),
        new DeploymentOptions().setConfig(config.copy().put("host", "localhost").put("port", 8082)),
        v -> {
          deploy.countDown();
        }
    );
    deploy.awaitSuccess();

    String source = createDummyFile(sourceFileSize);

    Async operation = testContext.async();
    vertx.eventBus().send(
        ContentSenderVerticle.ADDRESS,
        new JsonObject().put("path", source),
        reply -> {
          try {
            if (reply.succeeded()) {
              JsonObject jsonObject = (JsonObject) reply.result().body();
              log().info("Succeeded " + jsonObject.getInteger("status"));
              testContext.assertTrue(
                  verifyFilesEqual(source, jsonObject.getString("path")),
                  "Transport corrupted files"
              );
            }
            else {
              log().info("Failed ", reply.cause());
              testContext.assertTrue(false, reply.cause().getMessage());
            }
          }
          finally {
            operation.complete();
          }
        }
    );
    operation.awaitSuccess();

    blockingCloseAll(ImmutableList.of(vertx));
  }
}
