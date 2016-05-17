package org.cstamas.vertx.content.examples;

import java.io.File;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.google.common.base.Throwables;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
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

  private static final byte[] SCALE = {
      0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f
  };

  public static String createDummyFile(final long size) {
    try {
      Path path = Files.createTempFile("vertx", "content");
      try (OutputStream output = Files.newOutputStream(path)) {
        for (int i = 0; i < size / 16; i++) {
          output.write(SCALE);
        }
        for (int i = 0; i < size % 16; i++) {
          output.write(SCALE[i]);
        }
      }
      return path.toAbsolutePath().toString();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static boolean verifyFilesEqual(final String path1, final String path2) {
    try {
      HashCode hash1 = com.google.common.io.Files.hash(new File(path1), Hashing.sha1());
      HashCode hash2 = com.google.common.io.Files.hash(new File(path2), Hashing.sha1());
      return hash1.equals(hash2);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
