/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.shuffle;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;

public class ExternalShuffleCleanupSuite {

  // Same-thread Executor used to ensure cleanup happens synchronously in test thread.
  private Executor sameThreadExecutor = MoreExecutors.sameThreadExecutor();
  private TransportConf conf = new TransportConf("shuffle", MapConfigProvider.EMPTY);
  private static final String SORT_MANAGER = "org.apache.spark.shuffle.sort.SortShuffleManager";

  @Test
  public void noCleanupAndCleanup() throws IOException {
    TestShuffleDataContext dataContext = createSomeData();

    ExternalShuffleBlockResolver resolver =
      new ExternalShuffleBlockResolver(conf, null, sameThreadExecutor);
    resolver.registerExecutor("app", "exec0", dataContext.createExecutorInfo(SORT_MANAGER));
    resolver.applicationRemoved("app", false /* cleanup */);

    assertStillThere(dataContext);

    resolver.registerExecutor("app", "exec1", dataContext.createExecutorInfo(SORT_MANAGER));
    resolver.applicationRemoved("app", true /* cleanup */);

    assertCleanedUp(dataContext);
  }

  @Test
  public void cleanupUsesExecutor() throws IOException {
    TestShuffleDataContext dataContext = createSomeData();

    AtomicBoolean cleanupCalled = new AtomicBoolean(false);

    // Executor which does nothing to ensure we're actually using it.
    Executor noThreadExecutor = runnable -> cleanupCalled.set(true);

    ExternalShuffleBlockResolver manager =
      new ExternalShuffleBlockResolver(conf, null, noThreadExecutor);

    manager.registerExecutor("app", "exec0", dataContext.createExecutorInfo(SORT_MANAGER));
    manager.applicationRemoved("app", true);

    assertTrue(cleanupCalled.get());
    assertStillThere(dataContext);

    dataContext.cleanup();
    assertCleanedUp(dataContext);
  }

  @Test
  public void cleanupMultipleExecutors() throws IOException {
    TestShuffleDataContext dataContext0 = createSomeData();
    TestShuffleDataContext dataContext1 = createSomeData();

    ExternalShuffleBlockResolver resolver =
      new ExternalShuffleBlockResolver(conf, null, sameThreadExecutor);

    resolver.registerExecutor("app", "exec0", dataContext0.createExecutorInfo(SORT_MANAGER));
    resolver.registerExecutor("app", "exec1", dataContext1.createExecutorInfo(SORT_MANAGER));
    resolver.applicationRemoved("app", true);

    assertCleanedUp(dataContext0);
    assertCleanedUp(dataContext1);
  }

  @Test
  public void cleanupOnlyRemovedApp() throws IOException {
    TestShuffleDataContext dataContext0 = createSomeData();
    TestShuffleDataContext dataContext1 = createSomeData();

    ExternalShuffleBlockResolver resolver =
      new ExternalShuffleBlockResolver(conf, null, sameThreadExecutor);

    resolver.registerExecutor("app-0", "exec0", dataContext0.createExecutorInfo(SORT_MANAGER));
    resolver.registerExecutor("app-1", "exec0", dataContext1.createExecutorInfo(SORT_MANAGER));

    resolver.applicationRemoved("app-nonexistent", true);
    assertStillThere(dataContext0);
    assertStillThere(dataContext1);

    resolver.applicationRemoved("app-0", true);
    assertCleanedUp(dataContext0);
    assertStillThere(dataContext1);

    resolver.applicationRemoved("app-1", true);
    assertCleanedUp(dataContext0);
    assertCleanedUp(dataContext1);

    // Make sure it's not an error to cleanup multiple times
    resolver.applicationRemoved("app-1", true);
    assertCleanedUp(dataContext0);
    assertCleanedUp(dataContext1);
  }

  @Test
  public void cleanupPartialShuffleData() throws IOException {
    TestShuffleDataContext dataContext0 = createSomeData();
    TestShuffleDataContext dataContext1 = createSomeData();

    // For executor 0, create data for 2 stages
    createShuffleDataForDataContext(dataContext0, 1);
    createShuffleDataForDataContext(dataContext0, 2);
    // For executor 1, create data for 3 stages
    createShuffleDataForDataContext(dataContext1, 1);
    createShuffleDataForDataContext(dataContext1, 2);
    createShuffleDataForDataContext(dataContext1, 3);

    ExternalShuffleBlockResolver resolver =
        new ExternalShuffleBlockResolver(conf, null, sameThreadExecutor);

    resolver.registerExecutor("app", "exec0", dataContext0.createExecutorInfo(SORT_MANAGER));
    resolver.registerExecutor("app", "exec1", dataContext1.createExecutorInfo(SORT_MANAGER));
    // executor to Shuffle data mapping -> exec0: [1, 2], exec1: [1, 2, 3]

    // Check data exists before calling deleteShuffleDataForShuffleId
    assertShuffleIdDataPresent(dataContext0, 1);
    assertShuffleIdDataPresent(dataContext1, 1);
    assertShuffleIdDataPresent(dataContext0, 2);
    assertShuffleIdDataPresent(dataContext1, 2);
    assertShuffleIdDataPresent(dataContext1, 3);

    // Delete shuffle data for stage 1
    // Shuffle data for stage 1 deleted. So exec0: [2], exec1: [2,3]
    resolver.deleteShuffleFiles(1, "app");
    assertShuffleIdDataPresent(dataContext0, 2);
    assertShuffleIdDataPresent(dataContext1, 2);
    assertShuffleIdDataCleanedUp(dataContext0, 1);
    assertShuffleIdDataCleanedUp(dataContext1, 1);
    assertShuffleIdDataPresent(dataContext1, 3);

    // Delete shuffle data for stage 2
    // Shuffle data for stage 1 deleted. So exec0: [], exec1: [3]
    resolver.deleteShuffleFiles(2, "app");
    assertShuffleIdDataCleanedUp(dataContext0, 1);
    assertShuffleIdDataCleanedUp(dataContext0, 2);
    assertShuffleIdDataCleanedUp(dataContext1, 1);
    assertShuffleIdDataCleanedUp(dataContext1, 2);
    assertShuffleIdDataPresent(dataContext1, 3);
  }

  private static void assertStillThere(TestShuffleDataContext dataContext) {
    for (String localDir : dataContext.localDirs) {
      assertTrue(localDir + " was cleaned up prematurely", new File(localDir).exists());
    }
  }

  private static void assertCleanedUp(TestShuffleDataContext dataContext) {
    for (String localDir : dataContext.localDirs) {
      assertFalse(localDir + " wasn't cleaned up", new File(localDir).exists());
    }
  }

  private static void assertShuffleIdDataPresent(TestShuffleDataContext dataContext,
                                                 int shuffleId) {
    FilenameFilter filter = new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        // Don't delete shuffle data or shuffle index files.
        return name.startsWith("shuffle_"+shuffleId+"_");
      }
    };
    int totalFiles = 0;
    for (String localDir : dataContext.localDirs) {
      totalFiles += listLeafFiles(new File(localDir), filter).size();
    }
    assertTrue("Shuffle files for " + shuffleId + " were cleaned up prematurely", totalFiles > 0);

  }

  private static void assertShuffleIdDataCleanedUp(TestShuffleDataContext dataContext,
                                                   int shuffleId) throws IOException {

    FilenameFilter filter = new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        // Don't delete shuffle data or shuffle index files.
        return name.startsWith("shuffle_"+shuffleId+"_");
      }
    };
    for (String localDir : dataContext.localDirs) {
      assertTrue( "Shuffle files for " + shuffleId + " weren't cleaned up",
          listLeafFiles(new File(localDir), filter).isEmpty());
    }
  }

  private static List<File> listLeafFiles(File dir, FilenameFilter filter) {
    List<File> fileTree = new ArrayList<>();
    if(dir == null || dir.listFiles(filter) == null) {
      return fileTree;
    }
    for (File entry : dir.listFiles()) {
      if (entry.isFile() && filter.accept(entry.getParentFile(), entry.getName())) {
        fileTree.add(entry);
      } else {
        fileTree.addAll(listLeafFiles(entry, filter));
      }
    }
    return fileTree;
  }

  private static TestShuffleDataContext createSomeData() throws IOException {
    Random rand = new Random(123);
    TestShuffleDataContext dataContext = new TestShuffleDataContext(10, 5);

    dataContext.create();
    dataContext.insertSortShuffleData(rand.nextInt(1000), rand.nextInt(1000), new byte[][] {
        "ABC".getBytes(StandardCharsets.UTF_8),
        "DEF".getBytes(StandardCharsets.UTF_8)});
    return dataContext;
  }

  private static void createShuffleDataForDataContext(TestShuffleDataContext dataContext,
                                                      int shuffleId) throws IOException {
    Random rand = new Random(123);
    dataContext.insertSortShuffleData(shuffleId, rand.nextInt(1000), new byte[][] {
        "ABC".getBytes(StandardCharsets.UTF_8),
        "DEF".getBytes(StandardCharsets.UTF_8)});
  }
}