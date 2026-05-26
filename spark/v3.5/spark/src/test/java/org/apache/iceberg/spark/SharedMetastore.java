/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.hive.TestHiveMetastore;

/**
 * A single {@link TestHiveMetastore} shared by every Spark test class running in the same JVM fork.
 *
 * <p>Each test class used to start and stop its own metastore in
 * {@code @BeforeAll}/{@code @AfterAll}, paying a fresh Thrift server + client pool per class. Since
 * the Derby schema is already created once per JVM (in {@code TestHiveMetastore}'s static
 * initializer), the only per-class state that needs clearing between classes is the catalog
 * content, which {@link TestHiveMetastore#reset()} handles (it is also what {@code stop()} runs
 * first). Sharing one instance and resetting between classes therefore preserves the existing
 * isolation while removing the repeated start/stop.
 *
 * <p>Initialized lazily and safely via the Bill Pugh holder idiom; the metastore is stopped once at
 * JVM exit through a shutdown hook.
 */
public final class SharedMetastore {

  private SharedMetastore() {}

  private static final class Holder {
    static final TestHiveMetastore METASTORE = start();
    static final HiveConf HIVE_CONF = METASTORE.hiveConf();

    private static TestHiveMetastore start() {
      TestHiveMetastore metastore = new TestHiveMetastore();
      metastore.start();
      Runtime.getRuntime().addShutdownHook(new Thread(() -> stopQuietly(metastore)));
      return metastore;
    }
  }

  public static TestHiveMetastore get() {
    return Holder.METASTORE;
  }

  public static HiveConf hiveConf() {
    return Holder.HIVE_CONF;
  }

  public static void reset() throws Exception {
    Holder.METASTORE.reset();
  }

  private static void stopQuietly(TestHiveMetastore metastore) {
    try {
      metastore.stop();
    } catch (Exception e) {
      // best-effort cleanup at JVM exit; nothing actionable remains
    }
  }
}
