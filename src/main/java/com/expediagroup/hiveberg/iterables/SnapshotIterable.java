/**
 * Copyright (C) 2020 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.expediagroup.hiveberg.iterables;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;

/**
 * To create an iterator of Records for snapshot metadata rows that Hive can read.
 */
public class SnapshotIterable implements Iterable  {

  private Table table;

  public SnapshotIterable(Table table) {
    this.table = table;
  }

  public Iterator<Record> iterator() {
    Iterable<Snapshot> snapshots = table.snapshots();
    List<Record> snapRecords = new ArrayList<>();
    snapshots.forEach(snapshot -> snapRecords.add(createSnapshotRecord(snapshot)));

    return snapRecords.iterator();
  }

  /**
   * Populating a Record with snapshot metadata
   */
  private Record createSnapshotRecord(Snapshot snapshot) {
    Record snapRecord = GenericRecord.create(table.schema());
    snapRecord.setField("committed_at", OffsetDateTime.of(
        LocalDateTime.ofEpochSecond(snapshot.timestampMillis(), 0, ZoneOffset.UTC), ZoneOffset.UTC));
    snapRecord.setField("snapshot_id", snapshot.snapshotId());
    snapRecord.setField("parent_id", snapshot.parentId());
    snapRecord.setField("operation", snapshot.operation());
    snapRecord.setField("manifest_list", snapshot.manifestListLocation());
    snapRecord.setField("summary", snapshot.summary());
    return snapRecord;
  }
}
