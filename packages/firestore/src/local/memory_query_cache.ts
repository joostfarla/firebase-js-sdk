/**
 * Copyright 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Query } from '../core/query';
import { SnapshotVersion } from '../core/snapshot_version';
import { TargetId } from '../core/types';
import { documentKeySet, DocumentKeySet } from '../model/collections';
import { DocumentKey } from '../model/document_key';
import { ObjectMap } from '../util/obj_map';

import { GarbageCollector } from './garbage_collector';
import { PersistenceTransaction } from './persistence';
import { PersistencePromise } from './persistence_promise';
import { QueryCache } from './query_cache';
import { QueryData } from './query_data';
import { ReferenceSet } from './reference_set';
import { assert } from '../util/assert';
import { SortedMap } from '../util/sorted_map';
import { primitiveComparator } from '../util/misc';

type TargetSnapshot = { targetId: TargetId; snapshotVersion: SnapshotVersion };

export class MemoryQueryCache implements QueryCache {
  /**
   * Maps a query to the data about that query
   */
  private queries = new ObjectMap<Query, QueryData>(q => q.canonicalId());

  /**
   * Tracks the updated keys of a query by snapshot version.
   */
  private targetChanges = new SortedMap<TargetSnapshot, DocumentKeySet>(
    (left, right) => {
      const cmp = primitiveComparator(left.targetId, right.targetId);
      return cmp !== 0
        ? cmp
        : left.snapshotVersion.compareTo(right.snapshotVersion);
    }
  );

  /** The last received snapshot version. */
  private lastRemoteSnapshotVersion = SnapshotVersion.MIN;
  /** The highest numbered target ID encountered. */
  private highestTargetId: TargetId = 0;
  /**
   * A ordered bidirectional mapping between documents and the remote target
   * IDs.
   */
  private references = new ReferenceSet();

  private targetCount = 0;

  start(transaction: PersistenceTransaction): PersistencePromise<void> {
    // Nothing to do.
    return PersistencePromise.resolve();
  }

  getLastRemoteSnapshotVersion(): SnapshotVersion {
    return this.lastRemoteSnapshotVersion;
  }

  getHighestTargetId(): TargetId {
    return this.highestTargetId;
  }

  setLastRemoteSnapshotVersion(
    transaction: PersistenceTransaction,
    snapshotVersion: SnapshotVersion
  ): PersistencePromise<void> {
    this.lastRemoteSnapshotVersion = snapshotVersion;
    return PersistencePromise.resolve();
  }

  private saveQueryData(queryData: QueryData): void {
    this.queries.set(queryData.query, queryData);
    const targetId = queryData.targetId;
    if (targetId > this.highestTargetId) {
      this.highestTargetId = targetId;
    }
    // TODO(GC): track sequence number
  }

  addQueryData(
    transaction: PersistenceTransaction,
    queryData: QueryData
  ): PersistencePromise<void> {
    assert(
      !this.queries.has(queryData.query),
      'Adding a query that already exists'
    );
    this.saveQueryData(queryData);
    this.targetCount += 1;
    return PersistencePromise.resolve();
  }

  updateQueryData(
    transaction: PersistenceTransaction,
    queryData: QueryData
  ): PersistencePromise<void> {
    assert(this.queries.has(queryData.query), 'Updating a non-existent query');
    this.saveQueryData(queryData);
    return PersistencePromise.resolve();
  }

  removeQueryData(
    transaction: PersistenceTransaction,
    queryData: QueryData
  ): PersistencePromise<void> {
    assert(this.targetCount > 0, 'Removing a target from an empty cache');
    assert(
      this.queries.has(queryData.query),
      'Removing a non-existent target from the cache'
    );
    this.queries.delete(queryData.query);
    this.references.removeReferencesForId(queryData.targetId);
    this.targetCount -= 1;
    return PersistencePromise.resolve();
  }

  get count(): number {
    return this.targetCount;
  }

  getQueryData(
    transaction: PersistenceTransaction,
    query: Query
  ): PersistencePromise<QueryData | null> {
    const queryData = this.queries.get(query) || null;
    return PersistencePromise.resolve(queryData);
  }

  addMatchingKeys(
    txn: PersistenceTransaction,
    keys: DocumentKeySet,
    targetId: TargetId,
    snapshotVersion: SnapshotVersion
  ): PersistencePromise<void> {
    this.references.addReferences(keys, targetId);
    return this.getChanges(txn, targetId, snapshotVersion).next(
      documentUpdates => {
        this.targetChanges = this.targetChanges.insert(
          { targetId, snapshotVersion },
          documentUpdates.unionWith(keys)
        );
      }
    );
  }

  removeMatchingKeys(
    txn: PersistenceTransaction,
    keys: DocumentKeySet,
    targetId: TargetId,
    snapshotVersion: SnapshotVersion
  ): PersistencePromise<void> {
    this.references.removeReferences(keys, targetId);
    return this.getChanges(txn, targetId, snapshotVersion).next(
      documentUpdates => {
        this.targetChanges = this.targetChanges.insert(
          { targetId, snapshotVersion },
          documentUpdates.unionWith(keys)
        );
      }
    );
  }

  removeMatchingKeysForTargetId(
    txn: PersistenceTransaction,
    targetId: TargetId,
    snapshotVersion: SnapshotVersion
  ): PersistencePromise<void> {
    this.references.removeReferencesForId(targetId);

    const snapshotsToDelete: TargetSnapshot[] = [];
    const it = this.targetChanges.getIteratorFrom({
      targetId,
      snapshotVersion: SnapshotVersion.MIN
    });
    while (it.hasNext()) {
      const key = it.getNext().key;
      if (
        key.targetId !== targetId ||
        key.snapshotVersion.compareTo(snapshotVersion) > 0
      ) {
        break;
      }
      snapshotsToDelete.push(key);
    }

    snapshotsToDelete.forEach(key => {
      this.targetChanges = this.targetChanges.remove(key);
    });

    return PersistencePromise.resolve();
  }

  getMatchingKeysForTargetId(
    txn: PersistenceTransaction,
    targetId: TargetId
  ): PersistencePromise<DocumentKeySet> {
    const matchingKeys = this.references.referencesForId(targetId);
    return PersistencePromise.resolve(matchingKeys);
  }

  setGarbageCollector(gc: GarbageCollector | null): void {
    this.references.setGarbageCollector(gc);
  }

  containsKey(
    txn: PersistenceTransaction | null,
    key: DocumentKey
  ): PersistencePromise<boolean> {
    return this.references.containsKey(txn, key);
  }

  addModifiedKeys(
    txn: PersistenceTransaction,
    keys: DocumentKeySet,
    targetId: TargetId,
    snapshotVersion: SnapshotVersion
  ): PersistencePromise<void> {
    return this.getChanges(txn, targetId, snapshotVersion).next(
      documentUpdates => {
        this.targetChanges = this.targetChanges.insert(
          { targetId, snapshotVersion },
          documentUpdates.unionWith(keys)
        );
      }
    );
  }

  getAccumulatedChanges(
    transaction: PersistenceTransaction,
    targetId: TargetId,
    fromVersion: SnapshotVersion
  ): PersistencePromise<DocumentKeySet> {
    let documentUpdates = documentKeySet();
    const it = this.targetChanges.getIteratorFrom({
      targetId,
      snapshotVersion: fromVersion
    });

    while (it.hasNext()) {
      const entry = it.getNext();
      if (entry.key.targetId !== targetId) {
        break;
      }
      documentUpdates = documentUpdates.unionWith(entry.value);
    }

    return PersistencePromise.resolve(documentUpdates);
  }

  private getChanges(
    transaction: PersistenceTransaction,
    targetId: TargetId,
    snapshotVersion: SnapshotVersion
  ): PersistencePromise<DocumentKeySet> {
    const changes = this.targetChanges.get({ targetId, snapshotVersion });

    if (!changes) {
      return PersistencePromise.resolve(documentKeySet());
    }

    return PersistencePromise.resolve(changes);
  }
}
