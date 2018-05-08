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

import { SnapshotVersion } from '../core/snapshot_version';
import { ProtoByteString, TargetId } from '../core/types';
import { QueryData, QueryPurpose } from '../local/query_data';
import {
  maybeDocumentMap,
  documentKeySet,
  DocumentKeySet
} from '../model/collections';
import { Document, NoDocument } from '../model/document';
import { DocumentKey } from '../model/document_key';
import { emptyByteString } from '../platform/platform';
import { assert, fail } from '../util/assert';
import { FirestoreError } from '../util/error';
import * as objUtils from '../util/obj';

import { ExistenceFilter } from './existence_filter';
import { RemoteEvent, TargetChange } from './remote_event';
import { ChangeType } from '../core/view_snapshot';
import { SortedMap } from '../util/sorted_map';
import { SortedSet } from '../util/sorted_set';
import { primitiveComparator } from '../util/misc';

/**
 * Internal representation of the watcher API protocol buffers.
 */
export type WatchChange =
  | DocumentWatchChange
  | WatchTargetChange
  | ExistenceFilterChange;

/**
 * Represents a changed document and a list of target ids to which this change
 * applies.
 *
 * If document has been deleted NoDocument will be provided.
 */
export class DocumentWatchChange {
  constructor(
    /** The new document applies to all of these targets. */
    public updatedTargetIds: TargetId[],
    /** The new document is removed from all of these targets. */
    public removedTargetIds: TargetId[],
    /** The key of the document for this change. */
    public key: DocumentKey,
    /**
     * The new document or NoDocument if it was deleted. Is null if the
     * document went out of view without the server sending a new document.
     */
    public newDoc: Document | NoDocument | null
  ) {}
}

export class ExistenceFilterChange {
  constructor(
    public targetId: TargetId,
    public existenceFilter: ExistenceFilter
  ) {}
}

export enum WatchTargetChangeState {
  NoChange,
  Added,
  Removed,
  Current,
  Reset
}

export class WatchTargetChange {
  constructor(
    /** What kind of change occurred to the watch target. */
    public state: WatchTargetChangeState,
    /** The target IDs that were added/removed/set. */
    public targetIds: TargetId[],
    /**
     * An opaque, server-assigned token that allows watching a query to be
     * resumed after disconnecting without retransmitting all the data that
     * matches the query. The resume token essentially identifies a point in
     * time from which the server should resume sending results.
     */
    public resumeToken: ProtoByteString = emptyByteString(),
    /** An RPC error indicating why the watch failed. */
    public cause: FirestoreError | null = null
  ) {}
}

/**
 * A helper class to accumulate watch changes into a RemoteEvent and other
 * target information.
 */
export class WatchChangeAggregator {
  constructor(
    private queryDataCallback: (targetId: TargetId) => QueryData | null,
    private syncedKeysCallback: (targetId: TargetId) => DocumentKeySet
  ) {}

  /** The number of pending responses that we are waiting on from watch. */
  private pendingTargetResponses: { [targetId: number]: number } = {};

  /** Keeps track of the current target mappings */
  private snapshotChanges: {
    [targetId: number]: SortedMap<DocumentKey, ChangeType>;
  } = {};

  private currentTargets = new SortedSet<TargetId>(primitiveComparator);

  private resumeTokens = new SortedMap<TargetId, ProtoByteString>(
    primitiveComparator
  );

  /** Keeps track of document to update */
  private documentUpdates = maybeDocumentMap();

  /** Tracks which document updates are due only to limbo target resolution */
  private limboDocuments = documentKeySet();

  /** Aggregates a watch change into the current state */
  add(watchChange: DocumentWatchChange | WatchTargetChange): void {
    if (watchChange instanceof DocumentWatchChange) {
      this.addDocumentChange(watchChange);
    } else if (watchChange) {
      this.addTargetChange(watchChange);
    }
  }

  /**
   * Converts the current state into a remote event with the snapshot version
   * provided via the constructor.
   */
  createRemoteEvent(snapshotVersion: SnapshotVersion): RemoteEvent {
    const targetChanges: { [targetId: number]: TargetChange } = {};

    // Remove all the non-active targets from the remote event.
    objUtils.forEachNumber(this.snapshotChanges, (targetId, changes) => {
      if (this.isActiveTarget(targetId)) {
        let addedDocuments = documentKeySet();
        let modifiedDocuments = documentKeySet();
        let removedDocuments = documentKeySet();

        changes.forEach((key, changeType) => {
          switch (changeType) {
            case ChangeType.Added:
              addedDocuments = addedDocuments.add(key);
              break;
            case ChangeType.Modified:
              modifiedDocuments = modifiedDocuments.add(key);
              break;
            case ChangeType.Removed:
              removedDocuments = removedDocuments.add(key);
              break;
            default:
              fail('...');
          }
        });

        targetChanges[targetId] = {
          current: this.currentTargets.has(targetId),
          snapshotVersion,
          resumeToken: this.resumeTokens.get(targetId),
          addedDocuments,
          modifiedDocuments,
          removedDocuments
        };
      }
    });

    const remoteEvent = {
      snapshotVersion,
      targetChanges,
      documentUpdates: this.documentUpdates,
      limboDocuments: this.limboDocuments
    };

    this.snapshotChanges = {};
    this.documentUpdates = maybeDocumentMap();

    return remoteEvent;
  }

  private ensureChangeSet(targetId: TargetId): void {
    if (!this.snapshotChanges[targetId]) {
      this.snapshotChanges[targetId] = new SortedMap<DocumentKey, ChangeType>(
        DocumentKey.comparator
      );
    }
  }

  /**
   * Defers to queryForActiveTarget to determine if the given targetId
   * corresponds to an active target.
   *
   * This method is visible for testing.
   */
  protected isActiveTarget(targetId: TargetId): boolean {
    return (
      this.queryDataCallback(targetId) != null &&
      !objUtils.contains(this.pendingTargetResponses, targetId)
    );
  }

  /**
   * Updates limbo document tracking for a given target-document mapping change.
   * If the target is a limbo target, and the change for the document has only
   * seen limbo targets so far, and we are not already tracking a change for
   * this document, then consider this document a limbo document update.
   * Otherwise, ensure that we don't consider this document a limbo document.
   * Returns true if the change still has only seen limbo resolution changes.
   */
  private updateLimboDocuments(
    key: DocumentKey,
    queryData: QueryData,
    isOnlyLimbo: boolean
  ): boolean {
    if (!isOnlyLimbo) {
      // It wasn't a limbo doc before, so it definitely isn't now.
      return false;
    }
    if (!this.documentUpdates.get(key)) {
      // We haven't seen the document update for this key yet.
      if (queryData.purpose === QueryPurpose.LimboResolution) {
        this.limboDocuments = this.limboDocuments.add(key);
        return true;
      } else {
        // We haven't seen the document before, but this is a non-limbo target.
        // Since we haven't seen it, we know it's not in our set of limbo docs.
        // Return false to ensure that this key is marked as non-limbo.
        return false;
      }
    } else if (queryData.purpose === QueryPurpose.LimboResolution) {
      // We have only seen limbo targets so far for this document, and this is
      // another limbo target.
      return true;
    } else {
      // We haven't marked this as non-limbo yet, but this target is not a limbo
      // target. Mark the key as non-limbo and make sure it isn't in our set.
      this.limboDocuments = this.limboDocuments.delete(key);
      return false;
    }
  }

  private addDocumentChange(docChange: DocumentWatchChange): void {
    let relevant = false;
    let isOnlyLimbo = true;

    for (const targetId of docChange.updatedTargetIds) {
      const queryData = this.queryDataCallback(targetId);
      if (queryData) {
        this.ensureChangeSet(targetId);
        isOnlyLimbo = this.updateLimboDocuments(
          docChange.key,
          queryData,
          isOnlyLimbo
        );

        const existingKeys = this.syncedKeysCallback(targetId);
        this.snapshotChanges[targetId] = this.snapshotChanges[targetId].insert(
          docChange.key,
          existingKeys.has(docChange.key)
            ? ChangeType.Modified
            : ChangeType.Added
        );

        relevant = true;
      }
    }

    for (const targetId of docChange.removedTargetIds) {
      const queryData = this.queryDataCallback(targetId);
      if (queryData) {
        this.ensureChangeSet(targetId);
        isOnlyLimbo = this.updateLimboDocuments(
          docChange.key,
          queryData,
          isOnlyLimbo
        );

        const existingKeys = this.syncedKeysCallback(targetId);
        if (existingKeys.has(docChange.key)) {
          this.snapshotChanges[targetId] = this.snapshotChanges[
            targetId
          ].insert(docChange.key, ChangeType.Removed);
        } else {
          this.snapshotChanges[targetId] = this.snapshotChanges[
            targetId
          ].remove(docChange.key);
        }

        relevant = true;
      }
    }

    // Only update the document if there is a new document to replace to an
    // active target that is being listened to, this might be just a target
    // update instead.
    if (docChange.newDoc && relevant) {
      this.documentUpdates = this.documentUpdates.insert(
        docChange.key,
        docChange.newDoc
      );
    }
  }

  resetTarget(targetId: TargetId): void {
    this.ensureChangeSet(targetId);
    const documentSet = this.syncedKeysCallback(targetId);
    documentSet.forEach(key => {
      this.snapshotChanges[targetId] = this.snapshotChanges[targetId].insert(
        key,
        ChangeType.Removed
      );
    });
  }

  private addTargetChange(targetChange: WatchTargetChange): void {
    targetChange.targetIds.forEach(targetId => {
      switch (targetChange.state) {
        case WatchTargetChangeState.NoChange:
          if (this.isActiveTarget(targetId)) {
            // Creating the change above satisfies the semantics of no-change.
            this.updateResumeToken(targetId, targetChange.resumeToken);
          }
          break;
        case WatchTargetChangeState.Added:
          // We need to decrement the number of pending acks needed from watch
          // for this targetId.
          this.recordTargetResponse(targetId);
          if (!objUtils.contains(this.pendingTargetResponses, targetId)) {
            // We have a freshly added target, so we need to reset any state
            // that we had previously This can happen e.g. when remove and add
            // back a target for existence filter mismatches.
            this.currentTargets = this.currentTargets.delete(targetId);
          }
          this.updateResumeToken(targetId, targetChange.resumeToken);
          break;
        case WatchTargetChangeState.Removed:
          // We need to keep track of removed targets to we can
          // post-filter and remove any target changes.
          // We need to decrement the number of pending acks needed from watch
          // for this targetId.
          this.recordTargetResponse(targetId);
          assert(
            !targetChange.cause,
            'WatchChangeAggregator does not handle errored targets'
          );
          break;
        case WatchTargetChangeState.Current:
          if (this.isActiveTarget(targetId)) {
            this.currentTargets = this.currentTargets.add(targetId);
            this.updateResumeToken(targetId, targetChange.resumeToken);
          }
          break;
        case WatchTargetChangeState.Reset:
          if (this.isActiveTarget(targetId)) {
            // Overwrite any existing target mapping with a reset
            // mapping. Every subsequent update will modify the reset
            // mapping, not an update mapping.
            this.resetTarget(targetId);
            this.updateResumeToken(targetId, targetChange.resumeToken);
          }
          break;
        default:
          fail('Unknown target watch change state: ' + targetChange.state);
      }
    });
  }

  /**
   * Record that we get a watch target add/remove by decrementing the number of
   * pending target responses that we have.
   */
  private recordTargetResponse(targetId: TargetId): void {
    const newCount = (this.pendingTargetResponses[targetId] || 0) - 1;
    if (newCount === 0) {
      delete this.pendingTargetResponses[targetId];
    } else {
      this.pendingTargetResponses[targetId] = newCount;
    }
  }

  /**
   * Applies the resume token to the TargetChange, but only when it has a new
   * value. null and empty resumeTokens are discarded.
   */
  private updateResumeToken(
    targetId: TargetId,
    resumeToken: ProtoByteString
  ): void {
    if (resumeToken.length > 0) {
      this.resumeTokens = this.resumeTokens.insert(targetId, resumeToken);
    }
  }

  reset() {
    this.snapshotChanges = {};
    this.documentUpdates = maybeDocumentMap();
    this.currentTargets = new SortedSet<TargetId>(primitiveComparator);
    this.pendingTargetResponses = {};
  }

  addDocumentUpdate(deletedDoc: NoDocument) {
    this.documentUpdates = this.documentUpdates.insert(
      deletedDoc.key,
      deletedDoc
    );
  }

  getCurrentSize(targetId: TargetId) {
    this.ensureChangeSet(targetId);

    let currentSize = this.syncedKeysCallback(targetId).size;
    this.snapshotChanges[targetId].forEach((key, changeType) => {
      switch (changeType) {
        case ChangeType.Added:
          ++currentSize;
          break;
        case ChangeType.Modified:
          break;
        case ChangeType.Removed:
          --currentSize;
          break;
        default:
          fail('...');
      }
    });

    return currentSize;
  }
  /**
   * Increment the mapping of how many acks are needed from watch before we can
   * consider the server to be 'in-sync' with the client's active targets.
   */
  recordPendingTargetRequest(targetId: TargetId): void {
    // For each request we get we need to record we need a response for it.
    this.pendingTargetResponses[targetId] =
      (this.pendingTargetResponses[targetId] || 0) + 1;
  }
}
