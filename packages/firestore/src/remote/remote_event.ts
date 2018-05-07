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
import {
  documentKeySet,
  DocumentKeySet,
  MaybeDocumentMap
} from '../model/collections';
import { MaybeDocument } from '../model/document';
import { DocumentKey } from '../model/document_key';
import { emptyByteString } from '../platform/platform';

/**
 * An event from the RemoteStore. It is split into targetChanges (changes to the
 * state or the set of documents in our watched targets) and documentUpdates
 * (changes to the actual documents).
 */
export class RemoteEvent {
  constructor(
    /**
     * The snapshot version this event brings us up to, or MIN if not set.
     */
    readonly snapshotVersion: SnapshotVersion,
    /**
     * A map from target to changes to the target. See TargetChange.
     */
    readonly targetChanges: { [targetId: number]: TargetChange },
    /**
     * A set of which documents have changed or been deleted, along with the
     * doc's new values (if not deleted).
     */
    public documentUpdates: MaybeDocumentMap
  ) {}

  addDocumentUpdate(doc: MaybeDocument): void {
    this.documentUpdates = this.documentUpdates.insert(doc.key, doc);
  }

  handleExistenceFilterMismatch(targetId: TargetId): void {
    /*
     * An existence filter mismatch will reset the query and we need to reset
     * the mapping to contain no documents and an empty resume token.
     *
     * Note:
     *   * The reset mapping is empty, specifically forcing the consumer of the
     *     change to forget all keys for this targetID;
     *   * The resume snapshot for this target must be reset
     *   * The target must be unacked because unwatching and rewatching
     *     introduces a race for changes.
     */
    this.targetChanges[targetId] = new TargetChange(
      CurrentStatusUpdate.MarkNotCurrent,
      new ResetMapping(),
      SnapshotVersion.MIN,
      emptyByteString()
    );
  }
}

/**
 * Represents an update to the current status of a target, either explicitly
 * having no new state, or the new value to set. Note "current" has special
 * meaning for in the RPC protocol that implies that a target is both up-to-date
 * and consistent with the rest of the watch stream.
 */
export enum CurrentStatusUpdate {
  /** The current status is not affected and should not be modified. */
  None,
  /** The target must be marked as no longer "current". */
  MarkNotCurrent,
  /** The target must be marked as "current". */
  MarkCurrent
}

/**
 * Contains the set of changed documents for a single target in a TargetChange.
 * Changes are separated into document adds, modifies and removes.
 */
export interface TargetChangeSet {
  /** Whether this target change was a regular update or a target reset. */
  readonly changeType: 'update' | 'reset';
  /** The snapshot version that this target change brings us up to. */
  readonly snapshotVersion: SnapshotVersion;
  /** The resume token for this target change. */
  readonly resumeToken: ProtoByteString;
  readonly addedDocuments: DocumentKeySet;
  readonly modifiedDocuments: DocumentKeySet;
  readonly removedDocuments: DocumentKeySet;
}

/**
 * A part of a RemoteEvent specifying set of changes to a specific target. These
 * changes track what documents are currently included in the target as well as
 * the current snapshot version and resume token but the actual changes *to*
 * documents are not part of the TargetChange since documents may be part of
 * multiple targets.
 */
export class TargetChange {
  constructor(
    /**
     * The new "current" (synced) status of this target. Set to
     * CurrentStatusUpdateNone if the status should not be updated. Note "current"
     * has special meaning in the RPC protocol that implies that a target is
     * both up-to-date and consistent with the rest of the watch stream.
     */
    public currentStatusUpdate: CurrentStatusUpdate,
    /**
     * A set of changes to documents in this target.
     */
    public mapping: TargetMapping,
    /** The snapshot version that this target change brings us up to. */
    public snapshotVersion: SnapshotVersion,
    /**
     * An opaque, server-assigned token that allows watching a query to be resumed
     * after disconnecting without retransmitting all the data that matches the
     * query. The resume token essentially identifies a point in time from which
     * the server should resume sending results.
     */
    public resumeToken: ProtoByteString
  ) {}

  toChanges(existingKeys: DocumentKeySet): TargetChangeSet {
    return this.mapping.toChanges(
      this.snapshotVersion,
      this.resumeToken,
      existingKeys
    );
  }

  /** Returns 'true' if this target was marked CURRENT by the backend. */
  isCurrent(): boolean {
    return this.currentStatusUpdate === CurrentStatusUpdate.MarkCurrent;
  }
}

export type TargetMapping = ResetMapping | UpdateMapping;

const EMPTY_KEY_SET = documentKeySet();

export class ResetMapping {
  private docs: DocumentKeySet = EMPTY_KEY_SET;

  get documents(): DocumentKeySet {
    return this.docs;
  }

  add(key: DocumentKey): void {
    this.docs = this.docs.add(key);
  }

  delete(key: DocumentKey): void {
    this.docs = this.docs.delete(key);
  }

  isEqual(other: ResetMapping): boolean {
    return other !== null && this.docs.isEqual(other.docs);
  }

  toChanges(
    snapshotVersion: SnapshotVersion,
    resumeToken: ProtoByteString,
    existingKeys: DocumentKeySet
  ): TargetChangeSet {
    return {
      snapshotVersion,
      resumeToken,
      changeType: 'reset',
      addedDocuments: this.docs,
      modifiedDocuments: EMPTY_KEY_SET,
      removedDocuments: EMPTY_KEY_SET
    };
  }
}

export class UpdateMapping {
  addedDocuments: DocumentKeySet = EMPTY_KEY_SET;
  removedDocuments: DocumentKeySet = EMPTY_KEY_SET;

  applyToKeySet(keys: DocumentKeySet): DocumentKeySet {
    let result = keys;
    this.addedDocuments.forEach(key => (result = result.add(key)));
    this.removedDocuments.forEach(key => (result = result.delete(key)));
    return result;
  }

  add(key: DocumentKey): void {
    this.addedDocuments = this.addedDocuments.add(key);
    this.removedDocuments = this.removedDocuments.delete(key);
  }

  delete(key: DocumentKey): void {
    this.addedDocuments = this.addedDocuments.delete(key);
    this.removedDocuments = this.removedDocuments.add(key);
  }

  isEqual(other: UpdateMapping): boolean {
    return (
      other !== null &&
      this.addedDocuments.isEqual(other.addedDocuments) &&
      this.removedDocuments.isEqual(other.removedDocuments)
    );
  }

  /**
   * Strips out mapping changes that aren't actually changes. That is, if the document already
   * existed in the target, and is being added in the target, and this is not a reset, we can
   * skip doing the work to associate the document with the target because it has already been done.
   */
  toChanges(
    snapshotVersion: SnapshotVersion,
    resumeToken: ProtoByteString,
    existingKeys: DocumentKeySet
  ): TargetChangeSet {
    const changeSet = {
      snapshotVersion,
      resumeToken,
      changeType: 'update',
      addedDocuments: EMPTY_KEY_SET,
      modifiedDocuments: EMPTY_KEY_SET,
      removedDocuments: this.removedDocuments
    };

    this.addedDocuments.forEach(docKey => {
      if (existingKeys.has(docKey)) {
        changeSet.modifiedDocuments = changeSet.modifiedDocuments.add(docKey);
      } else {
        changeSet.addedDocuments = changeSet.addedDocuments.add(docKey);
      }
    });

    return changeSet as TargetChangeSet;
  }
}
