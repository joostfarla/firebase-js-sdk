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
import { DocumentKeySet, MaybeDocumentMap } from '../model/collections';
import { NoDocument } from '../model/document';
import { DocumentKey } from '../model/document_key';
import { assert } from '../util/assert';

/**
 * An event from the RemoteStore. It is split into targetChanges (changes to the
 * state or the set of documents in our watched targets) and documentUpdates
 * (changes to the actual documents).
 */
export interface RemoteEvent {
  /**
   * The snapshot version this event brings us up to, or MIN if not set.
   */
  readonly snapshotVersion: SnapshotVersion;
  /**
   * A map from target to changes to the target. See TargetChange.
   */
  readonly targetChanges: { [targetId: number]: TargetChange };
  /**
   * A set of which documents have changed or been deleted, along with the
   * doc's new values (if not deleted).
   */
  readonly documentUpdates: MaybeDocumentMap;
  /**
   * A set of which document updates are due only to limbo resolution targets.
   */
  readonly limboDocuments: DocumentKeySet;
}

/**
 * A part of a RemoteEvent specifying set of changes to a specific target. These
 * changes track what documents are currently included in the target as well as
 * the current snapshot version and resume token but the actual changes *to*
 * documents are not part of the TargetChange since documents may be part of
 * multiple targets.
 */
export interface TargetChange {
  /**
   * The new "current" (synced) status of this target. Set to
   * CurrentStatusUpdateNone if the status should not be updated. Note "current"
   * has special meaning in the RPC protocol that implies that a target is
   * both up-to-date and consistent with the rest of the watch stream.
   */
  readonly current: boolean;

  /** The snapshot version that this target change brings us up to. */
  readonly snapshotVersion: SnapshotVersion;

  /**
   * An opaque, server-assigned token that allows watching a query to be resumed
   * after disconnecting without retransmitting all the data that matches the
   * query. The resume token essentially identifies a point in time from which
   * the server should resume sending results.
   */
  readonly resumeToken: ProtoByteString;

  readonly addedDocuments: DocumentKeySet;

  readonly removedDocuments: DocumentKeySet;

  readonly modifiedDocuments: DocumentKeySet;
}

/**
 * Synthesize a delete change if necessary for the given limbo target.
 */
export function synthesizeDeleteForLimboTargetChange(
  remoteEvent: RemoteEvent,
  targetId: TargetId,
  key: DocumentKey
): RemoteEvent {
  const targetChange = remoteEvent.targetChanges[targetId];
  assert(!!targetChange, '...');

  let documentUpdates = remoteEvent.documentUpdates;
  let limboDocuments = remoteEvent.limboDocuments;

  if (targetChange.current && !remoteEvent.documentUpdates.get(key)) {
    // When listening to a query the server responds with a snapshot
    // containing documents matching the query and a current marker
    // telling us we're now in sync. It's possible for these to arrive
    // as separate remote events or as a single remote event.
    // For a document query, there will be no documents sent in the
    // response if the document doesn't exist.
    //
    // If the snapshot arrives separately from the current marker,
    // we handle it normally and updateTrackedLimbos will resolve the
    // limbo status of the document, removing it from limboDocumentRefs.
    // This works because clients only initiate limbo resolution when
    // a target is current and because all current targets are
    // always at a consistent snapshot.
    //
    // However, if the document doesn't exist and the current marker
    // arrives, the document is not present in the snapshot and our
    // normal view handling would consider the document to remain in
    // limbo indefinitely because there are no updates to the document.
    // To avoid this, we specially handle this case here:
    // synthesizing a delete.
    //
    // TODO(dimond): Ideally we would have an explicit lookup query
    // instead resulting in an explicit delete message and we could
    // remove this special logic.
    documentUpdates = documentUpdates.insert(
      key,
      new NoDocument(key, this.snapshotVersion)
    );
    limboDocuments = limboDocuments.add(key);
  }

  return {
    snapshotVersion: remoteEvent.snapshotVersion,
    targetChanges: this.targetChanges,
    documentUpdates,
    limboDocuments
  };
}
