import { Signal as Signal, type YjsSyncStatus } from "@liveblocks/core";
import { Base64 } from "js-base64";
import { Observable } from "lib0/observable";
import { IndexeddbPersistence } from "y-indexeddb";
import * as Y from "yjs";

export default class yDocHandler extends Observable<unknown> {
  private unsubscribers: Array<() => void> = [];

  private _synced = false;
  private doc: Y.Doc;
  private updateRoomDoc: (update: Uint8Array) => void;
  private fetchRoomDoc: (vector: string) => void;
  private useV2Encoding: boolean;

  private debounceTimer: ReturnType<typeof setTimeout> | null = null;
  private static readonly DEBOUNCE_INTERVAL_MS = 200;

  /**
   * Set to true once we've received any server response for the doc.
   */
  private remoteReadyΣ: Signal<boolean>;

  /**
   * Local changes that have been produced by Yjs but not yet flushed to the backend.
   * (Used for sync status only.)
   */
  private hasUnsentLocalChangesΣ: Signal<boolean>;

  /**
   * Number of outgoing updates flushed to the backend that haven't been acknowledged in the tail stream yet.
   * (Used for sync status only.)
   */
  private pendingOutgoingΣ: Signal<number>;

  constructor({
    doc,
    isRoot,
    updateDoc,
    fetchDoc,
    useV2Encoding,
  }: {
    doc: Y.Doc;
    isRoot: boolean;
    updateDoc: (update: Uint8Array, guid?: string) => void;
    fetchDoc: (vector: string, guid?: string) => void;
    useV2Encoding: boolean;
  }) {
    super();
    this.doc = doc;
    this.useV2Encoding = useV2Encoding;
    // this.doc.load(); // this just emits a load event, it doesn't actually load anything
    this.doc.on(useV2Encoding ? "updateV2" : "update", this.updateHandler);
    this.updateRoomDoc = (update: Uint8Array) => {
      updateDoc(update, isRoot ? undefined : this.doc.guid);
    };
    this.fetchRoomDoc = (vector: string) => {
      fetchDoc(vector, isRoot ? undefined : this.doc.guid);
    };

    this.remoteReadyΣ = new Signal<boolean>(false);
    this.hasUnsentLocalChangesΣ = new Signal<boolean>(false);
    this.pendingOutgoingΣ = new Signal<number>(0);

    this.syncDoc();
  }

  public handleServerUpdate = ({
    update,
    stateVector,
    readOnly,
    v2,
  }: {
    update: Uint8Array;
    stateVector: string | null;
    readOnly: boolean;
    v2?: boolean;
  }): void => {
    // apply update from the server, updates from the server can be v1 or v2
    const applyUpdate = v2 ? Y.applyUpdateV2 : Y.applyUpdate;
    // Ack packets may send an empty update; Yjs will throw if we try to decode it.
    if (update.byteLength > 0) {
      applyUpdate(this.doc, update, "backend");
    }
    // if this update is the result of a fetch, the state vector is included
    if (stateVector) {
      if (!readOnly) {
        // Use server state to calculate a diff and send it
        try {
          // send v1 or v2update according to client option
          const encodeUpdate = this.useV2Encoding
            ? Y.encodeStateAsUpdateV2
            : Y.encodeStateAsUpdate;
          const localUpdate = encodeUpdate(
            this.doc,
            Base64.toUint8Array(stateVector)
          );
          this.updateRoomDoc(localUpdate);
        } catch (e) {
          // something went wrong encoding local state to send to the server
          console.warn(e);
        }
      }
      // now that we've sent our local and received from server, we're in sync
      // calling `syncDoc` again will sync up the documents
      this.synced = true;
    }
    this.remoteReadyΣ.set(true);
  };

  public syncDoc = (): void => {
    this.synced = false;

    // The state vector is sent to the server so it knows what to send back
    // if you don't send it, it returns everything
    const encodedVector = Base64.fromUint8Array(Y.encodeStateVector(this.doc));
    this.fetchRoomDoc(encodedVector);
  };

  // The sync'd property is required by some provider implementations
  get synced(): boolean {
    return this._synced;
  }

  set synced(state: boolean) {
    if (this._synced !== state) {
      this._synced = state;
      this.emit("synced", [state]);
      this.emit("sync", [state]);
    }
  }

  private debounced_markLocalChanged() {
    if (this.debounceTimer) clearTimeout(this.debounceTimer);
    this.debounceTimer = setTimeout(() => {
      // No expensive snapshot hashing; just keep a cheap "local changed" marker.
      this.hasUnsentLocalChangesΣ.set(true);
      this.debounceTimer = null;
    }, yDocHandler.DEBOUNCE_INTERVAL_MS);
  }

  private updateHandler = (
    update: Uint8Array,
    origin: string | IndexeddbPersistence
  ) => {
    this.debounced_markLocalChanged();

    // don't send updates from indexedb, those will get handled by sync
    const isFromLocal = origin instanceof IndexeddbPersistence;
    if (origin !== "backend" && !isFromLocal) {
      this.updateRoomDoc(update);
    }
  };

  /**
   * Called by the Convex stream right after an outgoing update has been flushed.
   * This moves us from "unsent local changes" to "pending ack from server".
   */
  public notifyOutgoingUpdateSent() {
    this.hasUnsentLocalChangesΣ.set(false);
    this.pendingOutgoingΣ.set(this.pendingOutgoingΣ.get() + 1);
  }

  /**
   * Called by the Convex stream when it receives the ack-only tail packet for the local session.
   */
  public notifyOutgoingUpdateAcked() {
    const next = Math.max(0, this.pendingOutgoingΣ.get() - 1);
    this.pendingOutgoingΣ.set(next);
  }

  experimental_getSyncStatus(): YjsSyncStatus {
    if (!this.remoteReadyΣ.get()) {
      return "loading";
    }
    if (this.hasUnsentLocalChangesΣ.get() || this.pendingOutgoingΣ.get() > 0) {
      return "synchronizing";
    }
    return "synchronized";
  }

  destroy(): void {
    if (this.debounceTimer) clearTimeout(this.debounceTimer);
    this.doc.off("update", this.updateHandler);
    this.unsubscribers.forEach((unsub) => unsub());
    this._observers = new Map();
    this.doc.destroy();
  }
}
