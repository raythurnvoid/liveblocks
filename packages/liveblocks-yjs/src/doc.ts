import { Signal as Signal, type YjsSyncStatus } from "@liveblocks/core";
import { Base64 } from "js-base64";
import { Observable } from "lib0/observable";
import * as Y from "yjs";
import { pages_u8_equals } from "../app_lb_bridge.ts";

export default class yDocHandler extends Observable<unknown> {
  private unsubscribers: Array<() => void> = [];

  private _synced = false;
  private doc: Y.Doc;
  private updateRoomDoc: (update: Uint8Array) => void;
  private fetchRoomDoc: (vector: string) => void;

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
  }: {
    doc: Y.Doc;
    isRoot: boolean;
    updateDoc: (update: Uint8Array, guid?: string) => void;
    fetchDoc: (vector: string, guid?: string) => void;
  }) {
    super();
    this.doc = doc;
    // this.doc.load(); // this just emits a load event, it doesn't actually load anything
    this.doc.on("update", this.updateHandler);
    this.updateRoomDoc = (update: Uint8Array) => {
      updateDoc(update, isRoot ? undefined : this.doc.guid);
    };
    this.fetchRoomDoc = (vector: string) => {
      fetchDoc(vector, isRoot ? undefined : this.doc.guid);
    };

    this.remoteReadyΣ = new Signal<boolean>(false);
    this.hasUnsentLocalChangesΣ = new Signal<boolean>(false);
    this.pendingOutgoingΣ = new Signal<number>(0);
  }

  private computeDiffUpdateFromCurrentState(
    currentStateUpdate: Uint8Array
  ): Uint8Array {
    const yjsDoc = new Y.Doc();
    if (currentStateUpdate.byteLength > 0) {
      Y.applyUpdate(yjsDoc, currentStateUpdate, "backend");
    }

    const serverStateVector = Y.encodeStateVector(yjsDoc);
    return Y.encodeStateAsUpdate(this.doc, serverStateVector);
  }

  /**
   * Must not be called for ack updates (updates added to the stream by the local session)
   * because they are already applied to the document when the user edits the document.
   */
  public handleServerUpdate = (args: { update: Uint8Array }): void => {
    Y.applyUpdate(this.doc, args.update, "backend");

    // TODO: should this be here or just in handleDocSync?
    this.remoteReadyΣ.set(true);
  };

  public handleDocSync = (args: {
    currentStateUpdate: Uint8Array;
    canWrite: boolean;
  }) => {
    // The sync is expected to return the full current state
    // therefore pending updates are not expected
    this.pendingOutgoingΣ.set(0);

    Y.applyUpdate(this.doc, args.currentStateUpdate, "backend");

    if (args.canWrite) {
      try {
        // Only send a diff if the server and local state vectors differ.
        //
        // Yjs delete-set bookkeeping can cause `encodeStateAsUpdate()` to return a
        // small non-empty update even when visible content matches; state vectors do
        // not include delete sets, so equality here is a reliable "content match" signal.
        const serverDoc = new Y.Doc();
        Y.applyUpdate(serverDoc, args.currentStateUpdate, "backend");
        const serverVector = Y.encodeStateVector(serverDoc);
        const localVector = Y.encodeStateVector(this.doc);

        // Even if remote and local content matches, the diff update might still contain
        // metadata informations like GC or deletions and therefore checking its size cannot
        // be reliably used to determine if the the local doc is different from remote
        // to then trigger the update that will push the update blob to the server
        if (!pages_u8_equals(serverVector, localVector)) {
          const diffUpdate = this.computeDiffUpdateFromCurrentState(
            args.currentStateUpdate
          );
          this.updateRoomDoc(diffUpdate);
        }
      } catch (e) {
        // something went wrong encoding local state to send to the server
        console.error("Error handling doc sync", e);
      }
    }

    // now that we've sent our local and received from server, we're in sync
    // calling `syncDoc` again will sync up the documents
    this.synced = true;
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

  private updateHandler = (update: Uint8Array, origin: unknown) => {
    if (origin !== "backend") {
      this.debounced_markLocalChanged();
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
