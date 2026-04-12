import {
  DerivedSignal,
  type IYjsProvider,
  type YjsSyncStatus,
} from "@liveblocks/core";
import { Observable } from "lib0/observable";
import { Doc } from "yjs";
import { PermanentUserData, mergeUpdates } from "yjs";

import { Awareness } from "./awareness";
import yDocHandler from "./doc";
import {
  app_convex,
  app_convex_api,
  pages_u8_to_array_buffer,
  type app_convex_FunctionReturnType,
  type app_convex_Id,
  type app_convex_Watch,
  type pages_PresenceStore,
} from "../app_lb_bridge.ts";

type StreamStateKey = "__root" | (string & {});

function pages_convex_stream_key(guid: string | undefined): StreamStateKey {
  return guid ?? "__root";
}

type PagesConvexIncrementalUpdates = NonNullable<
  app_convex_FunctionReturnType<
    typeof app_convex_api.ai_docs_temp.yjs_get_incremental_updates
  >
>;

type PagesConvexYjsStream_Args = {
  pageId: app_convex_Id<"pages">;
  membershipId: app_convex_Id<"workspaces_projects_users">;
  presenceStore: pages_PresenceStore;
  onGoodUpdatePacket: (
    packet: PagesConvexIncrementalUpdates["updates"][number]
  ) => void;
  onAckUpdatePacket: (
    packet: PagesConvexIncrementalUpdates["updates"][number]
  ) => void;
  onOutgoingUpdateSent: () => void;
  onSync: (currentStateUpdate: Uint8Array) => void;
};

class PagesConvexYjsStream {
  args: PagesConvexYjsStream_Args;
  state: {
    syncing: boolean;
    ready: boolean;
    appliedSeq: number;
    incrementalUpdates: PagesConvexIncrementalUpdates | null;
  };

  private onRemoteUpdatePacket: PagesConvexYjsStream_Args["onGoodUpdatePacket"];
  private onAckUpdatePacket: PagesConvexYjsStream_Args["onAckUpdatePacket"];
  private onOutgoingUpdateSent: PagesConvexYjsStream_Args["onOutgoingUpdateSent"];
  private onSync: PagesConvexYjsStream_Args["onSync"];

  private watcher: app_convex_Watch<
    app_convex_FunctionReturnType<
      typeof app_convex_api.ai_docs_temp.yjs_get_incremental_updates
    >
  >;
  private unsubscribe: () => void;
  private disposed = false;

  private pendingOutgoingUpdates: Uint8Array[] = [];
  private outgoingUpdatesDebounceTimer: ReturnType<typeof setTimeout> | null =
    null;

  private incrementalUpdatesFirstValueReceived = Object.assign(
    Promise.withResolvers(),
    { initialized: false }
  );

  constructor(args: PagesConvexYjsStream_Args) {
    this.args = args;
    this.state = {
      syncing: false,
      ready: false,
      appliedSeq: 0,
      incrementalUpdates: null,
    };

    this.onRemoteUpdatePacket = args.onGoodUpdatePacket;
    this.onAckUpdatePacket = args.onAckUpdatePacket;
    this.onOutgoingUpdateSent = args.onOutgoingUpdateSent;
    this.onSync = args.onSync;

    this.watcher = app_convex.watchQuery(
      app_convex_api.ai_docs_temp.yjs_get_incremental_updates,
      {
        membershipId: args.membershipId,
        pageId: args.pageId,
      }
    );

    this.unsubscribe = this.watcher.onUpdate(() => {
      if (this.disposed) return;
      const updateData = this.watcher.localQueryResult();
      if (!this.incrementalUpdatesFirstValueReceived.initialized) {
        this.incrementalUpdatesFirstValueReceived.resolve(updateData);
        this.incrementalUpdatesFirstValueReceived.initialized = true;
      }
      if (!updateData) return;
      this.state.incrementalUpdates = updateData;
      this.handleIncrementalUpdates(updateData);
    });
  }

  private handleIncrementalUpdates(
    incrementalUpdates: PagesConvexIncrementalUpdates
  ) {
    if (this.disposed) return;
    if (!this.state.ready || this.state.syncing) return;

    let appliedSeq = this.state.appliedSeq;

    // Loop through updates in ascending order. The BE returns them in descending order.
    for (let i = incrementalUpdates.updates.length - 1; i >= 0; i--) {
      const updatePacket = incrementalUpdates.updates[i];

      if (!updatePacket || updatePacket.sequence <= appliedSeq) continue;

      // Only USER_EDIT with matching sessionId are treated as local (ack-only).
      // All other origins (USER_SNAPSHOT_RESTORE, USER_AI_EDIT, or USER_EDIT with different sessionId)
      // are treated as remote changes and applied to the document.
      const isLocalEdit =
        updatePacket.origin.type === "USER_EDIT" &&
        updatePacket.origin.session_id ===
          this.args.presenceStore.localSessionId;

      if (isLocalEdit) {
        // Local packets are "ack-only": applying them again would be redundant,
        // but we still need the ack to update sync status and keep stream
        // ordering consistent.
        this.onAckUpdatePacket(updatePacket);
      } else {
        // Remote changes: apply to the document
        this.onRemoteUpdatePacket(updatePacket);
      }

      appliedSeq = updatePacket.sequence;
    }

    this.state.appliedSeq = appliedSeq;
  }

  private flushUpdates() {
    if (this.disposed) return;
    if (this.pendingOutgoingUpdates.length === 0) return;

    const merged = mergeUpdates(this.pendingOutgoingUpdates);
    this.pendingOutgoingUpdates = [];

    if (merged.byteLength === 0) return;

    this.onOutgoingUpdateSent();

    app_convex
      .mutation(app_convex_api.ai_docs_temp.yjs_push_update, {
        membershipId: this.args.membershipId,
        pageId: this.args.pageId,
        update: pages_u8_to_array_buffer(merged),
        sessionId: this.args.presenceStore.localSessionId,
      })
      .then((result) => {
        if (result._nay) {
          console.warn("[ConvexYjsSync] submit_update failed", result._nay);
        }
      })
      .catch((err) => {
        console.warn("[ConvexYjsSync] submit_update errored", err);
      });
  }

  enqueueUpdate(update: Uint8Array) {
    if (this.disposed) return;
    this.pendingOutgoingUpdates.push(update);

    if (!this.outgoingUpdatesDebounceTimer) {
      this.outgoingUpdatesDebounceTimer = setTimeout(() => {
        this.outgoingUpdatesDebounceTimer = null;
        this.flushUpdates();
      }, 500);
    }
  }

  async sync() {
    if (this.disposed) return;
    if (this.state.syncing) return;

    this.state.syncing = true;

    let iteration = 0;

    try {
      do {
        iteration++;
        if (iteration > 10) {
          console.error(
            "PagesConvexYjsStream.sync: yjs sync failed after 10 retries",
            {
              membershipId: this.args.membershipId,
              pageId: this.args.pageId,
            }
          );
          break;
        }

        let result: app_convex_FunctionReturnType<
          typeof app_convex_api.ai_docs_temp.yjs_get_doc_last_snapshot
        > | null = null;

        try {
          [result] = await Promise.all([
            app_convex.query(
              app_convex_api.ai_docs_temp.yjs_get_doc_last_snapshot,
              {
                membershipId: this.args.membershipId,
                pageId: this.args.pageId,
              }
            ),
            this.incrementalUpdatesFirstValueReceived.promise,
          ]);
        } catch (err) {
          console.warn(
            "PagesConvexYjsStream.sync: snapshot query failed, retrying",
            err
          );
          // Backoff a bit before retrying to avoid hot-looping on transient errors.
          await new Promise((resolve) => setTimeout(resolve, 500));
          continue;
        }

        if (!result) {
          console.error("PagesConvexYjsStream.sync: fetch_doc returned null");
          break;
        }

        if (this.disposed) break;

        const snapshotUpdate = new Uint8Array(result.snapshot_update);

        let lastSequence = result.sequence;
        let updatesAfterSnapshot;

        if (this.state.incrementalUpdates?.updates.length) {
          updatesAfterSnapshot = [] as Uint8Array[];

          // Loop through updates in ascending order. The BE returns them in descending order.
          for (
            let i = this.state.incrementalUpdates.updates.length - 1;
            i >= 0;
            i--
          ) {
            const updateData = this.state.incrementalUpdates?.updates[i];
            if (!updateData) continue;

            if (updateData.sequence <= lastSequence) {
              continue;
            }

            // Only USER_EDIT with matching sessionId are treated as local (ack-only).
            // All other origins (USER_SNAPSHOT_RESTORE, USER_AI_EDIT, or USER_EDIT with different sessionId)
            // are treated as remote changes and applied to the document.
            const isLocalEdit =
              updateData.origin.type === "USER_EDIT" &&
              updateData.origin.session_id ===
                this.args.presenceStore.localSessionId;

            lastSequence = updateData.sequence;

            // Only skip local edits AFTER we've hydrated at least once.
            // On the first sync of a fresh provider, the doc has NOT applied them yet.
            if (this.state.ready && isLocalEdit) continue;

            updatesAfterSnapshot.push(new Uint8Array(updateData.update));
          }
        }

        const currentStateUpdate = updatesAfterSnapshot
          ? mergeUpdates([snapshotUpdate, ...updatesAfterSnapshot])
          : snapshotUpdate;

        // `onSync()` applies `currentStateUpdate` to the Yjs doc synchronously.
        // There is no possible interleaving *inside* that apply step.
        // The only interleaving point is earlier in this `sync()` at `await`s: the query watcher
        // may update `this.state.incrementalUpdates` while we are awaiting the snapshot/query.

        this.onSync(currentStateUpdate);
        this.state.ready = true;
        this.state.appliedSeq = lastSequence;

        break;
      } while (true);
    } finally {
      this.state.syncing = false;
    }
  }

  dispose() {
    if (this.disposed) return;
    this.disposed = true;
    if (this.outgoingUpdatesDebounceTimer) {
      clearTimeout(this.outgoingUpdatesDebounceTimer);
      this.outgoingUpdatesDebounceTimer = null;
    }
    this.pendingOutgoingUpdates = [];
    this.unsubscribe();
  }
}

export type LiveblocksYjsProvider_Args = {
  pageId: app_convex_Id<"pages">;
  enablePermanentUserData?: boolean;
  presenceStore: pages_PresenceStore;
  membershipId: app_convex_Id<"workspaces_projects_users">;
};

export class LiveblocksYjsProvider
  extends Observable<unknown>
  implements IYjsProvider
{
  args: LiveblocksYjsProvider_Args;

  private readonly rootDoc: Doc;
  private isPaused = false;

  private readonly unsubscribers: Array<() => void> = [];

  public readonly awareness: Awareness;

  public readonly rootDocHandler: yDocHandler;

  private readonly syncStatusΣ: DerivedSignal<YjsSyncStatus>;

  public readonly permanentUserData?: PermanentUserData;

  private readonly convexStreams = new Map<
    StreamStateKey,
    PagesConvexYjsStream
  >();

  constructor(args: LiveblocksYjsProvider_Args) {
    super();
    this.args = args;
    this.rootDoc = new Doc();

    this.rootDocHandler = new yDocHandler({
      doc: this.rootDoc,
      isRoot: true,
      updateDoc: this.updateDoc,
      fetchDoc: this.fetchDoc,
    });

    if (this.args.enablePermanentUserData) {
      this.permanentUserData = new PermanentUserData(this.rootDoc);
    }

    // Construct Convex-backed awareness
    if (!this.args.presenceStore) {
      throw new Error(
        "convexPresenceConfig is required for LiveblocksYjsProvider"
      );
    }

    this.awareness = new Awareness(this.rootDoc, this.args.presenceStore);

    // different consumers listen to sync and synced
    this.rootDocHandler.on("synced", () => {
      const state = this.rootDocHandler.synced;

      this.emit("synced", [state]);
      this.emit("sync", [state]);
    });
    this.syncDoc();

    if (this.args.presenceStore) {
      this.ensureConvexStream({
        yDocHandler: this.rootDocHandler,
        presenceStore: this.args.presenceStore,
      });
    }

    this.syncStatusΣ = DerivedSignal.from(() => {
      return this.rootDocHandler.experimental_getSyncStatus();
    });

    this.emit("status", [this.getStatus()]);

    this.unsubscribers.push(
      this.syncStatusΣ.subscribe(() => {
        this.emit("status", [this.getStatus()]);
      })
    );
  }

  private updateDoc = (update: Uint8Array) => {
    const canWrite = true;
    if (!canWrite || this.isPaused) return;
    if (update.byteLength === 0) return;

    const stream = this.ensureConvexStream({
      yDocHandler: this.rootDocHandler,
      presenceStore: this.args.presenceStore,
    });
    stream.enqueueUpdate(update);
  };

  private fetchDoc = () => {
    const stream = this.ensureConvexStream({
      yDocHandler: this.rootDocHandler,
      presenceStore: this.args.presenceStore,
    });
    stream.sync();
  };

  private ensureConvexStream(args: {
    yDocHandler: yDocHandler;
    presenceStore: pages_PresenceStore;
    guid?: string;
  }) {
    const key = pages_convex_stream_key(args.guid);
    let stream = this.convexStreams.get(key);
    if (stream) {
      return stream;
    }

    // TODO: add permissions to room user state
    const canWrite = true;

    stream = new PagesConvexYjsStream({
      pageId: this.args.pageId,
      membershipId: this.args.membershipId,
      presenceStore: args.presenceStore,
      onGoodUpdatePacket: (updateItem) => {
        args.yDocHandler.handleServerUpdate({
          update: new Uint8Array(updateItem.update),
        });
      },
      onAckUpdatePacket: () => {
        // Do not re-apply local updates (already applied optimistically).
        // Still use the ack so sync status can converge to "synchronized"
        // and keep seq ordering consistent.
        args.yDocHandler.notifyOutgoingUpdateAcked();
      },
      onOutgoingUpdateSent: () => {
        args.yDocHandler.notifyOutgoingUpdateSent();
      },
      onSync: (currentStateUpdate) => {
        args.yDocHandler.handleDocSync({
          currentStateUpdate,
          canWrite: canWrite,
        });
      },
    });

    this.convexStreams.set(key, stream);

    this.unsubscribers.push(() => {
      stream.dispose();
    });

    return stream;
  }

  // attempt to load a subdoc of a given guid
  public loadSubdoc = (guid: string): boolean => {
    for (const subdoc of this.rootDoc.subdocs) {
      if (subdoc.guid === guid) {
        subdoc.load();
        return true;
      }
    }
    // should we throw instead?
    return false;
  };

  private syncDoc = () => {
    this.rootDocHandler.syncDoc();
  };

  // The sync'd property is required by some provider implementations
  get synced(): boolean {
    return this.rootDocHandler.synced;
  }

  async pause(): Promise<void> {
    this.isPaused = true;
  }

  unpause(): void {
    this.isPaused = false;
    this.rootDocHandler.syncDoc();
  }

  public getStatus(): YjsSyncStatus {
    return this.syncStatusΣ.get();
  }

  destroy(): void {
    this.unsubscribers.forEach((unsub) => unsub());
    this.awareness.destroy();
    this.rootDocHandler.destroy();
    this._observers = new Map();
    super.destroy();
  }

  getYDoc(): Doc {
    return this.rootDoc;
  }

  // Some provider implementations expect to be able to call connect/disconnect, implement as noop
  disconnect(): void {
    // This is a noop for liveblocks as connections are managed by the room
  }

  connect(): void {
    // This is a noop for liveblocks as connections are managed by the room
  }
}
