import {
  DerivedSignal,
  type IYjsProvider,
  type YjsSyncStatus,
} from "@liveblocks/core";
import { MutableSignal } from "@liveblocks/core";
import { Base64 } from "js-base64";
import { Observable } from "lib0/observable";
import { IndexeddbPersistence } from "y-indexeddb";
import { Doc } from "yjs";
import { PermanentUserData, mergeUpdates } from "yjs";

import { Awareness } from "./awareness";
import yDocHandler from "./doc";
import {
  app_convex,
  app_convex_api,
  pages_u8_to_array_buffer,
  type app_convex_FunctionReturnType,
  type app_convex_Watch,
  type pages_PresenceStore,
  type pages_YjsTailUpdates,
} from "../app_lb_bridge.ts";

export type ProviderOptions = {
  enablePermanentUserData?: boolean;
  presenceStore?: pages_PresenceStore;
};

type StreamStateKey = "__root" | (string & {});

function pages_convex_stream_key(guid: string | undefined): StreamStateKey {
  return guid ?? "__root";
}

type PagesConvexTailUpdates = app_convex_FunctionReturnType<
  typeof app_convex_api.yjs_sync.tail_updates
>;

type PagesConvexYjsStream_Args = {
  roomId: string;
  guid?: string;
  presenceStore: pages_PresenceStore;
  onMissingUpdatePacket: () => void;
  onGoodUpdatePacket: (
    packet: PagesConvexTailUpdates["updates"][number]
  ) => void;
  onAckUpdatePacket: (
    packet: PagesConvexTailUpdates["updates"][number]
  ) => void;
  onSync: (
    result: app_convex_FunctionReturnType<
      typeof app_convex_api.yjs_sync.fetch_doc
    >
  ) => void;
};

class PagesConvexYjsStream {
  roomId: PagesConvexYjsStream_Args["roomId"];
  guid: NonNullable<PagesConvexYjsStream_Args["guid"]> | null;
  state: {
    appliedSeq: number;
    ready: boolean;
    lastTail: pages_YjsTailUpdates | null;
  };
  presenceStore: pages_PresenceStore;

  private onMissingUpdatePacket: PagesConvexYjsStream_Args["onMissingUpdatePacket"];
  private onRemoteUpdatePacket: PagesConvexYjsStream_Args["onGoodUpdatePacket"];
  private onAckUpdatePacket: PagesConvexYjsStream_Args["onAckUpdatePacket"];
  private onSync: PagesConvexYjsStream_Args["onSync"];

  private watcher: app_convex_Watch<PagesConvexTailUpdates>;
  private unsubscribe: () => void;
  private disposed = false;

  private pendingOutgoingUpdates: Uint8Array[] = [];
  private outgoingUpdatesDebounceTimer: ReturnType<typeof setTimeout> | null =
    null;

  constructor(args: PagesConvexYjsStream_Args) {
    this.roomId = args.roomId;
    this.guid = args.guid ?? null;
    this.state = {
      appliedSeq: 0,
      ready: false,
      lastTail: null,
    };
    this.presenceStore = args.presenceStore;

    this.onMissingUpdatePacket = args.onMissingUpdatePacket;
    this.onRemoteUpdatePacket = args.onGoodUpdatePacket;
    this.onAckUpdatePacket = args.onAckUpdatePacket;
    this.onSync = args.onSync;

    this.watcher = app_convex.watchQuery(app_convex_api.yjs_sync.tail_updates, {
      roomId: args.roomId,
      guid: args.guid ?? null,
      limit: 256,
    });

    this.unsubscribe = this.watcher.onUpdate(() => {
      if (this.disposed) return;
      const updateData = this.watcher.localQueryResult();
      if (!updateData) return;
      this.state.lastTail = updateData;
      this.handleTailUpdates(updateData);
    });
  }

  private handleTailUpdates(tailUpdates: PagesConvexTailUpdates) {
    if (this.disposed) return;
    if (!this.state.ready) return;

    let appliedSeq = this.state.appliedSeq;

    for (const updatePacket of tailUpdates.updates) {
      if (updatePacket.seq <= appliedSeq) continue;

      if (appliedSeq !== 0 && updatePacket.seq !== appliedSeq + 1) {
        this.onMissingUpdatePacket();
        return;
      }

      // Local packets are "ack-only": applying them again would be redundant,
      // but we still need their snapshotHash to update sync status and their
      // seq to keep stream ordering consistent.
      if (updatePacket.sessionId === this.presenceStore.localSessionId) {
        this.onAckUpdatePacket(updatePacket);
      } else {
        this.onRemoteUpdatePacket(updatePacket);
      }

      appliedSeq = updatePacket.seq;
    }

    this.state.appliedSeq = appliedSeq;
  }

  private flushUpdates() {
    if (this.disposed) return;
    if (this.pendingOutgoingUpdates.length === 0) return;

    const merged = mergeUpdates(this.pendingOutgoingUpdates);
    this.pendingOutgoingUpdates = [];

    if (merged.byteLength === 0) return;

    app_convex
      .mutation(app_convex_api.yjs_sync.submit_update, {
        roomId: this.roomId,
        guid: this.guid,
        update: pages_u8_to_array_buffer(merged),
        sessionId: this.presenceStore.localSessionId,
      })
      .catch((err) => {
        console.warn("[ConvexYjsSync] submit_update failed", err);
      });
  }

  enqueueUpdate(update: Uint8Array) {
    if (this.disposed) return;
    this.pendingOutgoingUpdates.push(update);

    if (!this.outgoingUpdatesDebounceTimer) {
      this.outgoingUpdatesDebounceTimer = setTimeout(() => {
        this.outgoingUpdatesDebounceTimer = null;
        this.flushUpdates();
      }, 100);
    }
  }

  async sync(currentVector: string) {
    if (this.disposed) return;
    const vectorBytes = Base64.toUint8Array(currentVector);

    const result = await app_convex.query(app_convex_api.yjs_sync.fetch_doc, {
      roomId: this.roomId,
      guid: this.guid,
      clientStateVector: pages_u8_to_array_buffer(vectorBytes),
    });

    if (this.disposed) return;
    this.onSync(result);
    this.state.ready = true;
    this.state.appliedSeq = result.latestSeq;
    if (this.state.lastTail) {
      this.handleTailUpdates(this.state.lastTail);
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

export class LiveblocksYjsProvider
  extends Observable<unknown>
  implements IYjsProvider
{
  private readonly roomId: string;
  private readonly rootDoc: Doc;
  private readonly options: ProviderOptions;
  private indexeddbProvider: IndexeddbPersistence | null = null;
  private isPaused = false;

  private readonly unsubscribers: Array<() => void> = [];

  public readonly awareness: Awareness;

  public readonly rootDocHandler: yDocHandler;
  private readonly subdocHandlersΣ = new MutableSignal<
    Map<string, yDocHandler>
  >(new Map());
  private readonly syncStatusΣ: DerivedSignal<YjsSyncStatus>;

  public readonly permanentUserData?: PermanentUserData;

  private readonly convexStreams = new Map<
    StreamStateKey,
    PagesConvexYjsStream
  >();

  constructor(roomId: string, options: ProviderOptions = {}) {
    super();
    this.rootDoc = new Doc();
    this.roomId = roomId;
    this.options = options;

    this.rootDocHandler = new yDocHandler({
      doc: this.rootDoc,
      isRoot: true,
      updateDoc: this.updateDoc,
      fetchDoc: this.fetchDoc,
      useV2Encoding: false,
    });

    if (this.options.enablePermanentUserData) {
      this.permanentUserData = new PermanentUserData(this.rootDoc);
    }

    // Construct Convex-backed awareness
    if (!this.options.presenceStore) {
      throw new Error(
        "convexPresenceConfig is required for LiveblocksYjsProvider"
      );
    }

    this.awareness = new Awareness(this.rootDoc, this.options.presenceStore);

    // different consumers listen to sync and synced
    this.rootDocHandler.on("synced", () => {
      const state = this.rootDocHandler.synced;
      for (const [_, handler] of this.subdocHandlersΣ.get()) {
        handler.syncDoc();
      }
      this.emit("synced", [state]);
      this.emit("sync", [state]);
    });
    this.syncDoc();

    if (this.options.presenceStore) {
      this.ensureConvexStream({
        yDocHandler: this.rootDocHandler,
        presenceStore: this.options.presenceStore,
      });
    }

    this.syncStatusΣ = DerivedSignal.from(() => {
      // If the root document is loading or synchronizing, we infer that the overall status is also loading or synchronizing.
      const rootDocumentStatus =
        this.rootDocHandler.experimental_getSyncStatus();
      if (
        rootDocumentStatus === "loading" ||
        rootDocumentStatus === "synchronizing"
      ) {
        return rootDocumentStatus;
      }

      // If the root document is synchronized, we check if all subdocs are synchronized. If at least one subdoc is not synchronized, we are still synchronizing.
      const subdocumentStatuses = Array.from(
        this.subdocHandlersΣ.get().values()
      ).map((handler) => handler.experimental_getSyncStatus());
      if (subdocumentStatuses.some((state) => state !== "synchronized")) {
        return "synchronizing";
      }
      return "synchronized";
    });

    this.emit("status", [this.getStatus()]);

    this.unsubscribers.push(
      this.syncStatusΣ.subscribe(() => {
        this.emit("status", [this.getStatus()]);
      })
    );
  }

  private updateDoc = (update: Uint8Array, guid?: string) => {
    const canWrite = true;
    if (!canWrite || this.isPaused) return;
    if (update.byteLength === 0) return;

    const yDocHandler =
      guid === undefined
        ? this.rootDocHandler
        : this.subdocHandlersΣ.get().get(guid);

    if (yDocHandler && this.options.presenceStore) {
      const stream = this.ensureConvexStream({
        yDocHandler,
        presenceStore: this.options.presenceStore,
        guid,
      });
      stream.enqueueUpdate(update);
    }
  };

  private fetchDoc = (vector: string, guid?: string) => {
    const yDocHandler =
      guid === undefined
        ? this.rootDocHandler
        : this.subdocHandlersΣ.get().get(guid);

    if (yDocHandler && this.options.presenceStore) {
      const streamState = this.ensureConvexStream({
        yDocHandler,
        presenceStore: this.options.presenceStore,
        guid,
      });
      streamState.sync(vector);
    }
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
      roomId: this.roomId,
      guid: args.guid,
      presenceStore: args.presenceStore,
      onMissingUpdatePacket: () => {
        args.yDocHandler.syncDoc();
      },
      onGoodUpdatePacket: (updateItem) => {
        args.yDocHandler.handleServerUpdate({
          update: new Uint8Array(updateItem.update),
          stateVector: null,
          readOnly: !canWrite,
          v2: false,
          remoteSnapshotHash: updateItem.snapshotHash,
        });
      },
      onAckUpdatePacket: (updateItem) => {
        // Do not re-apply local updates (already applied optimistically).
        // Still update remote snapshot hash so sync status can converge to
        // "synchronized" and keep seq ordering consistent.
        args.yDocHandler.handleServerUpdate({
          update: new Uint8Array(0),
          stateVector: null,
          readOnly: !canWrite,
          v2: false,
          remoteSnapshotHash: updateItem.snapshotHash,
        });
      },
      onSync: (result) => {
        args.yDocHandler.handleServerUpdate({
          update: new Uint8Array(result.update),
          stateVector: Base64.fromUint8Array(
            new Uint8Array(result.serverStateVector)
          ),
          readOnly: !canWrite,
          v2: false,
          remoteSnapshotHash: result.remoteSnapshotHash,
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
    for (const [_, handler] of this.subdocHandlersΣ.get()) {
      handler.syncDoc();
    }
  };

  // The sync'd property is required by some provider implementations
  get synced(): boolean {
    return this.rootDocHandler.synced;
  }

  async pause(): Promise<void> {
    await this.indexeddbProvider?.destroy();
    this.indexeddbProvider = null;
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
    for (const [_, handler] of this.subdocHandlersΣ.get()) {
      handler.destroy();
    }
    this.subdocHandlersΣ.get().clear();
    super.destroy();
  }

  async clearOfflineData(): Promise<void> {
    if (!this.indexeddbProvider) return;
    return this.indexeddbProvider.clearData();
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

  get subdocHandlers(): Map<string, yDocHandler> {
    return this.subdocHandlersΣ.get();
  }

  set subdocHandlers(value: Map<string, yDocHandler>) {
    this.subdocHandlersΣ.mutate((map) => {
      map.clear();
      for (const [key, handler] of value) {
        map.set(key, handler);
      }
    });
  }
}
