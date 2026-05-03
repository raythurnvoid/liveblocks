// TODO: apparently Yjs is full of anys or something, see if we can fix this
/* eslint-disable @typescript-eslint/no-unsafe-call */
/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import { Observable } from "lib0/observable";
import type * as Y from "yjs";
import type {
  files_PresenceStore,
  files_PresenceStore_Event,
} from "../app_lb_bridge.ts";

type MetaClientState = {
  clock: number;
  lastUpdated: number;
};

type YjsData = NonNullable<
  ReturnType<files_PresenceStore["sessionsData"]["get"]>
>["yjs_data"];

class BatchedEventsHandler<Events extends Event> {
  private events: Array<Events> = [];

  constructor(private handler: (events: Array<Events>) => void) {}

  push(event: Events): void {
    if (!this.events.length) {
      queueMicrotask(() => {
        try {
          this.handler(this.events);
        } finally {
          this.events = [];
        }
      });
    }

    this.events.push(event);
  }
}

/**
 * Event-driven Awareness class that listens to PresenceStore events.
 * Batches all changes using queueMicrotask before processing and emitting Yjs events.
 *
 * IMPORTANT: The Yjs awareness protocol uses ydoc.clientId to reference users
 * to their respective documents. We map sessionIds to Yjs clientIds:
 * - Local session: uses doc.clientID directly
 * - Remote sessions: uses a deterministic hash of sessionId
 *
 * This allows one user to have multiple devices/sessions, each with their own cursor.
 * Awareness only tracks OTHER sessions, not the local session.
 */
export class Awareness extends Observable<unknown> {
  private presenceStore: files_PresenceStore;
  public doc: Y.Doc;
  public states: Map<number, unknown> = new Map();
  /**
   * Mapping necessary because when a client disconnects we need to
   * remember its yjs client id since the store won't return data for its
   * session.
   */
  private sessionIdToYjsClientIdMap: Map<string, number> = new Map();
  // Meta is used to keep track and timeout users who disconnect. PresenceStore handles this for us,
  // so we don't need to manage it here. Unfortunately, it's expected to exist by various integrations,
  // so it's an empty map.
  public meta: Map<number, MetaClientState> = new Map();
  // _checkInterval this would hold a timer to remove users, but PresenceStore already handles this
  // unfortunately it's typed by various integrations
  public _checkInterval: number = 0;

  private abortController = new AbortController();

  constructor(doc: Y.Doc, presenceStore: files_PresenceStore) {
    super();
    this.doc = doc;
    this.presenceStore = presenceStore;

    const presence = this.presenceStore.getPresenceData();

    this.presenceStore.setSessionData({
      ...presence?.sessionData,
      yjs_clientId: this.doc.clientID,
      yjs_data: {
        ...(presence?.sessionData?.yjs_data ?? {}),
        user: {
          name: presence?.userData?.displayName ?? null,
          color: presence?.sessionData?.color ?? null,
        },
      },
    });

    for (const [
      sessionId,
      sessionPresence,
    ] of this.presenceStore.sessionsData.entries()) {
      if (sessionPresence.yjs_clientId !== undefined) {
        this.sessionIdToYjsClientIdMap.set(
          sessionId,
          sessionPresence.yjs_clientId
        );
      }
    }

    // Hydrate initial states so a newly opened tab immediately sees existing cursors.
    this.states = this.getStates();
    if (this.presenceStore.sessionsData.size > 0) {
      const added = Array.from(this.presenceStore.sessionsData.values()).map(
        (presence) => presence.yjs_clientId
      );
      this.emit("change", [{ added, updated: [], removed: [] }, "presence"]);
      this.emit("update", [{ added, updated: [], removed: [] }, "presence"]);
    }

    const batchedEventHandler = new BatchedEventsHandler(
      (events: Array<files_PresenceStore_Event["__union"]>) => {
        const added: number[] = [];
        const updated: number[] = [];
        const removed: number[] = [];

        for (const event of events) {
          const presence = this.presenceStore.sessionsData.get(
            event.detail.sessionId
          );

          if (presence?.yjs_clientId !== undefined) {
            this.sessionIdToYjsClientIdMap.set(
              event.detail.sessionId,
              presence.yjs_clientId
            );
          }

          switch (event.type) {
            case "disconnected": {
              const yjsClientId = this.sessionIdToYjsClientIdMap.get(
                event.detail.sessionId
              );

              if (yjsClientId !== undefined) {
                removed.push(yjsClientId);
              }

              this.sessionIdToYjsClientIdMap.delete(event.detail.sessionId);

              break;
            }

            case "connected": {
              if (presence?.yjs_clientId !== undefined) {
                added.push(presence.yjs_clientId);
              } else if (
                event.detail.sessionId === this.presenceStore.localSessionId
              ) {
                this.sessionIdToYjsClientIdMap.set(
                  this.presenceStore.localSessionId,
                  this.doc.clientID
                );

                this.presenceStore.setSessionData({
                  yjs_clientId: this.doc.clientID,
                });
              }
              break;
            }

            case "data_changed": {
              if (presence?.yjs_clientId !== undefined) {
                updated.push(presence.yjs_clientId);
              }
              break;
            }
          }
        }

        if (added.length > 0 || updated.length > 0 || removed.length > 0) {
          this.states = this.getStates();
          this.emit("change", [{ added, updated, removed }, "presence"]);
          this.emit("update", [{ added, updated, removed }, "presence"]);
        }
      }
    );

    const handlePresenceStoreEvent = (
      event: files_PresenceStore_Event["__union"]
    ): void => {
      // Filter out local session
      if (
        event.detail.sessionId === this.presenceStore.localSessionId &&
        event.type === "data_changed"
      ) {
        return;
      }

      batchedEventHandler.push(event);
    };

    // Listen to PresenceStore events
    this.presenceStore.addEventListener("connected", handlePresenceStoreEvent, {
      signal: this.abortController.signal,
    });
    this.presenceStore.addEventListener(
      "disconnected",
      handlePresenceStoreEvent,
      { signal: this.abortController.signal }
    );
    this.presenceStore.addEventListener(
      "data_changed",
      handlePresenceStoreEvent,
      { signal: this.abortController.signal }
    );
  }

  destroy(): void {
    this.emit("destroy", [this]);
    this.abortController.abort();
    super.destroy();
  }

  getLocalState() {
    const presence = this.presenceStore.sessionsData.get(
      this.presenceStore.localSessionId
    );
    if (!presence?.yjs_data) {
      return null;
    }
    return presence.yjs_data;
  }

  setLocalState(state: YjsData | null): void {
    const presence = this.presenceStore.getPresenceData();
    if (!presence) return;

    if (state === null) {
      this.presenceStore.setSessionData({
        ...presence.sessionData,
        yjs_data: null,
      });
      this.states = this.getStates();
      this.emit("update", [
        { added: [], updated: [], removed: [this.doc.clientID] },
        "local",
      ]);
      return;
    }

    // if presence was undefined, it's added, if not, it's updated
    const added =
      presence.sessionData.yjs_data === undefined ? [this.doc.clientID] : [];
    const updated =
      presence.sessionData.yjs_data === undefined ? [] : [this.doc.clientID];
    this.presenceStore.setSessionData({
      ...presence.sessionData,
      yjs_data: {
        ...(presence.sessionData.yjs_data || {
          user: {
            name: presence.userData.displayName ?? null,
            color: presence.sessionData.color ?? null,
          },
        }),
        ...(state || {}),
      },
    });
    this.states = this.getStates();
    this.emit("update", [{ added, updated, removed: [] }, "local"]);
  }

  setLocalStateField(field: string, value: unknown | null): void {
    const presence = this.presenceStore.getPresenceData();
    // If there's not presence it means the client disconnected (the page is not in foreground)
    if (!presence) return;

    this.presenceStore.setSessionData({
      ...presence.sessionData,
      yjs_data: {
        ...(presence?.sessionData?.yjs_data || {
          user: {
            name: presence.userData.displayName ?? null,
            color: presence.sessionData.color ?? null,
          },
        }),
        [field]: JSON.parse(JSON.stringify(value)),
      },
    });
  }

  // Translate PresenceStore data to yjs awareness
  getStates(): Map<number, unknown> {
    const presenceData = Array.from(this.presenceStore.sessionsData.entries());

    const states = new Map<number, unknown>();
    for (const [_sessionId, presence] of presenceData) {
      if (presence.yjs_data != null && presence.yjs_clientId != null) {
        states.set(presence.yjs_clientId, presence.yjs_data);
      }
    }

    return states;
  }
}
