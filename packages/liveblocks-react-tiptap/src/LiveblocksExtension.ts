// NOTE: Mentions integration (text-mentions endpoints + room private hooks) was
// used when this package was wired to the Liveblocks Room system. We migrated
// away from that integration, but we keep the code commented out for reference.
//
// import {
//   useCreateTextMention,
//   useDeleteTextMention,
// } from "@liveblocks/react/_private";
import type { AnyExtension, Editor } from "@tiptap/core";
import { Extension, Mark } from "@tiptap/core";
import Collaboration from "@tiptap/extension-collaboration";
import CollaborationCaret, {
  type CollaborationCaretOptions,
} from "@tiptap/extension-collaboration-caret";
import { useEffect, useRef } from "react";

import { AiExtension } from "./ai/AiExtension";
import {
  areSetsEqual,
  FILTERED_THREADS_PLUGIN_KEY,
} from "./comments/CommentsExtension";
// import { MentionExtension } from "./mentions/MentionExtension";
import type {
  LiveblocksExtensionOptions,
  LiveblocksExtensionStorage,
  ResolveContextualPromptArgs,
  ResolveContextualPromptResponse,
} from "./types";

type WithRequired<T, K extends keyof T> = T & { [P in K]-?: T[P] };

const DEFAULT_OPTIONS: WithRequired<LiveblocksExtensionOptions, "field"> = {
  field: "default",
  // TODO: to be refactored for Convex BE
  // mentions: true,
  offlineSupport_experimental: false,
  enablePermanentUserData: false,
};

const LiveblocksCollab = Collaboration.extend({
  // Override the onCreate method to warn users about potential misconfigurations
  onCreate() {
    if (
      !this.editor.extensionManager.extensions.find((e) => e.name === "doc")
    ) {
      console.warn(
        "[Liveblocks] The tiptap document extension is required for Liveblocks collaboration. Please add it or use Tiptap StarterKit extension."
      );
    }
    if (
      !this.editor.extensionManager.extensions.find(
        (e) => e.name === "paragraph"
      )
    ) {
      console.warn(
        "[Liveblocks] The tiptap paragraph extension is required for Liveblocks collaboration. Please add it or use Tiptap StarterKit extension."
      );
    }

    if (
      !this.editor.extensionManager.extensions.find((e) => e.name === "text")
    ) {
      console.warn(
        "[Liveblocks] The tiptap text extension is required for Liveblocks collaboration. Please add it or use Tiptap StarterKit extension."
      );
    }
    if (
      this.editor.extensionManager.extensions.find((e) => e.name === "undoRedo")
    ) {
      console.warn(
        "[Liveblocks] The undoRedo extension is enabled, Liveblocks extension provides its own. Please remove or disable the undoRedo extension to prevent conflicts."
      );
    }
  },
});

// Unnecessary for convex BE
// /**
//  * Returns whether the editor has loaded the initial text contents from the
//  * server and is ready to be used.
//  *
//  */
// export function useIsEditorReady(): boolean {
//   const yjsProvider = useYjsProvider();

//   const getSnapshot = useCallback(() => {
//     const status = yjsProvider?.getStatus();
//     return status === "synchronizing" || status === "synchronized";
//   }, [yjsProvider]);

//   const subscribe = useCallback(
//     (callback: () => void) => {
//       if (yjsProvider === undefined) return () => {};
//       yjsProvider.on("status", callback);
//       return () => {
//         yjsProvider.off("status", callback);
//       };
//     },
//     [yjsProvider]
//   );

//   return useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
// }

const YChangeMark = Mark.create({
  name: "ychange",
  inclusive: false,
  parseHTML() {
    return [{ tag: "ychange" }];
  },
  addAttributes() {
    return {
      user: {
        default: null,
        parseHTML: (element) => element.getAttribute("ychange_user") ?? null,
        renderHTML: (attributes: { user: string | null }) => {
          if (!attributes.user) {
            return {};
          }
          return { "data-ychange-user": attributes.user };
        },
      },
      type: {
        default: null,
        parseHTML: (element) => element.getAttribute("ychange_type") ?? null,
        renderHTML: (attributes: { type: string | null }) => {
          if (!attributes.type) {
            return {};
          }
          return {
            "data-ychange-type": attributes.type,
            "data-liveblocks": "",
            class: `lb-root lb-tiptap-change lb-tiptap-change-${attributes.type}`,
          };
        },
      },
      color: {
        default: null,
        parseHTML: (element) => {
          return element.getAttribute("ychange_color") ?? null;
        },
        renderHTML: () => {
          // attributes: { color: { light: string; dark: string } | null }
          return {}; // we don't need this color attribute for now
        },
      },
    };
  },
  renderHTML({ HTMLAttributes }) {
    return ["ychange", HTMLAttributes, 0];
  },
});

export const useLiveblocksExtension = (opts?: LiveblocksExtensionOptions) => {
  const options = {
    ...DEFAULT_OPTIONS,
    ...opts,
  };
  // Not needed for Convex BE
  // const textEditorType = useInitial<TextEditorType>(
  //   options.textEditorType ?? TextEditorType.TipTap
  // );
  const editor = useRef<Editor | null>(null);
  // Not needed for Convex BE
  // const room = useRoom();

  // Not needed for Convex BE
  // TODO: we don't need these things if comments isn't turned on...
  // TODO: we don't have a reference to the editor here, need to figure this out
  // useErrorListener((error) => {
  //   // If thread creation fails, we remove the thread id from the associated nodes and unwrap the nodes if they are no longer associated with any threads
  //   if (
  //     error.context.type === "CREATE_THREAD_ERROR" &&
  //     error.context.roomId === room.id
  //   ) {
  //     handleThreadDelete(error.context.threadId);
  //   }
  // });

  // const isEditorReady = useIsEditorReady();
  // const yjsProvider = useYjsProvider();

  // If the user provided initialContent, wait for ready and then set it
  // useEffect(() => {
  //   if (
  //     !isEditorReady ||
  //     !yjsProvider ||
  //     !options.initialContent ||
  //     !editor.current
  //   )
  //     return;

  //   // As noted in the tiptap documentation, you may not set initial content with collaboration.
  //   // The docs provide the following workaround:
  //   const ydoc = (yjsProvider as LiveblocksYjsProvider).getYDoc();
  //   const hasContentSet = ydoc.getMap("liveblocks_config").get("hasContentSet");
  //   if (!hasContentSet) {
  //     ydoc.getMap("liveblocks_config").set("hasContentSet", true);
  //     editor.current.commands.setContent(options.initialContent);
  //   }
  // }, [isEditorReady, yjsProvider, options.initialContent]);

  // useReportTextEditor(textEditorType, options.field ?? DEFAULT_OPTIONS.field);

  const prevThreadsRef = useRef<Set<string> | undefined>(undefined);

  useEffect(() => {
    // Not needed for Convex BE
    // if (!isEditorReady) return;

    if (!editor.current) return;

    const newThreads = options.threads_experimental
      ? new Set(options.threads_experimental.map((t) => t.id))
      : undefined;

    const hasFilteredThreadsChanged = !areSetsEqual(
      prevThreadsRef.current,
      newThreads
    );

    if (hasFilteredThreadsChanged) {
      prevThreadsRef.current = newThreads;
    }

    if (hasFilteredThreadsChanged) {
      editor.current.view.dispatch(
        editor.current.state.tr.setMeta(FILTERED_THREADS_PLUGIN_KEY, {
          filteredThreads: options.threads_experimental
            ? new Set(options.threads_experimental.map((t) => t.id))
            : undefined,
        })
      );
    }
    // }, [isEditorReady, options.threads_experimental]);
  }, [options.threads_experimental]);

  // TODO: to be refactored for Convex BE
  // const createTextMention = useCreateTextMention();
  // const deleteTextMention = useDeleteTextMention();

  // Tiptap has options default as any, in tiptap2, we could use never, but now we must use any
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return Extension.create<any, LiveblocksExtensionStorage>({
    name: "liveblocksExtension",

    onCreate() {
      editor.current = this.editor;
      if (this.editor.options.content) {
        console.warn(
          "[Liveblocks] Initial content must be set in the useLiveblocksExtension hook option. Remove content from your editor options."
        );
      }

      if (options.initialContent) {
        const provider = options.yjsProvider;
        if (!provider) {
          throw new Error("yjsProvider is required for useLiveblocksExtension");
        }
        const ydoc = provider.getYDoc();
        const field = options.field ?? DEFAULT_OPTIONS.field;

        const tryApplyInitialContent = () => {
          const status = provider.getStatus();
          const isReady =
            status === "synchronizing" || status === "synchronized";
          if (!isReady) return false;

          const config = ydoc.getMap("liveblocks_config");
          const hasContentSet = config.get("hasContentSet");
          if (hasContentSet) return true;

          config.set("hasContentSet", true);

          // Avoid overwriting documents that already have content but
          // the flag was not set.
          const fragment = ydoc.getXmlFragment(field);
          if (fragment.length > 0) {
            return true;
          }

          this.editor.commands.setContent(options.initialContent!);
          return true;
        };

        if (!tryApplyInitialContent()) {
          const onStatus = () => {
            if (tryApplyInitialContent()) {
              provider.off("status", onStatus);
            }
          };
          provider.on("status", onStatus);
          this.storage.unsubs.push(() => {
            provider.off("status", onStatus);
          });
        }
      }
      // TODO: to be refactored for Convex BE
      // if (
      //   options.mentions &&
      //   this.editor.extensionManager.extensions.find(
      //     (e) => e.name.toLowerCase() === "mention"
      //   )
      // ) {
      //   console.warn(
      //     "[Liveblocks] Liveblocks own mention plugin is enabled, using another mention plugin may cause a conflict."
      //   );
      // }

      if (!options.presenceStore) {
        throw new Error("presenceStore is required for useLiveblocksExtension");
      }

      const sessionId = options.presenceStore.localSessionId;
      const userId = options.presenceStore.sessionIdUserIdMap.get(sessionId);
      let presence =
        options.presenceStore.getPresenceData() /* assert not nullish or typescript complains */!;
      if (!presence) {
        throw new Error("presence for local session not found");
      }
      if (!userId) {
        throw new Error("userId for local session not found");
      }

      const updateUser = (info: {
        userId: string;
        name: string;
        color: string;
      }) => {
        if (!info) {
          return;
        }
        if (this.storage.permanentUserData) {
          const pud = this.storage.permanentUserData.clients.get(
            this.storage.doc.clientID
          );
          // Only update if there is no entry or if the entry is different
          if (!pud || pud !== info.userId) {
            this.storage.permanentUserData.setUserMapping(
              this.storage.doc,
              this.storage.doc.clientID,
              info.userId
            );
          }
        }

        const yjsPresence = this.storage.provider.awareness.getLocalState();
        if (
          info.name !== yjsPresence?.user?.name ||
          info.color !== yjsPresence?.user?.color
        ) {
          this.editor.commands.updateUser({
            name: info.name,
            color: info.color,
          });
        }
      };
      // if we already have user info, we update the user
      if (presence) {
        updateUser({
          userId,
          name: presence.userData.displayName,
          color: presence.sessionData.color,
        });
      }

      const abortController = new AbortController();
      options.presenceStore.addEventListener(
        "data_changed",
        (event) => {
          if (event.detail.sessionId === sessionId) {
            const oldPresenceData = presence;
            if (
              oldPresenceData.userData.displayName !==
                event.detail.userData.displayName ||
              oldPresenceData.sessionData.color !==
                event.detail.sessionData.color
            ) {
              updateUser({
                userId,
                name: event.detail.userData.displayName,
                color: event.detail.sessionData.color,
              });
            }

            presence = event.detail;
          }
        },
        { signal: abortController.signal }
      );

      // we also listen in case the user info changes
      this.storage.unsubs.push(() => abortController.abort());
    },
    onDestroy() {
      this.storage.unsubs.forEach((unsub) => unsub());
    },
    addGlobalAttributes() {
      return [
        {
          types: ["paragraph", "heading"],
          attributes: {
            ychange: { default: null },
          },
        },
      ];
    },
    addStorage() {
      if (!options.presenceStore) {
        throw new Error("presenceStore is required for useLiveblocksExtension");
      }
      if (!options.yjsProvider) {
        throw new Error("yjsProvider is required for useLiveblocksExtension");
      }

      const { yjsProvider } = options;
      const yDoc = options.yjsProvider.getYDoc();

      return {
        doc: yDoc,
        provider: yjsProvider,
        permanentUserData: yjsProvider.permanentUserData,
        unsubs: [() => yjsProvider.destroy()],
      };
    },
    addExtensions() {
      if (!options.presenceStore) {
        throw new Error("presenceStore is required for useLiveblocksExtension");
      }

      const presenceData = options.presenceStore.getPresenceData();
      if (!presenceData) {
        throw new Error("presenceData for local session not found");
      }

      const user = {
        name: presenceData.userData.displayName,
        color: presenceData.sessionData.color,
      };

      const extensions: AnyExtension[] = [
        YChangeMark,

        LiveblocksCollab.configure({
          ySyncOptions: {
            permanentUserData: this.storage.permanentUserData,
          },
          document: this.storage.doc,
          field: options.field,
          provider: this.storage.provider,
        }),
        CollaborationCaret.configure({
          user,
          provider: this.storage.provider,
        }) as Extension<CollaborationCaretOptions>,
      ];

      // TODO: to be refactored for Convex BE
      // if (options.mentions) {
      //   extensions.push(
      //     MentionExtension.configure({
      //       onCreateMention: (mention) => {
      //         createTextMention(mention.notificationId, mention);
      //       },
      //       onDeleteMention: deleteTextMention,
      //     })
      //   );
      // }
      if (options.ai) {
        const aiConfig = options.ai;
        const resolveContextualPrompt = async ({
          prompt,
          context,
          previous,
          signal,
        }: ResolveContextualPromptArgs): Promise<ResolveContextualPromptResponse> => {
          if (
            typeof aiConfig !== "boolean" &&
            aiConfig.resolveContextualPrompt
          ) {
            return aiConfig.resolveContextualPrompt({
              prompt,
              context,
              previous,
              signal,
            });
          }
          throw new Error(
            "resolveContextualPrompt is required when ai is enabled"
          );
        };

        extensions.push(
          AiExtension.configure({
            resolveContextualPrompt,
            ...(typeof options.ai === "boolean" ? {} : options.ai),
            doc: this.storage.doc,
            pud: this.storage.permanentUserData,
          })
        );
      }

      return extensions;
    },
  });
};
