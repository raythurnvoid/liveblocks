import { Extension, Mark, mergeAttributes } from "@tiptap/core";
import type { Node } from "@tiptap/pm/model";
import { Plugin, PluginKey } from "@tiptap/pm/state";
import { Decoration, DecorationSet } from "@tiptap/pm/view";

import type { ThreadPluginState } from "../types";
import {
  LIVEBLOCKS_COMMENT_MARK_TYPE,
  ThreadPluginActions,
  THREADS_PLUGIN_KEY,
} from "../types";

type ThreadPluginAction = {
  name: ThreadPluginActions;
  data: string | null;
};

export const FILTERED_THREADS_PLUGIN_KEY = new PluginKey<{
  filteredThreads?: Set<string>;
}>();

/**
 * Known issues: Overlapping marks are merged when reloading the doc. May be related:
 * https://github.com/ueberdosis/tiptap/issues/4339
 * https://github.com/yjs/@tiptap/y-tiptap/issues/47
 */
const Comment = Mark.create<{
  onThreadsChange?: (threadIds: string[]) => void;
}>({
  name: LIVEBLOCKS_COMMENT_MARK_TYPE,
  excludes: "",
  inclusive: false,
  keepOnSplit: true,
  parseHTML: () => {
    return [
      {
        tag: "span",
        getAttrs: (node) =>
          node.getAttribute("data-lb-thread-id") !== null && null,
      },
    ];
  },
  addAttributes() {
    // Return an object with attribute configuration
    return {
      orphan: {
        parseHTML: (element) => !!element.getAttribute("data-orphan"),
        renderHTML: (attributes) => {
          return (attributes as { orphan: boolean }).orphan
            ? {
                "data-orphan": "true",
              }
            : {};
        },
        default: false,
      },
      threadId: {
        parseHTML: (element) => element.getAttribute("data-lb-thread-id"),
        renderHTML: (attributes) => {
          return {
            "data-lb-thread-id": (attributes as { threadId: string }).threadId,
          };
        },
        default: "",
      },
    };
  },

  renderHTML({ HTMLAttributes }: { HTMLAttributes: Record<string, any> }) {
    const filteredThreads = this.editor
      ? FILTERED_THREADS_PLUGIN_KEY.getState(this.editor.state)?.filteredThreads
      : undefined;
    const threadId = (HTMLAttributes as { ["data-lb-thread-id"]: string })[
      "data-lb-thread-id"
    ];
    if (filteredThreads && !filteredThreads.has(threadId)) {
      return [
        "span",
        mergeAttributes(HTMLAttributes, {
          class: "lb-root lb-tiptap-thread-mark",
          "data-hidden": "",
        }),
      ];
    }

    return [
      "span",
      mergeAttributes(HTMLAttributes, {
        class: "lb-root lb-tiptap-thread-mark",
      }),
    ];
  },

  /**
   * This plugin tracks the (first) position of each thread mark in the doc and creates a decoration for the selected thread
   */
  addProseMirrorPlugins() {
    const updateState = (doc: Node, selectedThreadId: string | null) => {
      const threadPositions = new Map<string, { from: number; to: number }>();
      const threadIds = new Set<string>();
      const decorations: Decoration[] = [];
      // find all thread marks and store their position + create decoration for selected thread
      doc.descendants((node, pos) => {
        node.marks.forEach((mark) => {
          if (mark.type === this.type) {
            const thisThreadId = (
              mark.attrs as { threadId: string | undefined }
            ).threadId;
            if (!thisThreadId) {
              return;
            }
            const from = pos;
            const to = from + node.nodeSize;

            // FloatingThreads component uses "to" as the position, so always store the largest "to" found
            // AnchoredThreads component uses "from" as the position, so always store the smallest "from" found
            const currentPosition = threadPositions.get(thisThreadId) ?? {
              from: Infinity,
              to: 0,
            };
            threadPositions.set(thisThreadId, {
              from: Math.min(from, currentPosition.from),
              to: Math.max(to, currentPosition.to),
            });
            threadIds.add(thisThreadId);

            if (selectedThreadId === thisThreadId) {
              decorations.push(
                Decoration.inline(from, to, {
                  class: "lb-root lb-tiptap-thread-mark-selected",
                })
              );

              const decoration = this.editor.view.dom.querySelector(
                `.lb-tiptap-thread-mark[data-lb-thread-id="${thisThreadId}"]`
              );

              if (decoration) {
                decoration.scrollIntoView({
                  behavior: "smooth",
                  block: "nearest",
                });
              }
            }
          }
        });
      });
      return {
        decorations: DecorationSet.create(doc, decorations),
        selectedThreadId,
        threadPositions,
        threadIds,
        selectedThreadPos:
          selectedThreadId !== null
            ? (threadPositions.get(selectedThreadId)?.to ?? null)
            : null,
      };
    };

    const onThreadsChange = this.options.onThreadsChange;

    return [
      new Plugin({
        key: THREADS_PLUGIN_KEY,
        state: {
          init() {
            return {
              threadPositions: new Map<string, { from: number; to: number }>(),
              threadIds: new Set<string>(),
              selectedThreadId: null,
              selectedThreadPos: null,
              decorations: DecorationSet.empty,
            } as ThreadPluginState;
          },
          apply(tr, state) {
            const action = tr.getMeta(THREADS_PLUGIN_KEY) as ThreadPluginAction;
            if (!tr.docChanged && !action) {
              return state;
            }

            let nextState;

            if (!action) {
              // Doc changed, but no action, just update rects
              nextState = updateState(tr.doc, state.selectedThreadId);
            } else if (
              action.name === ThreadPluginActions.SET_SELECTED_THREAD_ID &&
              state.selectedThreadId !== action.data
            ) {
              // handle actions, possibly support more actions
              nextState = updateState(tr.doc, action.data);
            } else {
              return state;
            }

            // Notify about thread ID changes
            if (!areSetsEqual(state.threadIds, nextState.threadIds)) {
              onThreadsChange?.(Array.from(nextState.threadIds));
            }

            return nextState;
          },
        },
        props: {
          decorations: (state) => {
            return (
              THREADS_PLUGIN_KEY.getState(state)?.decorations ??
              DecorationSet.empty
            );
          },
          handleClick: (view, pos, event) => {
            if (event.button !== 0) {
              return;
            }

            const selectThread = (threadId: string | null) => {
              view.dispatch(
                view.state.tr.setMeta(THREADS_PLUGIN_KEY, {
                  name: ThreadPluginActions.SET_SELECTED_THREAD_ID,
                  data: threadId,
                })
              );
            };

            const node = view.state.doc.nodeAt(pos);
            if (!node) {
              selectThread(null);
              return;
            }
            const commentMark = node.marks.find(
              (mark) => mark.type === this.type && !mark.attrs.orphan
            );
            // nothing to select
            if (!commentMark) {
              selectThread(null);
              return;
            }
            const threadId = commentMark?.attrs.threadId as string | undefined;

            const filtered = FILTERED_THREADS_PLUGIN_KEY.getState(
              view.state
            )?.filteredThreads;
            if (threadId && filtered && !filtered.has(threadId)) {
              selectThread(null);
              return;
            }

            selectThread(threadId ?? null);
          },
        },
      }),
    ];
  },
});

export const CommentsExtension = Extension.create<{
  filteredThreads?: Set<string>;
  onThreadsChange?: (threadIds: string[]) => void;
}>({
  name: "liveblocksComments",
  priority: 95,
  addExtensions() {
    return [
      Comment.configure({
        onThreadsChange: this.options.onThreadsChange,
      }),
    ];
  },

  addCommands() {
    return {
      selectThread:
        (id: string | null) =>
        ({ tr }) => {
          const filtered = FILTERED_THREADS_PLUGIN_KEY.getState(
            this.editor.state
          )?.filteredThreads;
          if (id && filtered && !filtered.has(id)) {
            tr.setMeta(THREADS_PLUGIN_KEY, {
              name: ThreadPluginActions.SET_SELECTED_THREAD_ID,
              data: null,
            });
            return true;
          }

          tr.setMeta(THREADS_PLUGIN_KEY, {
            name: ThreadPluginActions.SET_SELECTED_THREAD_ID,
            data: id,
          });
          return true;
        },
      addComment:
        (id: string) =>
        ({ commands, state }) => {
          if (state.selection.empty) {
            return false;
          }
          commands.setMark(LIVEBLOCKS_COMMENT_MARK_TYPE, { threadId: id });
          return true;
        },
      markCommentAsOrphan:
        (args: { threadId: string; orphan: boolean }) =>
        ({ tr, state }) => {
          const markType = state.schema.marks[LIVEBLOCKS_COMMENT_MARK_TYPE];
          if (!markType) {
            return false;
          }

          state.doc.descendants((node, pos) => {
            node.marks.forEach((mark) => {
              if (mark.type !== markType) return;
              const threadId = mark.attrs.threadId as string | undefined;
              if (threadId !== args.threadId) return;

              tr.removeMark(pos, pos + node.nodeSize, mark).addMark(
                pos,
                pos + node.nodeSize,
                markType.create({
                  ...mark.attrs,
                  orphan: args.orphan,
                })
              );
            });
          });

          return true;
        },
    };
  },

  addProseMirrorPlugins() {
    return [
      new Plugin({
        key: FILTERED_THREADS_PLUGIN_KEY,
        state: {
          init: () => ({
            filteredThreads: this.options.filteredThreads,
          }),
          apply(tr, value) {
            const meta = tr.getMeta(FILTERED_THREADS_PLUGIN_KEY) as
              | { filteredThreads?: Set<string> }
              | undefined;
            if (meta?.filteredThreads) {
              return { filteredThreads: meta.filteredThreads };
            }
            return value;
          },
        },
        view: (view) => {
          const syncDom = () => {
            const filteredThreads = FILTERED_THREADS_PLUGIN_KEY.getState(
              view.state
            )?.filteredThreads;

            // Toggle attribute for all comment-mark spans
            const els = view.dom.querySelectorAll<HTMLElement>(
              "span.lb-tiptap-thread-mark[data-lb-thread-id]"
            );
            els.forEach((el) => {
              const id = el.getAttribute("data-lb-thread-id");
              if (!id) return;
              if (!filteredThreads || filteredThreads.has(id)) {
                el.removeAttribute("data-hidden");
              } else {
                el.setAttribute("data-hidden", "");
              }
            });
          };

          queueMicrotask(syncDom);

          return {
            update: (view, prevState) => {
              const curr = FILTERED_THREADS_PLUGIN_KEY.getState(
                view.state
              )?.filteredThreads;
              const prev =
                FILTERED_THREADS_PLUGIN_KEY.getState(
                  prevState
                )?.filteredThreads;

              if (
                !areSetsEqual(prev, curr) ||
                view.state.doc !== prevState.doc
              ) {
                syncDom();

                const selected = THREADS_PLUGIN_KEY.getState(
                  view.state
                )?.selectedThreadId;
                if (selected && curr && !curr.has(selected)) {
                  view.dispatch(
                    view.state.tr.setMeta(THREADS_PLUGIN_KEY, {
                      name: ThreadPluginActions.SET_SELECTED_THREAD_ID,
                      data: null,
                    })
                  );
                }
              }
            },
          };
        },
      }),
    ];
  },
});

export function areSetsEqual(a?: Set<string>, b?: Set<string>): boolean {
  if (a === b) return true;
  if (!a || !b) return false;
  if (a.size !== b.size) return false;
  for (const v of a) if (!b.has(v)) return false;
  return true;
}
