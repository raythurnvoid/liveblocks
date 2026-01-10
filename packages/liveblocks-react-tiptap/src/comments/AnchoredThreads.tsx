import { useLayoutEffect } from "@liveblocks/react/_private";
import { cn } from "@liveblocks/react-ui/_private";
import { type Editor, useEditorState } from "@tiptap/react";
import type {
  ComponentPropsWithoutRef,
  ComponentPropsWithRef,
  ReactNode,
} from "react";
import {
  createContext,
  use,
  useEffect,
  useEffectEvent,
  useMemo,
  useState,
} from "react";

import { THREADS_PLUGIN_KEY } from "../types";
import { getRectFromCoords } from "../utils";
import type { human_thread_messages_Thread } from "../../app_lb_bridge.ts";

function readThreadElementDataset(element: HTMLElement) {
  const threadId = element.dataset.threadId;
  const isActive = element.dataset.threadActive === "true";
  return { threadId, isActive };
}

function useThreadsEditorState(editor: Editor) {
  const state = useEditorState({
    editor,
    selector: (ctx) => {
      if (!ctx.editor.state) return { pluginState: undefined };
      const state = THREADS_PLUGIN_KEY.getState(ctx.editor.state);
      return {
        pluginState: state,
      };
    },
    equalityFn: (prev, next) => {
      if (!prev || !next) return false;
      return (
        prev.pluginState?.selectedThreadId ===
          next.pluginState?.selectedThreadId &&
        prev.pluginState?.threadPositions === next.pluginState?.threadPositions
      ); // new map is made each time threadPos updates so shallow equality is fine
    },
  });

  return useMemo(
    () =>
      state.pluginState
        ? {
            selectedThreadId: state.pluginState.selectedThreadId,
            threadPositions: state.pluginState.threadPositions,
          }
        : undefined,
    [state.pluginState]
  );
}

type AnchoredThreadsContext_Value = NonNullable<
  ReturnType<typeof useThreadsEditorState>
>;

const AnchoredThreadsContext =
  createContext<AnchoredThreadsContext_Value | null>(null);

type AnchoredThreadsItemContext_Value = {
  isActive: boolean;
};

const AnchoredThreadsItemContext =
  createContext<AnchoredThreadsItemContext_Value | null>(null);

export type AnchoredThreadsItem_Props = ComponentPropsWithRef<"div"> & {
  className?: string;
  thread: human_thread_messages_Thread;
  children: ReactNode;
};

export function AnchoredThreadsItem(props: AnchoredThreadsItem_Props) {
  const { className, thread, children, ...rest } = props;

  const context = use(AnchoredThreadsContext);
  if (!context)
    throw new Error(
      "AnchoredThreadsItem must be used within an AnchoredThreads component"
    );

  const threadId = thread.id;
  const isActive =
    Boolean(context.selectedThreadId) && context.selectedThreadId === thread.id;

  return (
    <AnchoredThreadsItemContext.Provider value={{ isActive }}>
      <div
        className={cn(className, "lb-tiptap-anchored-threads-item")}
        data-thread-id={threadId}
        data-thread-active={isActive ? "true" : "false"}
        {...rest}
      >
        {children}
      </div>
    </AnchoredThreadsItemContext.Provider>
  );
}

AnchoredThreadsItem.useContext = () => {
  const context = use(AnchoredThreadsItemContext);
  if (!context)
    throw new Error(
      "AnchoredThreadsItem.useContext must be used within an AnchoredThreadsItem component"
    );
  return context;
};

/**
 * CSS variables supported by AnchoredThreads component.
 * These can be set via inline styles or CSS to customize thread positioning.
 */
export type AnchoredThreads_CssVars = {
  "--lb-tiptap-anchored-threads-top": string;
};

type ThreadWithEditorPosition = {
  thread: human_thread_messages_Thread;
  position: { from: number; to: number };
};

export interface AnchoredThreadsProps extends ComponentPropsWithoutRef<"div"> {
  /**
   * The threads to display.
   */
  threads: human_thread_messages_Thread[];

  /**
   * The Tiptap editor.
   */
  editor: Editor;
}

export function AnchoredThreads({
  threads,
  className,
  style,
  editor,
  children,
  ...props
}: AnchoredThreadsProps) {
  const [container, setContainer] = useState<HTMLDivElement | null>(null);
  const [threadsWithEditorPosition, setThreadsWithEditorPosition] = useState<
    ThreadWithEditorPosition[]
  >([]);

  const threadsEditorState = useThreadsEditorState(editor);

  // TODO: lexical supoprts multiple threads being active, should probably do that here as well
  const handlePositionThreads = useEffectEvent(() => {
    if (container === null || !editor.view) return;

    let activeIndex = 0;
    const elements: HTMLElement[] = [];
    const elementsHeights = new Map<HTMLElement, number>();
    const elementsThreadsEditorPositionMap = new Map<
      HTMLElement,
      (typeof threadsWithEditorPosition)[number]
    >();

    for (const c of container.children) {
      const child = c as HTMLElement;
      elements.push(child);
      elementsHeights.set(child, child.getBoundingClientRect().height);
      const elDataset = readThreadElementDataset(child);
      if (elDataset.threadId != null) {
        const threadWithEditorPosition = threadsWithEditorPosition.find(
          (t) => t.thread.id === elDataset.threadId
        );

        if (threadWithEditorPosition) {
          elementsThreadsEditorPositionMap.set(child, threadWithEditorPosition);
        }
        if (elDataset.isActive) {
          activeIndex = elements.indexOf(child);
        }
      }
    }

    const ascending =
      activeIndex !== -1 ? elements.slice(activeIndex) : elements;
    const descending = activeIndex !== -1 ? elements.slice(0, activeIndex) : [];

    const containerTop = container.getBoundingClientRect().top;
    let baselineTop = undefined;
    let currentTop = 0;

    // Iterate over each thread and calculate its new position by taking into account
    // the position of the previously positioned threads
    for (const el of ascending) {
      const threadWithEditorPosition = elementsThreadsEditorPositionMap.get(el);

      if (threadWithEditorPosition) {
        const coords = editor.view.coordsAtPos(
          Math.min(
            threadWithEditorPosition.position.from,
            editor.view.state.doc.content.size - 1
          )
        );
        const rect = getRectFromCoords(coords);
        currentTop = Math.max(currentTop, rect.top - containerTop);
      }

      if (baselineTop === undefined) {
        baselineTop = currentTop;
      }

      el.style.setProperty(
        "--lb-tiptap-anchored-threads-top" satisfies keyof AnchoredThreads_CssVars,
        `${currentTop}px`
      );

      const elHeight = elementsHeights.get(el) ?? 0;
      currentTop += elHeight;
    }

    // Iterate over elements above the active one and set their position by taking into account
    // the position of elements positioned below
    currentTop = baselineTop ?? 0;

    for (const el of descending.reverse()) {
      const elHeight = elementsHeights.get(el) ?? 0;
      currentTop -= elHeight;

      const threadWithEditorPosition = elementsThreadsEditorPositionMap.get(el);

      if (threadWithEditorPosition) {
        const coords = editor.view.coordsAtPos(
          Math.min(
            threadWithEditorPosition.position.from,
            editor.view.state.doc.content.size - 1
          )
        );
        const rect = getRectFromCoords(coords);
        currentTop = Math.min(currentTop, rect.top - containerTop);
      }

      el.style.setProperty(
        "--lb-tiptap-anchored-threads-top" satisfies keyof AnchoredThreads_CssVars,
        `${currentTop}px`
      );
    }
  });

  useEffect(() => {
    if (!threadsEditorState?.threadPositions) return;
    const nextThreadsWithEditorPosition = Array.from(
      threadsEditorState.threadPositions,
      ([threadId, position]) => ({
        threadId,
        position,
      })
    ).reduce(
      (acc, { threadId, position }) => {
        const thread = threads.find(
          (thread) => thread.id === threadId && !thread.is_archived
        );
        if (!thread) return acc;
        acc.push({ thread, position });
        return acc;
      },
      [] as {
        thread: human_thread_messages_Thread;
        position: { from: number; to: number };
      }[]
    );
    setThreadsWithEditorPosition(nextThreadsWithEditorPosition);
    // disable exhaustive deps because we don't want an infinite loop
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [threadsEditorState, threads]);

  useLayoutEffect(() => {
    handlePositionThreads();
  }, [threadsWithEditorPosition]);

  useLayoutEffect(() => {
    if (!container) return;

    const resizeObserver = new ResizeObserver(() => handlePositionThreads());

    const mutationObserver = new MutationObserver((mutations) => {
      for (const mutation of mutations) {
        for (const addedNode of mutation.addedNodes) {
          if (addedNode instanceof HTMLElement) {
            resizeObserver.observe(addedNode);
          }
        }
      }
    });

    if (editor.view?.dom) {
      resizeObserver.observe(editor.view.dom);
    }
    for (const element of container.children) {
      resizeObserver.observe(element);
    }

    mutationObserver.observe(container, {
      childList: true,
    });

    return () => {
      mutationObserver.disconnect();
      resizeObserver.disconnect();
    };
  }, [container, editor]);

  if (!threadsEditorState) return null;

  return (
    <AnchoredThreadsContext.Provider value={threadsEditorState}>
      <div
        {...props}
        className={cn(className, "lb-tiptap-anchored-threads")}
        ref={setContainer}
        style={{
          position: "relative",
          ...style,
        }}
      >
        {children}
      </div>
    </AnchoredThreadsContext.Provider>
  );
}

AnchoredThreads.useContext = () => {
  const context = use(AnchoredThreadsContext);
  if (!context)
    throw new Error(
      "AnchoredThreads.useContext must be used within an AnchoredThreads component"
    );
  return context;
};
