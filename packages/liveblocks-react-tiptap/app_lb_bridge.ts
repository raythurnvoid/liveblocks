// Must use relative paths to avoid issues with multiple tsconfigs.

export type {
  human_thread_messages_Thread,
  human_thread_messages_Message,
} from "../../../../shared/human-thread-messages.ts";

export {
  ai_chat_HARDCODED_ORG_ID,
  ai_chat_HARDCODED_PROJECT_ID,
} from "../../../../src/lib/ai-chat.ts";

export { app_convex_api } from "../../../../src/lib/app-convex-client.ts";
