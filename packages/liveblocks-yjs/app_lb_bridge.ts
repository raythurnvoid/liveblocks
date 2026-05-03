// Must use relative paths to avoid issues with multiple tsconfigs.

export type {
  files_PresenceStore,
  files_PresenceStore_Event,
} from "../../../../src/lib/files.ts";

export {
  app_convex,
  app_convex_api,
  type app_convex_Id,
  type app_convex_Watch,
  type app_convex_FunctionArgs,
  type app_convex_FunctionReference,
  type app_convex_FunctionReturnType,
} from "../../../../src/lib/app-convex-client.ts";

export {
  files_yjs_doc_is_diff_update_empty,
  files_u8_to_array_buffer,
  files_u8_equals,
} from "../../../../shared/files.ts";
