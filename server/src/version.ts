export const SERVER_VERSION = "0.2.0";

// Compatibility metadata is intentionally explicit so the plugin can reason
// about safe upgrade paths before we add richer release-manifest logic.
export const SERVER_MIN_PLUGIN_VERSION: string | null = null;
export const SERVER_RECOMMENDED_PLUGIN_VERSION = "1.3.2";
export const SERVER_MIN_SCHEMA_VERSION = 2;
export const SERVER_MAX_SCHEMA_VERSION = 2;
export const SERVER_MIGRATION_REQUIRED = false;
