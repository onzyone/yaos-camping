import { MarkdownView, Modal, Notice, Plugin, TFile, normalizePath } from "obsidian";
import {
	DEFAULT_SETTINGS,
	VaultSyncSettingTab,
	generateVaultId,
	type VaultSyncSettings,
} from "./settings";
import { VaultSync, type ReconcileMode } from "./sync/vaultSync";
import { EditorBindingManager } from "./sync/editorBinding";
import { DiskMirror } from "./sync/diskMirror";
import { BlobSyncManager, type BlobQueueSnapshot } from "./sync/blobSync";
import { parseExcludePatterns } from "./sync/exclude";
import { isMarkdownSyncable, isBlobSyncable } from "./types";
import { applyDiffToYText } from "./sync/diff";
import {
	type DiskIndex,
	filterChangedFiles,
	updateIndex,
	moveIndexEntries,
	waitForDiskQuiet,
} from "./sync/diskIndex";
import {
	type BlobHashCache,
	moveCachedHashes,
} from "./sync/blobHashCache";
import {
	requestDailySnapshot,
	requestSnapshotNow,
	listSnapshots as fetchSnapshotList,
	downloadSnapshot,
	diffSnapshot,
	restoreFromSnapshot,
	type SnapshotIndex,
	type SnapshotDiff,
} from "./sync/snapshotClient";
import {
	appendTraceParams,
	PersistentTraceLogger,
	type TraceEventDetails,
	type TraceHttpContext,
} from "./debug/trace";

type SyncStatus = "disconnected" | "loading" | "syncing" | "connected" | "offline" | "error" | "unauthorized";

type PersistedPluginState = Partial<VaultSyncSettings> & {
	_diskIndex?: DiskIndex;
	_blobHashCache?: BlobHashCache;
	_blobQueue?: BlobQueueSnapshot;
};

/** Minimum interval between reconcile runs (prevents rapid reconnect churn). */
const RECONCILE_COOLDOWN_MS = 10_000;

export default class VaultCrdtSyncPlugin extends Plugin {
	settings: VaultSyncSettings = DEFAULT_SETTINGS;

	private vaultSync: VaultSync | null = null;
	private editorBindings: EditorBindingManager | null = null;
	private diskMirror: DiskMirror | null = null;
	private blobSync: BlobSyncManager | null = null;
	private statusBarEl: HTMLElement | null = null;
	private statusInterval: ReturnType<typeof setInterval> | null = null;

	/**
	 * True after initial reconciliation is complete.
	 * Vault events are ignored before this.
	 */
	private reconciled = false;

	/** True while a reconciliation is running — prevents overlapping runs. */
	private reconcileInFlight = false;

	/** Set to true if a reconnect arrives while reconciliation is in-flight. */
	private reconcilePending = false;

	/**
	 * Files on disk that weren't imported during conservative reconciliation.
	 * Imported when the provider eventually syncs.
	 */
	private untrackedFiles: string[] = [];

	/**
	 * The connection generation at which we last reconciled.
	 * Used to detect reconnects that need re-reconciliation.
	 */
	private lastReconciledGeneration = 0;

	/** Visibility change handler reference for cleanup. */
	private visibilityHandler: (() => void) | null = null;

	/** Track the set of currently observed file paths for disk mirror cleanup. */
	private openFilePaths = new Set<string>();
	private activeMarkdownPath: string | null = null;

	/** Parsed exclude patterns from settings. */
	private excludePatterns: string[] = [];

	/** Max file size in characters (derived from settings KB). */
	private maxFileSize = 0;

	/** Persisted disk index: {path -> {mtime, size}}. */
	private diskIndex: DiskIndex = {};

	/** Persisted blob hash cache: {path -> {mtime, size, hash}}. */
	private blobHashCache: BlobHashCache = {};

	/** Persisted blob queue snapshot for crash resilience. */
	private savedBlobQueue: BlobQueueSnapshot | null = null;
	private persistedState: PersistedPluginState = {};
	private persistWriteChain: Promise<void> = Promise.resolve();

	/** Pending stability checks for newly created/dropped files. */
	private pendingStabilityChecks = new Set<string>();

	/** Last time a reconciliation completed (for cooldown). */
	private lastReconcileTime = 0;

	/** Timer for delayed reconcile after cooldown expires. */
	private reconcileCooldownTimer: ReturnType<typeof setTimeout> | null = null;

	/** In-memory ring of recent high-level plugin events. */
	private eventRing: Array<{ ts: string; msg: string }> = [];

	/** Persistent trace journal/state writer (active when debug is enabled). */
	private traceLogger: PersistentTraceLogger | null = null;
	private traceStateInterval: ReturnType<typeof setInterval> | null = null;
	private traceStateTimer: ReturnType<typeof setTimeout> | null = null;
	private traceServerInterval: ReturnType<typeof setInterval> | null = null;
	private traceServerInFlight = false;
	private recentServerTrace: unknown[] = [];

	/**
	 * True when startup timed out waiting for provider sync.
	 * We use this to force one authoritative reconcile on the first late
	 * provider sync event, even if connection generation did not change.
	 */
	private awaitingFirstProviderSyncAfterStartup = false;

	async onload() {
		await this.loadSettings();

		let generatedVaultId = false;
		if (!this.settings.vaultId) {
			this.settings.vaultId = generateVaultId();
			await this.saveSettings();
			generatedVaultId = true;
		}

		if (!this.settings.deviceName) {
			this.settings.deviceName = `device-${Date.now().toString(36)}`;
			await this.saveSettings();
		}

		this.setupTraceLogger();
		if (generatedVaultId) {
			this.log(`Generated vault ID: ${this.settings.vaultId}`);
		}

		this.addSettingTab(new VaultSyncSettingTab(this.app, this));

		this.statusBarEl = this.addStatusBarItem();
		this.updateStatusBar("disconnected");

		if (!this.settings.host || !this.settings.token) {
			this.log("Host or token not configured — sync disabled");
			new Notice(
				"Vault CRDT sync: configure host and token in settings to enable sync.",
			);
			return;
		}

		// Parse exclude patterns and file size limit from settings
		this.excludePatterns = parseExcludePatterns(this.settings.excludePatterns);
		this.maxFileSize = this.settings.maxFileSizeKB * 1024;

		this.applyCursorVisibility();

		// Warn about insecure connections to non-localhost hosts
		if (this.settings.host) {
			try {
				const url = new URL(this.settings.host);
				const h = url.hostname;
				if (url.protocol === "http:" && h !== "localhost" && h !== "127.0.0.1" && h !== "[::1]") {
					this.log("WARNING: connecting over unencrypted HTTP to a remote host — token sent in plaintext");
					new Notice(
						"Vault CRDT sync: connecting over unencrypted HTTP. Your token will be sent in plaintext. Use https:// for production.",
						8000,
					);
				}
			} catch { /* invalid URL, will fail at connect */ }
		}

		void this.initSync();
	}

	private async initSync(): Promise<void> {
		try {
			// 1. Create VaultSync (Y.Doc + IndexedDB + provider in parallel)
			this.vaultSync = new VaultSync(this.settings, {
				traceContext: this.getTraceHttpContext(),
				trace: (source, msg, details) => this.trace(source, msg, details),
			});

			// 2. EditorBindingManager
			this.editorBindings = new EditorBindingManager(
				this.vaultSync,
				this.settings.debug,
				(source, msg, details) => this.trace(source, msg, details),
			);

			// 3. Global CM6 extension
			this.registerEditorExtension(
				this.editorBindings.getBaseExtension(),
			);

			// 4. DiskMirror
			this.diskMirror = new DiskMirror(
				this.app,
				this.vaultSync,
				this.editorBindings,
				this.settings.debug,
				(source, msg, details) => this.trace(source, msg, details),
			);
			this.diskMirror.startMapObservers();

			// 4b. BlobSyncManager (if attachment sync is enabled)
			if (this.settings.enableAttachmentSync) {
				this.blobSync = new BlobSyncManager(
					this.app,
					this.vaultSync,
					{
						host: this.settings.host,
						token: this.settings.token,
						vaultId: this.settings.vaultId,
						maxAttachmentSizeKB: this.settings.maxAttachmentSizeKB,
						attachmentConcurrency: this.settings.attachmentConcurrency,
						debug: this.settings.debug,
						trace: this.getTraceHttpContext(),
					},
					this.blobHashCache,
					(source, msg, details) => this.trace(source, msg, details),
				);
				this.blobSync.startObservers();

				// Restore persisted queue from previous session
				if (this.savedBlobQueue) {
					this.blobSync.importQueue(this.savedBlobQueue);
					this.savedBlobQueue = null;
				}
			}

			// 5. Status tracking
			this.vaultSync.provider.on("status", () => this.refreshStatusBar());
			this.statusInterval = setInterval(() => {
				this.refreshStatusBar();
				if (this.reconciled && this.editorBindings) {
					const touched = this.editorBindings.auditBindings("status-tick");
					if (touched > 0) {
						this.log(`Binding health audit (status-tick) — touched ${touched}`);
						this.scheduleTraceStateSnapshot("binding-audit:status-tick");
					}
				}
				// Periodically persist blob queue if transfers are active,
				// or clear persisted queue if transfers completed
				if (this.blobSync) {
					if (this.blobSync.pendingUploads > 0 || this.blobSync.pendingDownloads > 0) {
						void this.saveBlobQueue();
					} else {
						void this.clearSavedBlobQueue();
					}
				}
			}, 3000);
			this.register(() => {
				if (this.statusInterval) clearInterval(this.statusInterval);
			});

			// 6. Vault events (gated by this.reconciled)
			this.registerVaultEvents();

			// 7. Commands
			this.registerCommands();

			// 8. Rename batch callback → update editor bindings + disk mirror observers + disk index + blob hash cache
			this.vaultSync.onRenameBatchFlushed((renames) => {
				this.editorBindings?.updatePathsAfterRename(renames);

				// Move disk index entries
				moveIndexEntries(this.diskIndex, renames);

				// Move blob hash cache entries
				moveCachedHashes(this.blobHashCache, renames);

				// Move disk mirror observers and openFilePaths tracking
				// for any paths that were open before the rename.
				for (const [oldPath, newPath] of renames) {
					if (this.activeMarkdownPath === oldPath) {
						this.activeMarkdownPath = newPath;
					}
					if (this.openFilePaths.has(oldPath)) {
						this.diskMirror?.notifyFileClosed(oldPath);
						this.openFilePaths.delete(oldPath);
						this.diskMirror?.notifyFileOpened(newPath);
						this.openFilePaths.add(newPath);
						this.log(`Rename batch: moved observer "${oldPath}" -> "${newPath}"`);
					}
				}
			});

			// 9. Reconnection: re-reconcile when provider re-syncs
			this.setupReconnectionHandler();

			// 10. Visibility change: force reconnect on foreground
			this.setupVisibilityHandler();

			// -----------------------------------------------------------
			// STARTUP SEQUENCE
			// -----------------------------------------------------------

			this.updateStatusBar("loading");
			this.log("Waiting for IndexedDB persistence...");
			const localLoaded = await this.vaultSync.waitForLocalPersistence();
			this.log(`IndexedDB: ${localLoaded ? "loaded" : "timed out"}`);

			// Schema version check — refuse to run if a newer plugin wrote this data
			const schemaError = this.vaultSync.checkSchemaVersion();
			if (schemaError) {
				console.error(`[vault-crdt-sync] ${schemaError}`);
				new Notice(`Vault CRDT sync: ${schemaError}`);
				this.updateStatusBar("error");
				return;
			}

			// Check for fatal auth error before waiting for provider
			if (this.vaultSync.fatalAuthError) {
				this.log("Fatal auth error during startup");
				this.updateStatusBar("unauthorized");
				new Notice("Vault CRDT sync: unauthorized — check your token in settings.");
				// Still reconcile with whatever we have locally
				const mode = this.vaultSync.getSafeReconcileMode();
				await this.runReconciliation(mode);
				this.bindAllOpenEditors();
				this.validateAllOpenBindings("startup-auth-fallback");
				return;
			}

			this.updateStatusBar("syncing");
			this.log("Waiting for provider sync...");
			const providerSynced = await this.vaultSync.waitForProviderSync();
			this.log(`Provider: ${providerSynced ? "synced" : "timed out (offline)"}`);
			this.awaitingFirstProviderSyncAfterStartup = !providerSynced;
			this.log(
				`Startup sync gate: awaitingFirstProviderSyncAfterStartup=${this.awaitingFirstProviderSyncAfterStartup} ` +
				`(gen=${this.vaultSync.connectionGeneration})`,
			);

			if (this.vaultSync.fatalAuthError) {
				this.updateStatusBar("unauthorized");
				new Notice("Vault CRDT sync: unauthorized — check your token in settings.");
				return;
			}

			const mode = this.vaultSync.getSafeReconcileMode();
			this.log(`Reconciliation mode: ${mode}`);

			await this.runReconciliation(mode);
			this.lastReconciledGeneration = this.vaultSync.connectionGeneration;
			if (providerSynced) {
				this.awaitingFirstProviderSyncAfterStartup = false;
			}

			this.bindAllOpenEditors();
			this.validateAllOpenBindings("startup");

			this.refreshStatusBar();
			this.log("Startup complete");
			this.scheduleTraceStateSnapshot("startup-complete");

			// Trigger daily snapshot (noop if already taken today).
			// Fire-and-forget — don't block startup on snapshot creation.
			if (providerSynced) {
				void this.triggerDailySnapshot();
			}
		} catch (err) {
			console.error("[vault-crdt-sync] Failed to initialize sync:", err);
			new Notice(`Vault CRDT sync: failed to initialize — ${err}`);
			this.updateStatusBar("error");
		}
	}

	// -------------------------------------------------------------------
	// Reconnection
	// -------------------------------------------------------------------

	/**
	 * Listen for provider sync events after initial startup.
	 * When the provider syncs at a new generation (reconnect), trigger
	 * an authoritative re-reconciliation to catch any drift.
	 */
	private setupReconnectionHandler(): void {
		if (!this.vaultSync) return;

		this.vaultSync.onProviderSync((generation) => {
			// Skip the initial sync — that's handled by the startup sequence
			if (!this.reconciled) {
				this.log(`Provider sync ignored: initial startup still running (gen ${generation})`);
				return;
			}

			// If startup timed out waiting for provider sync, the first late
			// sync can arrive on the same connection generation. We still need
			// one authoritative reconcile to import any remote changes.
			if (this.awaitingFirstProviderSyncAfterStartup) {
				this.awaitingFirstProviderSyncAfterStartup = false;
				this.log(`Late first provider sync (gen ${generation}) — scheduling catch-up reconciliation`);
				if (this.reconcileInFlight) {
					this.log("Late first sync arrived during reconcile — marked pending");
					this.reconcilePending = true;
					return;
				}
				void this.runReconnectReconciliation(generation);
				return;
			}

			// Skip if we already reconciled at this generation
			if (generation <= this.lastReconciledGeneration) {
				this.log(
					`Provider sync ignored: generation ${generation} <= lastReconciledGeneration ${this.lastReconciledGeneration}`,
				);
				return;
			}

			this.log(`Reconnect detected (gen ${generation}) — scheduling re-reconciliation`);

			if (this.reconcileInFlight) {
				this.log("Reconnect sync arrived during reconcile — marked pending");
				this.reconcilePending = true;
				return;
			}

			void this.runReconnectReconciliation(generation);
		});
	}

	/**
	 * Lightweight authoritative reconcile after a reconnection.
	 * Fresh disk read to catch any drift during disconnect.
	 */
	private async runReconnectReconciliation(generation: number): Promise<void> {
		if (!this.vaultSync) return;

		this.log(`Running reconnect reconciliation (gen ${generation})`);
		this.validateAllOpenBindings(`reconnect-pre:${generation}`);

		// Also import any untracked files from a previous conservative run
		if (this.untrackedFiles.length > 0) {
			await this.importUntrackedFiles();
		}

		await this.runReconciliation("authoritative");
		this.lastReconciledGeneration = generation;
		this.awaitingFirstProviderSyncAfterStartup = false;
		this.bindAllOpenEditors();
		this.validateAllOpenBindings(`reconnect-post:${generation}`);

		// If another reconnect arrived during this reconcile, run again
		if (this.reconcilePending) {
			this.reconcilePending = false;
			if (this.vaultSync.connectionGeneration > this.lastReconciledGeneration) {
				void this.runReconnectReconciliation(this.vaultSync.connectionGeneration);
			}
		}
	}

	/**
	 * On visibility change (foreground): if the provider is disconnected,
	 * force a reconnect. Handles Android backgrounding and desktop
	 * sleep/wake where sockets silently die.
	 */
	private setupVisibilityHandler(): void {
		this.visibilityHandler = () => {
			if (document.visibilityState === "hidden") {
				void this.diskMirror?.flushOpenWrites("app-backgrounded");
				return;
			}
			if (document.visibilityState !== "visible") return;
			if (!this.vaultSync) return;
			if (this.vaultSync.fatalAuthError) return;

			if (!this.vaultSync.connected) {
				this.log("App foregrounded — provider disconnected, forcing reconnect");
				this.vaultSync.provider.disconnect();
				this.vaultSync.provider.connect();
			}
		};

		document.addEventListener("visibilitychange", this.visibilityHandler);
		this.register(() => {
			if (this.visibilityHandler) {
				document.removeEventListener("visibilitychange", this.visibilityHandler);
			}
		});
	}

	// -------------------------------------------------------------------
	// Reconciliation
	// -------------------------------------------------------------------

	private async runReconciliation(mode: ReconcileMode): Promise<void> {
		if (!this.vaultSync || !this.diskMirror) return;
		if (this.reconcileInFlight) {
			this.reconcilePending = true;
			this.log("Reconciliation already in flight — queued");
			return;
		}

		// Cooldown: prevent rapid successive reconciliations (flaky Wi-Fi)
		const now = Date.now();
		const elapsed = now - this.lastReconcileTime;
		if (this.lastReconcileTime > 0 && elapsed < RECONCILE_COOLDOWN_MS) {
			const delay = RECONCILE_COOLDOWN_MS - elapsed;
			this.log(`Reconcile cooldown: ${delay}ms remaining, scheduling delayed run`);
			this.reconcilePending = true;
			if (!this.reconcileCooldownTimer) {
				this.reconcileCooldownTimer = setTimeout(() => {
					this.reconcileCooldownTimer = null;
					if (this.reconcilePending) {
						this.reconcilePending = false;
						const m = this.vaultSync?.getSafeReconcileMode() ?? mode;
						void this.runReconciliation(m);
					}
				}, delay);
			}
			return;
		}

		this.reconcileInFlight = true;

		try {
			const diskFiles = new Map<string, string>();
			const diskPresentPaths = new Set<string>();
			const allMdFiles = this.app.vault.getMarkdownFiles();
			let excludedCount = 0;
			let oversizedCount = 0;
			let skippedByIndex = 0;

			// Filter by exclude patterns first
			const eligibleFiles: TFile[] = [];
			for (const file of allMdFiles) {
				if (!isMarkdownSyncable(file.path, this.excludePatterns)) {
					excludedCount++;
					continue;
				}
				eligibleFiles.push(file);
				diskPresentPaths.add(file.path);
			}

			// In conservative mode, use disk index optimization.
			// In authoritative mode, read all files for correctness so we can
			// detect and heal any disk<->CRDT drift after reconnect/startup.
			let changed: TFile[] = [];
			let unchanged: TFile[] = [];
			let allStats: Map<string, { mtime: number; size: number }> = new Map();
			if (mode === "authoritative") {
				changed = eligibleFiles;
				for (const file of eligibleFiles) {
					const stat = await this.app.vault.adapter.stat(file.path);
					if (stat) {
						allStats.set(file.path, { mtime: stat.mtime, size: stat.size });
					}
				}
				skippedByIndex = 0;
			} else {
				const indexResult = await filterChangedFiles(
					this.app,
					eligibleFiles,
					this.diskIndex,
				);
				changed = indexResult.changed;
				unchanged = indexResult.unchanged;
				allStats = indexResult.allStats;
				skippedByIndex = unchanged.length;
			}

			// For unchanged files, we still need them in the diskFiles map
			// so reconcileVault knows they exist on disk (for the "disk-only
			// vs CRDT-only" comparison). But we use a sentinel instead of
			// actual content to avoid reading them.
			// We mark them with a special marker and handle them in reconcileVault
			// by checking CRDT existence only (no content comparison needed).
			for (const file of unchanged) {
				// File exists on disk and hasn't changed — just mark presence
				// Use empty string as placeholder; reconcileVault only needs
				// to know the path exists for the "disk-only" check.
				// Content comparison isn't needed because nothing changed.
				const existingText = this.vaultSync.getTextForPath(file.path);
				if (existingText) {
					// Both exist and disk unchanged → skip read entirely
					continue;
				}
				// Disk-only (not in CRDT) but unchanged → need to read for seeding
				try {
					const content = await this.app.vault.read(file);
					if (this.maxFileSize > 0 && content.length > this.maxFileSize) {
						oversizedCount++;
						continue;
					}
					diskFiles.set(file.path, content);
				} catch (err) {
					console.error(
						`[vault-crdt-sync] Failed to read "${file.path}":`,
						err,
					);
				}
			}

			// Read changed files
			for (const file of changed) {
				try {
					const content = await this.app.vault.read(file);
					if (this.maxFileSize > 0 && content.length > this.maxFileSize) {
						oversizedCount++;
						this.log(`reconcile: skipping "${file.path}" (${Math.round(content.length / 1024)} KB exceeds limit)`);
						continue;
					}
					diskFiles.set(file.path, content);
				} catch (err) {
					console.error(
						`[vault-crdt-sync] Failed to read "${file.path}" during reconciliation:`,
						err,
					);
				}
			}

			if (excludedCount > 0) {
				this.log(`reconcile: excluded ${excludedCount} files by pattern`);
			}
			if (oversizedCount > 0) {
				this.log(`reconcile: skipped ${oversizedCount} oversized files`);
				new Notice(`Vault CRDT sync: skipped ${oversizedCount} files exceeding ${this.settings.maxFileSizeKB} KB size limit.`);
			}
			if (skippedByIndex > 0) {
				this.log(`reconcile: ${skippedByIndex} files unchanged (stat match), ${changed.length} changed`);
			}

			this.log(
				`Reconciling [${mode}]: diskPresent=${diskPresentPaths.size}, ` +
				`diskLoaded=${diskFiles.size} (${changed.length} read) vs ` +
				`${this.vaultSync.pathToId.size} CRDT paths`,
			);

			const result = this.vaultSync.reconcileVault(
				diskFiles,
				diskPresentPaths,
				mode,
				this.settings.deviceName,
			);

			// Safety brake: large create-on-disk batches are usually a signal that
			// presence bookkeeping is wrong or stale. Abort destructive writeback.
			const crdtPathCount = this.vaultSync.pathToId.size;
			const writeCount = result.createdOnDisk.length + result.updatedOnDisk.length;
			const writeRatio = crdtPathCount > 0 ? (writeCount / crdtPathCount) : 0;
			if (writeCount > 20 && writeRatio > 0.25) {
				const msg =
					`Reconcile safety brake: refusing to write ${writeCount} ` +
					`files to disk (${Math.round(writeRatio * 100)}% of CRDT paths).`;
				this.log(msg);
				console.error(`[vault-crdt-sync] ${msg}`);
				new Notice(`Vault CRDT sync: ${msg} Run "Export sync diagnostics" and inspect logs.`);
				// Continue with non-destructive state updates below; skip flushWrite loop.
			} else {
				for (const path of result.createdOnDisk) {
					await this.diskMirror.flushWrite(path);
				}
				for (const path of result.updatedOnDisk) {
					await this.diskMirror.flushWrite(path);
				}
			}

			this.untrackedFiles = result.untracked;
			this.reconciled = true;

			// Update disk index with fresh stats
			this.diskIndex = updateIndex(this.diskIndex, allStats);
			void this.saveDiskIndex();

			// Run integrity checks after reconciliation (orphan GC + duplicate detection)
			const integrity = this.vaultSync.runIntegrityChecks();
			if (integrity.duplicateIds > 0 || integrity.orphansCleaned > 0) {
				this.log(
					`Integrity: ${integrity.duplicateIds} duplicate IDs fixed, ` +
					`${integrity.orphansCleaned} orphans cleaned`,
				);
			}

			this.log(
				`Reconciliation [${mode}] complete: ` +
				`${result.seededToCrdt.length} seeded, ` +
				`${result.createdOnDisk.length} created on disk, ` +
				`${result.updatedOnDisk.length} updated on disk, ` +
				`${result.untracked.length} untracked, ` +
				`${result.skipped} tombstoned`,
			);

			// Blob reconciliation (if enabled)
			if (this.blobSync) {
				const blobResult = await this.blobSync.reconcile(
					mode,
					this.excludePatterns,
				);
				this.log(
					`Blob reconciliation [${mode}]: ` +
					`${blobResult.uploadQueued} uploads, ` +
					`${blobResult.downloadQueued} downloads, ` +
					`${blobResult.skipped} skipped`,
				);
			}
		} finally {
			this.reconcileInFlight = false;
			this.lastReconcileTime = Date.now();
			this.scheduleTraceStateSnapshot(`reconcile-${mode}`);
		}
	}

	private async importUntrackedFiles(): Promise<void> {
		if (!this.vaultSync) return;

		const toImport = [...this.untrackedFiles];
		this.untrackedFiles = [];
		let imported = 0;

		for (const path of toImport) {
			if (this.vaultSync.getTextForPath(path)) {
				this.log(`importUntracked: "${path}" now in CRDT, skipping`);
				continue;
			}

			const file = this.app.vault.getAbstractFileByPath(path);
			if (!(file instanceof TFile)) continue;

			try {
				const content = await this.app.vault.read(file);
				this.vaultSync.ensureFile(path, content, this.settings.deviceName);
				imported++;
			} catch (err) {
				console.error(
					`[vault-crdt-sync] importUntracked failed for "${path}":`,
					err,
				);
			}
		}

		if (!this.vaultSync.isInitialized) {
			this.vaultSync.markInitialized();
		}

		this.refreshStatusBar();
		this.log(`Imported ${imported} previously untracked files`);

		if (imported > 0) {
			new Notice(`Vault CRDT sync: imported ${imported} files after server sync.`);
		}
	}

	// -------------------------------------------------------------------
	// Editor binding
	// -------------------------------------------------------------------

	private bindAllOpenEditors(): void {
		this.app.workspace.iterateAllLeaves((leaf) => {
			if (leaf.view instanceof MarkdownView) {
				this.editorBindings?.bind(leaf.view, this.settings.deviceName);
				if (leaf.view.file) {
					this.trackOpenFile(leaf.view.file.path);
				}
			}
		});
		this.activeMarkdownPath = this.getActiveMarkdownPath();
	}

	private validateAllOpenBindings(reason: string): void {
		let touched = 0;

		this.app.workspace.iterateAllLeaves((leaf) => {
			if (!(leaf.view instanceof MarkdownView) || !leaf.view.file) {
				return;
			}

			const binding = this.editorBindings?.getBindingDebugInfoForView(leaf.view) ?? null;
			const health = this.editorBindings?.getBindingHealthForView(leaf.view) ?? null;

			if (health?.bound && (health.healthy || health.settling)) {
				return;
			}

			touched += 1;
			if (!binding || !health?.bound) {
				this.editorBindings?.bind(leaf.view, this.settings.deviceName);
				return;
			}

			const repaired = this.editorBindings?.heal(
				leaf.view,
				this.settings.deviceName,
				`validate:${reason}`,
			) ?? false;
			if (!repaired) {
				this.editorBindings?.rebind(
					leaf.view,
					this.settings.deviceName,
					`validate:${reason}`,
				);
			}
		});

		if (touched > 0) {
			this.log(`Validated open bindings (${reason}) — touched ${touched}`);
			this.scheduleTraceStateSnapshot(`validate-open-bindings:${reason}`);
		}
	}

	/**
	 * Track that a file is open. Notifies diskMirror to start observing.
	 * Also cleans up observers for files that are no longer open in any leaf.
	 */
	private trackOpenFile(path: string): void {
		// Notify disk mirror for the newly opened file
		if (!this.openFilePaths.has(path)) {
			this.diskMirror?.notifyFileOpened(path);
			this.openFilePaths.add(path);
		}

		// Scan all leaves to find which files are actually still open
		const currentlyOpen = new Set<string>();
		this.app.workspace.iterateAllLeaves((leaf) => {
			if (leaf.view instanceof MarkdownView && leaf.view.file) {
				currentlyOpen.add(leaf.view.file.path);
			}
		});

		// Close observers for files no longer open in any leaf
		for (const tracked of this.openFilePaths) {
			if (!currentlyOpen.has(tracked)) {
				this.diskMirror?.notifyFileClosed(tracked);
				this.openFilePaths.delete(tracked);
				this.log(`Closed observer for "${tracked}" (no longer open)`);
			}
		}
		this.scheduleTraceStateSnapshot("track-open-file");
	}

	private getActiveMarkdownPath(): string | null {
		const activeView = this.app.workspace.getActiveViewOfType(MarkdownView);
		return activeView?.file?.path ?? null;
	}

	private updateActiveMarkdownPath(nextPath: string | null, reason: string): void {
		const previousPath = this.activeMarkdownPath;
		this.activeMarkdownPath = nextPath;

		if (!previousPath || previousPath === nextPath) {
			return;
		}

		this.editorBindings?.clearLocalCursor(reason);
		void this.diskMirror?.flushOpenPath(previousPath, reason);
	}

	// -------------------------------------------------------------------
	// Vault event handlers
	// -------------------------------------------------------------------

	private registerVaultEvents(): void {
		// Layout change: clean up observers for closed files
		this.registerEvent(
			this.app.workspace.on("layout-change", () => {
				if (!this.reconciled) return;
				this.editorBindings?.clearLocalCursor("layout-change");
				const currentlyOpen = new Set<string>();
				this.app.workspace.iterateAllLeaves((leaf) => {
					if (leaf.view instanceof MarkdownView && leaf.view.file) {
						currentlyOpen.add(leaf.view.file.path);
					}
				});
				for (const tracked of this.openFilePaths) {
					if (!currentlyOpen.has(tracked)) {
						this.diskMirror?.notifyFileClosed(tracked);
						this.openFilePaths.delete(tracked);
						this.log(`layout-change: closed observer for "${tracked}"`);
					}
				}
				this.updateActiveMarkdownPath(
					this.getActiveMarkdownPath(),
					"layout-change-active-blur",
				);
				const touched = this.editorBindings?.auditBindings("layout-change") ?? 0;
				if (touched > 0) {
					this.log(`Binding health audit (layout-change) — touched ${touched}`);
					this.scheduleTraceStateSnapshot("binding-audit:layout-change");
				}
			}),
		);

		this.registerEvent(
			this.app.workspace.on("active-leaf-change", (leaf) => {
				if (!this.reconciled) return;
				const nextPath =
					leaf?.view instanceof MarkdownView ? (leaf.view.file?.path ?? null) : null;
				this.updateActiveMarkdownPath(nextPath, "active-leaf-change");
				if (!leaf) return;
				const view = leaf.view;
				if (view instanceof MarkdownView) {
					this.editorBindings?.bind(view, this.settings.deviceName);
					if (view.file) {
						this.trackOpenFile(view.file.path);
					}
				}
			}),
		);

		this.registerEvent(
			this.app.workspace.on("file-open", (file) => {
				if (!this.reconciled) return;
				this.updateActiveMarkdownPath(
					file?.path ?? null,
					"file-open-active-change",
				);
				if (!file) return;
				const view = this.app.workspace.getActiveViewOfType(MarkdownView);
				if (view && view.file?.path === file.path) {
					this.editorBindings?.bind(view, this.settings.deviceName);
					this.trackOpenFile(file.path);
				}

				// Prefetch embedded attachments for the opened note
				if (file.path.endsWith(".md") && this.blobSync) {
					this.prefetchEmbeddedAttachments(file);
				}
			}),
		);

		this.registerEvent(
			this.app.vault.on("modify", (file) => {
				if (!this.reconciled) return;
				if (!(file instanceof TFile)) return;

				if (isMarkdownSyncable(file.path, this.excludePatterns)) {
					void this.handleMarkdownModify(file);
				} else if (this.blobSync && isBlobSyncable(file.path, this.excludePatterns) && !this.blobSync.isSuppressed(file.path)) {
					this.blobSync.handleFileChange(file);
				}
			}),
		);

		// Rename: use batched queueRename for atomic folder renames.
		// Both markdown and blob files go through the same rename batch
		// since folder renames affect both types atomically.
		this.registerEvent(
			this.app.vault.on("rename", (file, oldPath) => {
				if (!this.reconciled) return;
				if (!(file instanceof TFile)) return;
				// Rename is relevant if either the old or new path is syncable
				const newSyncable = isMarkdownSyncable(file.path, this.excludePatterns)
					|| isBlobSyncable(file.path, this.excludePatterns);
				const oldSyncable = isMarkdownSyncable(oldPath, this.excludePatterns)
					|| isBlobSyncable(oldPath, this.excludePatterns);
				if (!newSyncable && !oldSyncable) return;
				this.vaultSync?.queueRename(oldPath, file.path);
				this.log(`Rename queued: "${oldPath}" -> "${file.path}"`);
			}),
		);

		this.registerEvent(
			this.app.vault.on("delete", (file) => {
				if (!this.reconciled) return;
				if (!(file instanceof TFile)) return;

				if (isMarkdownSyncable(file.path, this.excludePatterns)) {
					if (this.diskMirror?.consumeDeleteSuppression(file.path)) {
						this.log(`Suppressed delete event for "${file.path}"`);
						return;
					}
					this.editorBindings?.unbindByPath(file.path);
					this.diskMirror?.notifyFileClosed(file.path);
					this.openFilePaths.delete(file.path);

					this.vaultSync?.handleDelete(
						file.path,
						this.settings.deviceName,
					);
					this.log(`Delete: "${file.path}"`);
				} else if (this.blobSync && isBlobSyncable(file.path, this.excludePatterns) && !this.blobSync.isSuppressed(file.path)) {
					this.blobSync.handleFileDelete(file.path, this.settings.deviceName);
					this.log(`Delete (blob): "${file.path}"`);
				}
			}),
		);

		this.registerEvent(
			this.app.vault.on("create", (file) => {
				if (!this.reconciled) return;
				if (!(file instanceof TFile)) return;

				if (isMarkdownSyncable(file.path, this.excludePatterns)) {
					void this.handleMarkdownCreate(file);
				} else if (this.blobSync && isBlobSyncable(file.path, this.excludePatterns) && !this.blobSync.isSuppressed(file.path)) {
					// For blob files, use the same stability check before uploading
					if (this.pendingStabilityChecks.has(file.path)) return;
					this.pendingStabilityChecks.add(file.path);

					void waitForDiskQuiet(this.app, file.path).then((stable) => {
						this.pendingStabilityChecks.delete(file.path);
						if (stable) {
							this.blobSync?.handleFileChange(file);
						} else {
							this.log(`Create (blob): "${file.path}" unstable after timeout, skipping`);
						}
					});
				}
			}),
		);
	}

	// -------------------------------------------------------------------
	// Teardown + reinit (for reset commands)
	// -------------------------------------------------------------------

	/**
	 * Cleanly tear down all sync state: unbind editors, stop disk mirror,
	 * destroy provider + persistence + ydoc, reset all flags.
	 * After this, the plugin is in the same state as before initSync().
	 */
	private teardownSync(): void {
		this.log("teardownSync: tearing down all sync state");

		this.editorBindings?.unbindAll();
		this.diskMirror?.destroy();

		// Persist blob queue before destroying (crash resilience)
		if (this.blobSync) {
			const snapshot = this.blobSync.exportQueue();
			if (snapshot.uploads.length > 0 || snapshot.downloads.length > 0) {
				// Fire-and-forget — teardown can't be async
				void this.saveBlobQueue();
			}
		}
		this.blobSync?.destroy();

		if (this.statusInterval) {
			clearInterval(this.statusInterval);
			this.statusInterval = null;
		}
		if (this.reconcileCooldownTimer) {
			clearTimeout(this.reconcileCooldownTimer);
			this.reconcileCooldownTimer = null;
		}

		this.vaultSync?.destroy();

		this.vaultSync = null;
		this.editorBindings = null;
		this.diskMirror = null;
		this.blobSync = null;
		this.reconciled = false;
		this.reconcileInFlight = false;
		this.reconcilePending = false;
		this.untrackedFiles = [];
		this.lastReconciledGeneration = 0;
		this.awaitingFirstProviderSyncAfterStartup = false;
		this.openFilePaths.clear();
		this.activeMarkdownPath = null;

		this.updateStatusBar("disconnected");
	}

	// -------------------------------------------------------------------
	// Commands
	// -------------------------------------------------------------------

	private registerCommands(): void {
		this.addCommand({
			id: "vault-crdt-sync-reconnect",
			name: "Reconnect to sync server",
			callback: () => {
				if (this.vaultSync) {
					this.vaultSync.provider.disconnect();
					this.vaultSync.provider.connect();
					new Notice("Vault CRDT sync: reconnecting...");
				}
			},
		});

		this.addCommand({
			id: "vault-crdt-sync-force-reconcile",
			name: "Force reconcile vault with CRDT",
			callback: () => {
				if (!this.vaultSync) return;
				const mode = this.vaultSync.getSafeReconcileMode();
				void this.runReconciliation(mode).then(() => {
					this.bindAllOpenEditors();
					this.validateAllOpenBindings("manual-reconcile");
				});
			},
		});

		this.addCommand({
			id: "vault-crdt-sync-debug-status",
			name: "Show sync debug info",
			callback: () => {
				const info = this.buildDebugInfo();
				new Notice(info, 10000);
				console.log("[vault-crdt-sync] Debug status:\n" + info);
			},
		});

		this.addCommand({
			id: "vault-crdt-sync-copy-debug",
			name: "Copy debug info to clipboard",
			callback: () => {
				const info = this.buildDebugInfo();
				navigator.clipboard.writeText(info).then(
					() => new Notice("Debug info copied to clipboard."),
					() => new Notice("Failed to copy to clipboard. Check console.", 5000),
				);
				console.log("[vault-crdt-sync] Debug info:\n" + info);
			},
		});

		this.addCommand({
			id: "vault-crdt-sync-show-recent-events",
			name: "Show recent sync events",
			callback: () => {
				const text = this.buildRecentEventsText(80);
				new Notice("Recent sync events printed to console.", 5000);
				console.log("[vault-crdt-sync] Recent sync events:\n" + text);
			},
		});

		this.addCommand({
			id: "vault-crdt-sync-export-diagnostics",
			name: "Export sync diagnostics",
			callback: () => {
				void this.exportDiagnostics();
			},
		});

		this.addCommand({
			id: "vault-crdt-sync-import-untracked",
			name: "Import untracked files now",
			callback: () => {
				if (!this.vaultSync) {
					new Notice("Sync not initialized");
					return;
				}
				if (this.untrackedFiles.length === 0) {
					new Notice("No untracked files to import.");
					return;
				}
				const count = this.untrackedFiles.length;
				void this.importUntrackedFiles().then(() => {
					new Notice(`Imported ${count} untracked file(s).`);
				});
			},
		});

		this.addCommand({
			id: "vault-crdt-sync-reset-cache",
			name: "Reset local cache (re-sync from server)",
			callback: () => {
				if (!this.vaultSync) {
					new Notice("Sync not initialized");
					return;
				}

				const vaultId = this.settings.vaultId;
				new ConfirmModal(
					this.app,
					"Reset local cache",
					"This will clear the local IndexedDB cache and re-sync from the server. " +
					"Your disk files and server state are not affected. Continue?",
					async () => {
						this.log("Reset cache: starting");
						new Notice("Vault CRDT sync: clearing cache and re-syncing...");

						this.teardownSync();

						try {
							await VaultSync.deleteIdb(vaultId);
							this.log("Reset cache: IDB deleted");
						} catch (err) {
							console.error("[vault-crdt-sync] Failed to delete IDB:", err);
						}

						this.log("Reset cache: reinitializing");
						await this.initSync();
						new Notice("Vault CRDT sync: cache reset complete.");
					},
				).open();
			},
		});

		// --- Snapshot commands ---

		this.addCommand({
			id: "vault-crdt-sync-snapshot-now",
			name: "Take snapshot now",
			callback: async () => {
				if (!this.vaultSync) {
					new Notice("Sync not initialized");
					return;
				}
				if (!this.vaultSync.connected) {
					new Notice("Not connected to server — cannot create snapshot.");
					return;
				}

				new Notice("Creating snapshot...");
				try {
					const result = await requestSnapshotNow(
						this.settings,
						this.settings.deviceName,
						this.getTraceHttpContext(),
					);
					if (result.status === "created" && result.index) {
						new Notice(
							`Snapshot created: ${result.index.markdownFileCount} notes, ` +
							`${result.index.blobFileCount} attachments ` +
							`(${Math.round(result.index.crdtSizeBytes / 1024)} KB)`,
						);
					} else if (result.status === "unavailable") {
						new Notice(`Snapshot unavailable: ${result.reason ?? "R2 not configured"}`);
					} else {
						new Notice("Snapshot created.");
					}
				} catch (err) {
					console.error("[vault-crdt-sync] Snapshot failed:", err);
					new Notice(`Snapshot failed: ${err}`);
				}
			},
		});

		this.addCommand({
			id: "vault-crdt-sync-snapshot-list",
			name: "Browse and restore snapshots",
			callback: async () => {
				if (!this.vaultSync) {
					new Notice("Sync not initialized");
					return;
				}
				if (!this.vaultSync.connected) {
					new Notice("Not connected to server — cannot browse snapshots.");
					return;
				}
				await this.showSnapshotList();
			},
		});

		// --- Reset commands ---

		this.addCommand({
			id: "vault-crdt-sync-nuclear-reset",
			name: "Nuclear reset (wipe CRDT, re-seed from disk)",
			callback: () => {
				if (!this.vaultSync) {
					new Notice("Sync not initialized");
					return;
				}

				const pathCount = this.vaultSync.pathToId.size;
				new ConfirmModal(
					this.app,
					"Nuclear reset",
					`This will wipe all CRDT state (${pathCount} files) on both this device and the server, ` +
					`clear the local cache, then re-seed everything from your current disk files. ` +
					`Other connected devices will also see the reset. This cannot be undone. Continue?`,
					async () => {
						this.log("Nuclear reset: starting");
						new Notice("Vault CRDT sync: nuclear reset in progress...");

						// Clear CRDT maps BEFORE teardown so the deletions propagate
						// to the server while the provider is still connected.
						const counts = this.vaultSync!.clearAllMaps();
						this.log(
							`Nuclear reset: cleared ${counts.pathCount} paths, ` +
							`${counts.idCount} texts, ${counts.metaCount} meta, ` +
							`${counts.blobCount} blob paths`,
						);

						// Give the provider a moment to sync the deletions to server
						await new Promise((r) => setTimeout(r, 500));

						const vaultId = this.settings.vaultId;
						this.teardownSync();

						try {
							await VaultSync.deleteIdb(vaultId);
							this.log("Nuclear reset: IDB deleted");
						} catch (err) {
							console.error("[vault-crdt-sync] Failed to delete IDB:", err);
						}

						this.log("Nuclear reset: reinitializing (will re-seed from disk)");
						await this.initSync();
						new Notice(
							`Vault CRDT sync: nuclear reset complete. ` +
							`Re-seeded ${this.vaultSync?.pathToId.size ?? 0} files from disk.`,
						);
					},
				).open();
			},
		});
	}

	// -------------------------------------------------------------------
	// Helpers
	// -------------------------------------------------------------------

	/**
	 * When a note opens, parse its embedded links (![[...]]) via Obsidian's
	 * metadata cache and prefetch any missing blob attachments from R2.
	 * This ensures images/PDFs render immediately rather than waiting for
	 * the next reconcile or CRDT observer to trigger the download.
	 */
	private prefetchEmbeddedAttachments(file: TFile): void {
		if (!this.blobSync) return;

		const cache = this.app.metadataCache.getFileCache(file);
		if (!cache?.embeds) return;

		const pathsToFetch: string[] = [];

		for (const embed of cache.embeds) {
			// Resolve the link to an actual vault path.
			// getFirstLinkpathDest handles relative paths, aliases, etc.
			const resolved = this.app.metadataCache.getFirstLinkpathDest(
				embed.link,
				file.path,
			);

			if (resolved) {
				// File already exists on disk — skip
				continue;
			}

			// File doesn't exist on disk. Try to find it in the CRDT blob map.
			// The link could be just a filename (e.g. "image.png") or a path.
			// Check both the raw link text and common attachment patterns.
			const linkPath = (embed.link.split("#")[0] ?? "").split("|")[0] ?? ""; // strip anchors/aliases

			// Search pathToBlob for a matching path
			let blobPath: string | null = null;
			this.vaultSync?.pathToBlob.forEach((_ref, candidatePath) => {
				if (blobPath) return; // already found
				// Exact match
				if (candidatePath === linkPath) {
					blobPath = candidatePath;
					return;
				}
				// Filename-only match (Obsidian's default "shortest path" mode)
				const candidateFilename = candidatePath.split("/").pop();
				if (candidateFilename === linkPath) {
					blobPath = candidatePath;
				}
			});

			if (blobPath) {
				pathsToFetch.push(blobPath);
			}
		}

		if (pathsToFetch.length > 0) {
			const queued = this.blobSync.prioritizeDownloads(pathsToFetch);
			if (queued > 0) {
				this.log(`prefetch: queued ${queued} attachments for "${file.path}"`);
			}
		}
	}

	private async handleMarkdownModify(file: TFile): Promise<void> {
		if (await this.diskMirror?.shouldSuppressModify(file)) {
			this.log(`Suppressed modify event for "${file.path}"`);
			return;
		}

		await this.syncFileFromDisk(file);
	}

	private async handleMarkdownCreate(file: TFile): Promise<void> {
		if (await this.diskMirror?.shouldSuppressCreate(file)) {
			this.log(`Suppressed create event for "${file.path}"`);
			return;
		}

		if (this.vaultSync?.isPendingRenameTarget(file.path)) {
			this.log(`Create: "${file.path}" is a pending rename target, skipping import`);
			return;
		}

		// Debounce rapid creates (like unzip or folder paste)
		if (this.pendingStabilityChecks.has(file.path)) return;
		this.pendingStabilityChecks.add(file.path);

		// Give the adapter a short quiet window before importing a new file.
		const settled = await waitForDiskQuiet(this.app, file.path);
		this.pendingStabilityChecks.delete(file.path);
		if (!settled) {
			this.log(`Create: "${file.path}" still changing after quiet window, skipping import`);
			return;
		}

		await this.syncFileFromDisk(file);
	}

	private async syncFileFromDisk(file: TFile): Promise<void> {
		if (!this.vaultSync) return;
		if (!isMarkdownSyncable(file.path, this.excludePatterns)) return;

		const wasBound = this.editorBindings?.isBound(file.path) ?? false;

		// External edit policy gate: control whether disk changes to
		// *closed* files are imported into the CRDT.
		const policy = this.settings.externalEditPolicy;
		if (!wasBound && policy === "never") {
			this.log(`syncFileFromDisk: skipping "${file.path}" (external edit policy: never)`);
			return;
		}

		try {
			const content = await this.app.vault.read(file);

			// File size guard
			if (this.maxFileSize > 0 && content.length > this.maxFileSize) {
				this.log(`syncFileFromDisk: skipping "${file.path}" (${Math.round(content.length / 1024)} KB exceeds limit)`);
				return;
			}
			const existingText = this.vaultSync.getTextForPath(file.path);

			if (wasBound) {
				const handledBound = await this.handleBoundFileSyncGap(
					file,
					content,
					existingText,
				);
				if (handledBound) {
					await this.updateDiskIndexForPath(file.path);
					return;
				}
			}

			if (policy === "never") {
				this.log(`syncFileFromDisk: skipping "${file.path}" (external edit policy: never)`);
				await this.updateDiskIndexForPath(file.path);
				return;
			}

			if (existingText) {
				const crdtContent = existingText.toString();
				if (crdtContent === content) return;

				// Apply a line-level diff to the Y.Text instead of delete-all + insert-all.
				// This preserves CRDT history, cursor positions, and awareness state.
				// Works for both editor-bound files (external edit merges into live editor)
				// and unbound files (background sync).
				this.log(
					`syncFileFromDisk: applying diff to "${file.path}" (${crdtContent.length} -> ${content.length} chars)`,
				);
				applyDiffToYText(existingText, crdtContent, content, "disk-sync");
			} else {
				this.vaultSync.ensureFile(
					file.path,
					content,
					this.settings.deviceName,
				);
			}

			await this.updateDiskIndexForPath(file.path);
		} catch (err) {
			console.error(
				`[vault-crdt-sync] syncFileFromDisk failed for "${file.path}":`,
				err,
			);
		}
	}

	private getOpenMarkdownViewsForPath(path: string): MarkdownView[] {
		const views: MarkdownView[] = [];
		this.app.workspace.iterateAllLeaves((leaf) => {
			if (
				leaf.view instanceof MarkdownView
				&& leaf.view.file?.path === path
			) {
				views.push(leaf.view);
			}
		});
		return views;
	}

	private async handleBoundFileSyncGap(
		file: TFile,
		content: string,
		existingText: ReturnType<VaultSync["getTextForPath"]>,
	): Promise<boolean> {
		const openViews = this.getOpenMarkdownViewsForPath(file.path);
		if (openViews.length === 0) {
			this.trace("trace", "stale-bound-path-without-open-view", {
				path: file.path,
			});
			this.editorBindings?.unbindByPath(file.path);
			this.log(`syncFileFromDisk: cleared stale bound state for "${file.path}" (no live view)`);
			return false;
		}

		const crdtContent = existingText?.toString() ?? null;
		if (crdtContent === content) {
			this.log(`syncFileFromDisk: skipping "${file.path}" (editor-bound, crdt-current)`);
			return true;
		}

		const viewStates = openViews.map((view) => {
			const editorContent = view.editor.getValue();
			const binding = this.editorBindings?.getBindingDebugInfoForView(view) ?? null;
			const collab = this.editorBindings?.getCollabDebugInfoForView(view) ?? null;
			return {
				view,
				editorContent,
				editorMatchesDisk: editorContent === content,
				editorMatchesCrdt: crdtContent != null && editorContent === crdtContent,
				binding,
				collab,
			};
		});

		const localOnlyViews = viewStates.filter(
			(state) => state.editorMatchesDisk && !state.editorMatchesCrdt,
		);
		if (localOnlyViews.length > 0) {
			this.trace("trace", "bound-file-local-only-divergence", {
				path: file.path,
				diskLength: content.length,
				crdtLength: crdtContent?.length ?? null,
				viewCount: localOnlyViews.length,
				views: localOnlyViews.map((state) => ({
					leafId: state.binding?.leafId ?? null,
					storedCmId: state.binding?.storedCmId ?? null,
					liveCmId: state.binding?.liveCmId ?? null,
					cmMatches: state.binding?.cmMatches ?? null,
					hasSyncFacet: state.collab?.hasSyncFacet ?? null,
					awarenessMatchesProvider: state.collab?.awarenessMatchesProvider ?? null,
					yTextMatchesExpected: state.collab?.yTextMatchesExpected ?? null,
					undoManagerMatchesFacet: state.collab?.undoManagerMatchesFacet ?? null,
					facetFileId: state.collab?.facetFileId ?? null,
					expectedFileId: state.collab?.expectedFileId ?? null,
				})),
			});

			if (existingText) {
				this.log(
					`syncFileFromDisk: recovering "${file.path}" ` +
					`(editor-bound local-only divergence: ${crdtContent?.length ?? 0} -> ${content.length} chars)`,
				);
				applyDiffToYText(existingText, crdtContent ?? "", content, "disk-sync-recover-bound");
			} else {
				this.log(
					`syncFileFromDisk: recovering "${file.path}" ` +
					`(editor-bound, missing CRDT text: seeding ${content.length} chars)`,
				);
				this.vaultSync?.ensureFile(
					file.path,
					content,
					this.settings.deviceName,
				);
				}

				for (const state of localOnlyViews) {
					const repaired = this.editorBindings?.heal(
						state.view,
						this.settings.deviceName,
						"bound-file-local-only-divergence",
					) ?? false;
					if (!repaired) {
						this.editorBindings?.rebind(
							state.view,
							this.settings.deviceName,
							"bound-file-local-only-divergence",
						);
					}
				}

			this.scheduleTraceStateSnapshot("bound-file-desync-recovery");
			return true;
		}

		const crdtOnlyViews = viewStates.filter(
			(state) => state.editorMatchesCrdt && !state.editorMatchesDisk,
		);
		if (crdtOnlyViews.length > 0) {
			this.log(`syncFileFromDisk: skipping "${file.path}" (editor-bound, disk lag)`);
			return true;
		}

		this.trace("trace", "bound-file-ambiguous-divergence", {
			path: file.path,
			diskLength: content.length,
			crdtLength: crdtContent?.length ?? null,
			views: viewStates.map((state) => ({
				leafId: state.binding?.leafId ?? null,
				storedCmId: state.binding?.storedCmId ?? null,
				liveCmId: state.binding?.liveCmId ?? null,
				cmMatches: state.binding?.cmMatches ?? null,
				editorMatchesDisk: state.editorMatchesDisk,
				editorMatchesCrdt: state.editorMatchesCrdt,
				hasSyncFacet: state.collab?.hasSyncFacet ?? null,
				awarenessMatchesProvider: state.collab?.awarenessMatchesProvider ?? null,
				yTextMatchesExpected: state.collab?.yTextMatchesExpected ?? null,
				undoManagerMatchesFacet: state.collab?.undoManagerMatchesFacet ?? null,
				facetFileId: state.collab?.facetFileId ?? null,
				expectedFileId: state.collab?.expectedFileId ?? null,
			})),
		});
		this.log(`syncFileFromDisk: skipping "${file.path}" (editor-bound, ambiguous divergence)`);
		this.scheduleTraceStateSnapshot("bound-file-ambiguous");
		return true;
	}

	private async updateDiskIndexForPath(path: string): Promise<void> {
		try {
			const stat = await this.app.vault.adapter.stat(path);
			if (stat) {
				this.diskIndex[path] = { mtime: stat.mtime, size: stat.size };
			}
		} catch {
			// Stat failed, index will be stale for this path.
		}
	}

	/**
	 * Toggle remote cursor visibility via a CSS class on the document body.
	 * The actual cursor styles from y-codemirror.next are hidden when the
	 * class is absent; we add it when showRemoteCursors is true.
	 */
	applyCursorVisibility(): void {
		document.body.toggleClass(
			"vault-crdt-show-cursors",
			this.settings.showRemoteCursors,
		);
	}

	private refreshStatusBar(): void {
		if (!this.vaultSync) {
			this.updateStatusBar("disconnected");
			return;
		}

		if (this.vaultSync.fatalAuthError) {
			this.updateStatusBar("unauthorized");
			return;
		}

		if (!this.reconciled) {
			if (this.vaultSync.connected) {
				this.updateStatusBar("syncing");
			} else if (this.vaultSync.localReady) {
				this.updateStatusBar("loading");
			} else {
				this.updateStatusBar("disconnected");
			}
			return;
		}

		if (this.vaultSync.connected) {
			this.updateStatusBar("connected");
		} else if (this.vaultSync.localReady) {
			this.updateStatusBar("offline");
		} else {
			this.updateStatusBar("disconnected");
		}
	}

	private updateStatusBar(state: SyncStatus): void {
		if (!this.statusBarEl) return;
		const labels: Record<SyncStatus, string> = {
			disconnected: "CRDT: Disconnected",
			loading: "CRDT: Loading cache...",
			syncing: "CRDT: Syncing...",
			connected: "CRDT: Connected",
			offline: "CRDT: Offline",
			error: "CRDT: Error",
			unauthorized: "CRDT: Unauthorized",
		};
		let text = labels[state];

		// Append blob transfer progress if active
		const transfer = this.blobSync?.transferStatus;
		if (transfer) {
			text += ` (${transfer})`;
		}

		this.statusBarEl.setText(text);
	}

	private setupTraceLogger(): void {
		if (!this.settings.debug) return;

		this.traceLogger = new PersistentTraceLogger(this.app, {
			enabled: this.settings.debug,
			deviceName: this.settings.deviceName || "unknown-device",
			vaultId: this.settings.vaultId || "unknown-vault",
		});
		this.trace(
			"trace",
			"trace-session-start",
			{
				host: this.settings.host,
				enableAttachmentSync: this.settings.enableAttachmentSync,
				externalEditPolicy: this.settings.externalEditPolicy,
			},
		);

		this.traceStateInterval = setInterval(() => {
			this.scheduleTraceStateSnapshot("interval");
		}, 5000);
		this.traceServerInterval = setInterval(() => {
			void this.refreshServerTrace();
		}, 15000);
		void this.refreshServerTrace();

		const errorHandler = (event: ErrorEvent) => {
			this.trace("trace", "window-error", {
				message: event.message,
				filename: event.filename,
				lineno: event.lineno,
				colno: event.colno,
			});
			this.traceLogger?.captureCrash("window-error", event.error ?? event.message, {
				filename: event.filename,
				lineno: event.lineno,
				colno: event.colno,
			});
			this.scheduleTraceStateSnapshot("window-error");
		};

		const rejectionHandler = (event: PromiseRejectionEvent) => {
			this.trace("trace", "unhandled-rejection", {
				reason: String(event.reason),
			});
			this.traceLogger?.captureCrash("unhandled-rejection", event.reason);
			this.scheduleTraceStateSnapshot("unhandled-rejection");
		};

		window.addEventListener("error", errorHandler);
		window.addEventListener("unhandledrejection", rejectionHandler);
		this.register(() => {
			window.removeEventListener("error", errorHandler);
			window.removeEventListener("unhandledrejection", rejectionHandler);
		});

		this.register(() => {
			if (this.traceStateInterval) {
				clearInterval(this.traceStateInterval);
				this.traceStateInterval = null;
			}
			if (this.traceServerInterval) {
				clearInterval(this.traceServerInterval);
				this.traceServerInterval = null;
			}
		});
		this.scheduleTraceStateSnapshot("plugin-load");
	}

	private getTraceHttpContext(): TraceHttpContext | undefined {
		return this.traceLogger?.httpContext;
	}

	private trace(
		source: string,
		msg: string,
		details?: TraceEventDetails,
	): void {
		this.traceLogger?.record(source, msg, details);
	}

	private scheduleTraceStateSnapshot(reason: string): void {
		if (!this.traceLogger?.isEnabled) return;
		if (this.traceStateTimer) clearTimeout(this.traceStateTimer);
		this.traceStateTimer = setTimeout(() => {
			this.traceStateTimer = null;
			void this.writeTraceStateSnapshot(reason);
		}, 250);
	}

	private async writeTraceStateSnapshot(reason: string): Promise<void> {
		if (!this.traceLogger?.isEnabled) return;
		const snapshot = await this.buildTraceStateSnapshot(reason);
		this.traceLogger.updateCurrentState(snapshot);
	}

	private async refreshServerTrace(): Promise<void> {
		if (!this.traceLogger?.isEnabled) return;
		if (!this.settings.host || !this.settings.token || !this.settings.vaultId) return;
		if (this.traceServerInFlight) return;

		this.traceServerInFlight = true;
		try {
			const host = this.settings.host.replace(/\/$/, "");
			const roomId = `v1:${this.settings.vaultId}`;
			const roomPathCandidates = [roomId, encodeURIComponent(roomId)];
			let lastError: Error | null = null;

			for (const roomPath of roomPathCandidates) {
				const url = appendTraceParams(
					`${host}/parties/main/${roomPath}/debug/recent`,
					this.getTraceHttpContext(),
				);
				const res = await fetch(url, {
					method: "GET",
					headers: {
						Authorization: `Bearer ${this.settings.token}`,
					},
				});
				if (!res.ok) {
					lastError = new Error(`server debug fetch failed (${res.status})`);
					continue;
				}

				const payload = (await res.json()) as {
					recent?: unknown[];
					roomId?: unknown;
				};
				if (typeof payload.roomId === "string" && payload.roomId !== roomId) {
					lastError = new Error(
						`server debug fetch returned mismatched room (${payload.roomId})`,
					);
					continue;
				}

				this.recentServerTrace = Array.isArray(payload.recent)
					? payload.recent.slice(-120)
					: [];
				this.scheduleTraceStateSnapshot("server-trace-refresh");
				return;
			}

			throw lastError ?? new Error("server debug fetch failed");
		} catch (err) {
			this.trace("trace", "server-trace-fetch-failed", {
				error: String(err),
			});
		} finally {
			this.traceServerInFlight = false;
		}
	}

	private async buildTraceStateSnapshot(reason: string): Promise<Record<string, unknown>> {
		return {
			generatedAt: new Date().toISOString(),
			reason,
			trace: this.getTraceHttpContext() ?? null,
			settings: {
				host: this.settings.host,
				vaultId: this.settings.vaultId,
				deviceName: this.settings.deviceName,
				debug: this.settings.debug,
				enableAttachmentSync: this.settings.enableAttachmentSync,
				externalEditPolicy: this.settings.externalEditPolicy,
			},
			state: {
				reconciled: this.reconciled,
				reconcileInFlight: this.reconcileInFlight,
				reconcilePending: this.reconcilePending,
				awaitingFirstProviderSyncAfterStartup: this.awaitingFirstProviderSyncAfterStartup,
				lastReconciledGeneration: this.lastReconciledGeneration,
				openFileCount: this.openFilePaths.size,
			},
			sync: this.vaultSync?.getDebugSnapshot() ?? null,
			diskMirror: this.diskMirror?.getDebugSnapshot() ?? null,
			blobSync: this.blobSync?.getDebugSnapshot() ?? null,
			openFiles: await this.collectOpenFileTraceState(),
			recentEvents: {
				plugin: this.eventRing.slice(-120),
				sync: this.vaultSync?.getRecentEvents(120) ?? [],
			},
			serverTrace: this.recentServerTrace,
		};
	}

	private async collectOpenFileTraceState(): Promise<Array<Record<string, unknown>>> {
		if (!this.vaultSync) return [];

		const probes: Array<Record<string, unknown>> = [];
		const leaves: MarkdownView[] = [];
		this.app.workspace.iterateAllLeaves((leaf) => {
			if (leaf.view instanceof MarkdownView && leaf.view.file) {
				leaves.push(leaf.view);
			}
		});

		for (const view of leaves) {
			const file = view.file;
			if (!file) continue;

			const path = file.path;
			const editorContent = view.editor.getValue();
			const diskContent = await this.app.vault.read(file).catch(() => null);
			const crdtContent = this.vaultSync.getTextForPath(path)?.toString() ?? null;
			const binding = this.editorBindings?.getBindingDebugInfoForView(view) ?? null;
			const collab = this.editorBindings?.getCollabDebugInfoForView(view) ?? null;

			const [editorHash, diskHash, crdtHash] = await Promise.all([
				this.hashIfPresent(editorContent),
				this.hashIfPresent(diskContent),
				this.hashIfPresent(crdtContent),
			]);

			probes.push({
				path,
				leafId: binding?.leafId ?? ((view.leaf as unknown as { id?: string }).id ?? path),
				binding,
				collab,
				hashes: {
					editor: editorHash,
					disk: diskHash,
					crdt: crdtHash,
				},
				lengths: {
					editor: editorContent.length,
					disk: diskContent?.length ?? null,
					crdt: crdtContent?.length ?? null,
				},
				editorVsDisk: this.describeContentDiff(editorContent, diskContent),
				editorVsCrdt: this.describeContentDiff(editorContent, crdtContent),
				diskVsCrdt: this.describeContentDiff(diskContent, crdtContent),
			});
		}

		return probes;
	}

	private async hashIfPresent(text: string | null): Promise<string | null> {
		if (text == null) return null;
		return this.sha256Hex(text);
	}

	private describeContentDiff(
		left: string | null,
		right: string | null,
	): Record<string, unknown> {
		if (left == null || right == null) {
			return {
				comparable: false,
				leftLength: left?.length ?? null,
				rightLength: right?.length ?? null,
			};
		}

		const firstDiffIndex = this.findFirstDiffIndex(left, right);
		return {
			comparable: true,
			matches: firstDiffIndex === -1,
			firstDiffIndex: firstDiffIndex === -1 ? null : firstDiffIndex,
			leftLength: left.length,
			rightLength: right.length,
			leftSnippet: firstDiffIndex === -1 ? "" : left.slice(firstDiffIndex, firstDiffIndex + 160),
			rightSnippet: firstDiffIndex === -1 ? "" : right.slice(firstDiffIndex, firstDiffIndex + 160),
		};
	}

	private findFirstDiffIndex(left: string, right: string): number {
		const max = Math.min(left.length, right.length);
		for (let i = 0; i < max; i++) {
			if (left[i] !== right[i]) return i;
		}
		return left.length === right.length ? -1 : max;
	}

	onunload() {
		this.log("Unloading plugin");
		if (this.traceStateTimer) {
			clearTimeout(this.traceStateTimer);
			this.traceStateTimer = null;
		}
		if (this.traceStateInterval) {
			clearInterval(this.traceStateInterval);
			this.traceStateInterval = null;
		}
		if (this.traceServerInterval) {
			clearInterval(this.traceServerInterval);
			this.traceServerInterval = null;
		}
		void this.traceLogger?.shutdown();
		document.body.removeClass("vault-crdt-show-cursors");
		this.teardownSync();
	}

	async loadSettings() {
		const data = (await this.loadData()) as PersistedPluginState | null;
		this.persistedState = { ...(data ?? {}) };
		this.settings = Object.assign(
			{},
			DEFAULT_SETTINGS,
			data as Partial<VaultSyncSettings>,
		);
		// Load disk index from plugin data (stored under _diskIndex key)
		if (data && typeof data._diskIndex === "object" && data._diskIndex !== null) {
			this.diskIndex = data._diskIndex as DiskIndex;
		}
		// Load blob hash cache
		if (data && typeof data._blobHashCache === "object" && data._blobHashCache !== null) {
			this.blobHashCache = data._blobHashCache as BlobHashCache;
		}
		// Load persisted blob queue
		if (data && typeof data._blobQueue === "object" && data._blobQueue !== null) {
			this.savedBlobQueue = data._blobQueue as BlobQueueSnapshot;
		}
		this.refreshPersistedState();
	}

	async saveSettings() {
		await this.persistPluginState();
	}

	private async saveDiskIndex(): Promise<void> {
		await this.persistPluginState();
	}

	private async saveBlobQueue(): Promise<void> {
		if (!this.blobSync) return;
		const snapshot = this.blobSync.exportQueue();
		// Only write if there's actually something to persist
		if (snapshot.uploads.length === 0 && snapshot.downloads.length === 0) return;
		await this.persistPluginState((state) => {
			state._blobQueue = snapshot;
		});
	}

	/**
	 * Clear the persisted blob queue once all transfers are done.
	 * Only writes if there was previously a saved queue.
	 */
	private async clearSavedBlobQueue(): Promise<void> {
		if (!this.persistedState._blobQueue) return;
		await this.persistPluginState((state) => {
			delete state._blobQueue;
		});
	}

	private refreshPersistedState(): void {
		this.persistedState = {
			...this.persistedState,
			...this.settings,
			_diskIndex: this.diskIndex,
			_blobHashCache: this.blobHashCache,
		};
	}

	private async persistPluginState(
		mutate?: (state: PersistedPluginState) => void,
	): Promise<void> {
		// Serialize all plugin data writes so settings/index/blob queue updates
		// cannot clobber each other with interleaved load/merge/save cycles.
		const write = async () => {
			this.refreshPersistedState();
			mutate?.(this.persistedState);
			await this.saveData({ ...this.persistedState });
		};

		this.persistWriteChain = this.persistWriteChain
			.catch(() => undefined)
			.then(write);
		await this.persistWriteChain;
	}

	private buildDebugInfo(): string {
		if (!this.vaultSync) return "Sync not initialized";
		return [
			`Host: ${this.settings.host || "(not set)"}`,
			`Vault ID: ${this.settings.vaultId || "(not set)"}`,
			`Device: ${this.settings.deviceName || "(unnamed)"}`,
			`Trace ID: ${this.getTraceHttpContext()?.traceId ?? "(disabled)"}`,
			`Boot ID: ${this.getTraceHttpContext()?.bootId ?? "(disabled)"}`,
			`Connected: ${this.vaultSync.connected}`,
			`Local ready: ${this.vaultSync.localReady}`,
			`Provider synced: ${this.vaultSync.providerSynced}`,
			`Initialized (sentinel): ${this.vaultSync.isInitialized}`,
			`Reconcile mode: ${this.vaultSync.getSafeReconcileMode()}`,
			`Reconciled: ${this.reconciled}`,
			`Connection generation: ${this.vaultSync.connectionGeneration}`,
			`Last reconciled gen: ${this.lastReconciledGeneration}`,
			`Fatal auth error: ${this.vaultSync.fatalAuthError}`,
			`IndexedDB error: ${this.vaultSync.idbError}`,
			`CRDT paths: ${this.vaultSync.pathToId.size}`,
			`Blob paths: ${this.vaultSync.pathToBlob.size}`,
			`Untracked files: ${this.untrackedFiles.length}`,
			`Active disk observers: ${this.diskMirror?.activeObserverCount ?? 0}`,
			`External edit policy: ${this.settings.externalEditPolicy}`,
			`Attachment sync: ${this.settings.enableAttachmentSync ? "enabled" : "disabled"}`,
			...(this.blobSync ? [
				`Pending uploads: ${this.blobSync.pendingUploads}`,
				`Pending downloads: ${this.blobSync.pendingDownloads}`,
			] : []),
			`Open files: ${this.openFilePaths.size}`,
			`Server trace events: ${this.recentServerTrace.length}`,
			`Remote cursors: ${this.settings.showRemoteCursors ? "shown" : "hidden"}`,
		].join("\n");
	}

	private buildRecentEventsText(limit = 80): string {
		const mainEvents = this.eventRing.slice(-limit).map((e) => `[plugin] ${e.ts} ${e.msg}`);
		const syncEvents = this.vaultSync?.getRecentEvents(limit).map((e) => `[sync]   ${e.ts} ${e.msg}`) ?? [];
		const serverEvents = this.recentServerTrace
			.slice(-limit)
			.map((e) => {
				const entry = e as { ts?: string; event?: string; deviceName?: string; traceId?: string };
				return `[server] ${entry.ts ?? ""} ${entry.event ?? "event"}${entry.deviceName ? ` device=${entry.deviceName}` : ""}${entry.traceId ? ` trace=${entry.traceId}` : ""}`;
			});
		const merged = [...mainEvents, ...syncEvents, ...serverEvents].sort();
		if (merged.length === 0) return "No events recorded yet.";
		return merged.slice(-limit).join("\n");
	}

	private async sha256Hex(text: string): Promise<string> {
		const data = new TextEncoder().encode(text);
		const digest = await crypto.subtle.digest("SHA-256", data);
		return Array.from(new Uint8Array(digest))
			.map((b) => b.toString(16).padStart(2, "0"))
			.join("");
	}

	private async exportDiagnostics(): Promise<void> {
		if (!this.vaultSync) {
			new Notice("Sync not initialized");
			return;
		}

		new Notice("Exporting sync diagnostics...");
		const startedAt = Date.now();

		const diskFiles = this.app.vault.getMarkdownFiles()
			.filter((f) => isMarkdownSyncable(f.path, this.excludePatterns));

		const crdtPaths = new Set<string>();
		this.vaultSync.pathToId.forEach((_id, path) => {
			if (isMarkdownSyncable(path, this.excludePatterns)) {
				crdtPaths.add(path);
			}
		});

		const diskHashes = new Map<string, { hash: string; length: number }>();
		for (const file of diskFiles) {
			try {
				const content = await this.app.vault.read(file);
				diskHashes.set(file.path, {
					hash: await this.sha256Hex(content),
					length: content.length,
				});
			} catch (err) {
				this.log(`diagnostics: failed to read disk file "${file.path}": ${String(err)}`);
			}
		}

		const crdtHashes = new Map<string, { hash: string; length: number }>();
		for (const path of crdtPaths) {
			const ytext = this.vaultSync.getTextForPath(path);
			if (!ytext) continue;
			const content = ytext.toString();
			crdtHashes.set(path, {
				hash: await this.sha256Hex(content),
				length: content.length,
			});
		}

		const allPaths = new Set<string>([
			...Array.from(diskHashes.keys()),
			...Array.from(crdtHashes.keys()),
		]);

		const missingOnDisk: string[] = [];
		const missingInCrdt: string[] = [];
		const hashMismatches: Array<{ path: string; diskHash: string; crdtHash: string; diskLength: number; crdtLength: number }> = [];

		for (const path of allPaths) {
			const disk = diskHashes.get(path);
			const crdt = crdtHashes.get(path);
			if (!disk && crdt) {
				missingOnDisk.push(path);
				continue;
			}
			if (disk && !crdt) {
				missingInCrdt.push(path);
				continue;
			}
			if (disk && crdt && disk.hash !== crdt.hash) {
				hashMismatches.push({
					path,
					diskHash: disk.hash,
					crdtHash: crdt.hash,
					diskLength: disk.length,
					crdtLength: crdt.length,
				});
			}
		}

		const diagnostics = {
			generatedAt: new Date().toISOString(),
			generationMs: Date.now() - startedAt,
			trace: this.getTraceHttpContext() ?? null,
			settings: {
				host: this.settings.host,
				tokenPrefix: this.settings.token ? `${this.settings.token.slice(0, 8)}...` : "",
				vaultId: this.settings.vaultId,
				deviceName: this.settings.deviceName,
				debug: this.settings.debug,
				enableAttachmentSync: this.settings.enableAttachmentSync,
				externalEditPolicy: this.settings.externalEditPolicy,
			},
			state: {
				reconciled: this.reconciled,
				reconcileInFlight: this.reconcileInFlight,
				reconcilePending: this.reconcilePending,
				awaitingFirstProviderSyncAfterStartup: this.awaitingFirstProviderSyncAfterStartup,
				lastReconciledGeneration: this.lastReconciledGeneration,
				connected: this.vaultSync.connected,
				providerSynced: this.vaultSync.providerSynced,
				localReady: this.vaultSync.localReady,
				connectionGeneration: this.vaultSync.connectionGeneration,
				fatalAuthError: this.vaultSync.fatalAuthError,
				idbError: this.vaultSync.idbError,
				pathToIdCount: this.vaultSync.pathToId.size,
				blobPathCount: this.vaultSync.pathToBlob.size,
				diskFileCount: diskFiles.length,
				openFileCount: this.openFilePaths.size,
			},
			hashDiff: {
				missingOnDisk,
				missingInCrdt,
				hashMismatches,
				matchingCount: allPaths.size - missingOnDisk.length - missingInCrdt.length - hashMismatches.length,
				totalCompared: allPaths.size,
			},
			recentEvents: {
				plugin: this.eventRing.slice(-240),
				sync: this.vaultSync.getRecentEvents(240),
			},
			openFiles: await this.collectOpenFileTraceState(),
			diskMirror: this.diskMirror?.getDebugSnapshot() ?? null,
			blobSync: this.blobSync?.getDebugSnapshot() ?? null,
			serverTrace: this.recentServerTrace,
		};

		const diagDir = normalizePath(`${this.app.vault.configDir}/plugins/vault-crdt-sync/diagnostics`);
		if (!(await this.app.vault.adapter.exists(diagDir))) {
			await this.app.vault.adapter.mkdir(diagDir);
		}

		const stamp = new Date().toISOString().replace(/[:.]/g, "-");
		const fileName = `sync-diagnostics-${stamp}-${this.settings.deviceName || "device"}.json`;
		const outPath = normalizePath(`${diagDir}/${fileName}`);
		await this.app.vault.adapter.write(outPath, JSON.stringify(diagnostics, null, 2));

		this.log(
			`Diagnostics exported: ${outPath} ` +
			`(missingOnDisk=${missingOnDisk.length}, missingInCrdt=${missingInCrdt.length}, mismatches=${hashMismatches.length})`,
		);
		new Notice(`Sync diagnostics exported to ${outPath}`, 10000);
	}

	// -------------------------------------------------------------------
	// Snapshot helpers
	// -------------------------------------------------------------------

	/**
	 * Request the daily snapshot from the server.
	 * Called after provider syncs during startup.
	 * Silent noop if R2 isn't configured or snapshot already taken today.
	 */
	private async triggerDailySnapshot(): Promise<void> {
		try {
			const result = await requestDailySnapshot(
				this.settings,
				this.settings.deviceName,
				this.getTraceHttpContext(),
			);
			if (result.status === "created") {
				this.log(`Daily snapshot created: ${result.snapshotId}`);
			} else if (result.status === "noop") {
				this.log(`Daily snapshot: already taken today`);
			} else {
				this.log(`Daily snapshot: ${result.reason ?? "unavailable"}`);
			}
		} catch (err) {
			// Don't spam the user — snapshot failure is non-critical
			console.warn("[vault-crdt-sync] Daily snapshot failed:", err);
		}
	}

	/**
	 * Show a list of available snapshots and let the user pick one to diff/restore.
	 */
	private async showSnapshotList(): Promise<void> {
		new Notice("Loading snapshots...");

		try {
			const snapshots = await fetchSnapshotList(
				this.settings,
				this.getTraceHttpContext(),
			);

			if (snapshots.length === 0) {
				new Notice("No snapshots found. Take a snapshot first.");
				return;
			}

			new SnapshotListModal(this.app, snapshots, async (selected) => {
				await this.showSnapshotDiff(selected);
			}).open();
		} catch (err) {
			console.error("[vault-crdt-sync] Failed to list snapshots:", err);
			new Notice(`Failed to list snapshots: ${err}`);
		}
	}

	/**
	 * Download a snapshot, compute diff against current CRDT, and show the restore UI.
	 */
	private async showSnapshotDiff(snapshot: SnapshotIndex): Promise<void> {
		if (!this.vaultSync) return;

		new Notice("Downloading snapshot...");

		try {
			const snapshotDoc = await downloadSnapshot(
				this.settings,
				snapshot,
				this.getTraceHttpContext(),
			);
			const diff = diffSnapshot(snapshotDoc, this.vaultSync.ydoc);

			let destroyed = false;
			const cleanup = () => {
				if (!destroyed) {
					destroyed = true;
					snapshotDoc.destroy();
				}
			};

			new SnapshotDiffModal(
				this.app,
				snapshot,
				diff,
				async (markdownPaths, blobPaths) => {
					if (!this.vaultSync) return;

					// --- Pre-restore backup ---
					// Save current content of files we're about to overwrite
					// so the user can recover if the restore goes wrong.
					const backupDir = `.obsidian/plugins/vault-crdt-sync/restore-backups/${new Date().toISOString().replace(/[:.]/g, "-")}`;
					let backedUp = 0;
					for (const path of markdownPaths) {
						try {
							const file = this.app.vault.getAbstractFileByPath(path);
							if (file instanceof TFile) {
								const content = await this.app.vault.read(file);
								const backupPath = `${backupDir}/${path}`;
								// Ensure parent directories exist
								const parentDir = backupPath.substring(0, backupPath.lastIndexOf("/"));
								if (parentDir && !this.app.vault.getAbstractFileByPath(parentDir)) {
									await this.app.vault.createFolder(parentDir);
								}
								await this.app.vault.create(backupPath, content);
								backedUp++;
							}
						} catch (err) {
							// Non-fatal: file might not exist on disk (undelete case)
							this.log(`Backup skipped for "${path}": ${err}`);
						}
					}
					if (backedUp > 0) {
						this.log(`Pre-restore backup: ${backedUp} files saved to ${backupDir}`);
					}

					const result = restoreFromSnapshot(snapshotDoc, this.vaultSync.ydoc, {
						markdownPaths,
						blobPaths,
						device: this.settings.deviceName,
					});

					// Flush restored files to disk
					for (const path of markdownPaths) {
						await this.diskMirror?.flushWrite(path, true);
					}

					// Kick blob downloads for restored blob references
					if (blobPaths.length > 0 && this.blobSync) {
						const queued = this.blobSync.prioritizeDownloads(blobPaths);
						if (queued > 0) {
							this.log(`Restore: queued ${queued} blob downloads`);
						}
					}

					// Re-bind editors for restored files
					this.bindAllOpenEditors();
					this.validateAllOpenBindings("snapshot-restore");

					const parts: string[] = [];
					if (result.markdownRestored > 0) parts.push(`${result.markdownRestored} files restored`);
					if (result.markdownUndeleted > 0) parts.push(`${result.markdownUndeleted} files undeleted`);
					if (result.blobsRestored > 0) parts.push(`${result.blobsRestored} attachments restored`);
					if (backedUp > 0) parts.push(`backup in ${backupDir}`);

					const msg = parts.length > 0
						? `Restore complete: ${parts.join(", ")}.`
						: "No changes were applied.";
					new Notice(msg, 8000);
					this.log(`Restore from snapshot ${snapshot.snapshotId}: ${msg}`);

					cleanup();
				},
				cleanup,
			).open();
		} catch (err) {
			console.error("[vault-crdt-sync] Snapshot diff failed:", err);
			new Notice(`Failed to load snapshot: ${err}`);
		}
	}

	private log(msg: string): void {
		this.eventRing.push({ ts: new Date().toISOString(), msg });
		if (this.eventRing.length > 600) {
			this.eventRing.splice(0, this.eventRing.length - 600);
		}
		this.trace("plugin", msg);
		if (this.settings.debug) {
			console.log(`[vault-crdt-sync] ${msg}`);
		}
	}
}

/**
 * Simple confirmation modal with a message and confirm/cancel buttons.
 */
class ConfirmModal extends Modal {
	private title: string;
	private message: string;
	private onConfirm: () => void | Promise<void>;

	constructor(
		app: import("obsidian").App,
		title: string,
		message: string,
		onConfirm: () => void | Promise<void>,
	) {
		super(app);
		this.title = title;
		this.message = message;
		this.onConfirm = onConfirm;
	}

	onOpen() {
		const { contentEl } = this;
		contentEl.empty();

		contentEl.createEl("h3", { text: this.title });
		contentEl.createEl("p", { text: this.message });

		const buttonRow = contentEl.createDiv({ cls: "modal-button-container" });

		buttonRow
			.createEl("button", { text: "Cancel" })
			.addEventListener("click", () => this.close());

		const confirmBtn = buttonRow.createEl("button", {
			text: "Confirm",
			cls: "mod-warning",
		});
		confirmBtn.addEventListener("click", () => {
			this.close();
			void this.onConfirm();
		});
	}

	onClose() {
		this.contentEl.empty();
	}
}

/**
 * Modal that lists available snapshots and lets the user pick one.
 */
class SnapshotListModal extends Modal {
	constructor(
		app: import("obsidian").App,
		private snapshots: SnapshotIndex[],
		private onSelect: (snapshot: SnapshotIndex) => void | Promise<void>,
	) {
		super(app);
	}

	onOpen() {
		const { contentEl } = this;
		contentEl.empty();

		contentEl.createEl("h3", { text: "Available snapshots" });
		contentEl.createEl("p", {
			text: `${this.snapshots.length} snapshot(s) found. Select one to see a diff and restore files.`,
			cls: "setting-item-description",
		});

		const list = contentEl.createDiv({ cls: "snapshot-list" });

		for (const snap of this.snapshots) {
			const item = list.createDiv({ cls: "snapshot-list-item" });
			item.style.padding = "8px 0";
			item.style.borderBottom = "1px solid var(--background-modifier-border)";
			item.style.cursor = "pointer";

			const date = new Date(snap.createdAt);
			const dateStr = date.toLocaleDateString(undefined, {
				year: "numeric",
				month: "short",
				day: "numeric",
				hour: "2-digit",
				minute: "2-digit",
			});

			const title = item.createEl("div");
			title.createEl("strong", { text: dateStr });
			if (snap.triggeredBy) {
				title.createEl("span", {
					text: ` (${snap.triggeredBy})`,
					cls: "setting-item-description",
				});
			}

			item.createEl("div", {
				text: `${snap.markdownFileCount} notes, ${snap.blobFileCount} attachments ` +
					`(${Math.round(snap.crdtSizeBytes / 1024)} KB)`,
				cls: "setting-item-description",
			});

			item.addEventListener("click", () => {
				this.close();
				void this.onSelect(snap);
			});

			// Hover effect
			item.addEventListener("mouseenter", () => {
				item.style.backgroundColor = "var(--background-modifier-hover)";
			});
			item.addEventListener("mouseleave", () => {
				item.style.backgroundColor = "";
			});
		}
	}

	onClose() {
		this.contentEl.empty();
	}
}

/**
 * Modal that shows a diff between a snapshot and the current CRDT state.
 * Lets the user select files to restore.
 */
class SnapshotDiffModal extends Modal {
	private selectedMd = new Set<string>();
	private selectedBlobs = new Set<string>();
	/** Set to true when restore is initiated — prevents cleanup from running twice. */
	private didRestore = false;

	constructor(
		app: import("obsidian").App,
		private snapshot: SnapshotIndex,
		private diff: SnapshotDiff,
		private onRestore: (markdownPaths: string[], blobPaths: string[]) => void | Promise<void>,
		private cleanup: () => void,
	) {
		super(app);
	}

	onOpen() {
		const { contentEl } = this;
		contentEl.empty();

		const date = new Date(this.snapshot.createdAt);
		const dateStr = date.toLocaleDateString(undefined, {
			year: "numeric",
			month: "short",
			day: "numeric",
			hour: "2-digit",
			minute: "2-digit",
		});
		contentEl.createEl("h3", { text: `Snapshot: ${dateStr}` });

		const { diff } = this;
		const totalChanges = diff.deletedSinceSnapshot.length +
			diff.contentChanged.length +
			diff.blobsDeletedSinceSnapshot.length +
			diff.blobsChanged.length;

		if (totalChanges === 0 && diff.createdSinceSnapshot.length === 0) {
			contentEl.createEl("p", { text: "No differences found between the snapshot and current state." });
			return;
		}

		contentEl.createEl("p", {
			text: "Select files to restore from the snapshot. " +
				"Created-since-snapshot files are shown for reference but cannot be \"restored\" (they didn't exist yet).",
			cls: "setting-item-description",
		});

		// --- Deleted since snapshot (can restore = undelete) ---
		if (diff.deletedSinceSnapshot.length > 0) {
			this.renderSection(
				contentEl,
				"Deleted since snapshot (can undelete)",
				diff.deletedSinceSnapshot.map((d) => d.path),
				this.selectedMd,
			);
		}

		// --- Content changed (can restore to snapshot version) ---
		if (diff.contentChanged.length > 0) {
			this.renderSection(
				contentEl,
				"Content changed since snapshot",
				diff.contentChanged.map((d) => d.path),
				this.selectedMd,
			);
		}

		// --- Created since snapshot (informational only) ---
		if (diff.createdSinceSnapshot.length > 0) {
			const section = contentEl.createDiv();
			section.createEl("h4", { text: `Created since snapshot (${diff.createdSinceSnapshot.length})` });
			const listEl = section.createEl("ul");
			for (const path of diff.createdSinceSnapshot) {
				listEl.createEl("li", { text: path, cls: "setting-item-description" });
			}
		}

		// --- Blob changes ---
		if (diff.blobsDeletedSinceSnapshot.length > 0) {
			this.renderSection(
				contentEl,
				"Attachments deleted since snapshot",
				diff.blobsDeletedSinceSnapshot.map((d) => d.path),
				this.selectedBlobs,
			);
		}

		if (diff.blobsChanged.length > 0) {
			this.renderSection(
				contentEl,
				"Attachments changed since snapshot",
				diff.blobsChanged.map((d) => d.path),
				this.selectedBlobs,
			);
		}

		// --- Unchanged summary ---
		if (diff.unchanged.length > 0) {
			contentEl.createEl("p", {
				text: `${diff.unchanged.length} file(s) unchanged.`,
				cls: "setting-item-description",
			});
		}

		// --- Restore button ---
		const buttonRow = contentEl.createDiv({ cls: "modal-button-container" });
		buttonRow.style.marginTop = "16px";

		buttonRow
			.createEl("button", { text: "Cancel" })
			.addEventListener("click", () => this.close());

		const restoreBtn = buttonRow.createEl("button", {
			text: "Restore selected",
			cls: "mod-cta",
		});
		restoreBtn.addEventListener("click", () => {
			const mdPaths = Array.from(this.selectedMd);
			const blobPaths = Array.from(this.selectedBlobs);

			if (mdPaths.length === 0 && blobPaths.length === 0) {
				new Notice("No files selected for restore.");
				return;
			}

			this.didRestore = true;
			this.close();
			void this.onRestore(mdPaths, blobPaths);
		});
	}

	private renderSection(
		container: HTMLElement,
		title: string,
		paths: string[],
		selectedSet: Set<string>,
	): void {
		const section = container.createDiv();
		section.createEl("h4", { text: `${title} (${paths.length})` });

		// Select all toggle
		const toggleRow = section.createDiv();
		toggleRow.style.marginBottom = "4px";
		const selectAll = toggleRow.createEl("a", { text: "Select all", href: "#" });
		selectAll.addEventListener("click", (e) => {
			e.preventDefault();
			for (const p of paths) selectedSet.add(p);
			// Re-check all checkboxes in this section
			section.querySelectorAll<HTMLInputElement>("input[type=checkbox]").forEach(
				(cb) => { cb.checked = true; },
			);
		});

		for (const path of paths) {
			const row = section.createDiv();
			row.style.padding = "2px 0";
			const label = row.createEl("label");
			const cb = label.createEl("input", { type: "checkbox" });
			cb.style.marginRight = "6px";
			label.appendText(path);

			cb.addEventListener("change", () => {
				if (cb.checked) {
					selectedSet.add(path);
				} else {
					selectedSet.delete(path);
				}
			});
		}
	}

	onClose() {
		this.contentEl.empty();
		// Always clean up the snapshot doc unless restore already handled it.
		if (!this.didRestore) {
			this.cleanup();
		}
	}
}
