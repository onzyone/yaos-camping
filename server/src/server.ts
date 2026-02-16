import type * as Party from "partykit/server";
import { onConnect } from "y-partykit";

/**
 * PartyKit server for vault CRDT sync.
 *
 * - One room per vault: roomId = "v1:<vaultId>"
 * - Auth: ?token= query param compared to env SYNC_TOKEN
 * - Persistence: y-partykit snapshot mode (survives room hibernation)
 * - Hibernation: enabled for cost/scalability
 *
 * Auth failure:
 *   Sends a structured error message BEFORE closing with 1008,
 *   so the client can reliably detect auth failures even if the
 *   close code/reason gets swallowed by the transport layer.
 */
export default class VaultSyncServer implements Party.Server {
	static options: Party.ServerOptions = {
		hibernate: true,
	};

	constructor(readonly room: Party.Room) {}

	onConnect(conn: Party.Connection, ctx: Party.ConnectionContext) {
		const url = new URL(ctx.request.url);
		const token = url.searchParams.get("token");
		const expected = this.room.env.SYNC_TOKEN as string | undefined;

		if (!expected) {
			console.error(
				`[vault-sync] SYNC_TOKEN env var is not set — rejecting connection to room ${this.room.id}`,
			);
			conn.send(JSON.stringify({ type: "error", code: "server_misconfigured" }));
			conn.close(1008, "server misconfigured");
			return;
		}

		if (!token || token !== expected) {
			console.warn(
				`[vault-sync] Unauthorized connection attempt to room ${this.room.id}`,
			);
			conn.send(JSON.stringify({ type: "error", code: "unauthorized" }));
			conn.close(1008, "unauthorized");
			return;
		}

		console.log(
			`[vault-sync] Client connected to room ${this.room.id}`,
		);

		// Delegate to y-partykit for Yjs sync + snapshot persistence
		return onConnect(conn, this.room, {
			persist: { mode: "snapshot" },
		});
	}
}

VaultSyncServer satisfies Party.Worker;
