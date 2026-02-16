# vault-crdt-sync server

PartyKit server for real-time Obsidian vault sync using Yjs CRDTs.

## Architecture

- One PartyKit room per vault (`v1:<vaultId>`)
- [y-partykit](https://docs.partykit.io/reference/y-partykit-api/) handles Yjs sync protocol + snapshot persistence
- Hibernation enabled for cost efficiency on idle rooms
- Simple token auth via query param (compare to `SYNC_TOKEN` env var)

## Setup

```bash
cd server
npm install
```

Create a `.env` file (or copy `.env.example`):

```bash
cp .env.example .env
# Edit .env and set SYNC_TOKEN to a random secret
```

## Local development

```bash
npm run dev
```

This starts the PartyKit dev server at `http://127.0.0.1:1999`.

In the Obsidian plugin settings, set:
- **Host**: `http://127.0.0.1:1999`
- **Token**: the same value as `SYNC_TOKEN` in your `.env`
- **Vault ID**: any string (e.g. `my-vault-dev`)

## Deploy to PartyKit cloud

```bash
# Set the token as a secret (prompted interactively)
npx partykit env add SYNC_TOKEN

# Deploy
npm run deploy
```

After deploy, PartyKit prints the URL (e.g. `https://vault-crdt-sync.yourname.partykit.dev`).  
Use that as **Host** in plugin settings.

## Deploy to your own Cloudflare account

See [PartyKit docs: Deploy to Cloudflare](https://docs.partykit.io/guides/deploy-to-cloudflare/).

## Storage notes

- PartyKit room storage uses Cloudflare Durable Objects (128 KiB per value, 2 KB key limit).
- y-partykit handles sharding large Yjs snapshots across multiple keys automatically.
- Free-tier storage may be cleared periodically. For production, deploy to your own Cloudflare account.
