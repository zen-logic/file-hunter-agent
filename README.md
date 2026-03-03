# File Hunter Agent

A remote agent for [File Hunter](https://zen-logic.github.io/file-hunter/) that lets the central server catalog files on other machines across your network.

The agent runs on any machine whose files you want to include in the catalog. It connects outbound to the server via WebSocket (works through NAT and firewalls), so no port forwarding is needed on the agent side.

## Requirements

- Python 3.11+
- A running [File Hunter](https://zen-logic.github.io/file-hunter/) server with the Pro extension

## Installation

```bash
git clone https://github.com/zen-logic/file-hunter-agent.git
cd file-hunter-agent
python -m venv venv
source venv/bin/activate   # on Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Configuration

1. In the File Hunter UI, go to **Settings > Agents > Add Agent** and copy the pairing token.

2. Edit `config.json` in the agent directory:

```json
{
    "server_url": "http://your-server:8000",
    "token": "paste-your-token-here",
    "http_host": "0.0.0.0",
    "http_port": 8001,
    "locations": [
        {"name": "Photos", "path": "/mnt/photos"},
        {"name": "Music", "path": "/mnt/music"}
    ]
}
```

- **server_url** — the address of your File Hunter server
- **token** — the pairing token from the server UI
- **locations** — folders on this machine to expose to the catalog. Each needs a **name** (shown in the UI) and an absolute **path**. Only listed paths are accessible — the agent won't serve files outside configured roots.

## Usage

```bash
./filehunter-agent
```

Or with the `--server` and `--token` flags on first run (saves to `config.json`):

```bash
./filehunter-agent --server http://your-server:8000 --token <token>
```

The agent's locations appear in the File Hunter navigation tree as soon as it connects. Scans, previews, and downloads all work through the UI exactly like local locations.

To override the HTTP bind address or port:

```bash
./filehunter-agent --host 0.0.0.0 --port 9001
```

## Running as a systemd service (Linux)

Create a service file at `/etc/systemd/system/filehunter-agent.service`:

```ini
[Unit]
Description=File Hunter Agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=your-user
WorkingDirectory=/home/your-user/file-hunter-agent
ExecStart=/home/your-user/file-hunter-agent/venv/bin/python -m file_hunter_agent
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Replace `your-user` and paths to match your setup. Then enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable filehunter-agent
sudo systemctl start filehunter-agent
```

Check status and logs:

```bash
sudo systemctl status filehunter-agent
journalctl -u filehunter-agent -f
```

## How it works

- **WebSocket** (agent → server) — registration, scan commands, and streaming scan results. Auto-reconnects if the connection drops.
- **HTTP** (server → agent) — the server requests file content from the agent for previews and downloads. Authenticated with the same pairing token.

The agent has no database. All catalog data lives on the server.

## Security

- All HTTP endpoints require the pairing token via `Authorization: Bearer` header.
- File access is restricted to configured location roots. Requests outside those paths are rejected.
- The WebSocket connection is initiated by the agent, not the server.

## License

Copyright 2026 [Zen Logic Ltd.](https://zenlogic.co.uk)
