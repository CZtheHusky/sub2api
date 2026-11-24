#!/usr/bin/env python3
"""Compare two Codex CLI binaries against a loopback Responses WS provider.

Requires Python 3.10+ and websockets 10.4. Run with two executable paths and
optionally repeat --scenario to select cases. Results are JSON Lines.
"""

import argparse
import asyncio
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
from urllib.parse import urlsplit

import websockets


MODEL = "gpt-6.1-sol"
PROBE_TIMEOUT = 30
ERROR_OBSERVATION_SECONDS = 10
TOKEN = "loopback-placeholder"
PUBLIC_HEADERS = {
    "authorization", "connection", "host", "upgrade", "user-agent",
    "sec-websocket-key", "sec-websocket-protocol", "sec-websocket-version",
    "sec-websocket-extensions",
}
ERROR_CODES = {
    "bare-context": "context_length_exceeded",
    "bare-invalid-prompt": "invalid_prompt",
    "spend-key": "api_key_spend_limit_exceeded",
    "spend-project": "project_spend_limit_exceeded",
    "spend-org": "organization_spend_limit_exceeded",
}
SCENARIOS = ("normal", "interrupt", "steer", "close1009", "advertised-size", *ERROR_CODES)


def isolated_env(root):
    codex_home = root / "codex"
    codex_home.mkdir()
    scratch = root / "tmp"
    scratch.mkdir()
    return {
        "PATH": os.defpath,
        "HOME": str(root),
        "CODEX_HOME": str(codex_home),
        "XDG_CONFIG_HOME": str(root),
        "XDG_CACHE_HOME": str(root),
        "XDG_DATA_HOME": str(root),
        "TMPDIR": str(scratch),
        "SUB2API_TEST_TOKEN": TOKEN,
        "HTTP_PROXY": "http://127.0.0.1:1",
        "HTTPS_PROXY": "http://127.0.0.1:1",
        "ALL_PROXY": "http://127.0.0.1:1",
        "NO_PROXY": "127.0.0.1,localhost",
    }


def response(response_id, status, output=()):
    return {
        "id": response_id, "object": "response", "created_at": 1, "model": MODEL,
        "status": status, "output": list(output),
        "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
    }


def assistant_item(response_id, completed=False):
    return {
        "id": f"msg_{response_id}", "type": "message", "role": "assistant",
        "status": "completed" if completed else "in_progress",
        "content": [{"type": "output_text", "text": "LOOPBACK_OK", "annotations": []}]
        if completed else [],
    }


class Probe:
    def __init__(self, scenario):
        self.scenario = scenario
        self.requests = []
        self.handshakes = []
        self.header_sets = []
        self.turns = []
        self.error_sent = asyncio.Event()
        self.error_connection = None
        self.error_sent_at = None
        self.first_error_ms = None
        self.observation_timeout = False
        self.handler_failure = None

    async def serve(self, ws, path):
        try:
            await self.exchange(ws, path)
        except websockets.ConnectionClosed:
            return
        except Exception as exc:
            self.handler_failure = exc
            await ws.close(code=1011, reason="Loopback fixture error")

    async def exchange(self, ws, path):
        if urlsplit(path).path != "/v1/responses":
            raise RuntimeError("Unexpected provider path")
        if ws.request_headers.get("authorization") != f"Bearer {TOKEN}":
            raise RuntimeError("The CLI did not use the fixture token")
        headers = {name.lower() for name, _ in ws.request_headers.raw_items()}
        self.header_sets.append(headers)
        version = re.search(r"\b\d+\.\d+\.\d+\b", ws.request_headers.get("user-agent", ""))
        self.handshakes.append({
            "public_request_fields": sorted(headers & PUBLIC_HEADERS),
            "other_field_count": len(headers - PUBLIC_HEADERS),
            "user_agent_version": version.group() if version else None,
        })
        connection = len(self.handshakes)
        generated = 0
        current = None

        async def emit(kind, **fields):
            await ws.send(json.dumps({"type": kind, **fields}))

        async def finish(response_id):
            item = assistant_item(response_id, completed=True)
            await emit("response.output_item.done", output_index=0, item=item)
            await emit("response.completed", response=response(response_id, "completed", [item]))

        async for raw in ws:
            if not isinstance(raw, str):
                raise RuntimeError("Expected a WebSocket text frame")
            data = json.loads(raw)
            kind = data["type"]
            frame = {"connection": connection, "event": kind, "opcode": "text"}
            self.requests.append(frame)
            if kind == "response.interrupt":
                frame.update(mode=data.get("mode"), response_id=data.get("response_id"))
                if current is None or self.scenario not in ("interrupt", "steer"):
                    raise RuntimeError("Unexpected response.interrupt")
                if self.scenario == "interrupt":
                    await emit("response.interrupt.accepted", response_id=current)
                await finish(current)
                continue
            if kind != "response.create":
                raise RuntimeError("Unexpected client event")
            generate = data.get("generate", True)
            frame.update(
                generate=generate,
                previous_response_id=data.get("previous_response_id"),
                input_roles=[item.get("role") or item.get("type") for item in data.get("input", [])],
            )
            if not generate:
                await emit("response.created", response=response("resp_warmup", "in_progress"))
                await emit("response.completed", response=response("resp_warmup", "completed"))
                continue
            generated += 1
            current = f"resp_{generated}"
            if self.scenario == "close1009":
                await ws.close(code=1009, reason="Loopback size limit")
                return
            if self.scenario in ERROR_CODES:
                body = {"code": ERROR_CODES[self.scenario]}
                if self.scenario.startswith("spend-"):
                    body.update(type="invalid_request_error", message="Loopback provider error")
                self.error_connection = ws
                self.error_sent_at = asyncio.get_running_loop().time()
                await emit("error", error=body)
                self.error_sent.set()
                continue
            await emit("response.created", response=response(current, "in_progress"))
            item = assistant_item(current)
            await emit("response.output_item.added", output_index=0, item=item)
            await emit("response.output_text.delta", output_index=0, content_index=0,
                       item_id=item["id"], delta="LOOPBACK_OK")
            if self.scenario == "normal" or generated > 1:
                await finish(current)

    async def drive(self, app, root):
        clock = asyncio.get_running_loop().time

        async def receive(predicate, timeout=PROBE_TIMEOUT):
            deadline = clock() + timeout
            while True:
                raw = await asyncio.wait_for(app.stdout.readline(), max(0, deadline - clock()))
                if not raw:
                    raise RuntimeError("Codex app-server closed stdout")
                row = json.loads(raw)
                method = row.get("method")
                if method in ("error", "turn/completed") and self.error_sent_at is not None:
                    if self.first_error_ms is None:
                        self.first_error_ms = round((clock() - self.error_sent_at) * 1000, 2)
                if method == "turn/completed":
                    turn = row["params"]["turn"]
                    self.turns.append({
                        "status": turn["status"],
                        "error_kind": (turn.get("error") or {}).get("codexErrorInfo"),
                    })
                if predicate(row):
                    return row

        async def rpc(method, params=None, request_id=None):
            message = {"jsonrpc": "2.0", "method": method}
            if params is not None:
                message["params"] = params
            if request_id is not None:
                message["id"] = request_id
            app.stdin.write((json.dumps(message) + "\n").encode())
            await app.stdin.drain()
            if request_id is None:
                return None
            try:
                row = await receive(lambda event: event.get("id") == request_id)
            except asyncio.TimeoutError as exc:
                raise RuntimeError(f"No JSON-RPC reply for {method}") from exc
            if "error" in row:
                raise RuntimeError(f"{method} failed with JSON-RPC code {row['error'].get('code')}")
            return row["result"]

        async def wait_for_turn(count, timeout=PROBE_TIMEOUT):
            if len(self.turns) < count:
                await receive(lambda _: len(self.turns) >= count, timeout)

        await rpc("initialize", {"clientInfo": {"name": "local-fixture", "version": "1.0"}}, 1)
        await rpc("initialized")
        thread = await rpc("thread/start", {"cwd": str(root), "ephemeral": True}, 2)
        turn_params = {
            "threadId": thread["thread"]["id"],
            "input": [{"type": "text", "text": "Return LOOPBACK_OK"}],
        }
        turn = await rpc("turn/start", turn_params, 3)
        if self.scenario in ("interrupt", "steer"):
            await receive(lambda row: row.get("method") == "item/agentMessage/delta")
            if self.scenario == "interrupt":
                await rpc("turn/interrupt", {
                    "threadId": turn_params["threadId"], "turnId": turn["turn"]["id"],
                }, 4)
            else:
                await rpc("turn/steer", {**turn_params, "expectedTurnId": turn["turn"]["id"]}, 4)
        is_error_scenario = self.scenario in ERROR_CODES
        if is_error_scenario:
            await asyncio.wait_for(self.error_sent.wait(), PROBE_TIMEOUT)
        try:
            await wait_for_turn(1, ERROR_OBSERVATION_SECONDS if is_error_scenario else PROBE_TIMEOUT)
        except asyncio.TimeoutError:
            # These statusless errors can be ignored while the upstream stays open.
            if (self.scenario in ERROR_CODES and self.scenario != "bare-context"
                    and self.first_error_ms is None and self.error_connection.open):
                self.observation_timeout = True
                return
            raise
        if self.scenario == "normal":
            await rpc("turn/start", turn_params, 5)
            await wait_for_turn(2)


async def run_case(binary, scenario):
    probe = Probe(scenario)
    headers = [("x-codex-websocket-max-message-bytes", "1")] if scenario == "advertised-size" else []
    async with websockets.serve(probe.serve, "127.0.0.1", 0, extra_headers=headers) as server:
        port = server.sockets[0].getsockname()[1]
        with tempfile.TemporaryDirectory(prefix="sub2api-codex-ws-") as scratch:
            root = Path(scratch)
            env = isolated_env(root)
            config = Path(env["CODEX_HOME"]) / "config.toml"
            config.write_text(f'''model = "{MODEL}"
model_provider = "fixture"
check_for_update_on_startup = false
[features]
interrupt_on_cancel = {str(scenario == "interrupt").lower()}
[model_providers.fixture]
name = "Loopback fixture"
base_url = "http://127.0.0.1:{port}/v1"
wire_api = "responses"
env_key = "SUB2API_TEST_TOKEN"
supports_websockets = true
request_max_retries = 0
stream_max_retries = 0
''', encoding="utf-8")
            app = await asyncio.create_subprocess_exec(
                binary, "app-server", "--listen", "stdio://",
                cwd=root, env=env, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            try:
                await probe.drive(app, root)
            except Exception:
                if probe.handler_failure is not None:
                    raise RuntimeError("The loopback fixture failed") from probe.handler_failure
                raise
            finally:
                if app.returncode is None:
                    app.terminate()
                    try:
                        await asyncio.wait_for(app.wait(), 5)
                    except asyncio.TimeoutError:
                        app.kill()
                        await app.wait()
                        raise RuntimeError("Codex app-server failed to terminate")
    if probe.handler_failure is not None:
        raise RuntimeError("The loopback fixture failed") from probe.handler_failure
    if not probe.handshakes:
        raise RuntimeError("No WebSocket upgrade reached the fixture")
    return probe


async def compare(binaries, scenarios):
    versions = []
    for binary in binaries:
        with tempfile.TemporaryDirectory(prefix="sub2api-codex-version-") as scratch:
            version = subprocess.check_output(
                [binary, "--version"], cwd=scratch, env=isolated_env(Path(scratch)),
                timeout=PROBE_TIMEOUT, text=True,
            ).strip()
            if not version:
                raise RuntimeError(f"Could not read Codex CLI version from {binary}")
            versions.append(version)

    for scenario in scenarios:
        results = []
        for binary, version in zip(binaries, versions):
            try:
                probe = await run_case(binary, scenario)
            except asyncio.TimeoutError as exc:
                raise RuntimeError(f"{version} {scenario} exceeded the probe deadline") from exc
            except Exception as exc:
                raise RuntimeError(f"{version} {scenario} probe failed") from exc
            print(json.dumps({
                "scenario": scenario,
                "version": version,
                "connections": len(probe.handshakes),
                "handshakes": probe.handshakes,
                "requests": probe.requests,
                "turns": probe.turns,
                "first_error_elapsed_ms": probe.first_error_ms,
                "observation_timeout": probe.observation_timeout,
            }), flush=True)
            results.append(probe.header_sets)
        print(json.dumps({
            "scenario": scenario, "all_header_name_sets_match": results[0] == results[1],
        }), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", help="baseline Codex CLI executable")
    parser.add_argument("candidate", help="candidate Codex CLI executable")
    parser.add_argument("--scenario", choices=SCENARIOS, action="append", help="default: all")
    args = parser.parse_args()
    binaries = []
    for name in (args.baseline, args.candidate):
        binary = shutil.which(name)
        if binary is None:
            parser.error(f"executable not found: {name}")
        binaries.append(Path(binary).absolute())
    asyncio.run(compare(binaries, args.scenario or SCENARIOS))


if __name__ == "__main__":
    main()
