import http.server, json, os, pathlib, subprocess, tempfile, threading
hits=[]
class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self,*args): pass
    def do_POST(self):
        body=json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        hits.append({"path":self.path,"user_agent":self.headers.get("User-Agent"),"model":body.get("model"),"stream":body.get("stream")})
        item={"id":"msg_smoke","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"LOOPBACK_OK","annotations":[]}]}
        response={"id":"resp_smoke","object":"response","created_at":1,"model":"gpt-6.1-sol","status":"completed","output":[item],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}
        events=[{"type":"response.created","response":dict(response,status="in_progress",output=[])},{"type":"response.output_item.done","item":item,"output_index":0},{"type":"response.completed","response":response}]
        wire=''.join('data: '+json.dumps(event)+'\n\n' for event in events).encode()
        self.send_response(200)
        self.send_header("Content-Type","text/event-stream")
        self.send_header("Content-Length",str(len(wire)))
        self.end_headers()
        self.wfile.write(wire)
with http.server.ThreadingHTTPServer(("127.0.0.1",0),Handler) as server, tempfile.TemporaryDirectory(prefix="sub2api-codex-fixture-") as fixture_home:
    threading.Thread(target=server.serve_forever,daemon=True).start()
    pathlib.Path(fixture_home,"config.toml").write_text(f"""model = "gpt-6.1-sol"
model_provider = "loopback"
[model_providers.loopback]
name = "Loopback fixture"
base_url = "http://127.0.0.1:{server.server_port}/v1"
wire_api = "responses"
env_key = "SUB2API_TEST_TOKEN"
supports_websockets = false
""")
    cli_env=dict(os.environ,CODEX_HOME=fixture_home,SUB2API_TEST_TOKEN="loopback-placeholder")
    cli_env.update({k:"" for k in ("HTTP_PROXY","HTTPS_PROXY","ALL_PROXY","http_proxy","https_proxy","all_proxy")})
    cli_env["NO_PROXY"]="127.0.0.1,localhost"
    result=subprocess.run(["codex","exec","--skip-git-repo-check","--ephemeral","--json","Return LOOPBACK_OK"],cwd=fixture_home,env=cli_env,capture_output=True,text=True,timeout=25)
    print(json.dumps({"exit":result.returncode,"requests":hits,"completed":'"turn.completed"' in result.stdout,"mock_answer":"LOOPBACK_OK" in result.stdout},ensure_ascii=False))
    if result.returncode: print(result.stderr[-2200:],result.stdout[-1500:])
    assert result.returncode == 0 and len(hits) == 1 and hits[0]["path"] == "/v1/responses" and hits[0]["stream"] is True and "0.166.0" in hits[0]["user_agent"] and "LOOPBACK_OK" in result.stdout
