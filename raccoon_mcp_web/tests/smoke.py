"""End-to-end checks using only Python's standard library and the compiled Rust binary.

Default: start a disposable server with a temporary database and an available port.
--url: use an already running, ISOLATED test server (creates fixture coordination records).
"""
import argparse
import json
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[2]


class Client:
    def __init__(self, url):
        self.url = url.rstrip('/')
        self.session = None
        self.sequence = 0

    def http(self, path, method='GET', body=None, headers=None):
        request = urllib.request.Request(self.url + path, method=method, data=body, headers=headers or {})
        try:
            response = urllib.request.urlopen(request, timeout=10)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            return response.status, response.headers, response.read()

    def rpc(self, method, params):
        self.sequence += 1
        headers = {'Content-Type': 'application/json', 'MCP-Protocol-Version': '2025-11-25'}
        if self.session:
            headers['Mcp-Session-Id'] = self.session
        status, response_headers, body = self.http('/mcp', 'POST', json.dumps({
            'jsonrpc': '2.0', 'id': self.sequence, 'method': method, 'params': params,
        }).encode(), headers)
        assert status == 200, (status, body)
        self.session = response_headers.get('Mcp-Session-Id', self.session)
        response = json.loads(body)
        assert 'error' not in response, response
        return response['result']

    def tool(self, name, **arguments):
        response = self.rpc('tools/call', {'name': name, 'arguments': arguments})
        assert not response.get('isError'), (name, response)
        return response['structuredContent']

    def snapshot(self, project=None):
        path = '/api/dashboard/snapshot' + (f'?project_id={project}' if project else '')
        status, headers, body = self.http(path)
        assert status == 200, body
        assert headers['Cache-Control'] == 'no-store'
        return json.loads(body)


def check(client):
    status, headers, body = client.http('/dashboard')
    assert status == 200 and b'Mission Control' in body
    assert 'frame-ancestors' in headers['Content-Security-Policy']
    assert client.http('/', headers={'Accept': 'text/html'})[1]['Content-Type'].startswith('text/html')
    assert json.loads(client.http('/')[2])['mcp_endpoint'] == '/mcp'
    for asset in ['app.js', 'model.js', 'demo.js', 'style.css', 'favicon.svg']:
        assert client.http('/dashboard/assets/' + asset)[0] == 200
    assert client.http('/dashboard/assets/missing.js')[0] == 404
    assert client.http('/api/dashboard/snapshot?project_id=invalid')[0] == 400
    assert client.http('/api/dashboard/snapshot', 'POST', b'{}')[0] == 405
    assert client.http('/api/dashboard/snapshot', headers={'Origin': 'https://untrusted.example'})[0] == 403
    assert client.http('/api/dashboard/snapshot', headers={'Sec-Fetch-Site': 'cross-site'})[0] == 403
    client.rpc('initialize', {'protocolVersion': '2025-11-25', 'capabilities': {}, 'clientInfo': {'name': 'dashboard-smoke', 'version': '1'}})
    suffix = uuid.uuid4().hex[:8]
    first = client.tool('connect_orchestrator', project_key=f'command-center-{suffix}', project_display_name='Command Center QA',
                        repository_key='backend', repository_display_name='Backend QA', repository_path=str(ROOT), capabilities={'spawning': True})
    peer = client.tool('connect_orchestrator', project_key=f'command-center-{suffix}', repository_key='frontend',
                       repository_display_name='Frontend QA', repository_path=str(ROOT / 'raccoon_mcp_web'))
    other = client.tool('connect_orchestrator', project_key=f'other-{suffix}', repository_key='isolated', repository_path=str(ROOT))
    actor = first['instance']['instance_id']
    repository = first['repository']['repository_id']
    project = first['project']['project_id']
    feature = client.tool('create_feature', instance_id=actor, title='Live dashboard verification', summary='Real MCP lifecycle rendered in the browser.')
    work = client.tool('create_work_item', instance_id=actor, feature_id=feature['feature_id'], owner_repository_id=repository,
                       title='Verify live coordination telemetry', summary='Connect, claim, activate, publish and complete through MCP tools.')
    client.tool('claim_work_item', instance_id=actor, work_item_id=work['work_item_id'], expected_revision=work['revision'])
    worker = client.tool('create_worker_run', instance_id=actor, role='executor', work_item_id=work['work_item_id'], task_summary='Dashboard smoke: execute a real work card')
    worker_id = worker['worker_run_id']
    client.tool('activate_worker_run', worker_run_id=worker_id)
    live = client.snapshot(project)
    assert len(live['repositories']) == 2
    assert all(row['project_id'] == project for key in ['repositories', 'instances', 'features', 'work_items', 'workers', 'events'] for row in live[key])
    assert not any(row['repository_id'] == other['repository']['repository_id'] for row in live['repositories'])
    assert any(row['worker_run_id'] == worker_id and row['status'] == 'active' for row in live['workers'])
    assert any(row['instance_id'] == actor and row['capabilities']['spawning'] for row in live['instances'])
    # Observe without renewing leases or registering a new agent.
    observed = client.snapshot(project)
    assert live['instances'] == observed['instances']
    report_body = '# Live implementation report\n\nVerified the actual MCP lifecycle.\n\n<script>untrusted body stays text</script>\n\nUnicode: Привет, енот 🦝'
    report = client.tool('publish_message', worker_run_id=worker_id, kind='report', target_repository_ids=[peer['repository']['repository_id']],
                         feature_id=feature['feature_id'], work_item_id=work['work_item_id'], summary='Live verification report ready',
                         body=report_body, artifact_kind='implementation_report', artifact_title='QA implementation report')
    artifact_id = report['artifact']['artifact_id']
    client.tool('complete_worker_run', worker_run_id=worker_id, result_summary='Live lifecycle verified')
    current = client.tool('get_work_item', work_item_id=work['work_item_id'])
    client.tool('update_work_item', instance_id=actor, work_item_id=work['work_item_id'], expected_revision=current['revision'], status='completed', implementation_artifact_id=artifact_id)
    after = client.snapshot(project)
    assert next(row for row in after['workers'] if row['worker_run_id'] == worker_id)['status'] == 'completed'
    completed = next(row for row in after['work_items'] if row['work_item_id'] == work['work_item_id'])
    assert completed['status'] == 'completed' and completed['verification_status'] == 'pending_audit'
    metadata = next(row for row in after['artifacts'] if row['artifact_id'] == artifact_id)
    assert 'content' not in metadata and metadata['work_item_id'] == work['work_item_id']
    status, _, body = client.http('/api/dashboard/artifacts/' + artifact_id)
    assert status == 200 and json.loads(body)['content'] == report_body
    assert client.http('/api/dashboard/artifacts/' + str(uuid.uuid4()))[0] == 404
    assert client.http('/api/dashboard/artifacts/invalid')[0] == 400
    assert client.http('/api/dashboard/artifacts/' + artifact_id, headers={'Origin': 'https://untrusted.example'})[0] == 403
    assert any(row['recipient_count'] == 2 for row in after['events']), 'Fan-out notifications should be merged'
    print('PASS: assets, API compatibility, origin checks, project isolation, lease preservation, live worker lifecycle, report retrieval, event fan-out')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', help='An already running ISOLATED test server; fixture records will be created.')
    args = parser.parse_args()
    if args.url:
        check(Client(args.url))
        return
    binary = ROOT / 'target' / 'debug' / ('raccoon-mcp.exe' if __import__('os').name == 'nt' else 'raccoon-mcp')
    if not binary.exists():
        raise SystemExit('Run cargo build from the repository root first.')
    with tempfile.TemporaryDirectory(prefix='raccoon-dashboard-smoke-') as folder:
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            port = listener.getsockname()[1]
        server = subprocess.Popen([str(binary), '--port', str(port), '--database', str(Path(folder) / 'hub.sqlite3'), '--log-level', 'ERROR'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            client = Client(f'http://127.0.0.1:{port}')
            for attempt in range(50):
                try:
                    if client.http('/healthz')[0] == 200:
                        break
                except (urllib.error.URLError, ConnectionError):
                    time.sleep(.1)
            check(client)
        finally:
            server.terminate()
            server.wait(timeout=10)


if __name__ == '__main__':
    main()
