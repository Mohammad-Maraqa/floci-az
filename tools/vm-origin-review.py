"""Local-only VM REST compatibility and Azure Core LRO polling experiment."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from urllib.parse import urlsplit

import requests
from azure.core import PipelineClient
from azure.core.pipeline.policies import HeadersPolicy, RequestIdPolicy, RetryPolicy, SansIOHTTPPolicy
from azure.core.polling import LROPoller
from azure.core.polling.base_polling import LROBasePolling, OperationResourcePolling
from azure.core.rest import HttpRequest


BASE = 'http://127.0.0.1:14577'
ROOT = Path(__file__).resolve().parents[1]
TRACE = []


def interrupted(signum, frame):
    raise RuntimeError('Validation interrupted by signal ' + str(signum))


signal.signal(signal.SIGTERM, interrupted)


class LoopbackOnlyTrace(SansIOHTTPPolicy):
    def on_request(self, request):
        url = request.http_request.url
        assert urlsplit(url).scheme == 'http' and urlsplit(url).netloc == '127.0.0.1:14577', url
        TRACE.append((request.http_request.method, url))


env = dict(os.environ, QUARKUS_HTTP_HOST='127.0.0.1', FLOCI_AZ_PORT='14577',
           FLOCI_AZ_BASE_URL='http://127.0.0.1:1', FLOCI_AZ_STORAGE_MODE='memory',
           FLOCI_AZ_TLS_ENABLED='false', FLOCI_AZ_SERVICES_VM_MOCKED='true',
           NO_PROXY='127.0.0.1,localhost', no_proxy='127.0.0.1,localhost')
for service in ['EVENT_HUB', 'SERVICE_BUS', 'FUNCTIONS', 'COSMOS', 'SQL', 'POSTGRES',
                'MYSQL', 'MARIA_DB', 'AKS', 'ACI', 'CONTAINER_APPS', 'REDIS', 'ACR']:
    env['FLOCI_AZ_SERVICES_' + service + '_ENABLED'] = 'false'

log = (ROOT / 'vm-origin-jvm.log').open('w', encoding='utf-8')
proc = subprocess.Popen(['java', '-Xmx768m', '-jar', 'target/quarkus-app/quarkus-run.jar'],
                        cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
session = requests.Session()
session.trust_env = False
passed = False
try:
    for attempt in range(120):
        if proc.poll() is not None:
            raise AssertionError('JVM exited before readiness')
        try:
            if session.get(BASE + '/ready', timeout=2).status_code == 200:
                break
        except requests.RequestException:
            pass
        if attempt == 119:
            raise AssertionError('JVM readiness deadline exceeded')
        time.sleep(1)

    # VM tests define their own fixtures; unrelated storage SDK conftest is excluded.
    result = subprocess.run([sys.executable, '-m', 'pytest', '--noconftest', '-v',
                             'tests/test_vm.py'], cwd=ROOT / 'compatibility-tests/sdk-test-python',
                            env=dict(env, FLOCI_AZ_ENDPOINT=BASE), timeout=180)
    assert result.returncode == 0, 'Existing Python VM REST compatibility suite failed'
    print(json.dumps({'phase': 'repository_vm_rest_suite', 'passed': True}), flush=True)

    path = '/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/vm-origin-sdk'
    headers = {'Authorization': 'Bearer fake', 'Content-Type': 'application/json'}
    rg = session.put(BASE + path + '?api-version=2021-04-01',
                     json={'location': 'eastus'}, headers=headers, timeout=10)
    assert rg.status_code in (200, 201), rg.text
    vm = BASE + path + '/providers/Microsoft.Compute/virtualMachines/vm-origin-sdk'
    created = session.put(vm + '?api-version=2024-11-01',
                          json={'location': 'eastus', 'properties': {}}, headers=headers, timeout=10)
    assert created.status_code == 201, created.text

    with PipelineClient(BASE, policies=[LoopbackOnlyTrace(), HeadersPolicy(headers), RequestIdPolicy(),
                                       RetryPolicy(retry_total=0)],
                        connection_timeout=3, read_timeout=10, use_env_settings=False) as client:
        initial = client.send_request(HttpRequest('POST', vm + '/restart?api-version=2024-11-01',
                                                  headers=headers), _return_pipeline_response=True)
        response = initial.http_response
        assert response.status_code == 202
        async_url = response.headers['Azure-AsyncOperation']
        assert response.headers['Location'] == async_url
        assert async_url.startswith(BASE + '/subscriptions/'), async_url
        poller = LROPoller(client, initial, lambda result: result.http_response.json(),
                          LROBasePolling(timeout=0,
                                         lro_algorithms=[OperationResourcePolling('Azure-AsyncOperation')],
                                         connection_timeout=3, read_timeout=10))
        outcome = poller.result(timeout=20)
        assert poller.done() and poller.status() == 'Succeeded', poller.status()
        assert outcome['status'] == 'Succeeded', outcome
        assert ('GET', async_url) in TRACE, TRACE
        print(json.dumps({'phase': 'azure_core_polling', 'azure_core': '1.41.0',
                          'configured_origin': 'http://127.0.0.1:1', 'request_origin': BASE,
                          'advertised_async_url': async_url, 'requests': TRACE, 'status': poller.status()}),
              flush=True)
    passed = True
finally:
    session.close()
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)
    log.close()
    print(json.dumps({'phase': 'task_jvm_stopped', 'exit_code': proc.returncode}), flush=True)
    if passed:
        assert proc.returncode in (0, 143), proc.returncode
