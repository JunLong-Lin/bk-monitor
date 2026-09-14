"""Local integration: real HTTP/DataAPI, Redis Lua, Celery and file artifacts.

UnifyQuery responses and metadata/authorization setup are controlled fixtures;
this does not verify the production query service's scroll protocol.
"""

import json
import shutil
import subprocess
import sqlite3
import tarfile
import tempfile
import threading
import time
from contextlib import contextmanager
from copy import deepcopy
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from unittest import skipUnless
from unittest.mock import Mock, patch

import redis
from celery import Celery
from celery.contrib.testing.worker import start_worker
from django.test import TransactionTestCase, override_settings
from django.db import connections

from apps.api.modules.unify_query import _UnifyQueryApi
from apps.log_search.export_adapter import NativeQuery, export_identity
from apps.log_search.export_coordinator import Coordinator, RedisBudget
from apps.log_search.export_models import ExportPart
from apps.log_search.export_planner import plan_job
from apps.log_search.tasks import sharded_export
from apps.log_search.tests.export_fixtures import create_job


@skipUnless(shutil.which("redis-server"), "private Redis is required")
class LocalLoopTest(TransactionTestCase):
    def test_plan_dispatch_real_celery_http_and_local_artifact(self):
        requests = []

        class Endpoint(BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                path = self.path.split("?", 1)[0]
                requests.append((path, body))
                if path == "/reference/":
                    response = {"series": [{"values": [[0, 2]]}]}
                else:
                    response = {"list": [{"message": "first"}, {"message": "最后"}], "total": 2, "done": True}
                payload = json.dumps(response).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass

        with tempfile.TemporaryDirectory(prefix="bklog-local-loop-") as directory:
            original_main = connections["default"]
            original_main.ensure_connection()
            database_settings = deepcopy(original_main.settings_dict)
            database_settings["NAME"] = str(Path(directory) / "worker.sqlite3")
            with sqlite3.connect(database_settings["NAME"]) as target:
                original_main.connection.backup(target)
            main_connection = type(original_main)(deepcopy(database_settings), alias="default")
            connections["default"] = main_connection
            socket = str(Path(directory) / "redis.sock")
            server = subprocess.Popen(
                ["redis-server", "--port", "0", "--save", "", "--appendonly", "no", "--unixsocket", socket],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            client = redis.Redis(unix_socket_path=socket, socket_timeout=1)
            endpoint = HTTPServer(("127.0.0.1", 0), Endpoint)
            thread = threading.Thread(target=endpoint.serve_forever, daemon=True)
            thread.start()
            celery = Celery("local-export-test", broker="memory://", backend="cache+memory://", set_as_current=False)
            celery.conf.update(worker_prefetch_multiplier=1, task_serializer="json", accept_content=["json"])

            @celery.task(
                bind=True,
                shared=False,
                name="test.sharded_part",
                acks_late=True,
                reject_on_worker_lost=True,
            )
            def execute_test_part(self, part_id):
                # A private file DB survives Celery's connection cleanup and
                # gives the coordinator and worker independent connections.
                original = connections["default"]
                worker_connection = type(original)(deepcopy(database_settings), alias="default")
                connections["default"] = worker_connection
                try:
                    return sharded_export.execute_sharded_export_part.run.__func__(self, part_id)
                finally:
                    worker_connection.close()
                    connections["default"] = original

            task = execute_test_part
            try:
                for _ in range(100):
                    try:
                        client.ping()
                        break
                    except redis.ConnectionError:
                        time.sleep(0.02)
                else:
                    self.fail("private Redis did not start")
                base = {"query_list": [{"table_id": "fixed", "reference_name": "a"}], "bk_biz_id": 2}
                job = create_job(
                    query_snapshot={
                        "unify_query": base,
                        "time_units_per_second": 1000,
                        "search_params": {"bk_biz_id": 2},
                    },
                    routing_snapshot={"query_list": base["query_list"]},
                    policy_snapshot={"query_end_mode": "exclusive"},
                )

                @contextmanager
                def query_factory(job):
                    with export_identity(job), patch("apps.log_search.export_adapter.UnifyQueryApi", _UnifyQueryApi()):
                        handler = Mock()
                        handler._deal_query_result.side_effect = lambda response: {"origin_log_list": response["list"]}
                        query = NativeQuery(job, handler)
                    for name, path in (
                        ("query_ts_raw", "raw"),
                        ("query_ts_reference", "reference"),
                        ("query_ts_raw_with_scroll", "scroll"),
                    ):
                        query.apis[name].url = f"http://127.0.0.1:{endpoint.server_port}/{path}/"
                    with export_identity(job):
                        yield query

                with (
                    override_settings(
                        ASYNC_EXPORT_VERIFIED_QUERY_KINDS=["single"],
                        ASYNC_EXPORT_CONTROL_ENABLED=True,
                        ASYNC_EXPORT_GLOBAL_LIMIT=2,
                        ASYNC_EXPORT_INDEX_LIMIT=1,
                        ASYNC_EXPORT_LEASE_SECONDS=60,
                        ASYNC_EXPORT_NAMESPACE="isolated-local-loop",
                        ASYNC_EXPORT_LOCAL_ARTIFACT_ROOT=directory,
                        ASYNC_EXPORT_ARTIFACT_STORE_FACTORY="apps.log_search.export_worker.local_artifact_store",
                    ),
                    patch.object(sharded_export, "adapter_factory", return_value=query_factory),
                    patch.object(sharded_export, "get_redis_connection", return_value=client),
                ):
                    plan = plan_job(job.pk, query_factory)
                    job.refresh_from_db()
                    self.assertIsNotNone(plan, (job.status, job.error_code, job.error_detail, [p for p, _ in requests]))
                    results = []

                    def publish(part):
                        results.append(
                            task.apply_async(
                                args=[part.pk],
                                headers={
                                    "export_generation": part.dispatch_generation,
                                    "export_lease_id": part.lease_id,
                                },
                            )
                        )

                    coordinator = Coordinator(RedisBudget(client, "isolated-local-loop"), publish)
                    with start_worker(celery, pool="solo", concurrency=1, perform_ping_check=False):
                        coordinator.tick(max_dispatches=1)
                        self.assertEqual(len(results), 1)
                        results[0].get(timeout=10)
                    part = plan.parts.get()
                    self.assertEqual(part.status, ExportPart.Status.SUCCESS)
                    self.assertEqual(part.actual_rows, 2)
                    with tarfile.open(Path(directory) / part.object_key.removeprefix("local:")) as bundle:
                        rows = [json.loads(line) for line in bundle.extractfile("logs.txt")]
                    self.assertEqual(rows[-1], {"message": "最后"})
                    self.assertEqual([p for p, _ in requests], ["/raw/", "/raw/", "/reference/", "/scroll/"])
                    self.assertTrue(requests[-1][1]["clear_cache"])
                    self.assertEqual(requests[-1][1]["slice_max"], 0)
            finally:
                celery.close()
                endpoint.shutdown()
                endpoint.server_close()
                thread.join(timeout=2)
                client.close()
                server.terminate()
                server.wait(timeout=5)
                main_connection.close()
                connections["default"] = original_main
