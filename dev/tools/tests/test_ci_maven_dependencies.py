"""Dependency setup can recover without ever invoking or retrying a test lifecycle."""

import argparse
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import re
import shutil
import socket
import subprocess
import tempfile
import threading
import unittest
import xml.etree.ElementTree as ET
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[3]
from dev.tools.ci import maven_dependencies as PREPARE


class MavenDependenciesTest(unittest.TestCase):
    @unittest.skipUnless(shutil.which("mvn"), "Maven is required for repository routing")
    def test_central_artifacts_avoid_vendor_requests_and_vendor_only_parents_still_resolve(self):
        namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
        repositories = [r.findtext("m:id", namespaces=namespace)
                        for r in ET.parse(ROOT / "pom.xml").findall("m:repositories/m:repository", namespace)]
        if "central" not in repositories:
            repositories.append("central")
        requests = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                repository = self.path.strip("/").split("/", 1)[0]
                artifact = "central-parent" if "/central-parent/" in self.path else "vendor-parent"
                found = repository == ("central" if artifact == "central-parent" else "confluent")
                requests.append((repository, artifact, found))
                if not found:
                    self.send_error(404)
                    return
                parent = ("<project><modelVersion>4.0.0</modelVersion>"
                          "<groupId>test.repository.order</groupId><artifactId>" + artifact
                          + "</artifactId><version>1</version><packaging>pom</packaging></project>").encode()
                body = hashlib.sha1(parent).hexdigest().encode() if self.path.endswith(".sha1") else parent
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory(prefix="repository-routing-") as directory:
                root = Path(directory)
                settings = root / "settings.xml"
                settings.write_text("<settings/>")
                declarations = "".join(
                    f"<repository><id>{name}</id><url>http://127.0.0.1:{server.server_port}/{name}/</url></repository>"
                    for name in repositories)
                for artifact in ("central-parent", "vendor-parent"):
                    pom = root / (artifact + ".pom")
                    pom.write_text(
                        "<project><modelVersion>4.0.0</modelVersion><parent>"
                        "<groupId>test.repository.order</groupId><artifactId>" + artifact
                        + "</artifactId><version>1</version><relativePath/></parent>"
                        "<artifactId>probe</artifactId><packaging>pom</packaging><repositories>"
                        + declarations + "</repositories></project>")
                    for _ in range(2):
                        result = subprocess.run(
                            ["mvn", "-B", "-ntp", "-U", "-s", str(settings), "-gs", str(settings),
                             f"-Dmaven.repo.local={root / 'm2'}", "-f", str(pom), "validate"],
                            capture_output=True, text=True, timeout=30)
                        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                self.assertFalse(any(repo == "confluent" and artifact == "central-parent"
                                     for repo, artifact, found in requests))
                self.assertTrue(any(repo == "confluent" and artifact == "vendor-parent" and found
                                    for repo, artifact, found in requests))
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def options(self):
        return argparse.Namespace(maven="mvn", profile="flink-1.18", projects="streamfusion-runtime",
                                  also_make=True, pom=Path("checkout with spaces/pom.xml"),
                                  repository=Path("repository with spaces"), artifact=[], model_only=False)

    def test_mediated_artifact_fetch_does_not_reintroduce_excluded_transitives(self):
        options = self.options()
        options.artifact = ['org.apache.hive:hive-metastore:2.3.1:jar']
        options.no_transitive = True
        with patch.object(PREPARE.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0)) as run:
            self.assertEqual(0, PREPARE.resolve(options))
        self.assertIn('-Dtransitive=false', run.call_args.args[0])

    def invoke(self, statuses):
        results = [subprocess.CompletedProcess([], status) if isinstance(status, int) else status
                   for status in statuses]
        with patch.object(PREPARE.subprocess, "run", side_effect=results) as run, \
                patch.object(PREPARE.time, "sleep") as sleep:
            status = PREPARE.resolve(self.options())
        return status, run.call_args_list, sleep.call_args_list

    def test_success_needs_no_retry_and_preserves_resolution_scope(self):
        status, calls, sleeps = self.invoke([0])
        self.assertEqual(0, status)
        self.assertFalse(sleeps)
        self.assertEqual(1, len(calls))
        command = calls[0].args[0]
        self.assertNotIn("-U", command)
        self.assertIn("-Pflink-1.18", command)
        self.assertIn("streamfusion-runtime", command)
        self.assertIn("-am", command)
        self.assertIn("checkout with spaces/pom.xml", command)
        self.assertIn("-Dmaven.repo.local=repository with spaces", command)
        self.assertEqual(PREPARE.GOAL, command[-1])
        self.assertFalse({"test", "verify", "package", "install"} & set(command))

    def test_standalone_preparation_resolves_the_mediated_classpath(self):
        options = self.options()
        options.resolve_project = True
        options.define = ["flink.version=2.2.0", "delta.version=4.4.0"]
        options.profile, options.projects, options.also_make = "", None, False
        with patch.object(PREPARE.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)) as run:
            self.assertEqual(0, PREPARE.resolve(options))
        self.assertEqual(["-Dflink.version=2.2.0", "-Ddelta.version=4.4.0",
                          PREPARE.GOAL, PREPARE.PROJECT_GOAL], run.call_args.args[0][-4:])

    def test_consumer_runtime_resolves_the_exact_model_and_plugin_in_separate_steps(self):
        options = self.options()
        options.test_runtime = ["-B", "-ntp", "-e", "-o", "-s", "user settings.xml",
                                "-gs", "global settings.xml", "-f", "generated runtime/pom.xml",
                                "-pl", ":common", "-Pflink1", "-Dflink.version=1.20.1",
                                "-Dflink.version=1.18.1", "-Dmaven.repo.local=isolated m2",
                                "-Dtest=UnchangedITCase", "-Dmaven.test.failure.ignore=false"]
        options.offline_first = True
        with patch.object(PREPARE.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)) as run:
            self.assertEqual(0, PREPARE.resolve(options))
        commands = [call.args[0] for call in run.call_args_list]
        self.assertEqual(2, len(commands))
        expected = [arg for arg in options.test_runtime if arg != "-o"]
        for command, goal_count in zip(commands, (1, 3)):
            self.assertEqual("-o", command[1])
            self.assertEqual(1, command.count("-o"))
            for flag in ("-s", "-gs"):
                self.assertEqual(1, command.count(flag))
            self.assertEqual(expected, command[-len(expected) - goal_count:-goal_count])
            self.assertFalse({"test", "verify", "package", "install", PREPARE.GOAL} & set(command))
        self.assertEqual(PREPARE.PROJECT_GOAL, commands[0][-1])
        self.assertFalse(any("includeArtifactIds" in arg for arg in commands[0]))
        self.assertEqual([PREPARE.PLUGIN_GOAL, "-DincludeGroupIds=org.apache.maven.plugins",
                          "-DincludeArtifactIds=maven-surefire-plugin"], commands[1][-3:])

    def test_consumer_runtime_retries_only_dependency_resolution_and_keeps_failure_blocking(self):
        options = self.options()
        options.test_runtime = ["-o", "-f", "runtime.xml"]
        options.offline_first = True
        for statuses, expected in (((1, 7, 7, 7), 7), ((0, 1, 7, 7, 7), 7), ((0, 1, 7, 0), 0)):
            with self.subTest(statuses=statuses), \
                    patch.object(PREPARE.subprocess, "run", side_effect=[
                        subprocess.CompletedProcess([], status) for status in statuses]) as run, \
                    patch.object(PREPARE.time, "sleep"):
                self.assertEqual(expected, PREPARE.resolve(options))
                commands = [call.args[0] for call in run.call_args_list]
                self.assertEqual(len(statuses), len(commands))
                self.assertEqual(1 if statuses[0] == 0 else 4,
                                 sum(PREPARE.PROJECT_GOAL in command for command in commands))
                for command in commands:
                    self.assertFalse({"test", "verify", "install"} & set(command))
                    if "-U" in command:
                        self.assertNotIn("-o", command)

    def test_consumer_runtime_rejects_lifecycles_and_unbounded_maven_options(self):
        for arguments in (["test"], ["surefire:test@integration-tests"], ["-f"], ["-pl", "-o"],
                          ["--fail-never"], ["-U"], ["org.example:plugin:1:goal"]):
            with self.subTest(arguments=arguments):
                result = subprocess.run(
                    ["python3", str(ROOT / "bin/ci-maven-dependencies.py"), "--test-runtime", *arguments],
                    capture_output=True, text=True)
                self.assertEqual(2, result.returncode)
                self.assertRegex(result.stderr, "Not a Maven test-runtime model option|Missing value")

    def test_project_models_retry_without_building_or_resolving_reactor_artifacts(self):
        options = self.options()
        options.model_only = True
        with patch.object(PREPARE.subprocess, "run", side_effect=[
                subprocess.CompletedProcess([], status) for status in (1, 0)]) as run, \
                patch.object(PREPARE.time, "sleep"):
            self.assertEqual(0, PREPARE.resolve(options))
        command = run.call_args_list[0].args[0]
        self.assertEqual([x for x in command if x != "-U"], [x for x in run.call_args_list[1].args[0] if x != "-U"])
        self.assertIn(PREPARE.MODEL_GOAL, command)
        self.assertIn("-Dexpression=project.id", command)
        self.assertIn("streamfusion-runtime", command)
        self.assertIn("-am", command)
        self.assertNotIn(PREPARE.GOAL, command)
        self.assertFalse({"test", "verify", "package", "install"} & set(command))

    def test_missing_download_is_rechecked_and_can_recover(self):
        status, calls, sleeps = self.invoke([1, 1, 0])
        self.assertEqual(0, status)
        self.assertEqual(3, len(calls))
        self.assertNotIn("-U", calls[0].args[0])
        self.assertTrue(all("-U" in call.args[0] for call in calls[1:]))
        self.assertTrue(15 <= sleeps[0].args[0] <= 22.5)
        self.assertTrue(45 <= sleeps[1].args[0] <= 67.5)

    def test_offline_first_reuses_a_complete_cache_without_an_online_attempt(self):
        options = self.options()
        options.offline_first = True
        with patch.object(PREPARE.subprocess, "run",
                          return_value=subprocess.CompletedProcess([], 0)) as run:
            self.assertEqual(0, PREPARE.resolve(options))
        run.assert_called_once()
        command = run.call_args.args[0]
        self.assertEqual(["mvn", "-o"], command[:2])
        self.assertEqual(PREPARE.GOAL, command[-1])

    def test_offline_first_cache_miss_preserves_bounded_online_recovery(self):
        options = self.options()
        options.offline_first = True
        results = [subprocess.CompletedProcess([], status) for status in (1, 1, 0)]
        with patch.object(PREPARE.subprocess, "run", side_effect=results) as run, \
                patch.object(PREPARE.time, "sleep") as sleep:
            self.assertEqual(0, PREPARE.resolve(options))
        offline, online, retry = [call.args[0] for call in run.call_args_list]
        self.assertEqual(["mvn", "-o", *online[1:]], offline)
        self.assertEqual(["mvn", "-U", *online[1:]], retry)
        sleep.assert_called_once()

    def test_persistent_failure_and_timeout_remain_blocking(self):
        for statuses, expected in (([7, 7, 7], 7),
                                   ([subprocess.TimeoutExpired("mvn", 600)] * 3, 124)):
            with self.subTest(expected=expected):
                status, calls, sleeps = self.invoke(statuses)
                self.assertEqual(expected, status)
                self.assertEqual(3, len(calls))
                self.assertTrue(all(call.kwargs["timeout"] == 600 for call in calls))
                self.assertEqual(2, len(sleeps))

    def test_test_goals_cannot_be_passed_to_the_retry_stage(self):
        result = subprocess.run(["python3", str(ROOT / "dev/tools/ci/maven_dependencies.py"), "test"],
                                capture_output=True, text=True)
        self.assertEqual(2, result.returncode)
        self.assertIn("unrecognized arguments", result.stderr)

    @unittest.skipUnless(shutil.which("mvn"), "Maven is required for the HTTP transport integration")
    def test_http_transports_recover_two_failed_requests_within_one_setup_attempt(self):
        version = subprocess.check_output(["mvn", "--version"], text=True)
        print(version, flush=True)
        match = re.search(r"Apache Maven (\d+)\.(\d+)", version)
        self.assertIsNotNone(match, version)
        transports = (("native", "wagon") if tuple(map(int, match.groups())) >= (3, 9)
                      else ("wagon",))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            options = self.options()
            options.pom = root / "pom.xml"
            options.repository = root / "m2"
            options.profile, options.projects, options.also_make = "", None, False
            options.model_only = True
            options.pom.write_text('<project><modelVersion>4.0.0</modelVersion><groupId>test.transport</groupId>'
                                   '<artifactId>probe</artifactId><version>1</version></project>')
            # Populate only the fixed Help goal first; fault injection then needs no public downloads.
            self.assertEqual(0, PREPARE.resolve(options))
            for transport in transports:
                with self.subTest(transport=transport):
                    parent = ('<project><modelVersion>4.0.0</modelVersion><groupId>test.transport</groupId>'
                              f'<artifactId>parent</artifactId><version>{transport}</version>'
                              '<packaging>pom</packaging></project>').encode()
                    path = f'/test/transport/parent/{transport}/parent-{transport}.pom'
                    requests = []

                    class Handler(BaseHTTPRequestHandler):
                        def do_GET(self):
                            if self.path == path:
                                requests.append(self.path)
                                if len(requests) <= 2:
                                    if transport == "native":
                                        self.send_error(429 if len(requests) == 1 else 502)
                                    else:
                                        self.connection.shutdown(socket.SHUT_RDWR)
                                        self.connection.close()
                                    return
                                body = parent
                            elif self.path == path + '.sha1':
                                body = hashlib.sha1(parent).hexdigest().encode()
                            else:
                                self.send_error(404)
                                return
                            self.send_response(200)
                            self.send_header('Content-Length', str(len(body)))
                            self.end_headers()
                            self.wfile.write(body)

                        def log_message(self, *args):
                            pass

                    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
                    thread = threading.Thread(target=server.serve_forever, daemon=True)
                    thread.start()
                    try:
                        options.pom.write_text(
                            '<project><modelVersion>4.0.0</modelVersion><parent><groupId>test.transport</groupId>'
                            f'<artifactId>parent</artifactId><version>{transport}</version><relativePath/></parent>'
                            '<artifactId>probe</artifactId><repositories><repository><id>central</id>'
                            f'<url>http://127.0.0.1:{server.server_port}</url></repository></repositories></project>')
                        transport_options = [*PREPARE.TRANSPORT_OPTIONS,
                                             "-Dmaven.resolver.transport=" + transport,
                                             "-Daether.connector.http.retryHandler.interval=1",
                                             "-Daether.transport.http.retryHandler.interval=1"]
                        with patch.object(PREPARE, "DELAYS", ()), \
                                patch.object(PREPARE, "TIMEOUT_SECONDS", 120), \
                                patch.object(PREPARE, "TRANSPORT_OPTIONS", transport_options):
                            self.assertEqual(0, PREPARE.resolve(options))
                            self.assertEqual(3, len(requests))
                            options.offline_first = True
                            try:
                                self.assertEqual(0, PREPARE.resolve(options))
                            finally:
                                options.offline_first = False
                        self.assertEqual(3, len(requests))
                    finally:
                        server.shutdown()
                        server.server_close()
                        thread.join()

    def test_runtime_graphs_resolve_separately_and_retry_only_the_failed_download(self):
        options = self.options()
        options.artifact = ["org.apache.maven.surefire:surefire-junit-platform:3.2.2",
                            "org.junit.platform:junit-platform-launcher:1.11.4"]
        with patch.object(PREPARE.subprocess, "run", side_effect=[
                subprocess.CompletedProcess([], status) for status in (0, 1, 0)]) as run, \
                patch.object(PREPARE.time, "sleep") as sleep:
            self.assertEqual(0, PREPARE.resolve(options))
        commands = [call.args[0] for call in run.call_args_list]
        self.assertIn("-Dartifact=" + options.artifact[0], commands[0])
        self.assertIn("-Dartifact=" + options.artifact[1], commands[1])
        self.assertNotIn("-U", commands[1])
        self.assertIn("-U", commands[2])
        self.assertEqual(commands[1], [x for x in commands[2] if x != "-U"])
        self.assertTrue(15 <= sleep.call_args_list[0].args[0] <= 22.5)
        for command in commands:
            self.assertIn(PREPARE.ARTIFACT_GOAL, command)
            self.assertIn("-Dtransitive=true", command)
            self.assertEqual(str(PREPARE.ARTIFACT_POM), command[command.index("-f") + 1])
            self.assertNotIn("-pl", command)
            self.assertNotIn("-am", command)
            self.assertFalse(any(arg.startswith("-P") for arg in command))
            self.assertFalse({"test", "verify", "package", "install"} & set(command))

    def test_failed_runtime_graph_blocks_later_downloads(self):
        options = self.options()
        options.artifact = ["group:provider:1", "group:launcher:1"]
        with patch.object(PREPARE.subprocess, "run", return_value=subprocess.CompletedProcess([], 7)) as run, \
                patch.object(PREPARE.time, "sleep"):
            self.assertEqual(7, PREPARE.resolve(options))
        self.assertEqual(3, run.call_count)
        self.assertTrue(all("-Dartifact=group:provider:1" in call.args[0] for call in run.call_args_list))


if __name__ == "__main__":
    unittest.main()
