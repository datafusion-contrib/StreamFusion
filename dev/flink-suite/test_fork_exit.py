#!/usr/bin/env python3
"""Real launcher/Maven/Surefire TCP-fork exit regressions (JDK 17+, Maven).

Package the standalone suite agent before running this file in CI.
These tests never build the agent or check out Flink. Only the upstream checkout
and its mvnw entry point are fixtures: Maven, Surefire 3.2.2, JUnit, the shaded
agent, reports, receipts and exit statuses are real. Released Maven dependencies
share the normal local cache; only projects and test outputs are disposable.
Network access is needed for uncached dependencies.
All process output, XML, dumps and receipts are printed before fixture cleanup,
even on failure. A late halt must fail the launcher, not merely the JVM audit.
"""

import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET


REPO_ROOT = Path(__file__).resolve().parents[2]
RUNNER = REPO_ROOT / "bin/flink-suite.sh"
AGENT = REPO_ROOT / "dev/flink-suite/agent/target/streamfusion-flink-suite-agent-1.0-SNAPSHOT.jar"
SETTINGS = REPO_ROOT / "dev/flink-suite/settings.xml"
MAVEN_TIMEOUT_SECONDS = 300

POM = """<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>tech.streamfusion.fixture</groupId>
  <artifactId>real-fork-exit</artifactId>
  <version>1.0</version>
  <properties>
    <maven.compiler.release>17</maven.compiler.release>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
  </properties>
  <dependencies>
    <dependency>
      <groupId>org.junit.jupiter</groupId>
      <artifactId>junit-jupiter</artifactId>
      <version>5.10.1</version>
      <scope>test</scope>
    </dependency>
  </dependencies>
  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-compiler-plugin</artifactId>
        <version>3.13.0</version>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-surefire-plugin</artifactId>
        <version>3.2.2</version>
        <configuration>
          <forkNode implementation="org.apache.maven.plugin.surefire.extensions.SurefireForkNodeFactory"/>
          <forkCount>1</forkCount>
          <reuseForks>true</reuseForks>
          <testFailureIgnore>true</testFailureIgnore>
          <failIfNoTests>true</failIfNoTests>
          <forkedProcessTimeoutInSeconds>90</forkedProcessTimeoutInSeconds>
          <forkedProcessExitTimeoutInSeconds>60</forkedProcessExitTimeoutInSeconds>
          <reportsDirectory>${fixture.reports}</reportsDirectory>
          <argLine>${surefire.module.config}</argLine>
          <systemPropertyVariables>
            <fixture.reports>${fixture.reports}</fixture.reports>
            <fixture.marker>${fixture.marker}</fixture.marker>
          </systemPropertyVariables>
        </configuration>
      </plugin>
    </plugins>
  </build>
</project>
"""

# This shim accepts exactly the launcher's cleanliness query, never real Git.
GIT_SHIM = """#!/bin/sh
if [ "$#" -eq 4 ] && [ "$1" = "-C" ] && [ "$2" = "$REAL_FORK_FLINK" ] && [ "$3" = "status" ] && [ "$4" = "--short" ]; then
  printf 'checked clean fixture checkout\\n' > "$REAL_FORK_ROOT/git.log"
  exit 0
fi
printf 'Unexpected Git operation blocked\\n' >&2
exit 97
"""

# Translate only the absent upstream reactor/goal to a minimal real project.
# No generated reports/receipts, inferred exit codes, or Maven-output rewriting.
MVNW_SHIM = """#!/bin/bash
set -u
printf '%s\\n' "$@" > "$REAL_FORK_ROOT/launcher-args.log"
properties=()
for argument in "$@"; do
  case "$argument" in
        -Dmaven.repo.local=*) ;;  # Reuse Maven's dependency cache, not the disposable Flink cache.
    -D*) properties+=("$argument") ;;
  esac
done
printf '%s\\n' "$REAL_FORK_MVN" > "$REAL_FORK_ROOT/maven-command.log"
printf '%s\\n' -B -ntp -s "$REAL_FORK_SETTINGS" -f "$REAL_FORK_PROJECT/pom.xml" "${properties[@]}" "-Dfixture.reports=$REAL_FORK_REPORTS" "-Dfixture.marker=$REAL_FORK_ROOT/fork.marker" test >> "$REAL_FORK_ROOT/maven-command.log"
"$REAL_FORK_MVN" -B -ntp -s "$REAL_FORK_SETTINGS" -f "$REAL_FORK_PROJECT/pom.xml" "${properties[@]}" "-Dfixture.reports=$REAL_FORK_REPORTS" "-Dfixture.marker=$REAL_FORK_ROOT/fork.marker" test
maven_exit=$?
printf '%s\\n' "$maven_exit" > "$REAL_FORK_ROOT/maven.exit"
printf 'REAL_MAVEN_EXIT=%s\\n' "$maven_exit"
exit "$maven_exit"
"""


class ForkExitTest(unittest.TestCase):
    def test_late_halt_after_ok_and_passing_xml_fails_launcher(self):
        maven = shutil.which("mvn")
        self.assertIsNotNone(maven, "Prerequisite missing: real Maven on PATH")
        self.assertIsNotNone(shutil.which("java"), "Prerequisite missing: Java 17+")
        self.assertIsNotNone(shutil.which("javac"), "Prerequisite missing: JDK 17+")
        self.assertTrue(AGENT.is_file(), "Prerequisite missing: package the shaded suite agent first")
        with tempfile.TemporaryDirectory(prefix="sf-real-fork-late-") as temporary:
            root = Path(temporary).resolve()
            suite = root / "suite with spaces"
            flink = suite / "flink-2.2.1"
            (flink / ".git").mkdir(parents=True)
            (suite / "streamfusion-classpath.txt").write_text("", encoding="utf-8")
            (suite / "flink-table-planner-2.2.1-unshaded.jar").touch()
            reports = flink / "flink-table/flink-table-planner/target/surefire-reports"
            project = root / "project"
            sources = project / "src/test/java"
            sources.mkdir(parents=True)
            (project / "pom.xml").write_text(POM, encoding="utf-8")
            (sources / "RealForkExitTest.java").write_text("""import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.TimeUnit;
import javax.xml.parsers.DocumentBuilderFactory;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RealForkExitTest {
    @Test
    void passesBeforeLateHalt() throws Exception {
        Path audit = Path.of(System.getProperty("streamfusion.flink-suite.audit-dir"));
        Path reports = Path.of(System.getProperty("fixture.reports"));
        Path marker = Path.of(System.getProperty("fixture.marker"));
        Path pending;
        try (var entries = Files.list(audit)) {
            pending = entries.filter(path -> path.toString().endsWith(".pending"))
                    .findFirst().orElseThrow();
        }
        Path ok = pending.resolveSibling(pending.getFileName().toString().replace(".pending", ".ok"));
        Path report = reports.resolve("TEST-RealForkExitTest.xml");
        WatchService watcher = audit.getFileSystem().newWatchService();
        audit.register(watcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
        reports.register(watcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Surefire's Java streams may already be closed after its TCP bye.
            PrintStream rawError = new PrintStream(new FileOutputStream(FileDescriptor.err), true, StandardCharsets.UTF_8);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            try (watcher) {
                while (true) {
                    // Register before shutdown, then check existing files before polling:
                    // neither receipt-before-registration nor receipt-before-poll is lost.
                    if (Files.isRegularFile(ok) && Files.isRegularFile(report)) {
                        try {
                            var xml = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(report.toFile());
                            var result = xml.getDocumentElement();
                            if (!"1".equals(result.getAttribute("tests"))
                                    || !"0".equals(result.getAttribute("failures"))
                                    || !"0".equals(result.getAttribute("errors"))
                                    || !"0".equals(result.getAttribute("skipped"))
                                    || result.getElementsByTagName("testcase").getLength() != 1
                                    || result.getElementsByTagName("failure").getLength() != 0
                                    || result.getElementsByTagName("error").getLength() != 0) {
                                throw new IllegalStateException("JUnit report was not one passing test");
                            }
                            String evidence = "REAL_FORK_LATE_HALT_70_AFTER_OK_AND_PASSING_XML\\n"
                                    + "ok=" + ok + "\\npassingXml=" + report + "\\n";
                            Files.writeString(marker, evidence);
                            rawError.print(evidence);
                            rawError.flush();
                            Runtime.getRuntime().halt(70);
                        } catch (org.xml.sax.SAXException | java.io.IOException incompleteReport) {
                            // A create/modify event may precede the XML writer's close.
                        }
                    }
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        throw new IllegalStateException("Timed out waiting for .ok and passing XML");
                    }
                    WatchKey key = watcher.poll(remaining, TimeUnit.NANOSECONDS);
                    if (key == null) {
                        throw new IllegalStateException("Timed out waiting for audit/report notification");
                    }
                    key.pollEvents();
                    if (!key.reset()) {
                        throw new IllegalStateException("Audit/report watch became invalid");
                    }
                }
            } catch (Throwable failure) {
                rawError.println("REAL_FORK_FIXTURE_ERROR");
                failure.printStackTrace(rawError);
                rawError.flush();
                Runtime.getRuntime().halt(71);
            }
        }, "real-fork-late-halt"));
        assertEquals(2, 1 + 1);
    }
}
""", encoding="utf-8")
            shim_bin = root / "shim-bin"
            shim_bin.mkdir()
            (shim_bin / "git").write_text(GIT_SHIM, encoding="utf-8")
            (shim_bin / "git").chmod(0o755)
            (shim_bin / "python3").symlink_to(sys.executable)
            (flink / "mvnw").write_text(MVNW_SHIM, encoding="utf-8")
            (flink / "mvnw").chmod(0o755)
            env = os.environ.copy()
            env.update({
                "PATH": str(shim_bin) + os.pathsep + os.environ.get("PATH", os.defpath),
                "FLINK_SUITE_ROOT": str(suite), "FLINK_SUITE_REUSE_BUILD": "true",
                "FLINK_VERSION": "2.2.1", "FLINK_SUITE_TEST": "RealForkExitTest",
                "FLINK_SUITE_UNIT_FORKS": "1", "FLINK_SUITE_IT_FORKS": "1",
                "REAL_FORK_ROOT": str(root), "REAL_FORK_FLINK": str(flink),
                "REAL_FORK_PROJECT": str(project), "REAL_FORK_REPORTS": str(reports),
                "REAL_FORK_MVN": maven, "REAL_FORK_SETTINGS": str(SETTINGS),
                "PYTHONDONTWRITEBYTECODE": "1",
            })
            env.pop("BASH_ENV", None)
            env.pop("ENV", None)
            env.pop("JAVA_TOOL_OPTIONS", None)
            env.pop("JDK_JAVA_OPTIONS", None)
            env.pop("_JAVA_OPTIONS", None)
            env.pop("MAVEN_OPTS", None)
            env.pop("MAVEN_ARGS", None)
            env.pop("MAVEN_PROJECTBASEDIR", None)
            timed_out = False

            with (root / "launcher.log").open("w", encoding="utf-8") as output:
                process = subprocess.Popen(
                    ["/bin/bash", str(RUNNER), "runtime"], cwd=root, env=env,
                    stdout=output, stderr=subprocess.STDOUT, start_new_session=True,
                )
                try:
                    process.wait(timeout=MAVEN_TIMEOUT_SECONDS)
                except subprocess.TimeoutExpired:
                    timed_out = True
                finally:
                    # Kill only this fixture's session, including any orphaned fork.
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait(timeout=10)
                    output.flush()
                    print(f"\n=== {self.id()} outer_exit={process.returncode} timeout={timed_out} ===", flush=True)
                    print((root / "launcher.log").read_text(encoding="utf-8", errors="replace"), flush=True)
                    # Diagnostic enumeration only; each behavioral assertion is inline below.
                    for artifact in sorted(reports.glob("*")) + sorted(suite.glob("audit-*/*")) + sorted(root.glob("*.marker")) + sorted(root.glob("maven.*")) + sorted(root.glob("*-args.log")):
                        if artifact.is_file():
                            print(f"--- {artifact.relative_to(root)} ---\n{artifact.read_text(encoding='utf-8', errors='replace')}", flush=True)

            log = (root / "launcher.log").read_text(encoding="utf-8", errors="replace")
            self.assertFalse(timed_out, "Real Maven/launcher exceeded the timeout; see captured diagnostics")
            self.assertTrue((root / "maven.exit").is_file(), log)
            maven_exit = int((root / "maven.exit").read_text(encoding="utf-8"))
            self.assertIn(f"REAL_MAVEN_EXIT={maven_exit}", log)
            self.assertRegex(log, r"(?:maven-surefire-plugin|surefire):3\.2\.2:test")
            self.assertNotIn("REAL_FORK_FIXTURE_ERROR", log)
            self.assertIn("REAL_FORK_LATE_HALT_70_AFTER_OK_AND_PASSING_XML", log)
            self.assertTrue((root / "fork.marker").is_file(), log)
            self.assertIn("REAL_FORK_LATE_HALT_70_AFTER_OK_AND_PASSING_XML", (root / "fork.marker").read_text(encoding="utf-8"))
            audits = list(suite.glob("audit-runtime.*"))
            self.assertEqual(len(audits), 1, log)
            receipts = list(audits[0].iterdir())
            self.assertEqual(len(receipts), 1, log)
            self.assertTrue(receipts[0].is_file(), log)
            self.assertEqual(receipts[0].suffix, ".ok", log)
            self.assertIn(f"ok={receipts[0]}", (root / "fork.marker").read_text(encoding="utf-8"))
            self.assertEqual(len(list(reports.glob("TEST-*.xml"))), 1, log)
            report = reports / "TEST-RealForkExitTest.xml"
            self.assertIn(f"passingXml={report}", (root / "fork.marker").read_text(encoding="utf-8"))
            xml = ET.parse(report).getroot()
            self.assertEqual(xml.get("tests"), "1")
            self.assertEqual(xml.get("failures"), "0")
            self.assertEqual(xml.get("errors"), "0")
            self.assertEqual(xml.get("skipped"), "0")
            self.assertEqual(len(xml.findall("testcase")), 1)
            self.assertEqual(xml.find("testcase").get("name"), "passesBeforeLateHalt")
            self.assertIsNone(xml.find("testcase/failure"))
            self.assertIsNone(xml.find("testcase/error"))
            self.assertEqual((root / "git.log").read_text(encoding="utf-8"), "checked clean fixture checkout\n")
            self.assertIn("-Dmaven.test.failure.ignore=true", (root / "launcher-args.log").read_text(encoding="utf-8").splitlines())
            self.assertNotEqual(
                process.returncode, 0,
                f"FALSE GREEN: real fork halted 70 after .ok and passing XML; Maven returned {maven_exit}, launcher returned 0.\n{log}",
            )

    def test_halt_before_audit_leaves_pending_and_fails_launcher(self):
        maven = shutil.which("mvn")
        self.assertIsNotNone(maven, "Prerequisite missing: real Maven on PATH")
        self.assertIsNotNone(shutil.which("java"), "Prerequisite missing: Java 17+")
        self.assertIsNotNone(shutil.which("javac"), "Prerequisite missing: JDK 17+")
        self.assertTrue(AGENT.is_file(), "Prerequisite missing: package the shaded suite agent first")
        with tempfile.TemporaryDirectory(prefix="sf-real-fork-early-") as temporary:
            root = Path(temporary).resolve()
            suite = root / "suite with spaces"
            flink = suite / "flink-2.2.1"
            (flink / ".git").mkdir(parents=True)
            (suite / "streamfusion-classpath.txt").write_text("", encoding="utf-8")
            (suite / "flink-table-planner-2.2.1-unshaded.jar").touch()
            reports = flink / "flink-table/flink-table-planner/target/surefire-reports"
            project = root / "project"
            sources = project / "src/test/java"
            sources.mkdir(parents=True)
            (project / "pom.xml").write_text(POM, encoding="utf-8")
            (sources / "RealForkExitTest.java").write_text("""import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RealForkExitTest {
    @Test
    void haltsBeforeAudit() throws Exception {
        assertEquals(2, 1 + 1);
        String marker = "REAL_FORK_EARLY_HALT_70_BEFORE_AUDIT";
        Files.writeString(Path.of(System.getProperty("fixture.marker")), marker);
        PrintStream rawError = new PrintStream(new FileOutputStream(FileDescriptor.err), true, StandardCharsets.UTF_8);
        rawError.println(marker);
        rawError.flush();
        Runtime.getRuntime().halt(70);
    }
}
""", encoding="utf-8")
            shim_bin = root / "shim-bin"
            shim_bin.mkdir()
            (shim_bin / "git").write_text(GIT_SHIM, encoding="utf-8")
            (shim_bin / "git").chmod(0o755)
            (shim_bin / "python3").symlink_to(sys.executable)
            (flink / "mvnw").write_text(MVNW_SHIM, encoding="utf-8")
            (flink / "mvnw").chmod(0o755)
            env = os.environ.copy()
            env.update({
                "PATH": str(shim_bin) + os.pathsep + os.environ.get("PATH", os.defpath),
                "FLINK_SUITE_ROOT": str(suite), "FLINK_SUITE_REUSE_BUILD": "true",
                "FLINK_VERSION": "2.2.1", "FLINK_SUITE_TEST": "RealForkExitTest",
                "FLINK_SUITE_UNIT_FORKS": "1", "FLINK_SUITE_IT_FORKS": "1",
                "REAL_FORK_ROOT": str(root), "REAL_FORK_FLINK": str(flink),
                "REAL_FORK_PROJECT": str(project), "REAL_FORK_REPORTS": str(reports),
                "REAL_FORK_MVN": maven, "REAL_FORK_SETTINGS": str(SETTINGS),
                "PYTHONDONTWRITEBYTECODE": "1",
            })
            env.pop("BASH_ENV", None)
            env.pop("ENV", None)
            env.pop("JAVA_TOOL_OPTIONS", None)
            env.pop("JDK_JAVA_OPTIONS", None)
            env.pop("_JAVA_OPTIONS", None)
            env.pop("MAVEN_OPTS", None)
            env.pop("MAVEN_ARGS", None)
            env.pop("MAVEN_PROJECTBASEDIR", None)
            timed_out = False

            with (root / "launcher.log").open("w", encoding="utf-8") as output:
                process = subprocess.Popen(
                    ["/bin/bash", str(RUNNER), "runtime"], cwd=root, env=env,
                    stdout=output, stderr=subprocess.STDOUT, start_new_session=True,
                )
                try:
                    process.wait(timeout=MAVEN_TIMEOUT_SECONDS)
                except subprocess.TimeoutExpired:
                    timed_out = True
                finally:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait(timeout=10)
                    output.flush()
                    print(f"\n=== {self.id()} outer_exit={process.returncode} timeout={timed_out} ===", flush=True)
                    print((root / "launcher.log").read_text(encoding="utf-8", errors="replace"), flush=True)
                    for artifact in sorted(reports.glob("*")) + sorted(suite.glob("audit-*/*")) + sorted(root.glob("*.marker")) + sorted(root.glob("maven.*")) + sorted(root.glob("*-args.log")):
                        if artifact.is_file():
                            print(f"--- {artifact.relative_to(root)} ---\n{artifact.read_text(encoding='utf-8', errors='replace')}", flush=True)

            log = (root / "launcher.log").read_text(encoding="utf-8", errors="replace")
            self.assertFalse(timed_out, "Real Maven/launcher exceeded the timeout; see captured diagnostics")
            self.assertTrue((root / "maven.exit").is_file(), log)
            maven_exit = int((root / "maven.exit").read_text(encoding="utf-8"))
            self.assertIn(f"REAL_MAVEN_EXIT={maven_exit}", log)
            self.assertRegex(log, r"(?:maven-surefire-plugin|surefire):3\.2\.2:test")
            self.assertIn("REAL_FORK_EARLY_HALT_70_BEFORE_AUDIT", log)
            self.assertEqual((root / "fork.marker").read_text(encoding="utf-8"), "REAL_FORK_EARLY_HALT_70_BEFORE_AUDIT")
            self.assertIn("Process Exit Code: 70", log)
            audits = list(suite.glob("audit-runtime.*"))
            self.assertEqual(len(audits), 1, log)
            receipts = list(audits[0].iterdir())
            self.assertEqual(len(receipts), 1, log)
            self.assertTrue(receipts[0].is_file(), log)
            self.assertEqual(receipts[0].suffix, ".pending", log)
            self.assertEqual(list(audits[0].glob("*.ok")), [])
            self.assertEqual(list(reports.glob("TEST-*.xml")), [], log)
            self.assertEqual((root / "git.log").read_text(encoding="utf-8"), "checked clean fixture checkout\n")
            self.assertIn("-Dmaven.test.failure.ignore=true", (root / "launcher-args.log").read_text(encoding="utf-8").splitlines())
            self.assertNotEqual(process.returncode, 0, f"FALSE GREEN: real fork halted before audit; Maven returned {maven_exit}.\n{log}")

    def test_expected_junit_failure_has_ok_audit_and_zero_launcher_exit(self):
        maven = shutil.which("mvn")
        self.assertIsNotNone(maven, "Prerequisite missing: real Maven on PATH")
        self.assertIsNotNone(shutil.which("java"), "Prerequisite missing: Java 17+")
        self.assertIsNotNone(shutil.which("javac"), "Prerequisite missing: JDK 17+")
        self.assertTrue(AGENT.is_file(), "Prerequisite missing: package the shaded suite agent first")
        with tempfile.TemporaryDirectory(prefix="sf-real-fork-xfail-") as temporary:
            root = Path(temporary).resolve()
            suite = root / "suite with spaces"
            flink = suite / "flink-2.2.1"
            (flink / ".git").mkdir(parents=True)
            (suite / "streamfusion-classpath.txt").write_text("", encoding="utf-8")
            (suite / "flink-table-planner-2.2.1-unshaded.jar").touch()
            reports = flink / "flink-table/flink-table-planner/target/surefire-reports"
            project = root / "project"
            sources = project / "src/test/java/org/apache/flink/table/planner/runtime/batch/sql"
            sources.mkdir(parents=True)
            (project / "pom.xml").write_text(POM, encoding="utf-8")
            (sources / "CalcITCase.java").write_text("""package org.apache.flink.table.planner.runtime.batch.sql;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CalcITCase {
    @Test
    void testCurrentDate() throws Exception {
        String marker = "REAL_FORK_EXPECTED_JUNIT_ASSERTION_EXECUTED";
        Files.writeString(Path.of(System.getProperty("fixture.marker")), marker);
        System.out.println(marker);
        assertEquals("expected date", "different date", marker);
    }
}
""", encoding="utf-8")
            shim_bin = root / "shim-bin"
            shim_bin.mkdir()
            (shim_bin / "git").write_text(GIT_SHIM, encoding="utf-8")
            (shim_bin / "git").chmod(0o755)
            (shim_bin / "python3").symlink_to(sys.executable)
            (flink / "mvnw").write_text(MVNW_SHIM, encoding="utf-8")
            (flink / "mvnw").chmod(0o755)
            env = os.environ.copy()
            env.update({
                "PATH": str(shim_bin) + os.pathsep + os.environ.get("PATH", os.defpath),
                "FLINK_SUITE_ROOT": str(suite), "FLINK_SUITE_REUSE_BUILD": "true",
                "FLINK_VERSION": "2.2.1", "FLINK_SUITE_TEST": "org.apache.flink.table.planner.runtime.batch.sql.CalcITCase",
                "FLINK_SUITE_UNIT_FORKS": "1", "FLINK_SUITE_IT_FORKS": "1",
                "REAL_FORK_ROOT": str(root), "REAL_FORK_FLINK": str(flink),
                "REAL_FORK_PROJECT": str(project), "REAL_FORK_REPORTS": str(reports),
                "REAL_FORK_MVN": maven, "REAL_FORK_SETTINGS": str(SETTINGS),
                "PYTHONDONTWRITEBYTECODE": "1",
            })
            env.pop("BASH_ENV", None)
            env.pop("ENV", None)
            env.pop("JAVA_TOOL_OPTIONS", None)
            env.pop("JDK_JAVA_OPTIONS", None)
            env.pop("_JAVA_OPTIONS", None)
            env.pop("MAVEN_OPTS", None)
            env.pop("MAVEN_ARGS", None)
            env.pop("MAVEN_PROJECTBASEDIR", None)
            timed_out = False

            with (root / "launcher.log").open("w", encoding="utf-8") as output:
                process = subprocess.Popen(
                    ["/bin/bash", str(RUNNER), "runtime"], cwd=root, env=env,
                    stdout=output, stderr=subprocess.STDOUT, start_new_session=True,
                )
                try:
                    process.wait(timeout=MAVEN_TIMEOUT_SECONDS)
                except subprocess.TimeoutExpired:
                    timed_out = True
                finally:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait(timeout=10)
                    output.flush()
                    print(f"\n=== {self.id()} outer_exit={process.returncode} timeout={timed_out} ===", flush=True)
                    print((root / "launcher.log").read_text(encoding="utf-8", errors="replace"), flush=True)
                    for artifact in sorted(reports.glob("*")) + sorted(suite.glob("audit-*/*")) + sorted(root.glob("*.marker")) + sorted(root.glob("maven.*")) + sorted(root.glob("*-args.log")):
                        if artifact.is_file():
                            print(f"--- {artifact.relative_to(root)} ---\n{artifact.read_text(encoding='utf-8', errors='replace')}", flush=True)

            log = (root / "launcher.log").read_text(encoding="utf-8", errors="replace")
            self.assertFalse(timed_out, "Real Maven/launcher exceeded the timeout; see captured diagnostics")
            self.assertTrue((root / "maven.exit").is_file(), log)
            maven_exit = int((root / "maven.exit").read_text(encoding="utf-8"))
            self.assertEqual(maven_exit, 0, log)
            self.assertIn("REAL_MAVEN_EXIT=0", log)
            self.assertRegex(log, r"(?:maven-surefire-plugin|surefire):3\.2\.2:test")
            self.assertIn("REAL_FORK_EXPECTED_JUNIT_ASSERTION_EXECUTED", log)
            self.assertEqual((root / "fork.marker").read_text(encoding="utf-8"), "REAL_FORK_EXPECTED_JUNIT_ASSERTION_EXECUTED")
            audits = list(suite.glob("audit-runtime.*"))
            self.assertEqual(len(audits), 1, log)
            receipts = list(audits[0].iterdir())
            self.assertEqual(len(receipts), 1, log)
            self.assertTrue(receipts[0].is_file(), log)
            self.assertEqual(receipts[0].suffix, ".ok", log)
            self.assertEqual(len(list(reports.glob("TEST-*.xml"))), 1, log)
            xml = ET.parse(reports / "TEST-org.apache.flink.table.planner.runtime.batch.sql.CalcITCase.xml").getroot()
            self.assertEqual(xml.get("tests"), "1")
            self.assertEqual(xml.get("failures"), "1")
            self.assertEqual(xml.get("errors"), "0")
            self.assertEqual(xml.get("skipped"), "0")
            self.assertEqual(len(xml.findall("testcase")), 1)
            case = xml.find("testcase")
            self.assertEqual(case.get("classname"), "org.apache.flink.table.planner.runtime.batch.sql.CalcITCase")
            self.assertEqual(case.get("name"), "testCurrentDate")
            self.assertIsNone(case.find("skipped"))
            self.assertIsNone(case.find("error"))
            failure = case.find("failure")
            self.assertIsNotNone(failure)
            self.assertEqual(failure.get("type"), "org.opentest4j.AssertionFailedError")
            self.assertIn("REAL_FORK_EXPECTED_JUNIT_ASSERTION_EXECUTED", failure.get("message", ""))
            self.assertEqual((root / "git.log").read_text(encoding="utf-8"), "checked clean fixture checkout\n")
            self.assertIn("-Dmaven.test.failure.ignore=true", (root / "launcher-args.log").read_text(encoding="utf-8").splitlines())
            self.assertIn("- Expected upstream failures: 1", log)
            self.assertIn("- Failures: 0", log)
            self.assertIn("- Errors: 0", log)
            self.assertIn("- Skipped: 0", log)
            self.assertIn("org.apache.flink.table.planner.runtime.batch.sql.CalcITCase#testCurrentDate", log)
            self.assertNotIn("## Infrastructure failures", log)
            self.assertEqual(process.returncode, 0, log)


if __name__ == "__main__":
    unittest.main(verbosity=2)
