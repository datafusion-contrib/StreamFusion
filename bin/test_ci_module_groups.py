"""Keep tagged tests covered and preserve failures when package checks are grouped."""

import os
import copy
from pathlib import Path
import re
import subprocess
import tempfile
import textwrap
import unittest
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / '.github/workflows/ci.yml'
NS = {'m': 'http://maven.apache.org/POM/4.0.0'}


def module_tags():
    return {tag for source in (ROOT / 'src/test').rglob('*.java')
            for tag in re.findall(r'@Tag\("(streamfusion-[^\"]+)"\)', source.read_text())}


def plain(element):
    element = copy.deepcopy(element)
    for node in element.iter():
        node.tag = node.tag.split('}')[-1]
    return ET.tostring(element, encoding='unicode')


class ModuleGroupsTest(unittest.TestCase):
    def test_grouping_retains_all_original_lake_configurations(self):
        workflow = WORKFLOW.read_text().split('  lake-module-tests:', 1)[1].split('    steps:', 1)[0]
        observed = set()
        jobs = re.findall(r'          - name: [^\n]+\n((?:            [^\n]+\n)+)', workflow)
        self.assertEqual(9, len(jobs))
        for job in jobs:
            fields = {key: value.strip().strip("'\"")
                      for key, value in re.findall(r'            (\w+): (.+)', job)}
            formats = ('parquet', 'orc') if fields['module'] == 'streamfusion-paimon' else (None,)
            observed.update((fields['module'], fields['jdk'], fields['profile'], format) for format in formats)
        expected = {('streamfusion-delta', jdk, 'delta', None) for jdk in ('11', '17')}
        expected.update(('streamfusion-hudi', jdk, 'hudi,flink-1.18', None) for jdk in ('11', '17'))
        expected.add(('streamfusion-iceberg', '17', 'iceberg,iceberg-upstream', None))
        expected.update(('streamfusion-paimon', jdk, profile, format)
                        for jdk in ('11', '17') for profile in ('paimon', 'paimon,flink-1.18')
                        for format in ('parquet', 'orc'))
        self.assertEqual(13, len(observed))
        self.assertEqual(expected, observed)

    def test_only_first_shard_runs_deployment_modules_and_every_shard_runs_runtime(self):
        runtime = ET.parse(ROOT / 'streamfusion-runtime/pom.xml')
        plugin = next(p for p in runtime.findall('m:build/m:plugins/m:plugin', NS)
                      if p.findtext('m:artifactId', namespaces=NS) == 'maven-surefire-plugin')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'pom.xml').write_text('''<project xmlns="http://maven.apache.org/POM/4.0.0">
<modelVersion>4.0.0</modelVersion><groupId>test</groupId><artifactId>reactor</artifactId>
<version>1</version><packaging>pom</packaging><modules><module>runtime</module><module>connector</module></modules>
<properties><maven.compiler.release>11</maven.compiler.release>
<sf.orc.test.groups>streamfusion-orc</sf.orc.test.groups></properties>
<dependencies><dependency><groupId>org.junit.jupiter</groupId><artifactId>junit-jupiter</artifactId>
<version>5.10.2</version><scope>test</scope></dependency></dependencies>
<build><plugins><plugin><groupId>org.apache.maven.plugins</groupId>
<artifactId>maven-compiler-plugin</artifactId><version>3.13.0</version></plugin>
<plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-surefire-plugin</artifactId>
<version>3.2.5</version></plugin></plugins></build></project>''')
            for module in ('runtime', 'connector'):
                target = root / module
                target.mkdir()
                config = '<build><plugins>' + plain(plugin) + '</plugins></build>' if module == 'runtime' else ''
                (target / 'pom.xml').write_text('''<project xmlns="http://maven.apache.org/POM/4.0.0">
<modelVersion>4.0.0</modelVersion><parent><groupId>test</groupId><artifactId>reactor</artifactId>
<version>1</version></parent><artifactId>''' + module + '</artifactId>' + config + '</project>')
                source = target / 'src/test/java'
                source.mkdir(parents=True)
                (source / 'SelectedTest.java').write_text('''class SelectedTest {
@org.junit.jupiter.api.Test void selected() throws Exception {
java.nio.file.Files.writeString(java.nio.file.Path.of("target/ran"), "ran");
}}''')
            for shard in (1, 2, 3):
                with self.subTest(shard=shard):
                    for module in ('runtime', 'connector'):
                        (root / module / 'target/ran').unlink(missing_ok=True)
                    result = subprocess.run(
                        ['mvn', '-B', '-ntp', 'test',
                         '-DskipTests=' + str(shard != 1).lower(), '-Dsf.runtime.tests.skip=false'],
                        cwd=root, capture_output=True, text=True, timeout=120)
                    self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                    self.assertTrue((root / 'runtime/target/ran').exists())
                    self.assertEqual(shard == 1, (root / 'connector/target/ran').exists())

    def test_every_module_tag_has_an_isolated_native_package_check(self):
        workflow = WORKFLOW.read_text()
        declared = module_tags()
        self.assertTrue(declared)
        packages = set(re.search(r'^          NATIVE_PACKAGES: (.+)$', workflow, re.MULTILINE)[1].split())
        expected = {'streamfusion-avro' if tag == 'streamfusion-avro-confluent' else tag for tag in declared}
        self.assertEqual(expected, packages)

    def test_full_suite_selects_every_module_tag_and_untagged_tests_exactly_once(self):
        parent = ET.parse(ROOT / 'pom.xml')
        runtime = ET.parse(ROOT / 'streamfusion-runtime/pom.xml')
        parent_plugin = next(p for p in parent.findall('.//m:plugin', NS)
                             if p.findtext('m:artifactId', namespaces=NS) == 'maven-surefire-plugin')
        plugin = copy.deepcopy(next(p for p in runtime.findall('m:build/m:plugins/m:plugin', NS)
                                    if p.findtext('m:artifactId', namespaces=NS) == 'maven-surefire-plugin'))
        ET.SubElement(plugin, 'version').text = parent_plugin.findtext('m:version', namespaces=NS)
        tags = sorted(module_tags()) + [None]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'pom.xml').write_text('''<project xmlns="http://maven.apache.org/POM/4.0.0">
<modelVersion>4.0.0</modelVersion><groupId>test</groupId><artifactId>module-selection</artifactId><version>1</version>
<properties><maven.compiler.release>11</maven.compiler.release><sf.runtime.tests.skip>false</sf.runtime.tests.skip>
<sf.orc.test.groups>streamfusion-orc</sf.orc.test.groups></properties>
<dependencies><dependency><groupId>org.junit.jupiter</groupId><artifactId>junit-jupiter</artifactId><version>5.10.2</version><scope>test</scope></dependency></dependencies>
<build><plugins><plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-compiler-plugin</artifactId><version>3.13.0</version></plugin>
''' + plain(plugin) + '</plugins></build></project>')
            source = root / 'src/test/java'
            source.mkdir(parents=True)
            for index, tag in enumerate(tags):
                annotation = f'@org.junit.jupiter.api.Tag("{tag}")' if tag else ''
                (source / f'Module{index}Test.java').write_text(f'''import java.nio.file.*;
{annotation}
class Module{index}Test {{
  @org.junit.jupiter.api.Test void selectedExactlyOnce() throws Exception {{
    Files.createDirectories(Path.of("target/selected"));
    Files.writeString(Path.of("target/selected/{index}"), "selected", StandardOpenOption.CREATE_NEW);
  }}
}}''')
            result = subprocess.run(['mvn', '-B', '-ntp', 'test'], cwd=root,
                                    capture_output=True, text=True, timeout=120)
            self.assertEqual(0, result.returncode, result.stdout + result.stderr)
            self.assertEqual({str(index) for index in range(len(tags))},
                             {path.name for path in (root / 'target/selected').iterdir()})

    def test_standalone_checks_continue_and_fail_the_group_if_any_package_fails(self):
        lines = WORKFLOW.read_text().splitlines()
        start = lines.index('      - name: Check every standalone native package')
        start = lines.index('        run: |', start) + 1
        end = start
        while end < len(lines) and (not lines[end] or lines[end].startswith('          ')):
            end += 1
        script = textwrap.dedent('\n'.join(lines[start:end]))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'native').mkdir()
            cargo = root / 'cargo'
            cargo.write_text('#!/bin/bash\nprintf "%s\\n" "$*" >> "$CALLS"\n[[ "$3" != "$FAIL_PACKAGE" ]]\n')
            cargo.chmod(0o755)
            for failing in ('', 'a', 'b', 'c'):
                calls = root / 'calls'
                calls.unlink(missing_ok=True)
                result = subprocess.run(['bash', '-e', '-c', script], cwd=root, capture_output=True,
                                        env={**os.environ, 'PATH': f'{root}:{os.environ["PATH"]}',
                                             'CALLS': str(calls), 'FAIL_PACKAGE': failing,
                                             'NATIVE_PACKAGES': 'a b c'})
                self.assertEqual(bool(failing), result.returncode != 0, result.stderr)
                self.assertEqual(['check -p a --all-targets', 'check -p b --all-targets',
                                  'check -p c --all-targets'], calls.read_text().splitlines())


if __name__ == '__main__':
    unittest.main()
