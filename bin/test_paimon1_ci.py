"""Exercise the upstream profile's real Surefire fork substitution and isolation."""

import copy
import os
from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[1]
NS = {'m': 'http://maven.apache.org/POM/4.0.0'}


def plain(element):
    element = copy.deepcopy(element)
    for node in element.iter():
        node.tag = node.tag.split('}')[-1]
    return ET.tostring(element, encoding='unicode')


class PaimonCiTest(unittest.TestCase):
    def test_invalid_fork_count_is_rejected_before_building(self):
        for value in ('0', '-1', '1.5', '2C', 'oops'):
            result = subprocess.run(['bash', str(ROOT / 'bin/paimon1-suite.sh')],
                                    env={**os.environ, 'SF_PAIMON1_TEST_FORKS': value},
                                    capture_output=True, text=True)
            self.assertEqual(64, result.returncode)
            self.assertIn('must be a positive integer', result.stderr)

    def test_real_forks_have_separate_evidence_and_serial_tests(self):
        parent = ET.parse(ROOT / 'pom.xml')
        plugins = parent.findall('.//m:plugin', NS)
        surefire = next(p for p in plugins if p.findtext('m:artifactId', namespaces=NS) == 'maven-surefire-plugin')
        version = surefire.findtext('m:version', namespaces=NS)
        forks = plain(surefire.find('m:configuration/m:forkCount', NS))
        module = ET.parse(ROOT / 'streamfusion-paimon1/pom.xml')
        profile = next(p for p in module.findall('m:profiles/m:profile', NS)
                       if p.findtext('m:id', namespaces=NS) == 'upstream-paimon1')
        script = (ROOT / 'bin/paimon1-suite.sh').read_text()
        fork_arg_line = next(line for line in script.splitlines() if line.startswith("jvm_args+='"))
        fork_args = shlex.split(fork_arg_line.split('+=', 1)[1])[0]
        properties = plain(profile.find('.//m:systemPropertyVariables', NS))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'pom.xml').write_text(f'''<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion><groupId>test</groupId><artifactId>paimon-fork-probe</artifactId><version>1</version>
  <properties><maven.compiler.release>17</maven.compiler.release></properties>
  <dependencies><dependency><groupId>org.junit.jupiter</groupId><artifactId>junit-jupiter</artifactId><version>5.10.2</version><scope>test</scope></dependency></dependencies>
  <build><plugins>
    <plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-compiler-plugin</artifactId><version>3.13.0</version></plugin>
    <plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-surefire-plugin</artifactId><version>{version}</version>
      <configuration><argLine>{fork_args}</argLine>{forks}<reuseForks>true</reuseForks>{properties}</configuration>
    </plugin>
  </plugins></build>
</project>''')
            source = root / 'src/test/java'
            source.mkdir(parents=True)
            for name in ('FirstTest', 'SecondTest'):
                (source / f'{name}.java').write_text('''import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.*;
class NAME {
  @Test void isolated() throws Exception {
    String fork = System.getProperty("mvn.forkNumber");
    assertTrue(fork.matches("[12]"), fork);
    assertEquals("false", System.getProperty("junit.jupiter.execution.parallel.enabled"));
    for (String key : new String[]{"sql-inventory", "native-reports", "diagnostics"}) {
      Path path = Path.of(System.getProperty("streamfusion.flink-suite." + key));
      assertEquals("fork-" + fork, path.getFileName().toString());
      Files.createDirectories(path);
      Files.writeString(path.resolve("NAME"), fork);
    }
    Thread.sleep(1000);
  }
}'''.replace('NAME', name))
            result = subprocess.run(['mvn', '-B', '-ntp', '-f', str(root / 'pom.xml'),
                                     '-Dsf.testForks=2', 'test'], capture_output=True, text=True, timeout=120)
            self.assertEqual(0, result.returncode, result.stdout + result.stderr)
            for path in ('upstream-suite/inventory', 'upstream-suite/diagnostics', 'native-reports'):
                files = list((root / 'target' / path).glob('fork-*/*Test'))
                self.assertEqual({'FirstTest', 'SecondTest'}, {p.name for p in files})
                self.assertEqual({'1', '2'}, {p.read_text() for p in files})


if __name__ == '__main__':
    unittest.main()
