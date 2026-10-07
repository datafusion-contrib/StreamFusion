#!/usr/bin/env python3
"""Verify released Paimon bundles cannot shadow either supported host RowData API."""
import os
import pathlib
import subprocess
import tempfile
import unittest
import xml.etree.ElementTree as ET

ROOT = pathlib.Path(__file__).resolve().parents[1]
NS = {"m": "http://maven.apache.org/POM/4.0.0"}
ET.register_namespace("", NS["m"])


class PaimonHostClasspathTest(unittest.TestCase):
    def test_released_matrix_uses_host_rowdata_and_arraydata(self):
        pom = ET.parse(ROOT / "streamfusion-paimon/pom.xml").getroot()
        dependencies = list(pom.find("m:dependencies", NS))
        common = next(i for i, d in enumerate(dependencies)
                      if d.findtext("m:artifactId", namespaces=NS) == "flink-table-common")
        paimon = next(i for i, d in enumerate(dependencies)
                      if d.findtext("m:groupId", namespaces=NS) == "org.apache.paimon")
        self.assertLess(common, paimon)
        # Keep the same direct dependency order as the production connector module.
        selected = [dependencies[common], dependencies[paimon]]
        for line, version in [("1.18", "1.18.1"), ("2.2", "2.2.1")]:
            with self.subTest(line=line), tempfile.TemporaryDirectory() as temp:
                directory = pathlib.Path(temp)
                fragments = "".join(
                    ET.tostring(d, encoding="unicode").replace(
                        ' xmlns="http://maven.apache.org/POM/4.0.0"', "")
                    for d in selected)
                fragments = (fragments.replace("${flink.version}", version)
                             .replace("${flink.line}", line)
                             .replace("${paimon.version}", "2.0.0"))
                (directory / "pom.xml").write_text(
                    '<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion>'
                    '<groupId>test</groupId><artifactId>host-api-probe</artifactId><version>1</version>'
                    '<dependencies>' + fragments + '</dependencies></project>')
                subprocess.run(["mvn", "-B", "-ntp", "-f", str(directory / "pom.xml"),
                                "org.apache.maven.plugins:maven-dependency-plugin:3.11.0:build-classpath", "-Dmdep.outputFile=" + str(directory / "classpath")],
                               check=True, text=True)
                cp = (directory / "classpath").read_text().strip()
                # Compile the same shared row view that failed in the universal image reactor.
                projector = ROOT / "src/main/java/tech/streamfusion/operator/PrunedRowData.java"
                compatibility = ROOT / f"src/main/java-flink{line}/tech/streamfusion/compat/ProjectedRowDataCompat.java"
                subprocess.run(["javac", "-cp", cp, "-d", str(directory),
                                str(compatibility), str(projector)], check=True)
                if line == "1.18":
                    # Reproduce the original failure by letting the fat connector shadow the host.
                    entries = cp.split(os.pathsep)
                    connector = next(entry for entry in entries if entry.endswith("paimon-flink-1.18-2.0.0.jar"))
                    wrong_cp = os.pathsep.join([connector] + [entry for entry in entries if entry != connector])
                    wrong = subprocess.run(["javac", "-cp", wrong_cp, "-d", str(directory),
                                            str(compatibility), str(projector)], text=True,
                                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    self.assertNotEqual(0, wrong.returncode)
                    self.assertIn("getVariant(int)", wrong.stdout)
                source = directory / "HostProbe.java"
                source.write_text('''
public class HostProbe {
  public static void main(String[] args) throws Exception {
    for (String type : new String[]{"RowData", "ArrayData"}) {
      Class<?> api = Class.forName("org.apache.flink.table.data." + type);
      String origin = api.getProtectionDomain().getCodeSource().getLocation().toString();
      if (!origin.endsWith("flink-table-common-" + args[0] + ".jar")) throw new AssertionError(origin);
      boolean variant = java.util.Arrays.stream(api.getMethods()).anyMatch(m -> m.getName().equals("getVariant"));
      if (variant != args[0].startsWith("2.2")) throw new AssertionError(type + " variant=" + variant);
      System.out.println(type + " " + origin);
    }
  }
}
''')
                subprocess.run(["javac", "-cp", cp, str(source)], check=True)
                subprocess.run(["java", "-Xverify:all", "-cp", str(directory) + os.pathsep + cp,
                                "HostProbe", version], check=True)


if __name__ == "__main__":
    unittest.main()
