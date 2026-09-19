from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET

from prepare_paimon_runtime_pom import NS, prepare


class PaimonRuntimePomTest(unittest.TestCase):
    def test_released_runtime_precedes_dependencies_without_changing_tests_or_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            module = root / "upstream/common"
            (module / "target/test-classes").mkdir(parents=True)
            source = module / "pom.xml"
            original = (f'<project xmlns="{NS}"><parent><artifactId>parent</artifactId>'
                        '</parent><artifactId>common</artifactId><dependencies><dependency>'
                        '<artifactId>unchanged</artifactId></dependency></dependencies></project>')
            source.write_text(original)
            runtime = root / "m2/2.0.0/paimon-flink-1.18-2.0.0.jar"
            runtime.parent.mkdir(parents=True)
            runtime.touch()
            output = root / "diagnostics/runtime-pom.xml"
            prepare(source, runtime, output)
            self.assertEqual(original, source.read_text())
            pom = ET.parse(output).getroot()
            ns = {"m": NS}
            dependencies = pom.findall("m:dependencies/m:dependency", ns)
            self.assertEqual(["paimon-flink-1.18", "unchanged"],
                             [d.findtext("m:artifactId", namespaces=ns) for d in dependencies])
            self.assertEqual("2.0.0", dependencies[0].findtext("m:version", namespaces=ns))
            self.assertEqual("test", dependencies[0].findtext("m:scope", namespaces=ns))
            config = pom.find("m:build/m:plugins/m:plugin/m:configuration", ns)
            classes = Path(config.findtext("m:classesDirectory", namespaces=ns))
            self.assertEqual([], list(classes.iterdir()))
            (classes / "StaleHelper.class").write_bytes(b"old common helper")
            prepare(source, runtime, output)
            self.assertEqual([], list(classes.iterdir()))
            self.assertEqual(str(module.resolve() / "target/test-classes"),
                             config.findtext("m:testClassesDirectory", namespaces=ns))
            self.assertEqual(str(module.resolve()), config.findtext("m:workingDirectory", namespaces=ns))
            with self.assertRaisesRegex(ValueError, "remain unchanged"):
                prepare(source, runtime, source)
            source.write_text(original.replace("<artifactId>common</artifactId>",
                                               "<artifactId>paimon-flink-1.18</artifactId>"))
            prepare(source, runtime, output)
            line_pom = ET.parse(output).getroot()
            self.assertEqual("streamfusion-upstream-paimon-line-tests",
                             line_pom.findtext("m:artifactId", namespaces=ns))
            self.assertEqual("paimon-flink-1.18", line_pom.findtext(
                "m:dependencies/m:dependency/m:artifactId", namespaces=ns))
            wrong_line = runtime.with_name("paimon-flink-2.2-2.0.0.jar")
            wrong_line.touch()
            with self.assertRaisesRegex(ValueError, "canonical released Flink 1.18"):
                prepare(source, wrong_line, output)
            with self.assertRaisesRegex(ValueError, "must exist"):
                prepare(source, root / "missing.jar", output)


if __name__ == "__main__":
    unittest.main()
