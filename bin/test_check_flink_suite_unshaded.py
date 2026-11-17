"""Detect shaded reactor output replacing any prepared source-suite artifact."""

import importlib.util
from pathlib import Path
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location(
    "unshaded", Path(__file__).with_name("check-flink-suite-unshaded.py"))
UNSHADED = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(UNSHADED)


class UnshadedArtifactTest(unittest.TestCase):
    def fixture(self, root, version):
        repository = root / "m2"
        outputs = []
        for module in UNSHADED.MODULES:
            artifact = module + ("_2.12" if module == "flink-table-planner" else "")
            pom = root / f"{module}-{version}-effective.pom"
            pom.write_text(f'''<project xmlns="http://maven.apache.org/POM/4.0.0">
<groupId>org.apache.flink</groupId><artifactId>{artifact}</artifactId><version>{version}</version>
<dependencies><dependency><groupId>org.apache.calcite.avatica</groupId>
<artifactId>avatica-core</artifactId><version>1.22.0</version></dependency></dependencies>
</project>''')
            original = root / f"{module}-{version}-unshaded.jar"
            original.write_bytes(b"original unshaded classes: " + module.encode())
            target = repository / "org/apache/flink" / artifact / version
            target.mkdir(parents=True)
            for source, extension in ((pom, "pom"), (original, "jar")):
                output = target / f"{artifact}-{version}.{extension}"
                output.write_bytes(source.read_bytes())
                outputs.append(output)
        return repository, outputs

    def test_both_lines_reject_every_overwritten_or_missing_jar_and_pom(self):
        with tempfile.TemporaryDirectory() as directory:
            for version in ("1.18.1", "2.2.1"):
                root = Path(directory) / version
                root.mkdir()
                repository, outputs = self.fixture(root, version)
                UNSHADED.validate(root, repository, version)
                for output in outputs:
                    original = output.read_bytes()
                    for mutation in ("shaded", "missing"):
                        with self.subTest(version=version, artifact=output.name, mutation=mutation):
                            if mutation == "missing":
                                output.unlink()
                            else:
                                output.write_bytes(original.replace(b"avatica-core", b"removed")
                                                   if output.suffix == ".pom" else b"shaded classes")
                            with self.assertRaises((ValueError, FileNotFoundError)):
                                UNSHADED.validate(root, repository, version)
                            output.write_bytes(original)
                            UNSHADED.validate(root, repository, version)

    def test_unresolved_and_wrong_line_coordinates_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repository, _ = self.fixture(root, "1.18.1")
            pom = root / "flink-sql-parser-1.18.1-effective.pom"
            original = pom.read_text()
            for version in ("${revision}", "2.2.1"):
                pom.write_text(original.replace("<version>1.18.1</version>", f"<version>{version}</version>"))
                with self.subTest(version=version), self.assertRaises(ValueError):
                    UNSHADED.validate(root, repository, "1.18.1")


if __name__ == "__main__":
    unittest.main()
