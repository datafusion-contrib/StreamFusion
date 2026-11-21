from pathlib import Path
import unittest


from dev.tools.checks import native_glibc as checker


class GlibcRequirementsTest(unittest.TestCase):
    def test_released_flink_image_baseline(self):
        checker.validate_versions(
            "0x0010: Name: GLIBC_2.3.4 Flags: none Version: 12\n"
            "0x0020: Name: GLIBC_2.35 Flags: none Version: 11\n"
            "0x0030: Name: GLIBCXX_3.4.29 Flags: none Version: 10"
        )

    def test_newer_glibc_is_rejected(self):
        for version in ["2.36", "2.38", "2.100", "3.0"]:
            with self.subTest(version=version), self.assertRaisesRegex(ValueError, "baseline"):
                checker.validate_versions(f"Name: GLIBC_{version} Flags: none Version: 1")

    def test_unknown_or_missing_requirements_are_rejected(self):
        for output in ["No version information found", "Name: GLIBC_ABI_DT_RELR", "Name: GLIBC_PRIVATE"]:
            with self.subTest(output=output), self.assertRaises(ValueError):
                checker.validate_versions(output)


if __name__ == "__main__":
    unittest.main()
