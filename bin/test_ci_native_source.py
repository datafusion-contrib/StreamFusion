"""Keep transient API failures separate from native source and producer failures."""

import json
import subprocess
import unittest
from unittest.mock import patch

import ci_native_source as source


class NativeSourceTest(unittest.TestCase):
    def test_transient_server_errors_retry_and_permission_failures_stop(self):
        failure = subprocess.CompletedProcess([], 1, "", "gh: Server Error (HTTP 502)")
        success = subprocess.CompletedProcess([], 0, json.dumps({"value": "verified"}), "")
        with patch.object(source.subprocess, "run", side_effect=[failure, failure, success]) as run, \
                patch.object(source.time, "sleep") as sleep:
            self.assertEqual({"value": "verified"}, source.api("fixture"))
            self.assertEqual(3, run.call_count)
            self.assertEqual([1, 2], [call.args[0] for call in sleep.call_args_list])
        denied = subprocess.CompletedProcess([], 1, "", "gh: Forbidden (HTTP 403)")
        with patch.object(source.subprocess, "run", return_value=denied) as run:
            with self.assertRaisesRegex(RuntimeError, "403"):
                source.api("fixture")
            self.assertEqual(1, run.call_count)

    def test_repeated_outages_are_bounded(self):
        failure = subprocess.CompletedProcess([], 1, "", "gh: Server Error (HTTP 503)")
        with patch.object(source.subprocess, "run", return_value=failure) as run, \
                patch.object(source.time, "sleep"):
            with self.assertRaisesRegex(RuntimeError, "503"):
                source.api("fixture")
            self.assertEqual(3, run.call_count)

    def test_producer_pagination_retains_both_source_verification_commands(self):
        sha = "a" * 40
        ordinary = {"name": "Other job", "conclusion": "success"}
        producer = {"name": "Shared debug native libraries", "conclusion": "success"}
        with patch.object(source, "api", side_effect=[{"head_sha": sha}, {"jobs": [ordinary] * 20},
                                                       {"jobs": [producer]}]) as api, \
                patch.object(source.subprocess, "run") as run:
            self.assertEqual("debug-native-" + sha, source.verify("owner/repo", 123))
            self.assertIn("per_page=20&page=2", api.call_args_list[-1].args[0])
            self.assertEqual("fetch", run.call_args_list[0].args[0][1])
            diff = run.call_args_list[1].args[0]
            self.assertEqual(["git", "diff", "--exit-code", sha, "HEAD", "--", *source.INPUTS], diff)

    def test_failed_producer_and_changed_native_inputs_cannot_be_reused(self):
        sha = "a" * 40
        producer = {"name": "Shared debug native libraries", "conclusion": "failure"}
        with patch.object(source, "api", side_effect=[{"head_sha": sha}, {"jobs": [producer]}]), \
                patch.object(source.subprocess, "run") as run:
            with self.assertRaisesRegex(ValueError, "not succeeded"):
                source.verify("owner/repo", 123)
            run.assert_not_called()
        producer["conclusion"] = "success"
        with patch.object(source, "api", side_effect=[{"head_sha": sha}, {"jobs": [producer]}]), \
                patch.object(source.subprocess, "run", side_effect=[None, subprocess.CalledProcessError(1, "git diff")]):
            with self.assertRaises(subprocess.CalledProcessError):
                source.verify("owner/repo", 123)


if __name__ == "__main__":
    unittest.main()
