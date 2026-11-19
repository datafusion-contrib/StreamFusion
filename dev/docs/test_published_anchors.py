"""Protect old documentation bookmarks and their new topic destinations."""

from html.parser import HTMLParser
import json
from pathlib import Path
import unittest

import markdown

ROOT = Path(__file__).resolve().parents[2]


class Anchors(HTMLParser):
    def __init__(self, path):
        super().__init__()
        self.ids = set()
        self.feed(markdown.markdown(path.read_text(), extensions=[
            "toc", "attr_list", "tables", "admonition", "pymdownx.superfences",
            "pymdownx.details", "pymdownx.highlight", "pymdownx.inlinehilite"]))

    def handle_starttag(self, tag, attrs):
        for key, value in attrs:
            if key == "id":
                if value in self.ids:
                    raise ValueError(f"Duplicate published anchor: {value}")
                self.ids.add(value)


class PublishedAnchorsTest(unittest.TestCase):
    def test_every_original_bookmark_and_topic_anchor_remains_available(self):
        inventory = json.loads((ROOT / "dev/docs/published-anchors.json").read_text())
        self.assertEqual(1, inventory["schema_version"])
        paths = set(inventory["pages"])
        for anchors in inventory["pages"].values():
            paths.update(anchors.values())
        rendered = {name: Anchors(ROOT / name).ids for name in paths}
        for original, anchors in inventory["pages"].items():
            for anchor, destination in anchors.items():
                with self.subTest(original=original, anchor=anchor):
                    self.assertIn(anchor, rendered[original])
                    self.assertIn(anchor, rendered[destination])


if __name__ == "__main__":
    unittest.main()
