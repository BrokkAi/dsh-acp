from pathlib import Path
import re
import unittest

import release


class TargetLists(unittest.TestCase):
    """The release targets are listed in several languages; they must agree."""

    def test_launcher_matches_release(self):
        launcher = Path('npm/bin/dsh-acp.cjs').read_text()
        targets = re.findall(r"'(?:darwin|linux|win32)-(?:x64|arm64)': '([^']+)'", launcher)
        self.assertCountEqual(targets, release.TARGETS)

    def test_release_workflow_matches_release(self):
        workflow = Path('.github/workflows/release.yml').read_text()
        self.assertCountEqual(re.findall(r'^\s+target: (\S+)$', workflow, re.M), release.TARGETS)


if __name__ == '__main__':
    unittest.main()
