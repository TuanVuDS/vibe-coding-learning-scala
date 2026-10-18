"""Check chapter excerpts against compiled source and runtime-exported schemas.

Run after Scala CLI tests and ChapterExamples; this checks provenance, not compilation.
"""

import json
from pathlib import Path
import re
import textwrap


lab = Path(__file__).resolve().parent
chapter = lab.parent / "ch21-data-formats.md"
text = chapter.read_text(encoding="utf-8")
report = []

# Every Scala fence must name its source immediately above it.
all_scala = re.findall(r"```scala\n(.*?)\n```", text, re.S)
quoted = re.findall(r"Trích \[[^\]]+\]\(([^)]+)\):\n\n```scala\n(.*?)\n```", text, re.S)
assert len(all_scala) == len(quoted) and quoted, "missing source citation"
allowed = (lab / "src", lab.parent.parent / "solutions/ch21-code/src")
for index, (target, snippet) in enumerate(quoted, 1):
    source = (chapter.parent / target).resolve()
    assert any(source.is_relative_to(root.resolve()) for root in allowed), source
    assert source.suffix == ".scala", source
    lines = source.read_text(encoding="utf-8").splitlines()
    count = len(snippet.splitlines())
    assert 5 <= count <= 20, f"snippet {index}: {count} lines, expected 5..20"
    matches = [i for i in range(len(lines) - count + 1)
               if textwrap.dedent("\n".join(lines[i:i + count])) == snippet]
    assert matches, f"snippet {index} differs from {source}"
    relative = source.relative_to(lab.parent.parent).as_posix()
    report.append(f"Scala {index:02}: OK {relative}:{matches[0] + 1} ({count} lines)")

schemas = lab / "verification/schemas"
exports = {p.name: json.loads(p.read_text(encoding="utf-8")) for p in schemas.glob("*.avsc")}
assert len(exports) == 4, "run ChapterExamples and copy its four .avsc exports"
json_blocks = re.findall(r"```json\n(.*?)\n```", text, re.S)
assert len(json_blocks) == 4, "expected v1 plus default/alias/nullable variants"
seen = set()
for snippet in json_blocks:
    parsed = json.loads(snippet)
    matches = [name for name, value in exports.items() if value == parsed]
    assert len(matches) == 1, "JSON block differs from runtime-exported schema"
    assert matches[0] not in seen, "duplicate schema in chapter"
    seen.add(matches[0])
    report.append(f"JSON: OK verification/schemas/{matches[0]}")

sections = re.findall(r"^## (\d+)\. ", text, re.M)
assert sections == [str(i) for i in range(1, 18)], "expected 17 ordered lesson sections"
assert text.startswith("# Chương 21"), "title must precede version block"
report.append(f"PASS: {len(quoted)} Scala excerpts; {len(seen)} runtime schemas; 17 sections")
output = "\n".join(report) + "\n"
(lab / "verification/snippets-round2.txt").write_text(output, encoding="utf-8")
print(output, end="")
