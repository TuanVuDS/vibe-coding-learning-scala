"""Check that every Scala fence in Ch14 is an unchanged excerpt of lab source.

Run from any directory: python 05-backend/order-service/evidence/verify_snippets.py
Only removes the common indentation needed to display a nested method in Markdown.
Compilation and execution are recorded separately in tests-round2.txt.
"""

from pathlib import Path
import re
import textwrap


ROOT = Path(__file__).resolve().parents[3]
CHAPTER = ROOT / "05-backend/ch14-rest-apis.md"
LAB = ROOT / "05-backend/order-service"


def normalize(value: str) -> str:
    return textwrap.dedent(value).strip("\n")


def main() -> None:
    chapter = CHAPTER.read_text(encoding="utf-8")
    sources = {
        path: path.read_text(encoding="utf-8").splitlines()
        for path in LAB.rglob("*.scala")
        if not {".cache", ".scala-build", ".bsp", "target"}.intersection(path.parts)
    }
    fences = list(re.finditer(r"^```scala\n(.*?)\n```", chapter, re.M | re.S))
    assert fences, "No Scala excerpts found"
    for number, fence in enumerate(fences, 1):
        code = normalize(fence.group(1))
        length = len(code.splitlines())
        matches = [
            (path, start + 1)
            for path, lines in sources.items()
            for start in range(len(lines) - length + 1)
            if normalize("\n".join(lines[start:start + length])) == code
        ]
        assert matches, f"Scala excerpt {number} differs from compiled lab source"
        caption = re.search(r"Trích \[[^\]]+, từ dòng (\d+)\]\(([^)]+)\):\s*$", chapter[:fence.start()])
        assert caption, f"Scala excerpt {number} has no source caption"
        declared = ((CHAPTER.parent / caption.group(2)).resolve(), int(caption.group(1)))
        assert declared in matches, f"Scala excerpt {number} has an incorrect source caption"
        assert length <= 20, f"Scala excerpt {number} is too long"
        path, line = matches[0]
        print(f"excerpt {number}: {path.relative_to(ROOT).as_posix()}:{line} ({length} lines) OK")

    headings = re.findall(r"^## (\d+)\. ", chapter, re.M)
    assert headings == [str(i) for i in range(1, 18)], headings
    prose = re.sub(r"^```[^\n]*\n.*?^```", "", chapter, flags=re.M | re.S)
    prose = re.sub(r"`[^`]*`", "", prose)
    for label, destination in re.findall(r"\[([^\]]+)\]\(([^)]+)\)", prose):
        if "://" not in destination and not destination.startswith("#"):
            target = (CHAPTER.parent / destination.split("#")[0]).resolve()
            assert target.exists(), f"Missing local link: {label} -> {destination}"
    assert chapter.startswith("# Chương 14")
    print(f"{len(fences)}/{len(fences)} Scala excerpts match source; 17 sections; local links OK")

    summary_pattern = r"^Test run \S+ finished: (\d+) failed, (\d+) ignored, (\d+) total .+$"
    for path, expected in (
        (LAB / "evidence/tests-round2.txt", 26),
        (ROOT / "solutions/ch14-code/test-output-round2.txt", 6),
    ):
        log = path.read_text(encoding="utf-8")
        summaries = list(re.finditer(summary_pattern, log, re.M))
        assert summaries and sum(int(s.group(3)) for s in summaries) == expected
        assert all(s.group(1) == s.group(2) == "0" for s in summaries)
        if expected == 26:
            assert all(s.group(0) in chapter for s in summaries), "Chapter test output differs from log"
        print(f"{path.relative_to(ROOT).as_posix()}: {expected} passed, 0 failed, 0 ignored; OK")


if __name__ == "__main__":
    main()
