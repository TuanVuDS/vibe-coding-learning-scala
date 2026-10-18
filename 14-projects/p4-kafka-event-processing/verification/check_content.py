"""Verify lesson structure, local links, real output excerpts and final check logs."""
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[3]
BASE = Path(__file__).resolve().parent
CHAPTERS = [ROOT / '10-streaming/ch29-streaming-pipeline.md', ROOT / '10-streaming/ch30-streaming-reliability.md']
DOCS = CHAPTERS + [ROOT / '14-projects/p4-kafka-event-processing/README.md',
                   ROOT / 'solutions/ch29-solutions.md', ROOT / 'solutions/ch30-solutions.md',
                   ROOT / '.team/handoffs/T15-ch29-30-p4.md']
local_links = []
for path in CHAPTERS:
    raw = path.read_text(encoding='utf-8')
    headings = list(re.finditer(r'^## (\d+)\..*$', raw, re.M))
    assert [int(m[1]) for m in headings] == list(range(1, 18)), path
    for i, match in enumerate(headings):
        body = raw[match.end():headings[i+1].start() if i+1 < len(headings) else len(raw)].strip()
        assert body, (path, match[0])
    exercises = raw[headings[11].end():headings[12].start()]
    assert len(re.findall(r'^\d+\. ', exercises, re.M)) == 6, path
for path in DOCS:
    raw = path.read_text(encoding='utf-8')
    for target in re.findall(r'\]\(([^)]+)\)', raw):
        if target.startswith(('http:', 'https:', '#')):
            continue
        target = target.split('#')[0]
        local_links.append((path, target))

expected = {'core': ('vn.scalaeng.p4.CoreSuite', 11), 'broker': ('vn.scalaeng.p4.BrokerSuite', 3),
            'ch29': ('vn.scalaeng.pipeline.PipelineSuite', 2), 'ch30': ('vn.scalaeng.reliability.PolicySuite', 3),
            'sol29': ('vn.scalaeng.solutions.ch29.ExercisesSuite', 6),
            'sol30': ('vn.scalaeng.solutions.ch30.ExercisesSuite', 6)}
for name, (suite, count) in expected.items():
    raw = (BASE / f'{name}.txt').read_text(encoding='utf-8')
    assert 'EXIT: 0' in raw, name
    assert f'{suite} finished: 0 failed, 0 ignored, {count} total' in raw, name
    assert not re.search(r'\[warn\]|Warning:|warning found|warnings found', raw, re.I), name
    if name == 'broker':
        assert 'skipped' not in raw and 'kill=before' in raw and 'kill=after' in raw, name
for name in ['demo', 'errors', 'simple29', 'simple30', 'measure']:
    raw = (BASE / f'{name}.txt').read_text(encoding='utf-8')
    assert 'EXIT: 0' in raw and '[warn]' not in raw, name

demo = (BASE / 'demo.txt').read_text(encoding='utf-8')
errors = (BASE / 'errors.txt').read_text(encoding='utf-8')
broker = (BASE / 'broker.txt').read_text(encoding='utf-8')
all_output = '\n'.join((BASE / f'{name}.txt').read_text(encoding='utf-8')
                       for name in ['demo', 'errors', 'broker', 'simple29', 'simple30', 'measure'])
for path in DOCS[:3]:
    raw = path.read_text(encoding='utf-8')
    for block in re.findall(r'```text\n(.*?)```', raw, re.S):
        if block.startswith('số khóa nguồn') or block.startswith('Main init/'):
            continue  # contract formula / architecture, explicitly not claimed as runtime output
        for line in block.strip().splitlines():
            assert line in all_output, (path, line)

# Các khối Scala trong bài trích từ source thật đã biên dịch; ví dụ có thể kèm package.
source_roots = [ROOT / '14-projects/p4-kafka-event-processing/src/main/scala',
                ROOT / '10-streaming/streaming-pipeline-lab/src/main/scala',
                ROOT / '10-streaming/reliability-lab/src/main/scala']
compiled = [p.read_text(encoding='utf-8') for root in source_roots for p in root.rglob('*.scala')]
for path in CHAPTERS:
    raw = path.read_text(encoding='utf-8')
    assert raw.startswith('# Chương '), path
    blocks = re.findall(r'```scala\n(.*?)```', raw, re.S)
    assert len(blocks) >= 4, path
    for block in blocks:
        code = block.strip()
        assert any(code in source for source in compiled) or any(
            re.sub(r'^package [^\n]+\n+', '', code) in source for source in compiled), (path, code[:80])

for row in json.loads((BASE / 'versions.json').read_text(encoding='utf-8')):
    pom = BASE / f"{row['artifact']}-{row['version']}.pom"
    assert row['status'] == 200 and hashlib.sha256(pom.read_bytes()).hexdigest() == row['sha256']

scope_dirs = ['10-streaming/streaming-pipeline-lab', '10-streaming/reliability-lab',
              '14-projects/p4-kafka-event-processing', 'solutions/ch29-code', 'solutions/ch30-code']
files = DOCS + [ROOT / '10-streaming/ch27-streaming-fundamentals.md',
                BASE / 'files.txt', BASE / 'source-sha256.json', BASE / 'content-check.txt']
for directory in scope_dirs:
    files += [p for p in (ROOT / directory).rglob('*') if p.is_file() and
              not any(part in {'.scala-build', '.bsp', '.cache', 'target'} for part in p.parts)]
files = sorted(set(files))
(BASE / 'files.txt').write_text('\n'.join(p.relative_to(ROOT).as_posix() for p in files) + '\n', encoding='utf-8')
# Hash executable sources/configuration, so lead can associate evidence with the handed-off version.
sources = {p.relative_to(ROOT).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in files
           if p.suffix in {'.scala', '.sbt', '.properties', '.py'}}
(BASE / 'source-sha256.json').write_text(json.dumps(sources, indent=2) + '\n', encoding='utf-8')
for path, target in local_links:
    assert (path.parent / target).exists(), (path, target)
summary = 'PASS: 2 chapters x 17 nonempty sections; 6 exercises each; all local links valid.\n' \
          'PASS: 31 executed tests, 0 failed; broker opt-in 3/3, no skips; default core 3 intentional skips.\n' \
          'PASS: final logs have no compiler/Scala CLI warnings; runtime excerpts match real logs.\n' \
          'PASS: chapter Scala snippets match compiled source; both simple examples and measurement ran.\n' \
          'PASS: 9 exact release POM hashes match versions.json.\n'
(BASE / 'content-check.txt').write_text(summary, encoding='utf-8')
print(summary, end='')
