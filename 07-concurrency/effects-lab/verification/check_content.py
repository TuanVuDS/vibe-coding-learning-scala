"""Đối chiếu cấu trúc bài, code trích và output thật; không chạy unit test thay munit."""
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
chapter = ROOT / '07-concurrency/ch16b-cats-effect.md'
solution = ROOT / 'solutions/ch16b-solutions.md'
text = chapter.read_text(encoding='utf-8')
assert text.startswith('# Chương 16B')
assert list(map(int, re.findall(r'^## (\d+)\.', text, re.M))) == list(range(1, 18))

for path in [chapter, solution]:
    visible = re.sub(r'```.*?```', '', path.read_text(encoding='utf-8'), flags=re.S)
    visible = re.sub(r'`[^`\n]+`', '', visible)
    for url in re.findall(r'\]\(([^)]+)\)', visible):
        if '://' not in url:
            assert (path.parent / url.split('#')[0]).exists(), (path, url)

sources = []
for directory in ['07-concurrency/effects-lab/src', 'solutions/effects-code/src']:
    sources.extend((ROOT / directory).rglob('*.scala'))

def code_lines(value):
    return [line.split('//')[0].strip() for line in value.splitlines()
            if line.split('//')[0].strip()]

compiled_lines = set()
for source in sources:
    compiled_lines.update(code_lines(source.read_text(encoding='utf-8')))
snippet_count = 0
for number in range(2, 12):
    section = text.split(f'### 3.{number}.', 1)[1].split('### 3.', 1)[0]
    snippets = re.findall(r'```scala\n(.*?)```', section, re.S)
    assert snippets, number
    for snippet in snippets:
        assert 5 <= len(snippet.strip().splitlines()) <= 15, (number, snippet)
        for line in code_lines(snippet):
            assert line in compiled_lines, (number, line)
        snippet_count += 1

section9 = text.split('## 9.', 1)[1].split('## 10.', 1)[0]
for block, name in zip(re.findall(r'```text\n(.*?)```', section9, re.S), ['simple', 'demo'], strict=True):
    log_lines = (HERE / f'{name}.log').read_text(encoding='utf-8').splitlines()
    for line in block.splitlines():
        assert line in log_lines, (name, line)

for name, count in [('lab-test', 22), ('solutions-test', 12)]:
    log = (HERE / f'{name}.log').read_text(encoding='utf-8')
    assert 'Exit: 0' in log and f'0 failed, 0 ignored, {count} total' in log
    assert re.search(r'\bwarn(?:ing)?\b', log, re.I) is None, name

runtime = (HERE / 'errors.log').read_text(encoding='utf-8') + (HERE / 'demo.log').read_text(encoding='utf-8')
section10 = text.split('## 10.', 1)[1].split('## 11.', 1)[0]
for message in re.findall(r'`(java\.[^`]+)`', section10):
    assert message in runtime, message

for section, name in [('10.2', 'missing-syntax'), ('10.3', 'missing-runtime'),
                      ('10.4', 'discarded-io-nonunit-statement')]:
    excerpt = section10.split(f'### {section}.', 1)[1].split('### ', 1)[0]
    output = (HERE / f'compiler-{name}.log').read_text(encoding='utf-8')
    assert 'Exit: 1' in output
    for block in re.findall(r'```text\n(.*?)```', excerpt, re.S):
        for line in block.splitlines():
            assert line in output.splitlines(), (name, line)
for name in ['missing-syntax-baseline', 'missing-runtime-baseline', 'discarded-io-baseline',
             'discarded-io-value-discard']:
    output = (HERE / f'compiler-{name}.log').read_text(encoding='utf-8')
    assert 'Exit: 0' in output and 'warning found' not in output

paths = [chapter, solution]
for directory in ['07-concurrency/effects-lab', 'solutions/effects-code']:
    paths.extend(path for path in (ROOT / directory).rglob('*') if path.is_file()
                 and not any(part in {'.cache', '.scala-build', '.bsp', 'target'} for part in path.parts))
for path in paths:
    data = path.read_text(encoding='utf-8')
    assert data.strip() and '\ufffd' not in data, path
    if path.suffix != '.log':  # Log compiler giữ nguyên khoảng trắng của output thật.
        assert all(line == line.rstrip() for line in data.splitlines()), path

summary = (f'17/17 numbered sections; local links resolve; lesson {len(chapter.read_bytes())} bytes\n'
           f'Theory 3.2-3.11: {snippet_count} snippets, 5-15 lines each; every code line matches compiled lab/solutions source\n'
           'All section 9 output and section 10 runtime/compiler quotes match real logs\n'
           'Compiler: 3 baselines exit 0; 3 deliberate failures exit 1; -Wvalue-discard control exit 0\n'
           'Scala CLI: lab 22/22; solutions 12/12; 0 ignored; 0 compiler warnings\n'
           'Artifacts nonempty, UTF-8 readable; source/docs have no trailing whitespace (raw logs preserved)\n')
(HERE / 'content-check.log').write_text(summary, encoding='utf-8')
print(summary)
