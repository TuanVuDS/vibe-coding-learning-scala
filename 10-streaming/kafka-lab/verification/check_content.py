"""Structural QA for the two chapters, local links, real-output excerpts and successful logs."""
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[3]
chapters = {
    '10-streaming/ch27-streaming-fundamentals.md': ('demo', '10-streaming/streaming-lab/verification/demo.txt'),
    '10-streaming/ch28-kafka-scala.md': ('broker', '10-streaming/kafka-lab/verification/broker.txt'),
}

def output(log):
    text = (ROOT / log).read_text(encoding='utf-8').split('\n', 1)[1]
    return text.rsplit('\nEXIT:', 1)[0].strip()

def sync():
    for relative, (name, log) in chapters.items():
        path = ROOT / relative
        text = path.read_text(encoding='utf-8')
        extracted = output(log)
        if name == 'broker':
            extracted = extracted.split('Test run ', 1)[0].strip()
        pattern = rf'<!-- output:{name} -->.*?<!-- /output:{name} -->'
        replacement = f'<!-- output:{name} -->\n```text\n{extracted}\n```\n<!-- /output:{name} -->'
        path.write_text(re.sub(pattern, lambda _: replacement, text, flags=re.S), encoding='utf-8')

def check():
    files = [ROOT / f for f in chapters] + [ROOT / 'solutions/ch27-solutions.md', ROOT / 'solutions/ch28-solutions.md',
        ROOT / '.team/handoffs/T13-ch27-28-streaming-kafka.md']
    for path in files:
        text = path.read_text(encoding='utf-8')
        if path.name.startswith(('ch27-streaming', 'ch28-kafka')):
            headings = list(re.finditer(r'^## (\d+)\. .+$', text, flags=re.M))
            assert [int(h[1]) for h in headings] == list(range(1, 18)), path
            for i, h in enumerate(headings):
                end = headings[i + 1].start() if i < 16 else len(text)
                assert text[h.end():end].strip(), (path, h[1])
        assert 'Đang đồng bộ' not in text and 'TODO' not in text, path
        prose = re.sub(r'```.*?```|`[^`\n]*`', '', text, flags=re.S)
        for target in re.findall(r'\[[^\]\n]+\]\(([^)\n]+)\)', prose):
            if target.startswith(('https:', 'http:', '#')):
                continue
            local = target.split('#', 1)[0]
            assert (path.parent / local).resolve().exists(), (path, target)
        print(f'PASS content and local links: {path.relative_to(ROOT)}')
    logs = [
        '10-streaming/streaming-lab/verification/tests.txt',
        '10-streaming/streaming-lab/verification/demo.txt',
        '10-streaming/kafka-lab/verification/tests.txt',
        '10-streaming/kafka-lab/verification/broker.txt',
        '10-streaming/kafka-lab/verification/errors.txt',
        'solutions/ch27-code/tests.txt', 'solutions/ch28-code/tests.txt',
    ]
    for relative in logs:
        text = (ROOT / relative).read_text(encoding='utf-8')
        assert text.rstrip().endswith('EXIT: 0'), relative
        assert not re.search(r'warning found|warnings found|Compilation failed', text), relative
        print(f'PASS exit 0, no compiler warnings: {relative}')

if __name__ == '__main__':
    sync()
    check()
