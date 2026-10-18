from pathlib import Path
import re
root=Path(__file__).resolve().parents[3]
chapter=root/'05-backend/ch18-backend-architecture.md'
text=chapter.read_text(encoding='utf-8')
sections=re.findall(r'^## (\d+)\. ',text,re.M)
assert sections==[str(i) for i in range(1,18)],sections
bodies=re.split(r'^## \d+\. [^\n]+\n',text,flags=re.M)[1:]
assert len(bodies)==17 and all(b.strip() for b in bodies)
files=[chapter,root/'solutions/ch18-solutions.md',root/'14-projects/p2-backend-service/README.md']
for f in files:
    content=f.read_text(encoding='utf-8')
    for target in re.findall(r'\]\(([^)]+)\)',content):
        if '://' in target or target.startswith('#'): continue
        target=target.split('#')[0]
        assert (f.parent/target).exists(), (f,target)
    assert '???' not in content, f
print('17 lesson sections; local links resolve; no placeholder headings')
