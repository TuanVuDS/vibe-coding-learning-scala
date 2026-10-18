"""Check the chapter's external documentation links; keep status, not page contents."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
import re
import urllib.request

lab = Path(__file__).parent
chapter = (lab.parent / "ch21-data-formats.md").read_text(encoding="utf-8")
urls = sorted(set(re.findall(r"\]\((https://[^)]+)\)", chapter)))
urls = [u for u in urls if "repo1.maven.org" not in u]  # verified by verify-sources.py


def check(url):
    try:
        with urllib.request.urlopen(url, timeout=20) as response:
            return f"{response.status} {url} -> {response.url}"
    except Exception as error:
        return f"{type(error).__name__}: {error} {url}"


with ThreadPoolExecutor(max_workers=8) as pool:
    results = list(pool.map(check, urls))
output = f"Checked: {datetime.now().astimezone().isoformat()}\n" + "\n".join(results) + "\n"
(lab / "verification" / "doc-sources.txt").write_text(output, encoding="utf-8")
print(output)
