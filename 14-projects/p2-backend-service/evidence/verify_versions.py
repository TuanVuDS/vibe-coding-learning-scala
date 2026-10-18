"""Fetch exact release POMs from Maven Central; never infer Scala 3 from _2.13."""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from urllib.request import urlopen
import hashlib, json, re
from datetime import datetime, timezone
root = Path(__file__).resolve().parents[1]
coords = []
for line in (root / "project.scala").read_text(encoding="utf-8").splitlines():
    match = re.search(r"using (?:test\.)?dep ([^:]+)(::?)([^:]+):([^\s]+)", line)
    if match:
        group, sep, artifact, version = match.groups()
        coords.append((group, artifact + ("_3" if sep == "::" else ""), version))
coords += [("org.scala-lang", "scala3-compiler_3", "3.9.0"),
           ("org.scala-sbt", "sbt", "1.12.11"),
           ("com.eed3si9n", "sbt-assembly_2.12_1.0", "2.3.1")]
def check(coord):
    group, artifact, version = coord
    url = f"https://repo1.maven.org/maven2/{group.replace('.', '/')}/{artifact}/{version}/{artifact}-{version}.pom"
    with urlopen(url, timeout=40) as response:
        data = response.read()
        return {"coordinate": ":".join(coord), "url": url, "status": response.status,
                "sha256": hashlib.sha256(data).hexdigest()}
with ThreadPoolExecutor(max_workers=6) as pool:
    results = list(pool.map(check, coords))
output = {"verified_at": datetime.now(timezone.utc).isoformat(), "releases": results}
(root / "evidence/versions.json").write_text(json.dumps(output, ensure_ascii=False, indent=2)+"\n",encoding="utf-8")
print(f"Verified {len(results)} exact release POMs, all HTTP 200")
