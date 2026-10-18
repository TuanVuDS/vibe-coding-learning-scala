"""Đọc nguồn chính thức; không chạy trong unit test (cần mạng)."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import urllib.request
import xml.etree.ElementTree as ET

BASE = "https://repo.maven.apache.org/maven2/"
TARGET = Path(__file__).resolve().parent
NS = {"m": "http://maven.apache.org/POM/4.0.0"}
records = []


def fetch(url):
    with urllib.request.urlopen(url, timeout=30) as response:
        content = response.read()
    return content, {"url": url, "sha256": hashlib.sha256(content).hexdigest()}


for artifact in ["org/typelevel/cats-effect_3", "org/typelevel/munit-cats-effect_3", "dev/zio/zio_3"]:
    data, record = fetch(BASE + artifact + "/maven-metadata.xml")
    root = ET.fromstring(data)
    record.update(release=root.findtext("versioning/release"),
                  lastUpdated=root.findtext("versioning/lastUpdated"))
    if artifact.endswith("cats-effect_3") and "munit" not in artifact:
        versions = [v.text for v in root.findall("versioning/versions/version")
                    if v.text.startswith("3.7.") and v.text.count(".") == 2
                    and all(p.isdigit() for p in v.text.split("."))]
        record["latestStable37"] = max(versions, key=lambda v: tuple(map(int, v.split("."))))
    records.append(record)

for artifact, version in [("org/typelevel/cats-effect_3", "3.7.1"),
                          ("org/typelevel/cats-effect-testkit_3", "3.7.1"),
                          ("org/typelevel/munit-cats-effect_3", "2.2.0"),
                          ("org/typelevel/munit-cats-effect_3", "2.2.1"),
                          ("dev/zio/zio_3", "2.1.27")]:
    name = artifact.rsplit("/", 1)[1]
    data, record = fetch(f"{BASE}{artifact}/{version}/{name}-{version}.pom")
    root = ET.fromstring(data)
    record["dependencies"] = [{key: dep.findtext("m:" + key, namespaces=NS)
                               for key in ["groupId", "artifactId", "version", "scope"]}
                              for dep in root.findall("m:dependencies/m:dependency", NS)]
    records.append(record)

url = ("https://raw.githubusercontent.com/typelevel/cats-effect/v3.7.1/"
       "testkit/shared/src/main/scala/cats/effect/testkit/TestControl.scala")
data, record = fetch(url)
record["hasExecuteEmbed"] = "def executeEmbed[A]" in data.decode()
records.append(record)
result = {"checkedAtUtc": datetime.now(timezone.utc).isoformat(), "records": records}
(TARGET / "versions.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print("Verified Maven metadata, 5 POMs and TestControl source; saved versions.json")
