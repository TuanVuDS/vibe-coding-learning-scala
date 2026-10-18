"""Download Maven evidence using Python TLS (Windows sandbox Schannel is unavailable).
Run from formats-lab: python verify-sources.py. Only writes verification/ under this lab.
"""
from datetime import datetime, timezone
from pathlib import Path
import urllib.request
import xml.etree.ElementTree as ET

ARTIFACTS = [
    ("org.apache.avro", "avro", "1.12.2"),
    ("org.apache.parquet", "parquet-avro", "1.18.1"),
    ("org.apache.hadoop", "hadoop-client-api", "3.4.2"),
    ("org.apache.hadoop", "hadoop-client-runtime", "3.4.2"),
    ("org.apache.commons", "commons-csv", "1.14.1"),
    ("io.circe", "circe-parser_3", "0.14.17"),
    ("org.slf4j", "slf4j-nop", "2.0.17"),
    ("org.scalameta", "munit_3", "1.2.4"),
]
out = Path(__file__).parent / "verification"
out.mkdir(exist_ok=True)
lines = [f"Retrieved UTC: {datetime.now(timezone.utc).isoformat()}"]
for group, artifact, version in ARTIFACTS:
    base = f"https://repo1.maven.org/maven2/{group.replace('.', '/')}/{artifact}"
    metadata = urllib.request.urlopen(f"{base}/maven-metadata.xml").read()
    root = ET.fromstring(metadata)
    assert version in [v.text for v in root.findall("versioning/versions/version")]
    (out / f"{artifact}-metadata.xml").write_bytes(metadata)
    url = f"{base}/{version}/{artifact}-{version}.pom"
    pom = urllib.request.urlopen(url).read()
    (out / f"{artifact}-{version}.pom").write_bytes(pom)
    lines.append(f"{group}:{artifact}:{version} verified; metadata release={root.findtext('versioning/release')}; POM={url}")
(out / "sources.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
print("\n".join(lines))
