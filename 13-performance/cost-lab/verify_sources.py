"""Kiểm artifact/version đã pin trong POM và lưu bằng chứng HTTP từ nguồn chính thức."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import urllib.request
import xml.etree.ElementTree as ET
import winreg

OUT = Path(__file__).resolve().parent / "verification"
PINS = [
    ("org/scala-lang", "scala3-compiler_3", "3.9.0"),
    ("org/scalameta", "munit_3", "1.2.4"),
    ("io/circe", "circe-parser_3", "0.14.17"),
    ("org/apache/avro", "avro", "1.12.2"),
    ("org/slf4j", "slf4j-nop", "2.0.17"),
    ("org/scala-sbt", "sbt", "1.12.11"),
]
DOCS = [
    "https://raw.githubusercontent.com/openjdk/jmh/master/jmh-samples/src/main/java/org/openjdk/jmh/samples/JMHSample_08_DeadCode.java",
    "https://raw.githubusercontent.com/openjdk/jmh/master/jmh-samples/src/main/java/org/openjdk/jmh/samples/JMHSample_12_Forking.java",
    "https://docs.oracle.com/en/java/javase/21/docs/specs/man/java.html",
    "https://docs.oracle.com/en/java/javase/21/docs/api/java.management/java/lang/management/MemoryMXBean.html",
    "https://docs.oracle.com/en/java/javase/21/docs/api/jdk.management/com/sun/management/OperatingSystemMXBean.html",
    "https://avro.apache.org/docs/1.12.0/specification/",
    "https://spark.apache.org/docs/4.2.0/sql-performance-tuning.html",
    "https://spark.apache.org/docs/4.2.0/monitoring.html",
    "https://web.mit.edu/urban_or_book/www/book/chapter4/4.1.html",
]


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    records = []
    for group, artifact, version in PINS:
        url = f"https://repo.maven.apache.org/maven2/{group}/{artifact}/{version}/{artifact}-{version}.pom"
        with urllib.request.urlopen(url, timeout=30) as response:
            body = response.read()
            status = response.status
        root = ET.fromstring(body)
        ns = {"p": "http://maven.apache.org/POM/4.0.0"}
        assert root.findtext("p:artifactId", namespaces=ns) == artifact
        resolved_version = root.findtext("p:version", namespaces=ns) or root.findtext("p:parent/p:version", namespaces=ns)
        assert resolved_version == version, (artifact, resolved_version)
        filename = f"{artifact}-{version}.pom"
        (OUT / filename).write_bytes(body)
        records.append(dict(url=url, status=status, file=filename, sha256=hashlib.sha256(body).hexdigest()))
    for url in DOCS:
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                body = response.read()
                records.append(dict(url=url, status=response.status, bytes=len(body), sha256=hashlib.sha256(body).hexdigest()))
        except Exception as error:
            records.append(dict(url=url, error=str(error)))
    with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"HARDWARE\DESCRIPTION\System\CentralProcessor\0") as key:
        cpu = winreg.QueryValueEx(key, "ProcessorNameString")[0].strip()
    report = dict(verified_at_utc=datetime.now(timezone.utc).isoformat(), cpu_registry=cpu, sources=records)
    (OUT / "sources.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
