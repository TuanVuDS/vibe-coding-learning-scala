"""Download exact release POMs and record primary-source URLs, dates and hashes."""
from datetime import date
import hashlib
import json
from pathlib import Path
import urllib.request

BASE = Path(__file__).resolve().parent
COORDS = [
    ('org/scala-lang', 'scala3-compiler_3', '3.9.0'),
    ('org/apache/kafka', 'kafka-clients', '4.4.0'),
    ('io/github/embeddedkafka', 'embedded-kafka_3', '4.4.0'),
    ('org/apache/kafka', 'kafka_2.13', '4.4.0'),
    ('io/circe', 'circe-parser_3', '0.14.17'),
    ('io/circe', 'circe-generic_3', '0.14.17'),
    ('org/scalameta', 'munit_3', '1.2.4'),
    ('org/slf4j', 'slf4j-nop', '2.0.20'),
    ('org/scala-sbt', 'sbt', '1.12.11'),
]
rows = []
for group, artifact, version in COORDS:
    filename = f'{artifact}-{version}.pom'
    url = f'https://repo1.maven.org/maven2/{group}/{artifact}/{version}/{filename}'
    with urllib.request.urlopen(url, timeout=40) as response:
        data = response.read()
        status = response.status
    (BASE / filename).write_bytes(data)
    rows.append(dict(artifact=artifact, version=version, url=url, status=status,
                     checked=str(date.today()), sha256=hashlib.sha256(data).hexdigest()))
(BASE / 'versions.json').write_text(json.dumps(rows, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
print(f'{len(rows)} exact release POMs fetched, HTTP 200; date={date.today()}')
