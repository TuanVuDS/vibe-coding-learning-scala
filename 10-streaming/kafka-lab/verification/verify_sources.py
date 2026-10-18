"""Fetch exact POMs and metadata from Maven Central; record date, URL and SHA-256."""
from datetime import date
import hashlib
from pathlib import Path
import urllib.request
import xml.etree.ElementTree as ET

root = Path(__file__).resolve().parent
coordinates = [
    ('org.scala-lang', 'scala3-compiler_3', '3.9.0'),
    ('co.fs2', 'fs2-core_3', '3.13.0'),
    ('org.typelevel', 'cats-effect_3', '3.7.1'),
    ('org.scalameta', 'munit_3', '1.2.4'),
    ('org.typelevel', 'munit-cats-effect_3', '2.2.0'),
    ('org.apache.kafka', 'kafka-clients', '4.4.0'),
    ('org.apache.kafka', 'kafka_2.13', '4.4.0'),
    ('io.github.embeddedkafka', 'embedded-kafka_3', '4.4.0'),
    ('org.typelevel', 'fs2-kafka_3', '4.1.3'),
    ('io.circe', 'circe-parser_3', '0.14.17'),
    ('org.slf4j', 'slf4j-nop', '2.0.20'),
    ('org.scala-sbt', 'sbt', '1.12.11'),
]
lines = [f'Checked: {date.today().isoformat()}']
ns = {'m': 'http://maven.apache.org/POM/4.0.0'}
for group, artifact, version in coordinates:
    base = f'https://repo1.maven.org/maven2/{group.replace(".", "/")}/{artifact}/'
    url = f'{base}{version}/{artifact}-{version}.pom'
    data = urllib.request.urlopen(url, timeout=30).read()
    (root / f'{artifact}-{version}.pom').write_bytes(data)
    lines += [url, 'sha256=' + hashlib.sha256(data).hexdigest()]
    if artifact in {'fs2-kafka_3', 'embedded-kafka_3'}:
        for dep in ET.fromstring(data).findall('.//m:dependency', ns):
            values = [dep.findtext('m:' + field, namespaces=ns) or 'compile' for field in
                      ['groupId', 'artifactId', 'version', 'scope']]
            lines.append('dependency ' + ':'.join(values))
    if artifact in {'kafka-clients', 'fs2-kafka_3'}:
        metadata = urllib.request.urlopen(base + 'maven-metadata.xml', timeout=30).read()
        (root / f'{artifact}-metadata.xml').write_bytes(metadata)
        tree = ET.fromstring(metadata)
        lines.append('metadata latest=' + tree.findtext('versioning/latest') +
                     ' release=' + tree.findtext('versioning/release'))
(root / 'sources.txt').write_text('\n'.join(lines) + '\n', encoding='utf-8')
print('\n'.join(lines))
