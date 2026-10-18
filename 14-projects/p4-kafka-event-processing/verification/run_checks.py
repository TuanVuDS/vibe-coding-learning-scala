"""Reproducible checks; all generated caches/temp files remain inside repository .cache/."""
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
BASE = Path(__file__).resolve().parent
TEMP = ROOT / '.cache/t15-tmp'
TEMP.mkdir(parents=True, exist_ok=True)
ENV = dict(os.environ, COURSIER_CACHE=str(ROOT / '.cache/coursier'), TMP=str(TEMP), TEMP=str(TEMP))
TASKS = {
    'core': ('test', '14-projects/p4-kafka-event-processing', None, False),
    'broker': ('test', '14-projects/p4-kafka-event-processing', '*BrokerSuite', True),
    'ch29': ('test', '10-streaming/streaming-pipeline-lab', None, False),
    'ch30': ('test', '10-streaming/reliability-lab', None, False),
    'sol29': ('test', 'solutions/ch29-code', None, False),
    'sol30': ('test', 'solutions/ch30-code', None, False),
    'errors': ('run', '10-streaming/reliability-lab', 'vn.scalaeng.reliability.ErrorDemo', False),
    'demo': ('run', '10-streaming/streaming-pipeline-lab', 'vn.scalaeng.pipeline.Demo', False),
    'simple29': ('run', '10-streaming/streaming-pipeline-lab', 'vn.scalaeng.pipeline.SimpleDemo', False),
    'simple30': ('run', '10-streaming/reliability-lab', 'vn.scalaeng.reliability.SimpleDemo', False),
    'measure': ('run', '10-streaming/streaming-pipeline-lab', 'vn.scalaeng.pipeline.SnapshotMeasure', False),
}

def run(name):
    action, directory, only, integration = TASKS[name]
    env = ENV.copy()
    env.pop('RUN_KAFKA_IT', None)
    if integration:
        env['RUN_KAFKA_IT'] = '1'
    cmd = [shutil.which('scala-cli.bat'), action, directory, '--server=false',
           '--suppress-outdated-dependency-warning', '--java-opt', f'-Djava.io.tmpdir={TEMP}']
    if only:
        cmd += ['--test-only' if action == 'test' else '--main-class', only]
    result = subprocess.run(cmd, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            encoding='utf-8', errors='replace')
    output = re.sub(r'\x1b\[[0-9;]*m', '', result.stdout)
    (BASE / f'{name}.txt').write_text('COMMAND: ' + subprocess.list2cmdline(cmd) + '\n' + output +
                                    f'\nEXIT: {result.returncode}\n', encoding='utf-8')
    print(f'{name}: exit={result.returncode}\n{output[-6500:]}', flush=True)
    return result.returncode

if __name__ == '__main__':
    failures = [name for name in (sys.argv[1:] or TASKS) if run(name)]
    sys.exit(bool(failures))
