#!/usr/bin/env python3
"""Request bounded rsync telemetry for explicitly enrolled E source seeds."""
import json
from pathlib import Path

from scripts import run_workspace_source_staging as queue
from stockagent.data_sync import workspace_seed_observation as observer
from stockagent.data_sync import workspace_volume as volume


def main():
    volume.check()
    workspace = Path(volume.configuration()['mount_point'])
    result = {item['name']: observer.observe(item, workspace) for item in queue.load_sources()}
    active = {name: state for name, state in result.items() if state != 'not_seeding'}
    print(json.dumps({'contract': observer.CONTRACT, 'enrolled_sources': len(result),
                      'observed_sources': active}, ensure_ascii=False))


if __name__ == '__main__':
    main()
