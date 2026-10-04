"""Cancel only the explicitly owned foreground trial session."""
import subprocess
import sys

from scripts.benchmark_tw_public_remote_derivation import stop_owned


def test_owned_trial_stops_without_terminating_an_independent_job():
    unrelated = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'], start_new_session=True)
    owned = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'], start_new_session=True)
    try:
        stop_owned(owned)
        assert owned.poll() is not None
        assert unrelated.poll() is None
    finally:
        stop_owned(owned)
        stop_owned(unrelated)
