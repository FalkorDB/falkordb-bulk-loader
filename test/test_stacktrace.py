import os
import signal
import subprocess
import sys
import textwrap
import time

import pytest


pytestmark = pytest.mark.skipif(
    not hasattr(signal, "SIGUSR1"), reason="SIGUSR1 not available on this platform"
)


def test_register_returns_true_on_unix():
    from falkordb_bulk_loader.stacktrace import register_stacktrace_dump_handler

    previous_handler = signal.getsignal(signal.SIGUSR1)
    try:
        assert register_stacktrace_dump_handler() is True
    finally:
        signal.signal(signal.SIGUSR1, previous_handler)


def test_sigusr1_dumps_stacktrace(tmp_path):
    """Sending SIGUSR1 to a process using the helper writes a traceback to stderr."""
    script = tmp_path / "runner.py"
    script.write_text(
        textwrap.dedent(
            """
            import sys
            import time

            from falkordb_bulk_loader.stacktrace import register_stacktrace_dump_handler

            assert register_stacktrace_dump_handler() is True
            # Signal readiness to the parent before sleeping.
            sys.stdout.write("ready\\n")
            sys.stdout.flush()
            for _ in range(50):
                time.sleep(0.1)
            """
        )
    )

    env = os.environ.copy()
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env["PYTHONPATH"] = repo_root + os.pathsep + env.get("PYTHONPATH", "")

    proc = subprocess.Popen(
        [sys.executable, str(script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        text=True,
    )
    try:
        # Wait for the child to register the handler.
        ready_line = proc.stdout.readline()
        assert "ready" in ready_line

        proc.send_signal(signal.SIGUSR1)
        # Give faulthandler a moment to write the traceback.
        time.sleep(0.5)
    finally:
        proc.terminate()
        try:
            _, stderr = proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            _, stderr = proc.communicate()

    # faulthandler emits a header followed by frames; verify both are present.
    assert "Current thread" in stderr or "Thread" in stderr
    assert "runner.py" in stderr
