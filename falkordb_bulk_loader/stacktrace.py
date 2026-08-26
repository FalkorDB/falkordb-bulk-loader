"""Utilities for dumping a stack trace on demand via SIGUSR1.

When a process is sent SIGUSR1 (e.g. ``kill -SIGUSR1 <pid>``), the tracebacks
of all currently running Python threads are written to ``sys.stderr``. This is
useful for diagnosing hangs or slow progress in long running bulk load/update
operations without having to attach a debugger.

SIGUSR1 is not available on all platforms (notably Windows). On unsupported
platforms this helper is a no-op so the loaders continue to work normally.
"""

import faulthandler
import signal
import sys


def register_stacktrace_dump_handler(stream=None):
    """Register a SIGUSR1 handler that dumps tracebacks of all threads.

    Returns ``True`` if the handler was registered, ``False`` if registration
    could not be completed. This includes platforms where ``SIGUSR1`` is not
    available, as well as environments where signal registration is not
    supported (for example, outside the main thread or other restricted
    runtimes).
    """
    if not hasattr(signal, "SIGUSR1"):
        return False

    if stream is None:
        stream = sys.stderr

    try:
        faulthandler.register(
            signal.SIGUSR1, file=stream, all_threads=True, chain=False
        )
    except (ValueError, OSError, RuntimeError):
        # ValueError: signal only works in main thread
        # OSError/RuntimeError: signal cannot be registered in this environment
        return False

    return True
