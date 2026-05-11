import time
from typing import Any


def _close_ssh_streams(*streams: Any) -> None:
    for stream in streams:
        if stream is None:
            continue
        channel = getattr(stream, "channel", None)
        try:
            stream.close()
        except Exception:
            pass
        if channel is None:
            continue
        try:
            channel.close()
        except Exception:
            pass


def _wait_for_ssh_exit_status(
    stdout: Any,
    stderr: Any | None = None,
    *,
    timeout_sec: int,
) -> int:
    actual_timeout = max(1, int(timeout_sec or 1))
    channel = getattr(stdout, "channel", None)
    if channel is None or not hasattr(channel, "exit_status_ready"):
        return int(stdout.channel.recv_exit_status())

    set_timeout = getattr(channel, "settimeout", None)
    if callable(set_timeout):
        try:
            set_timeout(actual_timeout)
        except Exception:
            pass

    deadline = time.monotonic() + actual_timeout
    # Paramiko command timeout does not always bound recv_exit_status(),
    # so poll the channel and close it ourselves when the command stalls.
    while not channel.exit_status_ready():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            _close_ssh_streams(stdout, stderr)
            raise TimeoutError("timeout")
        time.sleep(min(0.1, remaining))

    return int(channel.recv_exit_status())
