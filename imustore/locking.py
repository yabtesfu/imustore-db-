from __future__ import annotations

import io
import os
import time


def _descriptor(fileobj):
    """Return the OS file descriptor, or None for an in-memory (simulated) file."""
    try:
        return fileobj.fileno()
    except io.UnsupportedOperation:
        return None


class FileLock:
    def __init__(self, fileobj):
        self._fileobj = fileobj
        self._sidecar_fd: int | None = None
        self._sidecar_path = f"{fileobj.name}.lock" if os.name == "nt" else None
        self.locked = False

    def lock(self) -> bool:
        if self.locked:
            return False

        if os.name == "nt":
            assert self._sidecar_path is not None
            while True:
                try:
                    self._sidecar_fd = os.open(self._sidecar_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                    os.write(self._sidecar_fd, str(os.getpid()).encode("ascii"))
                    break
                except FileExistsError:
                    time.sleep(0.01)
        else:
            import fcntl

            fd = _descriptor(self._fileobj)
            if fd is not None:  # in-memory files need no cross-process lock
                fcntl.flock(fd, fcntl.LOCK_EX)

        self.locked = True
        return True

    def unlock(self) -> None:
        if not self.locked:
            return

        if os.name == "nt":
            assert self._sidecar_path is not None
            if self._sidecar_fd is not None:
                os.close(self._sidecar_fd)
                self._sidecar_fd = None
            try:
                os.remove(self._sidecar_path)
            except FileNotFoundError:
                pass
        else:
            import fcntl

            fd = _descriptor(self._fileobj)
            if fd is not None:
                fcntl.flock(fd, fcntl.LOCK_UN)

        self.locked = False
