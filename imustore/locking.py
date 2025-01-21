from __future__ import annotations

import os
import time


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

            fcntl.flock(self._fileobj.fileno(), fcntl.LOCK_EX)

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

            fcntl.flock(self._fileobj.fileno(), fcntl.LOCK_UN)

        self.locked = False
