# python-drivelib
Drivelib makes GoogleDrive easily accessible from Python. It was written because [PyDrive](https://github.com/gsuitedevs/PyDrive) was lacking some important features and its development seems to have completely stopped.

What drivelib already has which PyDrive hasn't:
* *Real* resumable file transfer. Uploads can be resumed within one week after starting them.
* *Real* chunked file transfer. This makes implementing progress feedback and bandwidth control possible
* Much easier interface
* Support for Google APIv3

Drivelib aims to provide a [PathLike](https://docs.python.org/3/library/pathlib.html) interface for Google Drive. This is not yet implemented, though.

**Warning:** This library is still under development and cannot yet be considered stable. The API may change any time.
