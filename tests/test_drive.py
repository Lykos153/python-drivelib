import pytest
import string
import random
import os
from pathlib import Path
from hashlib import sha512 as hash

from drive import GoogleDrive
from drive import DriveFile
from drive import DriveFolder
from drive import ResumableMediaUploadProgress
from drive import CheckSumError


token_file = "tests/token.json"
token_file_appdata = "tests/token_appdata.json"
remote_tmpdir_prefix = "testremote"

@pytest.fixture(scope="module")
def gdrive() -> GoogleDrive:
    with open(token_file) as fh:
        credentials = fh.read()
    return GoogleDrive(credentials)

@pytest.fixture(scope="module")
def gdrive_appdata() -> GoogleDrive:
    with open(token_file_appdata) as fh:
        credentials = fh.read()
    return GoogleDrive(credentials)

@pytest.fixture(scope="module")
def remote_tmpdir(gdrive) -> DriveFolder:
    tmpdir = gdrive.create_path(remote_tmpdir_prefix)
    yield tmpdir
    tmpdir.remove()

@pytest.fixture(scope="function")
def remote_tmp_subdir(remote_tmpdir) -> DriveFolder:
    subdir = remote_tmpdir.mkdir(random_string())
    yield subdir
    subdir.remove()

@pytest.fixture(scope="function")
def tmpfile(tmp_path) -> string:
    def _make_tmpfile(filename=None, size_bytes=None):
        if filename == None:
            filename = random_string()
        file_path = tmp_path / filename
        if size_bytes != None:
            with file_path.open('wb') as fout:
                fout.write(os.urandom(size_bytes))
        return file_path
    return _make_tmpfile

def random_string(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def hash_file(file_path: Path):
    h = hash()
    with file_path.open('br') as fh:
        h.update(fh.read())
    return h.digest()

class AbortTransfer(Exception):
    pass

class ProgressExtractor():
    def __init__(self, abort_at=.0):
        if abort_at < 0 or abort_at > 1:
            raise ValueError("abort_at needs a value between 0 and 1")
        self.abort_at = abort_at
        self.status = None
        self.chunks = 0
        self.chunks_since_last_abort = 0
        self.bytes_at_last_abort = 0
    
    def update_status(self, status: ResumableMediaUploadProgress):
        self.status = status
        self.chunks += 1
        self.chunks_since_last_abort += 1
        if self.abort_at < 1 and self.status.progress() >= self.abort_at:
            self.chunks_since_last_abort = 0
            self.bytes_at_last_abort = status.resumable_progress
            raise AbortTransfer

    def bytes_since_last_abort(self) -> int:
        return self.status.resumable_progress - self.bytes_at_last_abort


class TestGoogleDrive:
    def test_appdata_folder(self, gdrive_appdata: GoogleDrive):
        assert hasattr(gdrive_appdata, "appdata")

    def test_json_creds(self, gdrive: GoogleDrive):
        GoogleDrive(gdrive.json_creds())

class TestDriveFolder:
    def test_mkdir(self, gdrive: GoogleDrive):
        folder = gdrive.mkdir(random_string())
        assert isinstance(folder, (DriveFolder))
        assert folder.isfolder()

    def test_mkdir_exists_folder(self, remote_tmpdir: DriveFolder):
        foldername = random_string()
        folder1 = remote_tmpdir.mkdir(foldername)
        folder2 = remote_tmpdir.mkdir(foldername)
        assert folder1 == folder2

    def test_mkdir_exists_file(self, remote_tmpdir: DriveFolder):
        foldername = random_string()
        remote_tmpdir.new_file(foldername).upload_empty()
        with pytest.raises(FileExistsError):
            remote_tmpdir.mkdir(foldername)

    def test_child_all(self, remote_tmpdir: DriveFolder):
        foldername = random_string()
        folder = remote_tmpdir.mkdir(foldername)
        assert folder == remote_tmpdir.child(foldername)

        filename = random_string()
        file_ = remote_tmpdir.new_file(filename)
        file_.upload_empty()
        assert file_ == remote_tmpdir.child(filename)

    def test_child_onlyfolders(self, remote_tmpdir: DriveFolder):
        foldername = random_string()
        folder = remote_tmpdir.mkdir(foldername)
        assert folder == remote_tmpdir.child(foldername, files=False)

        filename = random_string()
        remote_tmpdir.new_file(filename).upload_empty()
        with pytest.raises(FileNotFoundError):
            remote_tmpdir.child(filename, files=False)

    def test_child_onlyfiles(self, remote_tmpdir: DriveFolder):
        foldername = random_string()
        remote_tmpdir.mkdir(foldername)
        with pytest.raises(FileNotFoundError):
            remote_tmpdir.child(foldername, folders=False)

        filename = random_string()
        file_ = remote_tmpdir.new_file(filename)
        file_.upload_empty()
        assert file_ == remote_tmpdir.child(filename, folders=False)

    @pytest.mark.skip
    def test_child_trashed(self, remote_tmpdir: DriveFolder):
        raise NotImplementedError

    @pytest.mark.skip
    def test_children(self, remote_tmp_subdir: DriveFolder):
        raise NotImplementedError

    @pytest.mark.skip
    def test_children_ordered(self, remote_tmp_subdir: DriveFolder):
        raise NotImplementedError

    @pytest.mark.xfail
    def test_new_file(self, tmpfile: callable, remote_tmpdir: DriveFolder):
        new_file = remote_tmpdir.new_file(random_string())
        assert isinstance(new_file, (DriveFile))
        assert new_file.isfolder() == False
        assert new_file.id == None
        with pytest.raises(FileNotFoundError):
            new_file.download(tmpfile)

    def test_child_from_path(self, remote_tmpdir: DriveFolder):
        depth = 3
        folder = [remote_tmpdir]
        foldername = [""]
        for i in range(1,depth+1):
            foldername.append(random_string())
            folder.append(folder[i-1].mkdir(foldername[i]))
        path = "/".join(foldername)
        assert remote_tmpdir.child_from_path(path) == folder[depth]

    def test_create_path(self, remote_tmpdir: DriveFolder):
        # depends on test_child_from_path
        depth = 3
        path = "/".join((random_string() for _ in range(0,depth)))
        folder1 = remote_tmpdir.create_path(path)
        assert folder1 == remote_tmpdir.child_from_path(path)
        folder2 = remote_tmpdir.create_path(path)
        assert folder1 == folder2

class TestDriveFile:
    @pytest.mark.skip
    def test_download(self, tmpfile: callable, remote_tmpdir: DriveFolder):
        # depends on test_upload
        local_file1 = tmpfile(size_bytes = 1024)
        remote_file = remote_tmpdir.new_file(local_file1.parent)
        remote_file.upload(str(local_file1))
        local_file2 = tmpfile()
        remote_file.download(str(local_file2))
        assert hash_file(local_file1) == hash_file(local_file2)

    def test_download_empty_file(self, tmpfile: callable, remote_tmpdir: DriveFolder):
        # depends on test_upload_empty_file
        local_file = tmpfile()
        file_ = remote_tmpdir.new_file("test")
        file_.upload_empty()
        file_.download(local_file)
        assert local_file.stat().st_size == 0

    @pytest.mark.skip
    def test_download_continue(self, tmpfile: callable, remote_tmpdir: DriveFolder):
        raise NotImplementedError

    @pytest.mark.skip
    def test_download_local_file_does_not_match(self, tmpfile: callable, remote_tmpdir: DriveFolder):
        raise NotImplementedError

    @pytest.mark.skip
    def test_download_progress(self, tmpfile: callable, remote_tmpdir: DriveFolder):
        raise NotImplementedError

    @pytest.mark.skip
    def test_download_chunksize_too_small(self, tmpfile: callable, remote_tmpdir: DriveFolder):
        raise NotImplementedError

    @pytest.mark.skip
    def test_download_chunksize_bigger_than_filesize(self, tmpfile: callable, remote_tmpdir: DriveFolder):
        raise NotImplementedError
 
    def test_upload(self, tmpfile: callable, remote_tmpdir: DriveFolder):
        local_file = tmpfile(size_bytes = 1024)
        remote_file = remote_tmpdir.new_file(str(local_file.parent))
        remote_file.upload(str(local_file))

    def test_upload_empty_file(self, tmpfile: callable, remote_tmpdir: DriveFolder):
        local_file = tmpfile(size_bytes = 0)
        remote_file = remote_tmpdir.new_file(str(local_file.parent))
        remote_file.upload(str(local_file))

    def test_upload_nonexistent(self, tmpfile: callable, remote_tmpdir):
        local_file = tmpfile(size_bytes = None)
        remote_file = remote_tmpdir.new_file(str(local_file.parent))
        with pytest.raises(FileNotFoundError):
            remote_file.upload(str(local_file))

    def test_upload_progress_resume(self, tmpfile: callable, remote_tmpdir: DriveFolder):
        chunksize = 1024**2
        local_file = tmpfile(size_bytes=chunksize*2)
        remote_file = remote_tmpdir.new_file(local_file.name)
        progress = ProgressExtractor(abort_at=0.0)
        with pytest.raises(AbortTransfer):
            remote_file.upload(str(local_file), chunksize=chunksize, progress_handler=progress.update_status)
        assert progress.status.resumable_progress == chunksize
        progress.abort_at = 1
        remote_file.upload(str(local_file), chunksize=chunksize, progress_handler=progress.update_status)
        assert progress.chunks_since_last_abort == 1


    def test_upload_remote_file_does_not_match(self, tmpfile: callable, remote_tmpdir: DriveFolder):
        chunksize = 1024**2
        local_file1 = tmpfile(size_bytes=chunksize*5)
        remote_file = remote_tmpdir.new_file(local_file1.name)
        progress = ProgressExtractor(abort_at=0.0)
        with pytest.raises(AbortTransfer):
            remote_file.upload(str(local_file1), chunksize=chunksize, progress_handler=progress.update_status)
        assert progress.status.resumable_progress == chunksize

        # test checksum during upload
        local_file3 = tmpfile(size_bytes=chunksize*5)
        progress.abort_at = 1
        with pytest.raises(CheckSumError):
            remote_file.upload(str(local_file3), chunksize=chunksize, progress_handler=progress.update_status)

        # test checksum for finished file
        local_file2 = tmpfile(size_bytes=chunksize*3)
        progress.abort_at = 0.5
        with pytest.raises(CheckSumError):
            remote_file.upload(str(local_file2), chunksize=chunksize, progress_handler=progress.update_status)


    @pytest.mark.skip
    def test_upload_chunksize_too_small(self, tmpfile: callable, remote_tmpdir: DriveFolder):
        raise NotImplementedError

    @pytest.mark.skip
    def test_upload_chunksize_bigger_than_filesize(self, tmpfile: callable, remote_tmpdir: DriveFolder):
        raise NotImplementedError

 