import pytest
import string
import random
import os

from drive import GoogleDrive
from drive import DriveFile
from drive import DriveFolder
from drive import ResumableMediaUploadProgress


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
    return str(tmp_path / random_string())

def random_string(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def create_random_file(filename, size_bytes):
    with open(filename, 'wb') as fout:
        fout.write(os.urandom(size_bytes))

class AbortTransfer(Exception):
    pass

class ProgressExtractor():
    def __init__(self, abort_at=.0):
        self.status = None
        self.abort_at = abort_at
        self.chunks = 0
        self.chunks_since_last_abort = 0
    
    def update_status(self, status: ResumableMediaUploadProgress):
        self.status = status
        self.chunks += 1
        self.chunks_since_last_abort += 1
        if self.status.progress() >= self.abort_at:
            self.chunks_since_last_abort = 0
            raise AbortTransfer


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
    def test_new_file(self, tmpfile: string, remote_tmpdir: DriveFolder):
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

    @pytest.mark.skip
    def test_create_path(self, remote_tmpdir: DriveFolder):
        raise NotImplementedError

    @pytest.mark.skip
    def test_create_path_exists(self, remote_tmpdir: DriveFolder):
        raise NotImplementedError

class TestDriveFile:
    @pytest.mark.skip
    def test_download(self, tmpfile: string, remote_tmpdir: DriveFolder):
        raise NotImplementedError

    @pytest.mark.skip
    def test_download_empty_file(self, tmpfile: string, remote_tmpdir: DriveFolder):
        raise NotImplementedError

    @pytest.mark.skip
    def test_download_continue(self, tmpfile: string, remote_tmpdir: DriveFolder):
        raise NotImplementedError

    @pytest.mark.skip
    def test_download_local_file_does_not_match(self, tmpfile: string, remote_tmpdir: DriveFolder):
        raise NotImplementedError

    @pytest.mark.skip
    def test_download_progress(self, tmpfile: string, remote_tmpdir: DriveFolder):
        raise NotImplementedError

    @pytest.mark.skip
    def test_download_chunksize_too_small(self, tmpfile: string, remote_tmpdir: DriveFolder):
        raise NotImplementedError

    @pytest.mark.skip
    def test_download_chunksize_bigger_than_filesize(self, tmpfile: string, remote_tmpdir: DriveFolder):
        raise NotImplementedError
 
    def test_upload(self, tmpfile: string, remote_tmpdir: DriveFolder):
        create_random_file(tmpfile, 1024)
        new_file = remote_tmpdir.new_file(os.path.basename(tmpfile))
        new_file.upload(tmpfile)

    def test_upload_empty_file(self, tmpfile: string, remote_tmpdir: DriveFolder):
        create_random_file(tmpfile, 0)
        new_file = remote_tmpdir.new_file(os.path.basename(tmpfile))
        new_file.upload(tmpfile)

    def test_upload_progress_resume(self, tmpfile: string, remote_tmpdir: DriveFolder):
        chunksize = 1024**2
        create_random_file(tmpfile, int(chunksize*2))
        new_file = remote_tmpdir.new_file(os.path.basename(tmpfile))
        progress = ProgressExtractor(abort_at=0.0)
        with pytest.raises(AbortTransfer):
            new_file.upload(tmpfile, chunksize=chunksize, progress_handler=progress.update_status)
        assert progress.status.resumable_progress == chunksize
        # TODO: resume

    @pytest.mark.skip
    def test_upload_remote_file_does_not_match(self, tmpfile: string, remote_tmpdir: DriveFolder):
        raise NotImplementedError

    @pytest.mark.skip
    def test_upload_chunksize_too_small(self, tmpfile: string, remote_tmpdir: DriveFolder):
        raise NotImplementedError

    @pytest.mark.skip
    def test_upload_chunksize_bigger_than_filesize(self, tmpfile: string, remote_tmpdir: DriveFolder):
        raise NotImplementedError

 