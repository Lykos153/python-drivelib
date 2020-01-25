import pytest
import string
import random
import os
from pathlib import Path
from hashlib import md5

from drive import Credentials
from drive import GoogleDrive
from drive import DriveFile
from drive import DriveFolder
from drive import ResumableMediaUploadProgress

from drive import CheckSumError
from drive import HttpError


token_file = "tests/token.json"
token_file_appdata = "tests/token_appdata.json"
remote_tmpdir_prefix = "testremote"
chunksize_min = 1024*256

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
def remote_tmpdir(gdrive: GoogleDrive) -> DriveFolder:
    tmpdir = gdrive.create_path(remote_tmpdir_prefix)
    yield tmpdir
    tmpdir.remove()

@pytest.fixture(scope="function")
def remote_tmp_subdir(remote_tmpdir: DriveFolder) -> DriveFolder:
    subdir = remote_tmpdir.mkdir(random_string())
    yield subdir
    subdir.remove()

@pytest.fixture(scope="function")
def remote_tmpfile(remote_tmpdir: DriveFolder, tmpfile) -> callable:
    def _make_remote_tmpfile(size_bytes=0, filename=None, subdir=None):
        if filename is None:
            filename = random_string()
        local_file = tmpfile(size_bytes=size_bytes)
        if not subdir:
            subdir = remote_tmpdir
        remote_file = subdir.new_file(filename)
        remote_file.upload(str(local_file))
        return remote_file
    return _make_remote_tmpfile

@pytest.fixture(scope="function")
def tmpfile(tmp_path) -> string:
    def _make_tmpfile(size_bytes=None, filename=None):
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

def md5_file(fname):
    hash_md5 = md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

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

    def test_auth_json_creds(self, gdrive: GoogleDrive):
        GoogleDrive(gdrive.json_creds())

    def test_auth_creds(self):
        creds = Credentials.from_authorized_user_file(token_file)
        GoogleDrive(creds)

class TestDriveItem:
    def test_rename_flat(self, remote_tmpdir: DriveFolder):
        remote_file = remote_tmpdir.new_file(random_string())
        remote_file.upload_empty()
        new_name = random_string()

        remote_file.rename(new_name)
        assert remote_file.name == new_name
        assert remote_tmpdir.child(new_name) == remote_file

    def test_rename_subdir(self, remote_tmpdir: DriveFolder):
        remote_file = remote_tmpdir.new_file(random_string())
        remote_file.upload_empty()
        remote_tmpdir.create_path("1/2/3")
        remote_file.rename("1/2/3/b")
        assert remote_tmpdir.child_from_path("1/2/3/b") == remote_file

    def test_rename_target_dir_does_not_exist(self, remote_tmpdir: DriveFolder):
        remote_file = remote_tmpdir.new_file(random_string())
        remote_file.upload_empty()
        with pytest.raises(FileNotFoundError):
            remote_file.rename("a/b")

    def test_rename_target_dir_is_a_file(self, remote_tmpdir: DriveFolder):
        remote_file = remote_tmpdir.new_file(random_string())
        remote_file.upload_empty()
        remote_tmpdir.new_file("file").upload_empty()
        with pytest.raises(NotADirectoryError):
            remote_file.rename("file/b")

    @pytest.mark.skip
    def test_rename_only_target_dir_no_basename(self, remote_tmpdir: DriveFolder):
        remote_file = remote_tmpdir.new_file("filename")
        remote_file.upload_empty()
        remote_tmpdir.create_path("1/2/3")
        remote_file.rename("1/2/3")
        assert remote_tmpdir.child_from_path("1/2/3/filename") == remote_file

    @pytest.mark.skip
    def test_rename_target_exists(self, remote_tmpdir: DriveFolder):
        remote_file = remote_tmpdir.new_file(random_string())
        remote_file.upload_empty()
        remote_file.parent.new_file("existing_file").upload_empty()
        remote_file.rename("existing_file")
        assert remote_tmpdir.child("existing_file") == remote_file

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

    def test_children(self, remote_tmp_subdir: DriveFolder, remote_tmpfile):
        file_count = 5
        created_files = set()
        for _ in range(0,file_count):
            created_files.add(remote_tmpfile(subdir=remote_tmp_subdir))

        listed_files = set(remote_tmp_subdir.children())

        assert created_files == listed_files

    def test_children_onlyfolders(self, remote_tmpdir: DriveFolder):
        remote_folder = remote_tmpdir.mkdir(random_string())
        remote_file = remote_tmpdir.new_file(random_string())
        remote_file.upload_empty()

        filelist = list(remote_tmpdir.children(files=False))
        assert remote_folder in filelist
        assert remote_file not in filelist

    def test_children_onlyfiles(self, remote_tmpdir: DriveFolder):
        remote_folder = remote_tmpdir.mkdir(random_string())
        remote_file = remote_tmpdir.new_file(random_string())
        remote_file.upload_empty()

        filelist = list(remote_tmpdir.children(folders=False))
        assert remote_folder not in filelist
        assert remote_file in filelist

    def test_children_trashed(self, remote_tmpdir: DriveFolder):
        remote_file = remote_tmpdir.new_file(random_string())
        remote_file.upload_empty()
        remote_file.trash()

        assert remote_file not in remote_tmpdir.children()
        assert remote_file in remote_tmpdir.children(trashed=True)

    def test_children_ordered(self, remote_tmp_subdir: DriveFolder, remote_tmpfile):
        file_count = 5
        created_files = []
        for _ in range(0,file_count):
            created_files.append(remote_tmpfile(subdir=remote_tmp_subdir))
        created_files.sort(key=lambda x: x.name)

        listed_files = list(remote_tmp_subdir.children(orderBy="name"))

        assert created_files == listed_files

    def test_new_file(self, tmpfile: Path, remote_tmpdir: DriveFolder):
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

        # test . and ..
        assert remote_tmpdir.child_from_path(".") == remote_tmpdir
        path = "/".join((".", foldername[1], foldername[2], ".."))
        assert remote_tmpdir.child_from_path(path) == folder[1]

    def test_create_path(self, remote_tmpdir: DriveFolder):
        # depends on test_child_from_path
        depth = 3
        path = "/".join((random_string() for _ in range(0,depth)))
        folder1 = remote_tmpdir.create_path(path)
        assert folder1 == remote_tmpdir.child_from_path(path)
        folder2 = remote_tmpdir.create_path(path)
        assert folder1 == folder2

        # test . and ..
        assert remote_tmpdir.create_path(".") == remote_tmpdir
        folder = remote_tmpdir.create_path("./1/2a/../2b/")
        assert folder == remote_tmpdir.child_from_path("1/2b")

    def test_isempty(self, remote_tmp_subdir: DriveFolder):
        assert remote_tmp_subdir.isempty() == True
        remote_tmp_subdir.new_file(random_string()).upload_empty()
        assert remote_tmp_subdir.isempty() == False

class TestDriveFile:
    def test_download(self, tmpfile: Path, remote_tmpfile):
        # depends on test_upload
        remote_file = remote_tmpfile(size_bytes=1024)

        # No chunksize specified
        local_file = tmpfile()
        remote_file.download(str(local_file))
        assert md5_file(local_file) == remote_file.md5sum

        # Chunksize = None
        local_file = tmpfile()
        remote_file.download(str(local_file), chunksize=None)
        assert md5_file(local_file) == remote_file.md5sum

    def test_download_empty_file(self, tmpfile: Path, remote_tmpfile):
        # depends on test_upload_empty_file
        remote_file = remote_tmpfile(size_bytes=0)
        local_file = tmpfile()
        remote_file.download(local_file)
        assert local_file.stat().st_size == 0

    def test_download_resume(self, tmpfile: Path, remote_tmpfile: DriveFile):
        chunksize = chunksize_min
        remote_file = remote_tmpfile(size_bytes=chunksize*2)
        local_file = tmpfile(filename=remote_file.name)
        progress = ProgressExtractor(abort_at=0.0)
        with pytest.raises(AbortTransfer):
            remote_file.download(str(local_file), chunksize=chunksize, progress_handler=progress.update_status)
        assert progress.status.resumable_progress == chunksize
        progress.abort_at = 1
        remote_file.download(str(local_file), chunksize=chunksize, progress_handler=progress.update_status)
        assert progress.chunks_since_last_abort == 1

    def test_download_local_file_does_not_match(self, tmpfile: Path, remote_tmpfile: DriveFile):
        chunksize = chunksize_min
        remote_file = remote_tmpfile(size_bytes=chunksize*2)
        local_file = tmpfile(filename=remote_file.name)
        progress = ProgressExtractor(abort_at=0.0)
        with pytest.raises(AbortTransfer):
            remote_file.download(str(local_file), chunksize=chunksize, progress_handler=progress.update_status)
        assert progress.status.resumable_progress == chunksize
        local_file2 = tmpfile(size_bytes=chunksize)
        with pytest.raises(CheckSumError):
            remote_file.download(str(local_file2))

    def test_download_chunksize_bigger_than_filesize(self, tmpfile: Path, remote_tmpfile: DriveFile):
        chunksize = chunksize_min
        remote_file = remote_tmpfile(size_bytes=int(chunksize/2))
        local_file = tmpfile()
        remote_file.download(str(local_file), chunksize=chunksize)
        assert md5_file(local_file) == remote_file.md5sum

    def test_upload(self, tmpfile: Path, remote_tmpdir: DriveFolder):
        local_file = tmpfile(size_bytes = 1024)
        
        # No chunksize specified
        remote_file = remote_tmpdir.new_file(str(local_file.parent))
        remote_file.upload(str(local_file))
        assert md5_file(local_file) == remote_file.md5sum

        # Chunksize = None
        remote_file = remote_tmpdir.new_file(str(local_file.parent))
        remote_file.upload(str(local_file), chunksize=None)
        assert md5_file(local_file) == remote_file.md5sum

    def test_upload_empty_file(self, tmpfile: Path, remote_tmpdir: DriveFolder):
        local_file = tmpfile(size_bytes = 0)
        remote_file = remote_tmpdir.new_file(str(local_file.parent))
        remote_file.upload(str(local_file))
        assert md5_file(local_file) == remote_file.md5sum

    def test_upload_nonexistent(self, tmpfile: Path, remote_tmpdir):
        local_file = tmpfile(size_bytes = None)
        remote_file = remote_tmpdir.new_file(str(local_file.parent))
        with pytest.raises(FileNotFoundError):
            remote_file.upload(str(local_file))

    def test_upload_progress_resume(self, tmpfile: Path, remote_tmpdir: DriveFolder):
        chunksize = chunksize_min
        local_file = tmpfile(size_bytes=chunksize*5)
        remote_file = remote_tmpdir.new_file(local_file.name)
        progress = ProgressExtractor(abort_at=0.4)
        with pytest.raises(AbortTransfer):
            remote_file.upload(str(local_file), chunksize=chunksize, progress_handler=progress.update_status)
        assert progress.status.resumable_progress == 2*chunksize
        progress.abort_at = 1
        remote_file.upload(str(local_file), chunksize=chunksize, progress_handler=progress.update_status)
        assert progress.chunks_since_last_abort == 3

    def test_upload_remote_file_does_not_match(self, tmpfile: Path, remote_tmpdir: DriveFolder):
        chunksize = chunksize_min
        local_file_orig = tmpfile(size_bytes=chunksize*3)
        local_file_same_size = tmpfile(size_bytes=chunksize*3)
        local_file_different_size = tmpfile(size_bytes=chunksize*5)

        ## Test size mismatch
        remote_file = remote_tmpdir.new_file(local_file_orig.name)
        progress = ProgressExtractor(abort_at=0.0)
        with pytest.raises(AbortTransfer):
            remote_file.upload(str(local_file_orig), chunksize=chunksize, progress_handler=progress.update_status)
        assert progress.status.resumable_progress == chunksize

        with pytest.raises(HttpError):
            remote_file.upload(str(local_file_different_size), chunksize=chunksize)

        ## Test checksum mismatch on intermediate chunk
        remote_file = remote_tmpdir.new_file(local_file_orig.name)
        progress = ProgressExtractor(abort_at=0.0)
        with pytest.raises(AbortTransfer):
            remote_file.upload(str(local_file_orig), chunksize=chunksize, progress_handler=progress.update_status)
        assert progress.status.resumable_progress == chunksize

        with pytest.raises(CheckSumError):
            remote_file.upload(str(local_file_same_size), chunksize=chunksize)
        assert remote_file.resumable_uri == None

        ## Test checksum mismatch on last chunk
        remote_file = remote_tmpdir.new_file(local_file_orig.name)
        progress = ProgressExtractor(abort_at=0.6)
        with pytest.raises(AbortTransfer):
            remote_file.upload(str(local_file_orig), chunksize=chunksize, progress_handler=progress.update_status)
        assert progress.status.resumable_progress == 2*chunksize

        with pytest.raises(CheckSumError):
            remote_file.upload(str(local_file_same_size), chunksize=chunksize)
        assert remote_file.resumable_uri == None
        
    def test_upload_chunksize_too_small(self, tmpfile: Path, remote_tmpdir: DriveFolder):
        chunksize = 1
        local_file = tmpfile(size_bytes=chunksize*2)
        remote_file = remote_tmpdir.new_file(local_file.name)
        with pytest.raises(HttpError):
            remote_file.upload(str(local_file), chunksize=chunksize)

    def test_upload_chunksize_bigger_than_filesize(self, tmpfile: Path, remote_tmpdir: DriveFolder):
        chunksize = 100
        local_file = tmpfile(size_bytes=int(chunksize/2))
        remote_file = remote_tmpdir.new_file(local_file.name)
        remote_file.upload(str(local_file), chunksize=chunksize)
        assert md5_file(local_file) == remote_file.md5sum

    def test_upload_invalid_resumable_uri(self, tmpfile: Path, remote_tmpdir: DriveFolder):
        chunksize = chunksize_min
        local_file = tmpfile(size_bytes=chunksize*2)
        remote_file = remote_tmpdir.new_file(local_file.name)
        progress = ProgressExtractor(abort_at=0.0)
        with pytest.raises(AbortTransfer):
            remote_file.upload(str(local_file), chunksize=chunksize, progress_handler=progress.update_status)
        assert progress.status.resumable_progress == chunksize
        remote_file.resumable_uri = "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&upload_id=invalid_id"
        with pytest.raises(HttpError):
            remote_file.upload(str(local_file))

    def test_upload_existing_file(self, tmpfile: Path, remote_tmpfile: DriveFile):
        local_file = tmpfile(size_bytes=1024)
        remote_file = remote_tmpfile(size_bytes=1024)
        id_pre_upload = remote_file.id
        with pytest.raises(FileExistsError):
            remote_file.upload(str(local_file))
        assert remote_file.id == id_pre_upload

class TestMetadata:
    def test_get_metadata(self, remote_tmpfile: DriveFile):
        remote_file = remote_tmpfile(size_bytes=700)
        metadata = remote_file.meta_get("size, starred, trashed")
        assert metadata == {'size': '700', 'starred': False, 'trashed': False}

    def test_set_metadata(self, remote_tmpfile: DriveFile):
        metadata = {'description': "an interesting file",
                                'starred': True}
        remote_file = remote_tmpfile()
        remote_file.meta_set(metadata)
        assert remote_file.meta_get("description, starred") == metadata

    def test_get_nonexistent_metadata(self, remote_tmpfile: DriveFile):
        remote_file = remote_tmpfile(size_bytes=700)
        with pytest.raises(HttpError):
            metadata = remote_file.meta_get("doesnotexist")

    def test_set_nonexistent_metadata(self, remote_tmpfile: DriveFile):
        remote_file = remote_tmpfile(size_bytes=700)
        with pytest.raises(HttpError):
            metadata = remote_file.meta_set({"doesnotexist": True})

    def test_set_immutable_metadata(self, remote_tmpfile: DriveFile):
        remote_file = remote_tmpfile(size_bytes=700)
        with pytest.raises(HttpError):
            metadata = remote_file.meta_set({"version": 7})

    def test_custom_properties(self, remote_tmpfile: DriveFile):
        remote_file = remote_tmpfile()

        metadata = {'properties': {'prop1': random_string()}}
        remote_file.meta_set(metadata)
        assert remote_file.meta_get("properties") == metadata

        # add property
        metadata2 = {'properties': {'prop2': random_string()}}
        remote_file.meta_set(metadata2)
        metadata['properties'].update(metadata2['properties'])
        assert remote_file.meta_get("properties") == metadata

        # update prop1, delete prop2
        metadata['properties'] = {'prop1': random_string(),
                                  'prop2': None}
        remote_file.meta_set(metadata)
        del metadata['properties']['prop2']
        assert remote_file.meta_get("properties") == metadata

    def test_size(self, remote_tmpfile: DriveFile):
        remote_file = remote_tmpfile(size_bytes=700)
        assert remote_file.size == 700
