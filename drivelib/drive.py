from __future__ import annotations #only > 3.7, better to find a different solution

import os
from abc import ABC, abstractmethod
import json
import itertools
from functools import wraps

import hashlib
from urllib.parse import urlparse
from urllib.parse import parse_qs

from pathlib import PurePosixPath

try:
    from collections.abc import Sequence
except ImportError:
    from collections import Sequence

import google_auth_httplib2
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import google.oauth2.credentials
import oauth2client.client
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaUploadProgress
from googleapiclient.http import MediaDownloadProgress

from expiringdict import ExpiringDict
from datetime import datetime, timedelta

from drivelib.errors import GoogleDriveAPIError
from drivelib.errors import BackendError

import logging
logger = logging.getLogger('drivelib')
#logger.addHandler(logging.StreamHandler())
#logger.setLevel(logging.INFO)

# httplib2.debuglevel = 4

# Ideas: Make this lib more Path-like
# Multple parents could be modeled as inode number
# Only problem: Different files can have the same names
# Possible solution 1: all operations on files return generators or lists
# Possible solution 2: using __new__ method, lib will either return
#   DriveItem (if there is only one instance) or 
#   DriveItemList (if there are multiples)
# Possible solution 3: Don't allow this and raise Exception in these cases
#       
# Another problem: Drive allows / in Filenames and . and .. as filenames.


minimalChunksize = 1024*256
defaultChunksize = minimalChunksize*4

#TODO: Proper Exceptions

class NotAuthenticatedError(Exception):
    pass

class CheckSumError(Exception):
    pass

class AmbiguousPathError(Exception):
    def __init__(self, *args, **kwargs):
        if "duplicates" in kwargs:
            self.duplicates = kwargs["duplicates"]
        super().__init__(*args)

class InvalidUrlError(Exception):
    pass

class Credentials(google.oauth2.credentials.Credentials,
                oauth2client.client.Credentials):
    #TODO get rid of oauth2client dependency
    @classmethod
    def from_json(cls, json_string):
        a = json.loads(json_string)
        credentials = cls.from_authorized_user_info(a, a['scopes'])
        credentials.token = a['access_token']
        return credentials

    @classmethod
    def from_authorized_user_file(cls, filename):
        with open(filename, 'r') as fh:
            a = json.load(fh)
        return cls.from_authorized_user_info(a, a['scopes'])

    def to_json(self):
        to_serialize = dict()
        to_serialize['access_token'] = self.token
        to_serialize['refresh_token'] = self.refresh_token
        to_serialize['id_token'] = self.id_token
        to_serialize['token_uri'] = self.token_uri
        to_serialize['client_id'] = self.client_id
        to_serialize['client_secret'] = self.client_secret
        to_serialize['scopes'] = self.scopes
        return json.dumps(to_serialize)

class ResumableMediaUploadProgress(MediaUploadProgress):
    def __init__(self, resumable_progress, total_size, resumable_uri):
        super().__init__(resumable_progress, total_size)
        self.resumable_uri = resumable_uri

    def __str__(self):
        return "{}/{} ({:.0%}%) {}".format(
                                self.resumable_progress,
                                self.total_size,
                                self.progress(),
                                self.resumable_uri
                            )

def needs_id(f):
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        if not self.id:
            raise FileNotFoundError
        return f(self, *args, **kwargs)

    return wrapper

def autorefresh(f):
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        if self.last_refreshed + self.refresh_after <= datetime.now():
            self.refresh()
        return f(self, *args, **kwargs)
    return wrapper

class _DriveParents(Sequence):
    """This object provides sequence-like access to the logical ancestors
    of a path."""
    def __init__(self, startpoint: DriveItem):
        self._startpoint = startpoint
        self._parents = dict()

    def __getitem__(self, idx):
        if idx in self._parents and self._parents[idx] is not None:
            return self._parents[idx]
        if idx == 0:
            self._parents[idx] = self._startpoint.parent
        else:
            try:
                self._parents[idx] = self[idx - 1].parent
            except AttributeError:
                raise IndexError

        if self._parents[idx] is None:
            raise IndexError

        return self._parents[idx]

    def __len__(self):
        l = 0
        while True:
            try:
                self[l]
            except IndexError:
                return l
            else:
                l += 1

class DriveItem(ABC):
    #TODO: metadata as dict
    # Filename not as attribute but as key
    # OR: filename as property method

    def __init__(self, drive, parent_ids, name, id_, spaces, refresh_after=timedelta(minutes=30)):
        self.drive = drive
        self.name = name # TODO: property. Setter => rename
        self.id = id_ # TODO: property
        self.parent_ids = parent_ids
        self.spaces = spaces
        self.last_refreshed = datetime.now()
        self.refresh_after = refresh_after

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        # not yet sure if this is a good idea
        return hash(self.id)

    @property
    @autorefresh
    def parent(self):
        if not hasattr(self, '_parent'):
            if self.parent_ids:
                self._parent = self.drive.item_by_id(self.parent_ids[0])
            else:
                self._parent = None
        return self._parent

    @property
    def parents(self):
        "An immutable sequence providing access to the logical ancestors of the path"
        return _DriveParents(self)

    def resolve(self) -> str:
        return '/'.join((x.name if x != self.drive else '' for x in reversed(self.parents)))+'/'+self.name

    def rename(self, target, ignore_existing=False):
        splitpath = target.rsplit('/', 1)
        if len(splitpath) == 1:
            new_name = splitpath[0]
            parent = self.parent
        else:
            new_name = splitpath[1]
            parent = self.parent.child_from_path(splitpath[0])
            if not parent.isfolder():
                raise NotADirectoryError()

        self.move(parent, new_name, ignore_existing=ignore_existing)

    @needs_id
    def move(self, new_dest, new_name=None, ignore_existing=False):
        new_name = new_name or self.name
        if new_dest == self.parent and new_name == self.name:
            return
        if not ignore_existing:
            try:
                child = new_dest.child(new_name)
                raise FileExistsError("Filename already exists ({name}).".format(name=child.name))
            except FileNotFoundError:
                pass
            except AmbiguousPathError:
                raise FileExistsError("Filename already exists multiple times: {name}".format(name=new_name))

        #TODO: Don't use exception for flow control here. Maybe implement exists()
        try:
            result = self.drive.service.files().update(
                                    fileId=self.id,
                                    body={"name": new_name},
                                    addParents=new_dest.id,
                                    removeParents=self.parent.id,
                                    fields='name, parents',
                                    ).execute()
        except HttpError as err:
            raise GoogleDriveAPIError.from_http_error(err)
        self.name = result['name']
        self.parent_ids = result.get('parents', [])
        
    @needs_id
    def remove(self):
        try:
            self.drive.service.files().delete(fileId=self.id).execute()
        except HttpError as err:
            raise GoogleDriveAPIError.from_http_error(err)
        self.id = None

    def trash(self):
        try:
            self.meta_set({'trashed': True})
        except:
            raise HttpError("Could not trash file")

    @needs_id
    def meta_set(self, metadata: dict):
        try:
            result = self.drive.service.files().update(
                                    fileId=self.id,
                                    body=metadata,
                                    fields=','.join(metadata.keys()),
                                    ).execute()
        except HttpError as err:
            raise GoogleDriveAPIError.from_http_error(err)
        #TODO update local array

    @needs_id
    def meta_get(self, fields: str) -> dict:
        #TODO cache metadata
        try:
            return self.drive.service.files().get(fileId=self.id, fields=fields).execute()
        except HttpError as err:
            raise GoogleDriveAPIError.from_http_error(err)

    @needs_id
    def refresh(self):
        try:
            result = self.drive.service.files().get(
                                    fileId=self.id,
                                    fields=self.drive.default_fields
                                ).execute()
        except HttpError as err:
            raise GoogleDriveAPIError.from_http_error(err)
        self.name = result['name']
        self.parent_ids = result['parents']
        self.last_refreshed = datetime.now()

    @needs_id
    def create_shortcut(self, name, parent=None) -> DriveShortcut:
        if parent:
            parent_id = parent.id
        else:
            try:
                parent_id = self.parent.id
            except AttributeError:
                parent_id = 'root'
                
        shortcut_metadata = {
            'name': name,
            'parents': [parent_id],
            'mimeType': 'application/vnd.google-apps.shortcut',
            'shortcutDetails': {
                'targetId': self.id
            }
        }
        try:
            result = self.drive.service.files().create(
                                            body=shortcut_metadata,
                                            fields=self.drive.default_fields).execute()
        except HttpError as err:
            raise GoogleDriveAPIError.from_http_error(err)
        return self._reply_to_object(result)

    def _reply_to_object(self, reply) -> DriveItem:
        if reply['mimeType'] == 'application/vnd.google-apps.folder':
            new_item = DriveFolder(self.drive, reply.get('parents', []), reply['name'], reply['id'], spaces=",".join(reply['spaces']))
        elif reply['mimeType'] == 'application/vnd.google-apps.shortcut':
            new_item = DriveShortcut(self.drive, reply.get('parents', []), reply['name'], reply['id'], reply['shortcutDetails']['targetId'], spaces=",".join(reply['spaces']))
        else:
            new_item = DriveFile(self.drive, reply.get('parents', []), reply['name'], reply['id'], spaces=",".join(reply['spaces']))
        self.drive._id_cache[new_item.id] = new_item
        return new_item


    @abstractmethod
    def isfolder(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def isshortcut(self) -> bool:
        raise NotImplementedError

class DriveFolder(DriveItem):

    def isfolder(self) -> bool:
        return True  

    def isshortcut(self) -> bool:
        return False
    
    def child(self, name) -> DriveItem:
        child = self._get_from_name_cache(name)
        if child:
            logger.debug("Found {} in name cache".format(name))
            return child
        gen = self.children(name=name, pageSize=2)
        child = next(gen, None)
        if child is None:
            raise FileNotFoundError(name)
        next_child = next(gen, None)
        if next_child is not None:
            duplicates = itertools.chain((child, next_child), gen)
            raise AmbiguousPathError("Two or more files {name}".format(name=name), duplicates=duplicates)
        self._add_to_name_cache(child)
        return child

    @needs_id
    def _add_to_name_cache(self, item: DriveItem):
        self.drive._name_cache['/'.join((self.id, item.name))] = item

    @needs_id
    def _delete_from_name_cache(self, item: DriveItem):
        del self.drive._name_cache['/'.join((self.id, item.name))]

    def _get_from_name_cache(self, name: str):
        return self.drive._name_cache.get('/'.join(((self.id, name))), None)

    @needs_id
    def children(self, name=None, folders=True, files=True, trashed=False, pageSize=100, orderBy=None, skip=0) -> Iterator(DriveItem):
        query = "'{this}' in parents".format(this=self.id)

        if name:
            query += " and name='{}'".format(name)

        if not folders and not files:
            return iter(())
        if folders and not files:
            query += " and mimeType = 'application/vnd.google-apps.folder'"
        elif files and not folders:
            query += " and mimeType != 'application/vnd.google-apps.folder'"

        if trashed:
            query += " and trashed = true"
        else:
            query += " and trashed = false"

        return self.drive.items_by_query(query, pageSize=pageSize, orderBy=orderBy, spaces=self.spaces, skip=skip)

    @needs_id
    def mkdir(self, name, ignore_existing=False) -> DriveFolder:
        if not ignore_existing:
            try:
                file_ = self.child(name)
                if file_.isfolder():
                    return file_
                else:
                    raise FileExistsError("Filename already exists ({name}) and it's not a folder.".format(name=name))
            except FileNotFoundError:
                pass
            except AmbiguousPathError:
                raise FileExistsError("Filename already exists multiple times: {name}".format(name=name))
        #TODO: Don't use exception for flow control here. Maybe implement exists()
        file_metadata = {
            'name': name, 
                'name': name, 
            'name': name, 
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [self.id]
        }
        try:
            result = self.drive.service.files().create(body=file_metadata, fields=self.drive.default_fields).execute()
        except HttpError as err:
            raise GoogleDriveAPIError.from_http_error(err)
        return self._reply_to_object(result)
        
    @needs_id
    def new_file(self, filename, ignore_existing=False) -> DriveFile:
        if not ignore_existing:
            try:
                self.child(filename)
                raise FileExistsError("Filename already exists ({name}).".format(name=filename))
            except FileNotFoundError:
                pass
        return DriveFile(self.drive, [self.id], filename)
        
    def child_from_path(self, path) -> DriveItem:
        #TODO: Accept Path objects for path
        splitpath = path.strip('/').split('/', 1)

        if splitpath[0] == ".":
            child = self
        elif splitpath[0] == "..":
            child = self.parent
        else:
            child = self.child(splitpath[0])
            if child.name != splitpath[0]:
                # Handle Google Drive automaticly renaming files on creation
                raise Exception("Could not access {}".format(splitpath[0]))
        if len (splitpath) == 1:
            return child
        else:
            return child.child_from_path(splitpath[1])

    def create_path(self, path) -> DriveFolder:
        #TODO: Accept Path objects for path
        splitpath = path.strip('/').split('/', 1)

        if splitpath[0] == ".":
            child = self
        elif splitpath[0] == "..":
            child = self.parent
        else:
            child = self.mkdir(splitpath[0])
            if child.name != splitpath[0]:
                # Handle Google Drive automaticly renaming files on creation
                child.remove()
                raise Exception("Failed to create {}".format(splitpath[0]))
        if len (splitpath) == 1:
            return child
        else:
            return child.create_path(splitpath[1])

    def isempty(self) -> bool:
        gen = self.children(pageSize=1)
        if next(gen, None) is None:
            return True
        else:
            return False
        
class DriveFile(DriveItem):  

    def isfolder(self):
        return False  

    def isshortcut(self):
        return False

    def __init__(self, drive, parent_ids, filename, file_id=None, spaces='drive', resumable_uri=None):
        super().__init__(drive, parent_ids, filename, file_id, spaces)
        self.resumable_uri = resumable_uri
        
    @needs_id
    def download(self, local_file, chunksize=None, progress_handler=None):
        if not chunksize:
            chunksize = defaultChunksize
        #TODO: Accept Path objects for local_file

        range_md5 = hashlib.md5()
        try:
            local_file_size = os.path.getsize(local_file)
            with open(local_file, "rb") as f:
                for chunk in iter(lambda: f.read(chunksize), b""):
                    range_md5.update(chunk)
        except FileNotFoundError:
            local_file_size = 0
        

        download_url = "https://www.googleapis.com/drive/v3/files/{fileid}?alt=media".\
                                format(fileid=self.id)
        
        with open(local_file, 'ab') as fh:
            while local_file_size < self.size:
                download_range = "bytes={}-{}".\
                    format(local_file_size, local_file_size+chunksize-1)
                    
                # replace with googleapiclient.http.HttpRequest if possible
                # or patch MediaIoBaseDownload to support Range
                resp, content = self.drive.service._http.request(
                                            download_url,
                                            headers={'Range': download_range})
                if resp.status == 206:
                        fh.write(content)
                        local_file_size+=int(resp['content-length'])
                        range_md5.update(content)
                        if progress_handler:
                            progress_handler(MediaDownloadProgress(local_file_size, self.size))
                else:
                    raise GoogleDriveAPIError.from_reply(resp, content)
        if range_md5.hexdigest() != self.md5sum:
            os.remove(local_file)
            raise CheckSumError("Checksum mismatch. Need to repeat download.")

    def upload(self, local_file, chunksize=None,
                resumable_uri=None, progress_handler=None):
        if not chunksize:
            chunksize = defaultChunksize
        #TODO: Accept Path objects for local_file
        if self.id:
            raise FileExistsError("Uploading new revision not yet implemented")
        if os.path.getsize(local_file) == 0:
            self.upload_empty()
            return

        media = MediaFileUpload(local_file, resumable=True, chunksize=chunksize)
        file_metadata = {
            'name': self.name, 
            'parents': self.parent_ids
        }
                
        request = ResumableUploadRequest(self.drive.service, media_body=media, body=file_metadata)
        if resumable_uri:
            self.resumable_uri = resumable_uri
        request.resumable_uri=self.resumable_uri
            
        response = None
        while not response:
            try:
                status, response = request.next_chunk()
            except CheckSumError:
                self.resumable_uri = None
                raise
            self.resumable_uri = request.resumable_uri
            if status and progress_handler:
                progress_handler(status)
        result = json.loads(response)
        self.id = result['id']
        self.name = result['name']
        self.resumable_uri = None

    def upload_empty(self):
        file_metadata = {
            'name': self.name, 
            'parents': self.parent_ids
        }
        try:
            result = self.drive.service.files().create(body=file_metadata, fields=self.drive.default_fields).execute()
        except HttpError as err:
            raise GoogleDriveAPIError.from_http_error(err)
        self.id = result['id']
        self.name = result['name']

    def copy(self, dest=None, new_name=None, ignore_existing=False):
        if not dest:
            dest = self.parent
        if not new_name:
            new_name = self.name
        if not ignore_existing:
            try:
                dest.child(new_name)
                raise FileExistsError
            except FileNotFoundError:
                pass
        
        try:
            result = self.drive.service.files().copy(
                                fileId=self.id,
                                body={
                                    "name": new_name,
                                    "parents": [dest.id]
                                },
                                fields=self.drive.default_fields,
                                ).execute()
        except HttpError as err:
            raise GoogleDriveAPIError.from_http_error(err)
        return dest._reply_to_object(result)
       
    @property
    def md5sum(self):
        if not hasattr(self, "_md5sum"):
            self._md5_sum = self.meta_get("md5Checksum")["md5Checksum"]
        return self._md5_sum
       
    @property
    def size(self):
        if not hasattr(self, "_size"):
            self._size = int(self.meta_get("size")["size"])
        return self._size

class DriveShortcut(DriveItem):
    def __init__(self, drive, parent_ids, filename, file_id, target_id, spaces='drive'):
        super().__init__(drive, parent_ids, filename, file_id, spaces)
        self.target = self.drive.item_by_id(target_id)

    def isshortcut(self) -> bool:
        return True

    def isfolder(self) -> bool:
        return False

    def meta_get(self, fields: str) -> dict:
        return self.target.meta_get(fields)

    def meta_set(self, metadata: dict):
        return self.target.meta_set(metadata)

    def resolve(self) -> str:
        return self.target.resolve()

    def __getattr__(self, name):
        return getattr(self.target, name)

class ResumableUploadRequest:
    # TODO: actually implement interface for http_request
    # TODO: error handling
    def __init__(self, service, media_body, body, upload_id=None):
        self.service = service
        self.media_body = media_body
        self.body = body
        self.upload_id=upload_id
        self._resumable_progress = None
        self._resumable_uri = None
        self._range_md5 = None

    @property
    def upload_id(self):
        if self._upload_id is None:
            self._upload_id = parse_qs(urlparse(self.resumable_uri).query)['upload_id'][0]
        return self._upload_id
    
    @upload_id.setter
    def upload_id(self, upload_id):
        self._resumable_uri = "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&upload_id={}".format(upload_id)
        
    @property
    def resumable_uri(self):
        if self._resumable_uri is None:
            api_url = "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable" 
            status, resp = self.service._http.request(api_url, method='POST', headers={'Content-Type':'application/json; charset=UTF-8'}, body=json.dumps(self.body)) 
            if status['status'] != '200':
                raise GoogleDriveAPIError.from_reply(status, resp)
            self._resumable_uri = status['location']
        return self._resumable_uri
        
    @resumable_uri.setter
    def resumable_uri(self, resumable_uri):
        self._resumable_uri = resumable_uri
        
            
    @property
    def resumable_progress(self):
        if self._resumable_progress is None:
            upload_range = "bytes */{}".format(self.media_body.size())
            status, resp = self.service._http.request(self.resumable_uri, method='PUT', headers={'Content-Length':'0', 'Content-Range':upload_range})
            
            if status['status'] not in ('200', '308'):
                #Should 404 result in a FileNotFound error?
                raise GoogleDriveAPIError.from_reply(status, resp)

            self._range_md5 = hashlib.md5()
            def file_in_chunks(start_byte: int, end_byte: int, chunksize: int = 4*1024**2):
                while start_byte < end_byte:
                    content_length = min(chunksize, end_byte-start_byte)
                    yield self.media_body.getbytes(start_byte, content_length)
                    start_byte += content_length

            if status['status'] == '200':
                self._resumable_progress = self.media_body.size()

                for chunk in file_in_chunks(0, self._resumable_progress):
                    self._range_md5.update(chunk)
            elif 'range' in status.keys():
                self._resumable_progress = int(status['range'].replace('bytes=0-', '', 1))+1

                for chunk in file_in_chunks(0, self._resumable_progress):
                    self._range_md5.update(chunk)
                logger.debug("Local MD5 (0-%d): %s", self._resumable_progress, self._range_md5.hexdigest())
                logger.debug("Remote MD5 (0-%d): %s", self._resumable_progress, status['x-range-md5'])
                if status['x-range-md5'] != self._range_md5.hexdigest():
                    raise CheckSumError("Checksum mismatch. Need to repeat upload.")

            else:
                self._resumable_progress = 0

        return self._resumable_progress

    @resumable_progress.setter
    def resumable_progress(self, resumable_progress):
        self._resumable_progress = resumable_progress

    def next_chunk(self):
        content_length = min(self.media_body.size()-self.resumable_progress, self.media_body.chunksize()) 
        upload_range = "bytes {}-{}/{}".format(self.resumable_progress, self.resumable_progress+content_length-1, self.media_body.size()) 
        content = self.media_body.getbytes(self.resumable_progress, content_length)
        status, resp = self.service._http.request(self.resumable_uri, method='PUT', headers={'Content-Length':str(content_length), 'Content-Range':upload_range}, body=content)
        if status['status'] in ('200', '308'):
            self._range_md5.update(content)
            logger.debug("Local MD5 (0-%d): %s", self.resumable_progress+content_length, self._range_md5.hexdigest())
            if status['status'] == '308':
                logger.debug("Remote MD5 (0-%d): %s", self.resumable_progress+content_length, status['x-range-md5'])
                if status['x-range-md5'] != self._range_md5.hexdigest():
                    raise CheckSumError("Checksum mismatch. Need to repeat upload.")
                self.resumable_progress += content_length
            elif status['status'] == '200':
                self.resumable_progress = self.media_body.size()
                result = json.loads(resp)
                try:
                    remote_md5 = self.service.files().get(fileId=result['id'], fields="md5Checksum").execute()['md5Checksum']
                except HttpError as e:
                    if e.resp.status == 404:
                        raise FileNotFoundError("File was successfully uploaded but since has been deleted")
                    else:
                        raise GoogleDriveAPIError.from_http_error(e)
                logger.debug("Remote MD5 (0-%d): %s", self.resumable_progress, remote_md5)
                if remote_md5 != self._range_md5.hexdigest():
                    raise CheckSumError("Final checksum mismatch. Need to repeat upload.")
        else:
            raise GoogleDriveAPIError.from_reply(status, resp)
            
        return ResumableMediaUploadProgress(self.resumable_progress, self.media_body.size(), self.resumable_uri), resp


class GoogleDrive(DriveFolder):

    @classmethod
    def auth(cls, gauth, appdatafolder=False):
        SCOPES = ['https://www.googleapis.com/auth/drive']
        if appdatafolder:
            SCOPES += ['https://www.googleapis.com/auth/drive.appdata']

        flow = InstalledAppFlow.from_client_config(gauth, SCOPES)
        try:
            creds = flow.run_local_server()
        except OSError:
            creds = flow.run_console()
        if not creds.has_scopes(SCOPES):
            raise NotAuthenticatedError("Could not get requested scopes")
        return Credentials.to_json(creds)

    @classmethod
    def url_to_id(cls, url: str):
        url = urlparse(url)
        if url.netloc != 'drive.google.com':
            raise InvalidUrlError(url)
        if url.path in ('/uc', '/open'):
            try:
                return parse_qs(url.query)['id'][0]
            except KeyError:
                raise InvalidUrlError("Has no ID", url)
        else:
            try:
                path = PurePosixPath(url.path)
                assert path.parts[0:3] == ('/', 'file','d')
                return path.parts[3]
            except (AssertionError, KeyError):
                raise InvalidUrlError(url)

    def __init__(self, creds, autorefresh=True, caching=1000):
        try:
            self.creds = Credentials.from_json(creds)
        except TypeError:
            self.creds = creds

        if self.creds.expired and self.creds.refresh_token \
                and autorefresh:
            self.creds.refresh(Request())

        http = google_auth_httplib2.AuthorizedHttp(self.creds)

        #see bug https://github.com/googleapis/google-api-python-client/issues/803#issuecomment-578151576
        http.http.redirect_codes = set(http.http.redirect_codes) - {308}

        self._service = build('drive', 'v3', http=http)

        self._id_cache = ExpiringDict(max_len=caching, max_age_seconds=float('inf'))
        self._name_cache = ExpiringDict(max_len=caching, max_age_seconds=60)

        self.id = None
        self.drive = self
        self.default_fields = 'id, name, mimeType, parents, spaces, shortcutDetails'
        root_folder = self.item_by_id("root")

        super().__init__(self, root_folder.parent_ids, root_folder.name, root_folder.id, root_folder.spaces)

        if 'https://www.googleapis.com/auth/drive.appdata' in self.creds.scopes:
            self.appdata = self.item_by_id("appDataFolder")

    @property
    def service(self):
        return self._service

    def json_creds(self):
        return Credentials.to_json(self.creds)

    def items_by_query(self, query, pageSize=100, orderBy=None, spaces='drive', skip=0) -> DriveItem:
        pageSize = min(1000, max(pageSize, skip))
        result = {'nextPageToken': ''}
        while "nextPageToken" in result:
            try:
                result = self.service.files().list(
                        pageSize=pageSize,
                        spaces=spaces,
                        fields="nextPageToken, files({})".format(self.default_fields),
                        q=query,
                        pageToken=result['nextPageToken'],
                        orderBy=orderBy,
                    ).execute()
            except HttpError as err:
                raise GoogleDriveAPIError.from_http_error(err)
            items = result.get('files', [])

            for i, file_ in enumerate(items):
                if skip > i:
                    continue
                if skip == i:
                    pageSize = 100
                yield self._reply_to_object(file_)

    def item_by_id(self, id_) -> DriveItem:
        if hasattr(self, 'id') and id_ == self.id:
            return self

        if id_ in self._id_cache:
            logger.debug("Found {} in id cache".format((id_)))
            return self._id_cache[id_]

        try:
            result = self.service.files().get(
                                    fileId=id_,
                                    fields=self.default_fields
                                ).execute()
        except HttpError as err:
            raise GoogleDriveAPIError.from_http_error(err)
        return self._reply_to_object(result)

    def resolve(self) -> str:
        return '/'
    # def create_shortcut(self, *args, **kwargs):
    #     try:
    #         super().create_shortcut(*args, **kwargs)
    #     except BackendError:
    #         raise BackendError("Can't create shortcut to root")
