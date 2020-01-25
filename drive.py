from __future__ import annotations #only > 3.7, better to find a different solution

import os
from abc import ABC, abstractmethod
import json

import hashlib
from urllib.parse import urlparse
from urllib.parse import parse_qs

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import google.oauth2.credentials
import oauth2client.client
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaUploadProgress
from googleapiclient.http import MediaDownloadProgress

from google.auth.exceptions import RefreshError

# import logging
# import httplib2
# httplib2.debuglevel = 4
# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)

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
    pass

class Credentials(google.oauth2.credentials.Credentials,
                oauth2client.client.Credentials):
    @classmethod
    def from_json(cls, json_string):
        a = json.loads(json_string)
        return cls.from_authorized_user_info(a, a['scopes'])

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

class DriveItem(ABC):
    #TODO: metadata as dict
    # Filename not as attribute but as key
    # OR: filename as property method

    def __init__(self, drive, parent_ids, name, id_, spaces):
        self.drive = drive
        self.name = name # TODO: property. Setter => rename
        self.id = id_ # TODO: property
        self.parent_ids = parent_ids
        self.spaces = spaces

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        # not yet sure if this is a good idea
        return hash(self.id)

    @property
    def parent(self):
        if self.parent_ids:
            return self.drive.item_by_id(self.parent_ids[0])
        else:
            return None

    def rename(self, target):
        splitpath = target.rsplit('/', 1)
        if len(splitpath) == 1:
            new_name = splitpath[0]
            parent = self.parent
        else:
            new_name = splitpath[1]
            parent = self.parent.child_from_path(splitpath[0])
            if not parent.isfolder():
                raise NotADirectoryError()

        self.move(parent, new_name)

    def move(self, new_dest, new_name=None):
        result = self.drive.service.files().update(
                                fileId=self.id,
                                body={"name": new_name or self.name},
                                addParents=new_dest.id,
                                removeParents=self.parent.id,
                                fields='name, parents',
                                ).execute()
        self.name = result['name']
        self.parent_ids = result.get('parents', [])
        
    def remove(self):
        self.drive.service.files().delete(fileId=self.id).execute()
        self.id = None

    def trash(self):
        try:
            self.meta_set({'trashed': True})
        except:
            raise HttpError("Could not trash file")

    def meta_set(self, metadata: dict):
        result = self.drive.service.files().update(
                                fileId=self.id,
                                body=metadata,
                                fields=','.join(metadata.keys()),
                                ).execute()

    def meta_get(self, fields: str) -> dict:
        return self.drive.service.files().get(fileId=self.id, fields=fields).execute()

    def refresh(self):
        result = self.drive.service.files().get(
                                fileId=self.id,
                                fields=self.drive.default_fields
                            ).execute()
        self.name = result['name']
        self.parent_ids = result['parents']

    @abstractmethod
    def isfolder(self):
        pass

class DriveFolder(DriveItem):

    def isfolder(self):
        return True  
    
    def child(self, name):
        gen = self.children(name=name, pageSize=2)
        child = next(gen, None)
        if child is None:
            raise FileNotFoundError(name)
        if next(gen, None) is not None:
            raise AmbiguousPathError("Two or more files {name}".format(name=name))
        return child
        
    def children(self, name=None, folders=True, files=True, trashed=False, pageSize=100, orderBy=None):
        #TODO: Add "name" argument
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

        return self.drive.items_by_query(query, pageSize=pageSize, orderBy=orderBy, spaces=self.spaces)

    def mkdir(self, name):
        try:
            file_ = self.child(name)
            if not file_.isfolder():
                raise FileExistsError("Filename already exists ({name}) and it's not a folder.".format(name=name))
            return file_
        except FileNotFoundError:
            #TODO: Don't use exception for flow control here. Maybe implement exists()
            file_metadata = {
                'name': name, 
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [self.id]
            }
            result = self.drive.service.files().create(body=file_metadata, fields=self.drive.default_fields).execute()
            return self._reply_to_object(result)
        
    def new_file(self, filename):
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
        
    def _reply_to_object(self, reply):
        if reply['mimeType'] == 'application/vnd.google-apps.folder':
            return DriveFolder(self.drive, reply.get('parents', []), reply['name'], reply['id'], spaces=",".join(reply['spaces']))
        else:
            return DriveFile(self.drive, reply.get('parents', []), reply['name'], reply['id'], spaces=",".join(reply['spaces']))

class DriveFile(DriveItem):  

    def isfolder(self):
        return False  

    def __init__(self, drive, parent_ids, filename, file_id=None, spaces='drive', resumable_uri=None):
        super().__init__(drive, parent_ids, filename, file_id, spaces)
        self.resumable_uri = resumable_uri
        
    def download(self, local_file, chunksize=None, progress_handler=None):
        if not chunksize:
            chunksize = defaultChunksize
        #TODO: Accept Path objects for local_file
        if not self.id:
            raise FileNotFoundError

        range_md5 = hashlib.md5()
        try:
            local_file_size = os.path.getsize(local_file)
            with open(local_file, "rb") as f:
                for chunk in iter(lambda: f.read(chunksize), b""):
                    range_md5.update(chunk)
        except FileNotFoundError:
            local_file_size = 0
        

        remote_file_size = int(self.drive.service.files().\
                            get(fileId=self.id, fields="size").\
                            execute()['size'])
        
        download_url = "https://www.googleapis.com/drive/v3/files/{fileid}?alt=media".\
                                format(fileid=self.id)
        
        with open(local_file, 'ab') as fh:
            while local_file_size < remote_file_size:
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
                            progress_handler(MediaDownloadProgress(local_file_size, remote_file_size))
                else:
                    raise HttpError(resp, content)
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

    def upload_empty(self):
        file_metadata = {
            'name': self.name, 
            'parents': self.parent_ids
        }
        result = self.drive.service.files().create(body=file_metadata, fields=self.drive.default_fields).execute()
        self.id = result['id']
        self.name = result['name']
       
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
                raise HttpError(status, resp)
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
                raise HttpError(status, resp)

            if status['status'] == '200':
                self._resumable_progress = self.media_body.size()
            elif 'range' in status.keys():
                byte_range = status['range']
                self._resumable_progress = int(byte_range.replace('bytes=0-', '', 1))+1
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
            if not self._range_md5:
                self._range_md5 = hashlib.md5()
                def file_in_chunks():
                    start_byte = 0
                    while start_byte < self.resumable_progress:
                        content_length = min(self.media_body.chunksize(), self.resumable_progress-start_byte)
                        print(start_byte, start_byte+content_length)
                        yield self.media_body.getbytes(start_byte, content_length)
                        start_byte += content_length
                for chunk in file_in_chunks():
                    self._range_md5.update(chunk)
            self._range_md5.update(content)
            if status['status'] == '308':
                if status['x-range-md5'] != self._range_md5.hexdigest():
                    raise CheckSumError("Checksum mismatch. Need to repeat upload.")
                self.resumable_progress += content_length
            elif status['status'] == '200':
                self.resumable_progress = self.media_body.size()
                result = json.loads(resp)
                remote_md5 = self.service.files().get(fileId=result['id'], fields="md5Checksum").execute()['md5Checksum']
                if remote_md5 != self._range_md5.hexdigest():
                    raise CheckSumError("Checksum mismatch. Need to repeat upload.")
        else:
            raise HttpError(status, resp)
            
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

    def __init__(self, creds):
        try:
            self.creds = Credentials.from_json(creds)
        except TypeError:
            self.creds = creds

        if self.creds.expired and self.creds.refresh_token:
            self.creds.refresh(Request())

        self._service = build('drive', 'v3', credentials=self.creds)

        self.id = None
        self.drive = self
        self.default_fields = 'id, name, mimeType, parents, spaces'
        root_folder = self.item_by_id("root")

        super().__init__(self, root_folder.parent_ids, root_folder.name, root_folder.id, root_folder.spaces)

        if 'https://www.googleapis.com/auth/drive.appdata' in self.creds.scopes:
            self.appdata = self.item_by_id("appDataFolder")
        
        #self.caching = caching
        #TODO: Add caching ability

    @property
    def service(self):
        return self._service

    def json_creds(self):
        return Credentials.to_json(self.creds)

    def items_by_query(self, query, pageSize=100, orderBy=None, spaces='drive'):
        result = {'nextPageToken': ''}
        while "nextPageToken" in result:
            result = self.service.files().list(
                    pageSize=pageSize,
                    spaces=spaces,
                    fields="nextPageToken, files({})".format(self.default_fields),
                    q=query,
                    pageToken=result['nextPageToken'],
                    orderBy=orderBy,
                ).execute()
            items = result.get('files', [])

            for file_ in items:
                yield self._reply_to_object(file_)

    def item_by_id(self, id_):
        if hasattr(self, 'id') and id_ == self.id:
            return self
        result = self.service.files().get(
                                fileId=id_,
                                fields=self.default_fields
                            ).execute()
        return self._reply_to_object(result)