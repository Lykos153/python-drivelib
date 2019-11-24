import pickle

import os
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

from google.auth.exceptions import RefreshError

#TODO: Proper Exceptions

class NotAuthenticatedError(Exception):
    pass

class Credentials(google.oauth2.credentials.Credentials,
                oauth2client.client.Credentials):
    @classmethod
    def from_json(cls, json_string):
        a = json.loads(json_string)
        return Credentials(
            token=a['access_token'],
            refresh_token=a['refresh_token'],
            id_token=a['id_token'],
            token_uri=a['token_uri'],
            client_id=a['client_id'],
            client_secret=a['client_secret'],
            scopes=a['scopes'])

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

class DriveItem:
    #TODO: metadata as dict
    # Filename not as attribute but as key
    # OR: filename as property method

    @property
    def service(self):
        if hasattr(self, '_service'):
            return self._service
        return self.parent.service
        
    @service.setter
    def service(self, service):
        self._service = service

    def rename(self, new_name):
        result = self.service.files().update(
                                fileId=self.id,
                                body={"name": new_name},
                                fields='name',
                                ).execute()
        if 'name' in result and result['name'] == new_name:
            self.name = new_name
        else:
            raise Exception()


    def move(self, new_dest):
        result = self.service.files().update(
                                fileId=self.id,
                                addParents=new_dest.id,
                                removeParents=self.parent.id,
                                fields='parents',
                                ).execute()
        if 'parents' in result and new_dest.id in result['parents']:
            self.parent = new_dest
        else:
            raise Exception()

    def move_to_path(self, new_path):
        raise NotImplementedError
        
    def remove(self):
        self.service.files().delete(fileId=self.id).execute()
        self.id = None

class DriveFolder(DriveItem):
    def __init__(self, parent, name, id_):
        self.parent = parent
        self.id = id_
        self.name = name
        
    
    def child(self, name):
        result = self.service.files().list(
                pageSize=1,
                fields="nextPageToken, files(id, name, mimeType)",
                q="'{this}' in parents and name='{name}'".format(this=self.id, name=name)
            ).execute()
        if "nextPageToken" in result:
            raise Exception("Two or more files {name}".format(name=name))
        if not result['files']:
            raise FileNotFoundError(name)
        return self._reply_to_object(result["files"][0])
        
    def children(self, folders=True, files=True, trashed=False, pageSize=100, orderBy=None):
        query = "'{this}' in parents".format(this=self.id)
        if not folders and not files:
            return
        elif not files:
            query += " and mimeType = 'application/vnd.google-apps.folder'"
        elif not folders:
            query += " and mimeType != 'application/vnd.google-apps.folder'"
        if trashed:
            query += " and trashed = true"
        else:
            query += " and trashed = false"

        result = {'nextPageToken': ''}
        while "nextPageToken" in result:
            result = self.service.files().list(
                    pageSize=pageSize,
                    fields="nextPageToken, files(id, name, mimeType)",
                    q=query,
                    pageToken=result['nextPageToken'],
                    orderBy=orderBy,
                ).execute()
            items = result.get('files', [])

            for file_ in items:
                yield self._reply_to_object(file_)

    def mkdir(self, name):
        file_ = self.child(name)
        if file_:
            if not hasattr(file_, "child"):
                raise Exception("Filename already exists ({name}) and it's not a folder.".format(name=name))
            return file_

        file_metadata = {
            'name': name, 
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [self.id]
        }
        result = self.service.files().create(body=file_metadata, fields='id, name').execute()
        result['mimeType'] = 'application/vnd.google-apps.folder'
        return self._reply_to_object(result)
        
    def new_file(self, filename):
        return DriveFile(self, filename)
        
    def child_from_path(self, path):
        splitpath = path.strip('/').split('/', 1)
        child = self.child(splitpath[0])
        if len (splitpath) == 1:
            return child
        else:
            return child.child_from_path(splitpath[1])
        
    def _reply_to_object(self, reply):
        if reply['mimeType'] == 'application/vnd.google-apps.folder':
            return DriveFolder(self, reply['name'], reply['id'])
        else:
            return DriveFile(self, reply['name'], reply['id'])
            
class DriveFile(DriveItem):
    def __init__(self, parent, name, id_=None):
        self.name = name
        self.id = id_
        self.parent = parent
        self.state = dict()
        self.resumable_uri = None
        
    def download(self, local_file, chunksize=10**7, progress_handler=None):
        try:
            local_file_size = os.path.getsize(local_file)
        except FileNotFoundError:
            local_file_size = 0
        
        remote_file_size = int(self.service.files().\
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
                resp, content = self.service._http.request(
                                            download_url,
                                            headers={'Range': download_range})
                if resp.status == 206:
                        fh.write(content)
                        local_file_size+=int(resp['content-length'])
                        if progress_handler:
                            progress_handler(local_file_size)
                else:
                    raise HttpError(resp, content)

    def upload(self, local_file, chunksize=10*1024**2,
                resumable_uri=None, progress_handler=None):
        media = MediaFileUpload(local_file, resumable=True, chunksize=chunksize)
        body = {'name': self.name, 'parents': [self.parent.id]}
                
        request = ResumableUploadRequest(self.service, media_body=media, body=body)
        if resumable_uri:
            self.resumable_uri = resumable_uri
        request.resumable_uri=self.resumable_uri
            
        response = None
        while not response:
            status, response = request.next_chunk()
            self.resumable_uri = request.resumable_uri
            if status and progress_handler:
                progress_handler(status.resumable_progress)
        self.id = json.loads(response)['id']



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
        self._upload_id=upload_id
        if self._upload_id:
            self._resumable_uri = "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&upload_id={}".format(upload_id)
        
    @property
    def resumable_uri(self):
        if self._resumable_uri is None:
            api_url = "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable" 
            status, resp = self.service._http.request(api_url, method='POST', headers={'Content-Type':'application/json; charset=UTF-8'}, body=json.dumps(self.body)) 
            if status['status'] != '200':
                raise Exception(status)
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
                raise Exception(status)

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
        if status['status'] not in ('200', '308'):
            raise Exception(status)
        if status['status'] == '308':
            if not self._range_md5:
                self._range_md5 = hashlib.md5()
                self._range_md5.update(self.media_body.getbytes(0, self.resumable_progress))
            self._range_md5.update(content)
            if status['x-range-md5'] != self._range_md5.hexdigest():
                raise Exception("Checksum mismatch. Need to repeat upload.")
            self.resumable_progress += content_length
        elif status['status'] == '200':
            self.resumable_progress = self.media_body.size()
            # TODO: md5sum check for last chunk
            
        return MediaUploadProgress(self.resumable_progress, self.media_body.size()), resp


class GoogleDrive(DriveFolder):
    def __init__(self, gauth_json, creds_json=None, autoconnect=False):
        self.gauth = json.loads(gauth_json)
        if creds_json:
            self.creds = Credentials.from_json(creds_json)
        else:
            self.creds = None
        self.autoconnect = autoconnect
        self.id = "root"
        self._service = None
    
    @property
    def service(self):
        if self.autoconnect:
            self.connect()
        if self._service:
            return self._service
        else:
            raise Exception("Not connected. Execute connect() first.")
  
    def connect(self):
        if not self.creds:
            raise NotAuthenticatedError()
        if self.creds.expired and self.creds.refresh_token:
            self.creds.refresh(Request())

        self._service = build('drive', 'v3', credentials=self.creds)
        
    def auth(self):
        SCOPES = ['https://www.googleapis.com/auth/drive']
            
        if self.creds and self.creds.expired and self.creds.refresh_token:
            try:
                self.creds.refresh(Request())
            except RefreshError:
                pass

        if not self.creds or not self.creds.valid:
            flow = InstalledAppFlow.from_client_config(self.gauth, SCOPES)
            try:
                self.creds = flow.run_local_server()
            except OSError:
                self.creds = flow.run_console()
        return self.json_creds()

    def json_creds(self):
        return Credentials.to_json(self.creds)

    def items_by_query(self, query, maxResults=100, orderBy=None):
        raise NotImplementedError