#! /usr/bin/python
#
# requires flickrapi and gdata
#
# It's a little ugly, but it is heavily tested and works!
import flickrapi, StringIO
import gdata
import gdata.data
import gdata.photos.service
from getpass import getpass
from urllib import urlretrieve
from tempfile import mkstemp
from threadpool import ThreadPool, WorkRequest
import os
import sys, os.path, StringIO
import time
import random
import gdata.service
import gdata
import atom.service
import atom
import gdata.photos
from shutil import copyfile

from conf import *

from gdata.photos.service import GPHOTOS_INVALID_ARGUMENT, GPHOTOS_INVALID_CONTENT_TYPE, GooglePhotosException

video_too_large_save_location = os.path.join(os.path.sep.join(__file__.split(os.path.sep)[:-1]), 'picasa_videos')

if not os.path.exists(video_too_large_save_location):
    os.mkdir(video_too_large_save_location)

class VideoEntry(gdata.photos.PhotoEntry):
    pass
    
gdata.photos.VideoEntry = VideoEntry

def InsertVideo(self, album_or_uri, video, filename_or_handle, content_type='image/jpeg'):
    """Copy of InsertPhoto which removes protections since it *should* work"""
    try:
        assert(isinstance(video, VideoEntry))
    except AssertionError:
        raise GooglePhotosException({'status':GPHOTOS_INVALID_ARGUMENT,
            'body':'`video` must be a gdata.photos.VideoEntry instance',
            'reason':'Found %s, not PhotoEntry' % type(video)
        })
    try:
        majtype, mintype = content_type.split('/')
        #assert(mintype in SUPPORTED_UPLOAD_TYPES)
    except (ValueError, AssertionError):
        raise GooglePhotosException({'status':GPHOTOS_INVALID_CONTENT_TYPE,
            'body':'This is not a valid content type: %s' % content_type,
            'reason':'Accepted content types:'
        })
    if isinstance(filename_or_handle, (str, unicode)) and \
        os.path.exists(filename_or_handle): # it's a file name
        mediasource = gdata.MediaSource()
        mediasource.setFile(filename_or_handle, content_type)
    elif hasattr(filename_or_handle, 'read'):# it's a file-like resource
        if hasattr(filename_or_handle, 'seek'):
            filename_or_handle.seek(0) # rewind pointer to the start of the file
        # gdata.MediaSource needs the content length, so read the whole image 
        file_handle = StringIO.StringIO(filename_or_handle.read()) 
        name = 'image'
        if hasattr(filename_or_handle, 'name'):
            name = filename_or_handle.name
        mediasource = gdata.MediaSource(file_handle, content_type,
            content_length=file_handle.len, file_name=name)
    else: #filename_or_handle is not valid
        raise GooglePhotosException({'status':GPHOTOS_INVALID_ARGUMENT,
            'body':'`filename_or_handle` must be a path name or a file-like object',
            'reason':'Found %s, not path name or object with a .read() method' % \
            type(filename_or_handle)
        })

    if isinstance(album_or_uri, (str, unicode)): # it's a uri
        feed_uri = album_or_uri
    elif hasattr(album_or_uri, 'GetFeedLink'): # it's a AlbumFeed object
        feed_uri = album_or_uri.GetFeedLink().href

    try:
        return self.Post(video, uri=feed_uri, media_source=mediasource,
            converter=None)
    except gdata.service.RequestError, e:
        raise GooglePhotosException(e.args[0])
        
gdata.photos.service.PhotosService.InsertVideo = InsertVideo

def clear_input_retriever(setting):
    return raw_input(setting.name + ":")

def passwd_input_retriever(setting):
    return getpass(setting.name + ':')

class Setting(object):
    
    def __init__(self, name, default=None, input_retriever=clear_input_retriever, empty_value=None):
        self.name = name
        self._value = default
        self.input_retriever = input_retriever
        self.empty_value = empty_value
        
    @property
    def value(self):
        while self._value == self.empty_value:
            self._value = self.input_retriever(self)
            
        return self._value
    

FLICKR = None

picasa_username = Setting('Picasa Username(complete email)',GMAIL_ACCOUNT)
picasa_password = Setting('Picasa Password', default=GMAIL_PASSWORD,input_retriever=passwd_input_retriever)

flickr_api_key = Setting('Flickr API Key',default=FLICKR_API_KEY)
flickr_api_secret = Setting('Flickr API Secret',default=FLICKR_API_SECRET)

flickr_usernsid = None

def flickr_token_retriever(setting):
    global FLICKR
    global flickr_usernsid
    if FLICKR is None:
        FLICKR = flickrapi.FlickrAPI(flickr_api_key.value, flickr_api_secret.value)
    
    (token, frob) = FLICKR.get_token_part_one(perms='write')
    
    if not token: raw_input("Press ENTER after you authorized this program")
    
    FLICKR.get_token_part_two((token, frob))
    
    flickr_usernsid = FLICKR.auth_checkToken(auth_token=token).find('auth').find('user').get('nsid')
    
    return True
    

def get_gd_client():

    gd_client = gdata.photos.service.PhotosService()
    gd_client.email = picasa_username.value
    gd_client.password = picasa_password.value
    gd_client.source = 'migrate-flickr-to-picasa.py'
    gd_client.ProgrammaticLogin()

    return gd_client

def do_migration(threadpoolsize=7):

    print 'Authenticating with Picasa...'
    gd_client = get_gd_client()

    print 'Authenticating with Flickr..'
    flickr_token = Setting('Flickr Token', input_retriever=flickr_token_retriever)
    token = flickr_token.value # force retrieval of authentication information...

    sets = FLICKR.photosets_getList().find('photosets').getchildren()

    print 'Found %i sets to move over to Picasa.' % len(sets)


    def get_picasa_albums(id, aset, num_photos):
        all_picasa_albums = gd_client.GetUserFeed(user=picasa_username.value).entry
        picasa_albums = []
        id = id.strip()
    
        orig_id = id
    
        for i in range((num_photos/1000) + 1):
            if i > 0:
                id = orig_id + '-' + str(i)
        
            picasa_album = None
        
            for album in all_picasa_albums:
                if album.title.text == id:
                    picasa_album = album
                    break
            
            if picasa_album is not None:
                print '"%s" set already exists as an album in Picasa.' % id
            else:
                description = aset.find('description').text
                if description is not None and len(description) > 1000:
                    description = description[:1000]
                picasa_album = gd_client.InsertAlbum(title=id, summary=description, access='protected')
                print 'Created picasa album "%s".' % picasa_album.title.text
    
            picasa_albums.append(picasa_album)
    
        return picasa_albums
    

    def get_picasa_photos(picasa_albums):
        photos = []
    
        for album in picasa_albums:
            photos.extend(gd_client.GetFeed(album.GetFeedLink().href).entry)
    
        return photos

    def get_photo_url(photo):
        if photo.get('media') == 'video':
            return "http://www.flickr.com/photos/%s/%s/play/orig/%s" % (flickr_usernsid, photo.get('id'), photo.get('originalsecret'))
        else:
            return photo.get('url_o')


    def move_photo(flickr_photo, picasa_album):
    
        def download_callback(count, blocksize, totalsize):
            
            download_stat_print = set((0.0, .25, .5, 1.0))
            downloaded = float(count*blocksize)
            res = int((downloaded/totalsize)*100.0)
	
            for st in download_stat_print:
                dl = totalsize*st
                diff = downloaded - dl
                if diff >= -(blocksize/2) and diff <= (blocksize/2):
                    downloaded_so_far = float(count*blocksize)/1024.0/1024.0
                    total_size_in_mb = float(totalsize)/1024.0/1024.0
                    print "photo: %s, album: %s --- %i%% - %.1f/%.1fmb" % (flickr_photo.get('title'), picasa_album.title.text, res, downloaded_so_far, total_size_in_mb)

        dest = os.path.join(video_too_large_save_location, flickr_photo.get('title'))
        if os.path.exists(dest):
            print 'Video "%s" of "%s" already exists in download cache of files over 100MB. Aborting download.' % (flickr_photo.get('title'), picasa_album.title.text)
            return
    
        photo_url = get_photo_url(flickr_photo)
        print 'Downloading photo "%s" at url "%s".' % (flickr_photo.get('title'), photo_url)
        (fd, filename) = tmp_file = mkstemp()
        (filename, headers) = urlretrieve(photo_url, filename, download_callback)
        print 'Download Finished of %s for album %s at %s.' % (flickr_photo.get('title'), picasa_album.title.text, photo_url)
    
        size = os.stat(filename)[6]
        if size >= 100*1024*1024:
            print 'File "%s" of set "%s" larger than 100mb. Moving to download directory for manual handling. ' % (flickr_photo.get('title'), picasa_album.title.text)
            copyfile(filename, dest)
            os.close(fd)
            os.remove(filename)
            return
    
        print 'Uploading photo %s of album %s to Picasa.' % (flickr_photo.get('title'), picasa_album.title.text)

        if flickr_photo.get('media') == 'photo':
            picasa_photo = gdata.photos.PhotoEntry()
        else:
            picasa_photo = VideoEntry()

        picasa_photo.title = atom.Title(text=flickr_photo.get('title'))
        picasa_photo.summary = atom.Summary(text=flickr_photo.get('description'), summary_type='text')
        photo_info = FLICKR.photos_getInfo(photo_id=flickr_photo.get('id')).find('photo')
        picasa_photo.media.keywords = gdata.media.Keywords()
        picasa_photo.media.keywords.text = ', '.join([t.get('raw') for t in photo_info.find('tags').getchildren()])
        picasa_photo.summary.text = photo_info.find('description').text

        #random pause between 1 to 5 seconds
        s = random.randint(0,3)
        if s > 0:
          print 'Sleeping ' + str(s) + ' seconds...'
          time.sleep(s)

    
        if flickr_photo.get('media') == 'photo':
            gd_client.InsertPhoto(picasa_album, picasa_photo, filename, content_type=headers.get('content-type', 'image/jpeg'))
        else:
            gd_client.InsertVideo(picasa_album, picasa_photo, filename, content_type=headers.get('content-type', 'video/avi'))

        print 'Upload Finished of %s for album %s.' % (flickr_photo.get('title'), picasa_album.title.text)

        os.close(fd)
        os.remove(filename)
    

    threadpool = ThreadPool(threadpoolsize)

    for aset_id in range(len(sets)): # go through each flickr set
        aset = sets[aset_id]
        set_title = aset.find('title').text
        print 'Moving "%s" set over to a picasa album. %i/%i' % (set_title, aset_id + 1, len(sets))

        print 'Gathering set "%s" information.' % set_title
    
        num_photos = int(aset.get('photos')) + int(aset.get('videos'))
        all_photos = []
    
        page = 1
        while len(all_photos) < num_photos:
            all_photos.extend(
                FLICKR.photosets_getPhotos(
                    photoset_id=aset.get('id'),
                    per_page=500,
                    extras="url_o,media,original_format",
                    page=page,
                    media='all'
                ).find('photoset').getchildren()
            )
            page += 1


        print 'Found %i photos and videos in the %s flickr set.' % (num_photos, set_title)
    
        picasa_albums = get_picasa_albums(set_title, aset, len(all_photos))
        picasa_photos = get_picasa_photos(picasa_albums)
    
        for photo_id in range(len(all_photos)):
        
            photo = all_photos[photo_id]
            photo_found = False
        
            for p_photo in picasa_photos:
                if p_photo.title.text == photo.get('title'):
                    print 'Already have photo "%s", skipping' % photo.get('title')
                    photo_found = True
                    break

            if photo_found:
                continue
            else:
                print 'Queuing photo %i/%i, %s of album %s for moving.' % (photo_id + 1, len(all_photos), photo.get('title'), set_title)

            p_album = None
            for album in picasa_albums:
                if int(album.numphotosremaining.text) > 0:
                    album.numphotosremaining.text = str(int(album.numphotosremaining.text) - 1)
                    p_album = album
                    break
        
            req = WorkRequest(move_photo, [photo, p_album], {})
            threadpool.putRequest(req)
       
    
    threadpool.wait()
    
    
if __name__ == "__main__":
    
    print """
    This script will move all the photos and sets from flickr over to picasa. 
    That will require getting authentication information from both services...
    """
    random.seed(time.time())
    do_migration()
