#! /usr/bin/env python3

import os
import time
import sys
import xml.etree.ElementTree as et
import math

#import urllib2 # <= python2
import urllib.request as urllib # >= python3

# XXX Obtain a key and add here. XXX
API_KEYS = []
API_SECRETS = []

API_KEY = API_KEYS[0]
API_SECRET = API_SECRETS[0]

# A previously started run with URL as first column
# to exclude when resuming search.
EXCLUDE_FILES = []

HOST = 'https://api.flickr.com'
API = '/services/rest'

DOWNLOAD_IMAGE = False

######################################################################

msearch  = 'flickr.photos.search'
msizes   = 'flickr.photos.getSizes'
mcluster = 'flickr.tags.getClusters'

if len(sys.argv) < 2:
    print('Specify seed tag for search.', file=sys.stderr)
    sys.exit(1)
seedtag    = sys.argv[1]
# date-posted-asc, date-posted-desc, date-taken-asc, date-taken-desc,
# interestingness-desc, interestingness-asc, and relevance
sortby     = 'interesting'
nimages    = 10000
perpage    = 500

# To keep track of duplicates
urldb = set()

# fetch a raw URL
#url = None
def geturl(urlstr):
    #global url
    url = urllib.urlopen(urlstr)
    page = url.read()
    url.close()
    return page

# query Flickr based on given function, returns page
def getpage(method, options):
    urlstr = '{}{}?method={}'.format(HOST, API, method)
    urlstr += '&api_key={}'.format(API_KEY)
    for opt in options.keys():
        urlstr += '&{}={}'.format(opt, options[opt])
    page = geturl(urlstr)
    rsp = et.fromstring(page)
    if rsp.attrib['stat'] == 'fail':
        msg = rsp[0].attrib['msg']
        if 'photo not found' in msg.lower():
            print('# not found: ' + urlstr)
        else:
            raise Exception('Flickr: ' + urlstr + ': ' + msg)
    return page

# returns list of tags Flickr thinks are related
def getcluster(tag):
    tags = []
    options = { 'tag' : tag }
    page = getpage(mcluster, options)
    clusters = et.fromstring(page)[0]
    for t in clusters[0]: # just first one for now
        tags.append(t.text)
    return tags

def skipimage(etimage):
    attrib = etimage.attrib
    if 'herosjourneymythology' in attrib['owner']: # sketchy photos
        return True
    if '48312507@N02' in attrib['owner']:
        return True
    # add others
    return False

mgroupsearch = 'flickr.groups.search'
def findgroups(text):
    options = { 'text' : text }
    page = getpage(mgroupsearch, options)
    groups = et.fromstring(page)[0]

# should have URL in first column
# other columns don't matter
def loadurldb(paths):
    global urldb
    # example:
    # https://farm1.staticflickr.com/623/23432243139_33447f9148_o.jpg
    #                                     image id     key     size
    for path in paths:
        with open(path, 'r') as f:
            lines = f.readlines()
            for line in lines:
                url = line.split()[0]
                if 'http' not in url:
                    continue
                name = os.path.basename(url)
                iid = int(name.split('_')[0])
                urldb.add(iid)
    print('# Excluding {} images'.format(len(urldb)), file=sys.stderr)

tags = getcluster(seedtag)
#tags = ['people', 'group']
tagopt = ','.join(tags[0:20])
print(tagopt)

loadurldb(EXCLUDE_FILES)
print('url width height id')
npages = int(math.ceil(float(nimages) / perpage))
for pagen in range(1,npages+1):
    options = { 'safe_search'   : '1',
                'media'         : 'photos',
                'per_page'      : perpage,
                'page'          : pagen,
                'tags'          : tagopt,
                'text'          : seedtag,
                'content_type'  : '1', # 1: photos only
            }
    photos = et.fromstring(getpage(msearch, options))[0]
    for image in photos:
        if skipimage(image):
            continue

        imgid = image.attrib['id']
        fname = imgid + '.jpg'

        if (DOWNLOAD_IMAGE and os.path.exists(fname)) or (int(imgid) in urldb):
            nimages -= 1
            if nimages <= 0:
                break
            continue

        options = { 'photo_id' : imgid } 
        #print(image.attrib)

        page = getpage(msizes, options)
        sizes = et.fromstring(page)[0]
        nsizes = len(sizes)

        # choose specific size
        #for s in sizes:
        #    if s.attrib['width'] == '1024':
        #        #size = sizes[-1].attrib # largest
        #        size = s.attrib
        #        print('{}: {}x{} {}'.format(nimages, size['width'], size['height'], imgid))
        #        #img = geturl(size['source'])
        #        #with open(fname, 'w') as imgf:
        #            #imgf.write(img)

        # choose largest
        # for s in sizes:
        #     #size = sizes[s].attrib
        #     size = s.attrib
        #     mp = int(size['width']) * int(size['height']) / 1e6
        #     if mp >= 4:
        #         print('{} {} {} {}'.format(size['source'],
        #             size['width'], size['height'], imgid,
        #             ))

        if len(sizes) == 0:
            continue

        size = sizes[-1].attrib
        print('{} {} {} {}'.format(size['source'],
            size['width'], size['height'], imgid,
            ))

        # keep track of duplicates
        urldb.add(int(imgid))

        if DOWNLOAD_IMAGE:
            img = geturl(size['source'])
            with open(fname, 'w') as imgf:
                imgf.write(img)

        nimages -= 1
        if nimages <= 0:
            break

        sys.stdout.flush()
        time.sleep(1.2)

