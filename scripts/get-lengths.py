#! /usr/bin/env python3
import sys
import time
import threading
import urllib.request as ur # >= python3

def gethead(urlstr):
    head = ''
    req = ur.Request(url=urlstr, method='HEAD')
    with ur.urlopen(req) as u:
        head = u.info()
        u.close()
    return head

def getlen(urlstr):
    head = gethead(urlstr)
    l = 0
    for entry in head:
        if 'Length' in entry:
            l = int(head[entry])
            break
    return l

def printlen(urls):
    for url in urls:
        print(url + ' ' + str(getlen(str(url))))
        time.sleep(0.5)

def process(urls,nthreads):
    per = int(len(urls) / nthreads)
    while len(urls) > 0:
        s = set()
        for ii in range(0,per):
            s.add(urls.pop())
            if len(urls) == 0:
                break
        t = threading.Thread(target=printlen, args=(s,))
        t.daemon = False
        t.start()

def readurls():
    urls = set()
    for line in sys.stdin:
        url = line.split()[0]
        if 'http' not in url:
            print('Error: url malformed: ' + url,
                    file = sys.stderr)
            sys.exit(1)
        urls.add(url)
    if len(urls) == 0:
        raise Exception('Empty set of URLs')
    return urls

process( readurls(), 16 )

