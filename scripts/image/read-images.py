#! /usr/bin/env python2.6
# Need python2.6 to use PIL
from PIL import Image
import os, sys
from stat import *

# Show image sizes on disk and when loaded in memory

IMAGE_LIST_FILE = 'imagelist'

print('width height disk_bytes mem_bytes enc_ratio path')
with open(IMAGE_LIST_FILE, 'r') as f:
    for path in f:
        path = path.strip()
        im = None
        try:
            im = Image.open(path)
        except IOError:
            print('Error opening ' + path)
            sys.exit(1)
        #name = path.split('/')[-1]
        mode = im.mode
        mem = im.width * im.height
        if mode in ['RGB', 'YCbCr', 'LAB', 'HSV']:
            mem *= 3
        elif mode in ['RGBA', 'CMYK', 'I', 'F']:
            mem *= 4
        disk = os.stat(path)[ST_SIZE]
        print(str(im.width)
                + ' ' + str(im.height)
                + ' ' + str(disk)
                + ' ' + str(mem)
                + ' ' + str(round(float(mem)/disk,2))
                + ' ' + path)
