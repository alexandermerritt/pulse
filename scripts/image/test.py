# This script fetches webpage given as a first argument
# on command line and prints it on stdout.
import urllib
import sys
 
try:
    h = urllib.urlopen(sys.argv[1])
except IOError, e:
    print "Error! %s" % e
    sys.exit(1)
print h.read()
