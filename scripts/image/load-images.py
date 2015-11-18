#! /usr/bin/env python3
import subprocess as sp
import memcache, sys, time

# Start memcached with different "factor" values (which determine
# number and size of slab buckets for allocation), and for each
# launch, load all images in the input file and measure resulting
# memory usage.

# If script dies, be sure to kill the memc instance.

PATH_TO_MEMC    = '../../../memcached/memcached'
MEMC_IP         = '10.0.88.80'
MEMC_PORT       = '11211'
MEMC_MAX_MEM    = '16384' # in MiB

PATH_INPUT_FILE = 'imagelist'

def launch_mc(factor):
    cmd = [PATH_TO_MEMC,
            '-f', str(factor),
            '-l', MEMC_IP, '-p', MEMC_PORT,
            '-M', '-m', MEMC_MAX_MEM]
    p = sp.Popen(cmd, universal_newlines=True,
            stdout=sp.PIPE, stderr=sp.STDOUT)
    time.sleep(1)
    p.poll() # initialize returncode
    if p.returncode and p.returncode != 0:
        raise Exception(p.stdout.read())
    return p

def server_pid():
    cmd = ['pgrep', 'memcached']
    p = sp.run(cmd, universal_newlines=True,
            stdout=sp.PIPE, stderr=sp.STDOUT)
    if p.returncode != 0:
        print(p.stdout)
        sys.exit(1)
    pid = p.stdout.strip()
    return pid

def server_rss():
    pid = server_pid()
    # FIXME just read file and parse in python, not shell...
    cmd = ['cut', '-d', ' ', '-f', '24', '/proc/'+pid+'/stat']
    p = sp.run(cmd, universal_newlines=True,
            stdout=sp.PIPE, stderr=sp.STDOUT)
    if p.returncode != 0:
        print(p.stdout)
        sys.exit(1)
    rss_pages = float(p.stdout.strip())
    rss_bytes = (rss_pages * 4096)
    return rss_bytes

def load_all():
    mc = memcache.Client([MEMC_IP + ':' + MEMC_PORT], debug=0)
    disk_bytes = 0
    with open(PATH_INPUT_FILE, 'r') as f:
        for path in f:
            path = path.strip()
            with open(path, 'rb') as p:
                buf = p.read()
                #print(str(len(buf)) + ' ' + path)
                mc.set(path, buf)
                disk_bytes += len(buf)
    #print('server rss ' + str(server_rss()))
    mc.disconnect_all()
    return disk_bytes

def run_test():
    disk_bytes = load_all()
    rss_bytes = server_rss()

    #print('disk_bytes ' + str(disk_bytes))
    #print('rss_bytes ' + str(rss_bytes))
    if rss_bytes < disk_bytes:
        raise Exception('RSS less than sum of file sizes')

    eff = round(rss_bytes / disk_bytes, 2)
    print( str(factor)
            + ' ' + str(disk_bytes)
            + ' ' + str(rss_bytes)
            + ' ' + str(eff) )

# start here
print('factor disk_bytes rss_bytes efficiency')
factors = [ i / 10. for i in list(range(11,33,1)) ]
for factor in factors:
    p = launch_mc(factor)
    run_test()
    p.terminate()
    p.kill()
    time.sleep(2) # let kernel cleanup sockets

