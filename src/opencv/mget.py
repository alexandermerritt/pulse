#! /usr/bin/env python3
import sys
import os
import subprocess as sp

iters = 512
obj_p = 6
obj_n = 32
rogue_p = 23
rogue_low_p = 10

times = []

r_p = rogue_p
while (r_p >= rogue_low_p):
    rogue_n = (1<<(rogue_p-r_p))
    total = (obj_n+rogue_n)
    args = ['./mget', 'run', str(obj_p), str(r_p), str(rogue_n),
            str(total), str(iters)]
    print('# ' + ' '.join(args))
    out = sp.check_output(args, universal_newlines=True)
    t = []
    for line in out.split('\n'):
        if line == '' or line[0] == '#':
            continue
        t.append(int(line.strip()))
    times.append(t)
    r_p -= 1

l = list(range(rogue_low_p, rogue_p+1))
l.reverse()
header = [ str(i) for i in l ]
print(' '.join(header))
for i in range(0, iters):
    for order in l:
        print(str(times[rogue_p-order][i]), end=' ')
    print('')

