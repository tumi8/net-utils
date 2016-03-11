#!/usr/bin/env python3

import ipaddress
import re
import pdb
import sys
import csv
import io
import pprint
import numpy as np
import datetime
import os
import subprocess
import threading



def matchIPToPrefixlist(ip,ases,pfxes):
    resas = {}
    respfx = {}
    unannounced = list()
    j=0

    for i in ip:
        # IPv6 address as integer, to compare with numpy
        thishost = int(ipaddress.IPv6Address(i))

        resg = np.greater_equal(thishost, npfxLow)
        resl = np.less_equal(thishost,npfxHigh)

        match = resg * resl

        matchindex = np.argwhere(match==True)
        # This can return multiple matches!
        # IPs can be in multiple announced prefixes (sub delegations)
        # In our case we are only focussing on the longest prefix, so we need to find it:

        matchpfx = []
        matchsubnets = []

        # First: get all matching prefixes
        for m in matchindex:
            matchpfx.append(pfxlist[m[0]])
            # Extract subnet mask to compare it.
            matchsubnets.append(pfxlist[m[0]][1])

        # This is the ID of the longest prefix in matchpfx
        if len(matchsubnets) == 0:
            unannounced.append(i);
            pfxes.append("-");
            ases.append("-");
            continue
        else:
            longestprefix = matchsubnets.index(max(matchsubnets))

        # Retrieve the longest prefix
        realmatch = matchpfx[longestprefix]
        net = realmatch[0] + "/" + str(realmatch[1])
        netas = realmatch[2]
        pfxes.append(net);
        ases.append(netas);
        j=j+1;
        try:
            respfx[net] += 1
        except KeyError:
            respfx[net] = 1

        try:
            resas[netas] += 1
        except KeyError:
            resas[netas] = 1

    return (resas,respfx,unannounced)



filename=sys.argv[1];

# TODO: CHANGE HERE
pfxfile = 'ip2as/routeviews-rv6-20150906-1200.pfx2as'
pfxlist = []

with open(pfxfile, 'r') as pf:
    readcsv = csv.reader(pf, delimiter='\t')
    for row in readcsv:
        pfxlist.append([row[0], int(row[1]), row[2]])
pf.close()

pfxdate = re.findall('\d{8}', pfxfile)
print("Using Caida Prefix2AS mapping from:", pfxdate)

# Total ASes, total pfx:
pfxperAS = {}
subnetsizes = [0]*129
for pfx in pfxlist:
    try:
        pfxperAS[pfx[2]] =  pfxperAS[pfx[2]] + 1
    except KeyError:
        pfxperAS[pfx[2]] = 1
    try:
        subnetsizes[pfx[1]] = subnetsizes[pfx[1]] + 1
    except KeyError:
        subnetsizes[pfx[1]] = 1

totalASes = len(pfxperAS)
totalPrefixes = len(pfxlist)
print("Total ASes:    ", totalASes)
print("Total Prefixes:", totalPrefixes)





## Turn prefix list into numpy array of lowest and highest address

npfxLow = np.empty(0)
npfxHigh = np.empty(0)

for p in pfxlist:
    thisnet = ipaddress.IPv6Network(p[0]+"/"+str(p[1]))

    npfxLow  = np.append(npfxLow, int(thisnet[0]))
    npfxHigh = np.append(npfxHigh, int(thisnet[thisnet.num_addresses-1]))

## benchmark: reading takes ~5 seconds


ips = list();
ases=list();
pfxes=list();

### Goal memory-efficient readline
def ipReadline(i):

    with open(filename) as fh:
        for line in fh:
                ips.append(line.strip())

    return matchIPToPrefixlist(ips,ases,pfxes)
###

fh2 =open(filename+".aspfx.csv",'w');

ipReadline(0)
for i in np.arange(len(ips)):
    fh2.write(ips[i] + "," + ases[i] + "," + pfxes[i]+ "\n");
fh.close()
