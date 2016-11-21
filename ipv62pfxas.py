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
import pickle
import time

def matchIPToPrefixlist(ipin,ases,pfxes,s):
    resas = {}
    respfx = {}
    unannounced = list()
    ip = []
    npfxLow = s.npfxlow
    npfxHigh = s.npfxhigh
    pfxlist = s.pfxlist

    if not isinstance(ipin, list):
        ip.append(ipin)
    else:
        ip = ipin
    j=0
    print(ip,ipin)

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


def ownhelp():
    print("Usage: ", sys.argv[0], "[file with IP addresses] [pfx2as file]")

def readpfxfile(pfxfile):
    time_before = time.time()
    try:
        pklfile = open(pfxfile+".pickle",'rb')
        s = pickle.load(pklfile)
        pklfile.close()
        print("pickle loaded after: " , str(time.time()-time_before) )
        return s;
    except FileNotFoundError as e:
        print("FileNotFoundError :", e, "reading from raw data and creating pickle")
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
        print("Building numpy array from prefixes");
        npfxLow = np.empty(0)
        npfxHigh = np.empty(0)

        for p in pfxlist:
            thisnet = ipaddress.IPv6Network(p[0]+"/"+str(p[1]))
            npfxLow  = np.append(npfxLow, int(thisnet[0]))
            npfxHigh = np.append(npfxHigh, int(thisnet[thisnet.num_addresses-1]))

        ## benchmark: reading takes ~5 seconds
        s = Statevars(npfxLow, npfxHigh, pfxlist)
        print("pfxes read after: " + str(time.time()-time_before))
        pklfile=open(pfxfile+".pickle",'wb')
        # py3 automatically uses cpickle
        pickle.dump(s,pklfile)
        print("pickle dumped after: " + str(time.time()-time_before))
        pklfile.close()

        return s


def ip2pfxas(ips,s):
    ases = list()
    pfxes = list()
    resas, respfx, unannounced = matchIPToPrefixlist(ips,ases,pfxes,s);
    print(ips, ases, pfxes, resas, respfx, unannounced)

class Statevars:
    def __init__ (self,npfxlow, npfxhigh, pfxlist):
        self.npfxlow = npfxlow
        self.npfxhigh = npfxhigh
        self.pfxlist = pfxlist

def testmoas(s):
    print(ip2pfxas('2803:1a00:110c::',s))

def main(argv):

    if len(sys.argv) is not 3:
        print("Wrong number of arguments!")
        ownhelp()
        sys.exit(1)

    filename=sys.argv[1];
    pfxfile = sys.argv[2] #'ip2as/routeviews-rv6-20150906-1200.pfx2as'

    s = readpfxfile(pfxfile)
    ips = list();
    ases = list();
    pfxes = list();

    ### Goal memory-efficient readline
    def ipReadline(i):

        with open(filename) as fh:
            for line in fh:
                    ips.append(line.strip())

        return matchIPToPrefixlist(ips,ases,pfxes,s)
    ###

    fh2 = open(filename+".aspfx.csv",'w');
    print("Reading IPs from file ... ");
    ipReadline(0)
    print("Done reading IPs from file ... ");
    for i in np.arange(len(ips)):
        fh2.write(ips[i] + "," + ases[i] + "," + pfxes[i]+ "\n");
    fh2.close()

if __name__ == "__main__":
    main(sys.argv)
