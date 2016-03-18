#!/usr/bin/env python3

import ipaddress
import sys


def main():
    ip_fname = sys.argv[1]
    prefix_fname = sys.argv[2]

    prefixes = list()

    fh = open(prefix_fname)
    for line in fh.readlines():
        prefixes.append(line.strip())
    fh.close()

    global fh2
    fh2=open(ip_fname+".aspfx.csv",'w');

    with open(ip_fname) as fh:
        for line in fh.readlines():
            prefix_lookup(line.strip(), prefixes)
    fh2.close


def prefix_lookup(ip, prefixes):
    # first find starting /8 prefix entry, they are sorted numerically -> binary search on /8
    num_prefixes = len(prefixes)
    curr = int(num_prefixes/2)
    step = int(num_prefixes/2)
    correct = -1

    ip_slash8 = int(ip.split(".")[0])

    one = False

    while True:
        pfx_ip = prefixes[curr].split("\t")[0]
        # First byte -> /8
        if int(pfx_ip.split(".")[0]) == ip_slash8:
            if correct == -1 or curr < correct:
                correct = curr
            curr = curr - step
        elif int(pfx_ip.split(".")[0]) > ip_slash8:
            curr = curr - step
        elif int(pfx_ip.split(".")[0]) < ip_slash8:
            curr = curr + step

        curr = min(curr, len(prefixes) - 1)


        if step == 1:
            if one:
                break
            one = True
        else:
            step = int(step/2)

    curr = correct
    candidate = ipaddress.IPv4Network(prefixes[correct].split("\t")[0].split(".")[0] + ".0.0.0/8")

    ipa = ipaddress.IPv4Address(ip)

    while True:
        ip_network, pfx_network, _ = prefixes[curr].split("\t")
        network = ipaddress.IPv4Network(ip_network + "/" + pfx_network)
        # If this network does not overlap with the last candidate network
        if not network.overlaps(candidate):
            break
        # We want the most specific prefix
        elif int(pfx_network) < candidate.prefixlen:
            pass
        elif ipa in network:
            correct = curr
            candidate = ipaddress.IPv4Network(prefixes[correct].split("\t")[0] + "/" + prefixes[correct].split("\t")[1])

        curr += 1


    res = prefixes[correct].split("\t")
    #print(ip + "," + res[0] + "," + res[1] + "," + res[2])
    fh2.write(ip + "," + res[0] + "/" + res[1] + "," + res[2] + "\n")



if __name__ == "__main__":
    main()
