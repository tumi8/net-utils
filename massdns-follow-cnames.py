#!/usr/bin/env python3
# -*- coding:utf-8
# by scheitle@net.in.tum.de

import sys, csv

debug=False
cnames, ins = dict(), dict()
from contextlib import contextmanager
from io import StringIO

@contextmanager
def captured_output():
    """ to capture stdout and stderr for testing purposes"""
    new_out, new_err = StringIO(), StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err

def followdomain(domain, depth, origdomain):
    if depth > 20:
         sys.stderr.write("followdomain: depth exceeded for domain {} from origdomain {} \n".format(domain, origdomain))
         return

    # print all records that we have
    if domain in ins:
        for i in ins[domain]:
            print("{},{}".format(i,origdomain))

    # follow CNAMEs
    if domain in cnames:
        for i in cnames[domain]:
            if(debug):
                print("recursive followdomain for domain {} to cname {}".format(domain, i))
            followdomain(i, depth+1, origdomain)

    return

def usage():
     sys.stderr.write('./script domain-list massdns-output \nFollows CNAMEs through to a record, outputs to STDOUT. \n')
     sys.stderr.write("argv:" + str(sys.argv) + "\n")
     return

def massdns2dicts(massdnslist):
    # example structure
    # stresslesstips.org.au.  14400   IN  A   150.107.72.66
    # tips.org.au.    3600    IN  CNAME   prod-lb.hmri.org.au.
    cnames.clear()
    ins.clear()
    #datareader = csv.reader(massdnslist, delimiter=' ', skipinitialspace=True )
    #datareader = csv.reader(massdnslist, delimiter=' ', skipinitialspace=True )
    rows = (line.split() for line in massdnslist)
    for row in rows:
        try:
            # process CNAMEs into dict
            if(row[3] == "CNAME"):
                if not row[4].endswith("."):
                    sys.stderr.write("CNAME not ending with . : " + row[4] + "\n")
                    continue

                if row[0] in cnames:
                    cnames[row[0]].update([row[4]])
                    if(debug):
                        print("Adding CNAME {} to domain {}".format(row[4], row[0]))
                else:
                    cnames[row[0]] = set([row[4]])
                    if(debug):
                        print("Creating CNAME {} for domain {}".format(row[4], row[0]))

            # process records into dict
            elif row[3] == "A" or row[3] == "AAAA":
                if row[0] in ins:
                    ins[row[0]].update([row[4]])
                else:
                    ins[row[0]] = set([row[4]]);

        # e.g., empty lines throw a IndexError
        except IndexError:
            sys.stderr.write("IndexError: " + str(row)+"\n")
            continue

    if len(cnames) == 0  :
        sys.stderr.write("cnames dict empty! check input files: " + str(argv) + "\n")
    if len(ins) == 0 :
        sys.stderr.write("in rr dict empty! check input files! " + str(argv) + "\n")


def loopdomainlists(domainlist):
    # first arg is domain list
    for line in domainlist:
        # skip empty lines
        if(len(line)<2):
            continue

        line = line.rstrip('\n')
        line = line.rstrip('.')
        line += '.'
        line = line.lower()

        followdomain(line, 0, line)


def test(domainlist, massdnslist, expstdout, expstderr, testid):
    with captured_output() as (out, err):
        massdns2dicts(massdnslist)
        loopdomainlists(domainlist)

    output = out.getvalue().strip()
    outputerr = err.getvalue().strip()
    if output == expstdout:
        if outputerr == expstderr:
            sys.stderr.write('Test {} successful!\n'.format(testid))
        else:
            sys.stderr.write("stderr wrong: stdout: \"" + output + "\" err: \"" + outputerr + "\"\n")
    else:
        sys.stderr.write("stdout wrong: exp: \"" + expstdout + "\" stdout: \"" + output + "\" err: \"" + outputerr + "\"\n")


def runtest():
    # test1
    domainlist = ["fingertips.org.au", "mobiletips.org.au", "stresslesstips.org.au", "tips.org.au", "hmri.org.au" ]
    massdnslist = [
        "mobiletips.org.au.  1800    IN  A   119.148.66.108",
        "stresslesstips.org.au.  14400   IN  A   150.107.72.66",
        "tips.org.au.    3600    IN  CNAME   prod-lb.hmri.org.au.",
        "pondle.org.au.  3422    IN  CNAME   prod-lb.hmri.org.au.",
        "prod-lb.hmri.org.au.    3423    IN  A   134.148.39.155",
        "pondle.org.au.  3600    IN  CNAME   prod-lb.hmri.org.au.",
        "prod-lb.hmri.org.au.    3600    IN  A   134.148.39.155",
        "tips.org.au.    3600    IN  CNAME   prod-lb.hmri.org.au.",
        "prod-lb.hmri.org.au.    3598    IN  A   134.148.39.155",
    ]
    expstdout = "119.148.66.108,mobiletips.org.au.\n150.107.72.66,stresslesstips.org.au.\n134.148.39.155,tips.org.au."
    expstderr = ""
    test(domainlist, massdnslist, expstdout, expstderr, 1)

    # test2
    domainlist = ["test.de." ]
    massdnslist = [
        "test.de.    3600    IN  CNAME   test.de.",
    ]
    expstdout = ""
    expstderr = "dicts empty! check input files!\nfollowdomain: depth exceeded for domain test.de. from origdomain test.de."
    test(domainlist, massdnslist, expstdout, expstderr, 2)

    # test3
    domainlist = ["testing.de.", "test2.de."]
    # mixes tabs and spaces!
    massdnslist = [
        "testing.de.     3600   IN  CNAME   testcname.de.",
        "testcname.de. 3600   IN    CNAME   testcname2.de.",
        "testcname2.de. 3600 IN A 1.2.3.4"
    ]
    expstdout="1.2.3.4,testing.de."
    expstderr=""
    test(domainlist, massdnslist, expstdout, expstderr, 3)

    # test4
    domainlist = ["testing.de.", "testcname2.de."]
    massdnslist = [
        "testing.de.     3600   IN  CNAME   testcname.de.",
        "testcname.de.   3600 IN  CNAME     testcname2.de.",
        "testcname2.de.   3600 IN  A     1.2.3.4"
    ]
    expstdout="1.2.3.4,testing.de.\n1.2.3.4,testcname2.de."
    expstderr=""
    test(domainlist, massdnslist, expstdout, expstderr, 4)

    # test5
    domainlist = ["testing.de.", "testcname2.de."]
    massdnslist = [
        "testing.de.     3600   IN  CNAME   testcname.de.",
        "testing.de.     3600   IN  A   4.5.6.7",
        "testcname.de.   3600 IN  CNAME     testcname2.de.",
        "testcname2.de.   3600 IN  A     1.2.3.4"
    ]
    expstdout="4.5.6.7,testing.de.\n1.2.3.4,testing.de.\n1.2.3.4,testcname2.de."
    expstderr=""
    test(domainlist, massdnslist, expstdout, expstderr, 5)

    # test6
    cnames = dict()
    ins = dict()
    domainlist = ["testing.de."]
    massdnslist = [
        "testing.de.     3600   IN  CNAME   testcname.de",
        "testing.de.     3600   IN  A   4.5.6.7",
        "testcname.de.   3600 IN  CNAME     testcname2.de.",
        "testcname2.de.   3600 IN  A     1.2.3.4"
    ]
    expstdout="4.5.6.7,testing.de."
    expstderr="CNAME not ending with . : testcname.de"
    test(domainlist, massdnslist, expstdout, expstderr, 6)

    # test7 - domainlist without final . , records with final .
    cnames = dict()
    ins = dict()
    domainlist = ["testing.de"]
    massdnslist = [
        "testing.de.     3600   IN  A   4.5.6.7",
    ]
    expstdout="4.5.6.7,testing.de."
    expstderr="dicts empty! check input files!"
    test(domainlist, massdnslist, expstdout, expstderr, 7)

    # test8 - mixed A and AAAA
    # this is annoying, this test will fail like every 2nd run due to the random order when outputting set
    cnames = dict()
    ins = dict()
    domainlist = ["testing.de"]
    massdnslist = [
        "testing.de.     3600   IN  A   4.5.6.7",
        "testing.de.     3600   IN  AAAA   8::9",
    ]
    expstdout="8::9,testing.de.\n4.5.6.7,testing.de."
    expstderr="dicts empty! check input files!"
    test(domainlist, massdnslist, expstdout, expstderr, 8)

    # test9 - mixed caps
    cnames = dict()
    ins = dict()
    domainlist = ["TEST.de"]
    massdnslist = [
        "test.de.     3600   IN  A   4.5.6.7",
    ]
    expstdout="4.5.6.7,test.de."
    expstderr="dicts empty! check input files!"
    test(domainlist, massdnslist, expstdout, expstderr, 9)


def main(argv):
    if sys.argv[1] == "test":
        runtest()
        sys.exit(0)

    if len(argv) != 3:
         sys.stderr.write('Invalid Argument Count!\n')
         usage()
         sys.exit(1)

    if(debug):
        sys.stderr.write(str(argv)+"\n")
    with open(sys.argv[2]) as massdnslist:
        massdns2dicts(massdnslist)

    with open(sys.argv[1]) as domainlist:
        loopdomainlists(domainlist)


if __name__ == "__main__":
    main(sys.argv)
