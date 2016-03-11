#!/usrbin/python3
import IP2Location;
import csv;
import numpy as np;
import sys

IP2LocObj = IP2Location.IP2Location();
IP2LocObj.open(sys.argv[2]);
file=sys.argv[1]
data=np.genfromtxt(file, delimiter=',',dtype='str');#,max_rows=10000);

f = open(file+'.ipcc', 'w')
f2 = open(file+'.cc-count', 'w')
count=0;
d = { '0.0.0.0' : -1 }
for x in np.nditer(data):
    rec = IP2LocObj.get_all(str(x));
    count = count+1;
    if(count%1000000 == 0):
            print ("Done with " + str(count) + " lines.")
            #print (rec.ip + "," + rec.country_short);
    f.write(str(x) + "," + rec.country_short + "\n");
    if(rec.country_short in d):
        d[rec.country_short] = d[rec.country_short]+1;
    else:
        d[rec.country_short]=1
f.close()
del d['0.0.0.0']
print("Located " + str(len(data)) + " IPs in " + str(len(d)) + " countries:")
for k, v in d.items():
    print(str(k) + "," + str(v));
    f2.write(str(k) + "," + str(v) + "\n")
