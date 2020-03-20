import re
import os

filedir = '/home/hkucs/janus/archive/log'
filenames=os.listdir(filedir)
f=open('aggregateLog','w')
for filename in filenames:
    filepath = filedir+'/'+filename
    for line in open(filepath):
        f.writelines(line)
        f.write('\n')
f.close()


reg = re.compile('^(?P<I>[^ ]*) (?P<doc>[^ ]*) (?P<date>[^ ]*) (?P<time>[^ ]*) (?P<singal>[^ ]*) (?P<on>[^ ]*) (?P<status>[^:]*): (?P<txn>[^ ]*) (?P<my_time>[^ ]*)')
file = open("aggregateLog") 
output = open("output.csv",'w+') 
output.write('starus,txn_id,time\n')
for line in file.readlines():  
    line=line.strip('\n')  
    matchObj = re.match( r'(.*)scheduler.cc(.*?)',line)
    if matchObj:
        regMatch = reg.match(line)
        linebits = regMatch.groupdict()
        #print linebits
        for k, v in linebits.items() :
            if k=='my_time' or k=='status'or k=='txn':
    	        output.write(v+',')
	output.write('\n')
output.close()
file.close()
