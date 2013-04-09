#!/usr/bin/python
import os, sys, traceback, redis, pickle
from multiprocessing import Pool

def printQueue(q):
    print "%s:" % q, r_server.lrange(q, 0 , -1)

def printSet(q):
    print "%s:" % q, r_server.zrange(q, 0 , -1)

if __name__ == '__main__':
    
    r_server = redis.Redis("localhost")
    printQueue("Job_Queue")
    printQueue("EC2_Queue")
    printQueue("Local_Queue")
    printQueue("Granted_Queue")
    printSet("Running_Queue")
    printQueue("Result_Queue")

    remoteJobs = r_server.lrange("RemoteJobs", 0 , -1)

    print "----"
    print repr(remoteJobs)
    print "----"

    for j in remoteJobs:
        printQueue(j)
        printQueue("%s_Queue" % j)

    printQueue("LocalJobs")
    
    """
    lst = ["62460e7f-5680-5fe6-aa7d-d92aa87614b2", 
           "00f25b79-5a7a-525d-8138-c1bae17fa03b", 
           "c1df9-0e72-50d5-b8d1-7841c19af0bb",
           "669b6378-f11d-51e4-ab30-0dd402533ecb",
           "6bc177cc-5e1e-5ecf-aaf4-5b1c836d82b1",
           "3c8282a7-115a-5c3d-bc58-4cd9cab458f2"]
    for key in lst:
        try:
            groupDict = pickle.loads(r_server.get(key))
            print "%s GroupDict:s" % key
            for key, val in groupDict.iteritems():
                print key, val
        except:
            pass
    """
