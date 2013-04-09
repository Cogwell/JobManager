#!/usr/bin/python
import os, sys, traceback, redis

if __name__ == '__main__':
    
    r_server = redis.Redis("localhost")
    r_server.delete("EC2_Queue")
    r_server.delete("Remote_Queue")
    r_server.delete("Local_Queue")
    r_server.delete("Result_Queue")
    r_server.delete("Running_Queue")
    r_server.delete("Granted_Queue")
    r_server.delete("Job_Queue")
    
    remoteJobs = r_server.lrange("RemoteJobs", 0 , -1)

    for j in remoteJobs:
        r_server.delete(j)
        r_server.delete("%s_Queue" % j)
        
    r_server.delete("RemoteJobs")
    r_server.delete("LocalJobs")
    
    r_server.delete("Running_Set")
