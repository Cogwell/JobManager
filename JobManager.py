#!/usr/bin/python
import os, sys, redis, time, argparse, textwrap, pickle, traceback
# provides easy to use daemonization of process
from Daemon import Daemon
# used to load yaml configs into dictionaries
import YAML
# signals used to exit child process cleanly
from signal import signal, SIGTERM
# multiprocessing gives easy threading
import multiprocessing as MP
# used to create unbuffered output files
from UnbufferedOutput import UnbufferedOutput
# the types of job classes this manager takes care of
from Job import Job
from RemoteJob import RemoteJob
from RemoteConnection import RemoteConnection

from glob import glob

# used for Timer
import threading

from subprocess import Popen

from BotoConnection import BotoConnection 

class JobStarter:

    def __init__(self, yamlConfig, logFile='logs/JobStarter.log'):
        self.logFile = logFile
        self.boto_conn = BotoConnection(yamlConfig)
        self.runParamDict = None
        
        self.r_server = redis.Redis("localhost")

        sys.stdout = UnbufferedOutput( open(self.logFile,'a') )
        sys.stderr = sys.stdout # make it all go to log file

        # set clean-up handler for SIGTERM
        signal(SIGTERM, self.sigterm_handler)
        self.main()

    def main(self):
        print "JobStarter starting up"
        while True:
            rawData = self.r_server.blpop("Granted_Queue")[1] 
            self.runParamDict = pickle.loads(rawData)


            # write the paramFile back to disk (why?)
            #YAML.write(self.runParamDict, self.runParamDict['Filename'])

            try:
                # setup correct job class and start up job
                if self.runParamDict['ClassType'] == 'Local':
                    print "Starting local job thread for '%s'" % self.runParamDict['Filename']
                    j = Job(configDict=self.runParamDict)
                else:
                    print "Starting remote job for '%s'" % self.runParamDict['Filename']
                    j = RemoteJob(configDict=self.runParamDict)
                
                j.start()

                print "Job Started."
            
                pollTime = time.time() + self.runParamDict['PollPeriod'] * 60
                self.r_server.zadd("Running_Queue", rawData, pollTime)
            
            except:
                print "The following job failed:'%s'" % self.runParamDict['Filename']
                print time.asctime(),
                traceback.print_exc()
                print 
                self.r_server.rpush("Result_Queue", rawData)

            # since this blocks, clear after queuing up
            self.runParamDict = None

    def sigterm_handler(self, signum, frame):
        print "JobStarter shutting down"
        # if currently working on instance
        if self.runParamDict != None:
            # push data into waitingQueue and terminate instance
            if self.runParamDict['ClassType'] == 'EC2':
                self.r_server.rpush("Result_Queue", pickle.dumps(self.runParamDict))
        sys.exit()

class ResultFetcher:

    def __init__(self, yamlConfig, logFile='logs/ResultFetcher.log'):
        sys.stdout = UnbufferedOutput( open(logFile,'a') )
        sys.stderr = sys.stdout # make it all go to log file
        self.boto_conn = BotoConnection(yamlConfig)
        self.runParamDict = None

        self.r_server = redis.Redis("localhost")

        # set clean-up handler for SIGTERM
        signal(SIGTERM, self.sigterm_handler)
        self.main()

    def main(self):
        print "ResultFetcher starting up"
        while True:
            t = time.time()
            data = self.r_server.zrangebyscore("Running_Queue", 0, t)
            for d in data:
                self.runParamDict = pickle.loads(d)
                
                if self.runParamDict['ClassType'] == 'Local':
                    j = Job(configDict=self.runParamDict)
                    jobStr = 'local job'
                else:
                    j = RemoteJob(configDict=self.runParamDict)
                    jobStr = 'remote job'
                
                print "Polling %s for '%s'" % (jobStr, self.runParamDict['Filename'])
                
                retCode = j.isFinished()
                if retCode > 0:
                    if retCode == 1: # case: no except
                        print "Job complete for '%s'" % self.runParamDict['Filename']
                        try:
                            j.getReturnData()
                        except AttributeError: # pass if j without getReturnData() e.g. local
                            pass
                    else:
                        print "Exception for '%s'!!!" % self.runParamDict['Filename']
                    self.r_server.rpush("Result_Queue", pickle.dumps(self.runParamDict))
                else:
                    pollTime = time.time() + self.runParamDict['PollPeriod'] * 60
                    self.r_server.zadd("Running_Queue", pickle.dumps(self.runParamDict), pollTime)
                self.runParamDict = None
            self.r_server.zremrangebyscore("Running_Queue", 0, t)
            time.sleep(60) # hard wait (since no blocking method on ordered sets)

    def sigterm_handler(self, signum, frame):
        print "ResultFetcher shutting down"
        if self.runParamDict != None:
            # push data into waitingQueue and terminate instance
            if self.runParamDict['ClassType'] == 'EC2':
                self.r_server.rpush("Result_Queue", pickle.dumps(self.runParamDict))
        # Now unload the Running_Queue
        #while self.r_server.llen("Running_Queue") != 0:
        #    self.instance = pickle.loads( self.r_server.lpop("Running_Queue") )
        #    self.cleanUpInstance()
        # if currently working on instance
        sys.exit()

    """
    def cleanUpInstance(self):
        self.r_server = redis.Redis("localhost")
        self.r_server.rpush("waitingQueue", pickle.dumps(self.runParamDict) )
        # terminate current instance
        if self.runParamDict['ClassType'] == 'EC2':
            self.boto_conn.terminateInstance(self.runParamDict['Hostname'])
    """

class QueueTracker:
    def __init__(self, queues, flags):
        # NOTE: this NEEDS to retains order (which is why dictionary won't work)
        self.queues = queues 
        self.flags = flags 
    def setQueueFlag(self, queueStr, flag):
        # NOTE: need error catching here
        idx = self.queues.index(queueStr)
        self.flags[idx] = flag
    def getQueueList(self):
        strLst = []
        for i, flag in enumerate(self.flags):
            if flag:
                strLst.append(self.queues[i])
        return strLst
    def append(self, queue, flag=False):
        self.queues.append(queue)
        self.flags.append(flag)
    def remove(self, queue):
        i = self.queues.index(queue)
        self.queues.pop(i)
        self.flags.pop(i)

class EC2DelayedQueue(threading.Thread):
    """ this class is used to validate the new ec2 connection before proceeed """
    def __init__(self):
        self.r_server = redis.Redis("localhost")

    def start(self, delay, runParamDict):
        t = threading.Timer(delay, self._validate, [runParamDict]).start()

    def _validate(self, runParamDict):
        try:
            rc = RemoteConnection(configDict=runParamDict, testSSH=True)
            self.r_server.rpush("Granted_Queue", pickle.dumps(runParamDict))
        except:
            self.r_server.rpush("Result_Queue", pickle.dumps(runParamDict))
            self.r_server.rpush("Job_Queue", [runParamDict['Filename']])
        sys.exit(0)

#class JobManager(PipedDaemon):
class JobManager(Daemon):

    def sigterm_handler(self, signum, frame):
        """ catches SIGTERM and performs clean up - kill child procs, etc. """
        self.JobStarter.terminate()
        self.ResultFetcher.terminate()
        print "JobManager - workers killed."
        # need to clean up ResultQueue?
        print "JobManager - stopping."
        sys.exit()

    def __init__(self):
        self.cwd=os.getcwd() # used to restore cwd after daemonizing
        # TODO: check if logs folder exists and create if needed
        log = os.path.join(self.cwd, 'logs/JobManager.log')
        pid = os.path.join(self.cwd, '.pidfile')
        super(JobManager, self).__init__(pidPath=pid, stdout=log, stderr=log)
        self.r_server = redis.Redis("localhost")

    def validateRedis(self):
        if os.path.exists('/usr/bin/redis-server'): # NOTE: won't work for Win
            tries = 0
            while tries < 5:
                try:
                    self.r_server.ping()
                    break
                except redis.exceptions.ConnectionError:
                    # redis is still starting up
                    print "Redis server not responding. JobManager sleeping 30 seconds."
                    tries = tries + 1
                    time.sleep(30)
            if tries == 5:
                sys.exit("Failed to establish a connection with Redis. Please make sure redis-server is starting up.")
        else:
            sys.exit("Redis is not installed! Please install 'redis-server' for your distribution.")

    def run(self):
        """ JobManager - starts worker processes and manages redis data structures """

        sys.stdout = UnbufferedOutput(sys.stdout)
        sys.stderr = sys.stdout

        print "JobManager - starting."
        os.chdir(self.cwd) # restore cwd (this gets changed 
        
        # validate that redis is installed and running
        self.validateRedis()

        # setup ec2 connection (we are assuming ec2 with this JobManager
        boto_conn = BotoConnection(self.yamlConfig) 

        # start up workers
        self.JobStarter = MP.Process(target=JobStarter, args=(self.yamlConfig,))
        self.JobStarter.start()
        self.ResultFetcher = MP.Process(target=ResultFetcher, args=(self.yamlConfig,))
        self.ResultFetcher.start()

        # set clean-up handler for SIGTERM
        signal(SIGTERM, self.sigterm_handler)
        
        # this keeps track of which Queues need to be considered for blocking
        # NOTE: these starting values may not be necessary the only option
        self.queueTracker = QueueTracker( ["Job_Queue", "Result_Queue", "Local_Queue", "EC2_Queue"], 
                                          [True, False, False, False] ) 
        
        # *** MAIN LOOP ***
        while True:
            # multi-queue blocking with priority to left most keys (in list order) -- this list changes throughout execution
            queueName, poppedData = self.r_server.blpop(self.queueTracker.getQueueList())

            if queueName == "Job_Queue":
                # need to add data to tracker but first we parse the poppedData
                runFiles = poppedData[1:-1].replace('\'', '').split(', ')
                print "New list of files to run received: %s" % repr(runFiles)
                
                # add files to holding queues
                for runParamFile in runFiles:
                    try:
                        # http://stackoverflow.com/questions/8930915/python-append-dictionary-to-dictionary
                        runParamDict = dict(**self.yamlConfig)
                        runParamDict.update(YAML.load(runParamFile))
                        runParamDict['Filename'] = runParamFile

                        print "Queuing '%s' of ClassType" % runParamFile,
                        
                        if runParamDict['ClassType'] == 'EC2':
                            print "EC2 into 'EC2_Queue'"
                            self.r_server.rpush("EC2_Queue", pickle.dumps(runParamDict) )
                            if boto_conn.isInstanceAvailable():
                                self.queueTracker.setQueueFlag("EC2_Queue", True)
                        
                        elif runParamDict['ClassType'] == 'Remote':
                            self.r_server.rpush("RemoteJobs", runParamDict['Hostname'])
                            remoteQueue = "%s_Queue" % runParamDict['Hostname']
                            print "Remote into '%s'" % remoteQueue 
                            self.r_server.rpush(remoteQueue, pickle.dumps(runParamDict) )
                            self.queueTracker.append(remoteQueue, flag=True)

                        elif runParamDict['ClassType'] == 'Local':
                            print "Local into 'Local_Queue'"
                            self.r_server.rpush("Local_Queue", pickle.dumps(runParamDict) )
                            if self.r_server.llen("LocalJobs") < self.yamlConfig['LocalMaxJobs']: 
                                self.queueTracker.setQueueFlag("Local_Queue", True)
                    except:
                        print "Error with input file:", runParamFile
                        print "Skipping file."
            
            elif queueName == "EC2_Queue":
                runParamDict = pickle.loads(poppedData)
                
                # create EC2 instance
                instId, instAddr = boto_conn.getInstance(instDict=runParamDict)
                runParamDict['InstanceID'] = instId # new key
                runParamDict['Hostname'] = instAddr # replace old host value

                print "New EC2 job. Instance (%s) starting up at '%s'" % (instId, instAddr)
                dq = EC2DelayedQueue()
                dq.start(120, runParamDict)

                # start blocking on the Result_Queue
                self.queueTracker.setQueueFlag("Result_Queue", True)
                
                # if too many instances, stop blocking
                if not boto_conn.isInstanceAvailable():
                    self.queueTracker.setQueueFlag("EC2_Queue", False)

            elif queueName == "Local_Queue":
                runParamDict = pickle.loads(poppedData)

                print "New local job."
                
                # Use the filename as a unique identifier for job 
                self.r_server.rpush("LocalJobs", runParamDict['Filename'])
                
                # Queue the job up for processing
                self.r_server.rpush("Granted_Queue", pickle.dumps(runParamDict))
                
                # check to see if we DO NOT have room for another local job within constraints
                if self.r_server.llen("LocalJobs") >= self.yamlConfig['LocalMaxJobs']: 
                    self.queueTracker.setQueueFlag("Local_Queue", False)
                
                # start blocking on the Result_Queue
                self.queueTracker.setQueueFlag("Result_Queue", True)
            
            elif queueName == "Result_Queue":
                runParamDict = pickle.loads(poppedData)
                print "Receiving results." #NOTE: need to look at validating results

                if runParamDict['ClassType'] == 'EC2':
                    #NOTE: need to add recycling later
                    boto_conn.terminateInstance(runParamDict['InstanceID'])
                    self.queueTracker.setQueueFlag("EC2_Queue", True)

                elif runParamDict['ClassType'] == 'Remote':
                    remoteQueue = "%s_Queue" % runParamDict['Hostname']
                    cnt = self.r_server.llen(runParamDict['Hostname'])
                    if cnt == 1: # current machine no longer needs to be tracked
                        self.r_server.delete(runParamDict['Hostname'])
                        self.r_server.lrem("RemoteJobs", runParamDict['Hostname'])
                        self.queueTracker.remove(remoteQueue)
                    else:
                        self.r_server.lrem(runParamDict['Hostname'], runParamDict['Filename'])
                        self.queueTracker.setQueueFlag(queueName, True) 

                elif runParamDict['ClassType'] == 'Local':
                    self.r_server.lrem("LocalJobs", runParamDict['Filename'])
                    self.queueTracker.setQueueFlag("Local_Queue", True)
            
            # assuming we have a remote queue created on demand
            else:
                runParamDict = pickle.loads(poppedData)
                # Add to list Hostname the filename of the new job
                self.r_server.rpush(runParamDict['Hostname'], runParamDict['Filename'])
                print "New Remote job from %s" % queueName
                # Queue the job up for processing
                self.r_server.rpush("Granted_Queue", pickle.dumps(runParamDict))
                # check to see if we DO NOT have room for another remote job  on given machine within constraints
                if self.r_server.llen(runParamDict['Hostname']) >= self.yamlConfig['RemoteMaxJobsPerHost']:
                    self.queueTracker.setQueueFlag(queueName, False) # stop watching queue
                # start blocking on the Result_Queue
                self.queueTracker.setQueueFlag("Result_Queue", True)

    def start(self, args):
        # consider making all keys lower case (we do this in the autobhans)
        self.yamlConfig = YAML.load(args.yaml)
        super(JobManager, self).start()

    def stop(self, args):
        super(JobManager, self).stop()

    def status(self, args):
        super(JobManager, self).status()

    def restart(self, args):
        self.start(args)
        self.stop(args)

    def queueYaml(self, args):
        yaml = os.path.abspath(args.yamlFile)
        if os.path.splitext(yaml)[1] == '.yaml':
            self.r_server.rpush("Job_Queue",  [yaml])
        else:
            print "The file being queued does not appear to be a yaml file"
# NOTE: currently assuming only EC2 jobs
    def queueDir(self, args):
        dirPath = os.path.abspath(args.dirPath)
        if os.path.isdir(dirPath):
            data = glob( os.path.join(dirPath, args.ext) )
            self.r_server.rpush("Job_Queue",  data)

if __name__ == "__main__":
    #pipeFile = os.path.join(os.getcwd(), '.jobQueue')
    #daemon = JobManager( "jobManager", stdout=logFile, stderr=logFile, pipeFile=pipeFile)
    daemon = JobManager()

    desc = '''\
           JobManager uses a redis queue and yaml configurations to launch Jobs and RemoteJobs.'''
    parser = argparse.ArgumentParser( formatter_class=argparse.RawTextHelpFormatter,
                                      description=textwrap.dedent(desc) )
    subparser = parser.add_subparsers() #(help='sub-command help')

    start_parser = subparser.add_parser('start', help="Start the JobManager daemon")
    default_yaml_config = 'config/JobManager.yaml'
    start_parser.add_argument('-yaml', type=str, default=default_yaml_config, help="YAML file used to configure manager.")
    start_parser.set_defaults(func=daemon.start)

    stop_parser = subparser.add_parser('stop', help="Stop the JobManager daemon")
    stop_parser.set_defaults(func=daemon.stop)

    status_parser = subparser.add_parser('status', help="Determine if JobManager daemon is running")
    status_parser.set_defaults(func=daemon.status)

    restart_parser = subparser.add_parser('restart', help="Restart the JobManager daemon")
    restart_parser.add_argument('-yaml', type=str, default=default_yaml_config, help="YAML file used to configure manager.")
    restart_parser.set_defaults(func=daemon.restart)

    queue_parser = subparser.add_parser('queue', help="Used to queue up yaml file or dir path for processing")
    queue_subparser = queue_parser.add_subparsers()
    
    queue_yaml = queue_subparser.add_parser('yaml', help="Used to queue up yaml file for processing")
    queue_yaml.add_argument('yamlFile', type=str, help="Path to yaml file")
    queue_yaml.set_defaults(func=daemon.queueYaml)

    queue_dir = queue_subparser.add_parser('dir', help="Used to queue up dir path for processing")
    queue_dir.add_argument('dirPath', type=str, help="Path to dir with yaml files")
    queue_dir.add_argument('-ext', type=str, default='*_Filter.yaml', help="Extension for files being added.")
    queue_dir.set_defaults(func=daemon.queueDir)

    args = parser.parse_args()
    args.func(args)
