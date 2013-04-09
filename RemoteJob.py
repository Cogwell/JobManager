import yaml, YAML, os, argparse, textwrap
from RemoteConnection import RemoteConnection
#from threading import Thread
from multiprocessing import Process

class RemoteJob(Process):
    def __init__(self, configFile=None, configDict=None, logFile='job.log'):
        super(RemoteJob, self).__init__() # to implement threading
        
        if configFile:
            self.rc = RemoteConnection(configFile=configFile)
            self.yaml = YAML.load(configFile) 
            # use path of config for local WD less otherwise specified
            self.localWD, self.configFile = os.path.split(configFile) 
        elif configDict:
            self.rc = RemoteConnection(configDict=configDict)
            self.yaml = configDict 
            self.localWD, self.configFile = os.path.split(self.yaml['Filename'])
        else:
            sys.exit("Either configFile or configDict karg needs to be used.")
        
        if 'LocalWorkingDir' in self.yaml: 
            self.localWD = self._expandTilde( self.yaml['LocalWorkingDir'] )
            
        if 'LocalOutputDir' in self.yaml: 
            d = self.yaml['LocalOutputDir'] 
            if d[0] == '.':
                self.localOD = os.path.join( self.localWD, d[2:])
            else:
                self.localOD = self._expandTilde(d)
        else:
            self.localOD = self.localWD

        if not os.path.exists(self.localOD): os.mkdir(self.localOD)
        
        self.remoteWD = self._expandTilde( self.yaml['RemoteWorkingDir'] )
        if 'RemoteOutputDir' in self.yaml:
            self.remoteOD = self._expandTilde( self.yaml['RemoteOutputDir'] )
        else: # output dir = working dir
           self.remoteOD = self.remoteWD 

        self.logFile = logFile
    
    def _expandTilde(self, d):
        if d[0] == '~': # manually expand tilde (needed for paramiko)
            if len(d) > 2:
                d = d[2:]
            else:
                d = ""
            d = '/home/%s/%s' % (self.yaml['Username'], d)
        return d


    def run(self):
        # first put the job class on machine
        self.rc.put('Job.py', self.remoteWD)
        
        # NOTE: Job.py appears to be propogating its chdir() to the workers
        cwd = os.getcwd()
        os.chdir( self.localWD ) 

        # upload modules
        for name, path in self.yaml['Modules'].items():
            path = os.path.expanduser(path)
            self.rc.put(path, self.remoteWD)
        
        # upload files indicated for such
        if 'Uploads' in self.yaml:
            for f in self.yaml['Uploads']:
                f = os.path.expanduser(f)
                self.rc.put(f, self.remoteWD)
        
        remoteConfig = self.rc.put(self.configFile, self.remoteWD)
        
        os.chdir(cwd) # revert to the original cwd

        null = '/dev/null'
        cmd = 'python Job.py %s' % remoteConfig
        cmd = 'nohup %s &> %s < %s &' % (cmd, self.logFile, null) 
        self.rc.chdir(self.remoteWD)
        return self.rc.execute(cmd)

    def isFinished(self):
        jobStatFile = os.path.join(self.remoteWD, '.jobstatus')
        if self.rc.exists(jobStatFile):
            ret_code, stdout, stderr = self.rc.execute('cat %s' % jobStatFile)
            d = yaml.load(stdout)
            if d['status'][-1] == 'Done':
                return 1 # Finished with no error
            elif d['status'][-1] == 'Exception':
                return 2 # Finished with exception
        # else the remote job is still spinning
        return 0

    def getReturnData(self):
        if 'Downloads' in self.yaml:
            cwd = os.getcwd()
            os.chdir(self.localOD)
            for f in self.yaml['Downloads']:
                f = os.path.join(self.remoteOD, f)
                self.rc.get(f, '.')
            os.chdir(cwd)
        #else tell them they asked for nothing!
 
# NOTE: this wrapper is out of date
if __name__ == "__main__":
    desc = '''\
           Remote Job Class.'''
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,
                                description=textwrap.dedent(desc))
    parser.add_argument('-c', dest='config', type=str, default='./examples/RemoteJob/remoteJob.yaml', help="Path to yaml config.")
    args = parser.parse_args()
    
    rj = RemoteJob(args.config)
    rj.runJob()
