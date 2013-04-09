#!/usr/bin/python
import sys, os, argparse, textwrap, yaml, traceback
# Used for JobThread which launches a job in a thread
from threading import Thread
# signals used to exit child process cleanly
from signal import signal, SIGTERM

from multiprocessing import Process

# NOTE: this is included here to save an additional file upload when used via RemoteJob
# http://stackoverflow.com/questions/107705/python-output-buffering
class UnbufferedOutput:
    def __init__(self, stream):
        self.stream = stream
    def write(self, data):
        self.stream.write(data)
        self.stream.flush()
    def __getattr__(self, attr):
        return getattr(self.stream, attr)

def listDict2str(lst):
    """ Converts a list of one-entry dictionaries into a comma seperated string """
    s = ""
    for i in lst:
        k, v = i.items()[0]
        s = s + "%s, " % str(k)
    return s[:-2]

def list2str(lst):
    """ Converts a list into a comma seperated string """
    s = ""
    for i in lst:
        s = s + "%s, " % str(i)
    return s[:-2]

# NOTE: this is included here to save an additional file upload when use via RemoteJob
# YAMLload() and YAMLwrite() stolen from YAML.py
# wraps yaml module for easy of use with dictionaries and files
def YAMLload(filePath):
    """ Borrowed from Brian Clowers -- used to load yaml configuration file. """
    try:
        with open(filePath, 'r') as f:
            return yaml.load(f)
    except:
        sys.exit("Error loading YAML configuration file at '%s'." % filePath)

def YAMLwrite(data, filePath):
    try:
        with open(filePath, 'w') as f:
            yaml.dump(data, f, default_flow_style=False)
    except:
        sys.exit("Error writing YAML configuration file at '%s'." % filePath)

class Job(Process):
    def __init__(self, configFile=None, configDict=None, logFile='job.log', autoFunc=None):

        super(Job, self).__init__() # to implement threading

        # setup job using yaml
        if configFile:
            self.yaml = YAMLload(configFile)
            self.workingDir, self.configFile = os.path.split(configFile)
            # assume the job needs to run in the same location as the configFile
        elif configDict:
            self.yaml = configDict
            self.workingDir, self.configFile = os.path.split(self.yaml['Filename'])
        else:
            sys.exit("Either configFile or configDict karg needs to be used.")

        self.workingDir = os.path.abspath(self.workingDir)
        self.logFile = logFile

    def importModules(self, s):
        try:
            for name, path in self.yaml['Modules'].items():
                if name in globals():
                    reload( sys.modules[name] )
                else:
                    filepath, filename = os.path.split(path)
                    path = os.path.splitext(filename)[0]
                    filepath = os.path.expanduser(filepath)
                    if filepath not in sys.path:
                        sys.path.append(filepath)
                    globals()[name] = __import__(path, globals(), locals(), [name], -1)
        except:
            s['status'].append('Exception')
            YAMLwrite(s, '.jobstatus')
            print "Error loading modules. Check that file paths are correct."
            raise #reraise

    def run(self):
        
        try: # may not be necssary TODO
            os.chdir(self.workingDir) # change current working directory
        except:
            pass
        sys.path.append(self.workingDir) # append this new location to the current searched paths

        sys.stdout = open(self.logFile, 'w')
        # this assures all Job calls get log files that are unbuffered.
        sys.stdout = UnbufferedOutput( sys.stdout )
        sys.stderr = sys.stdout


        s = {} # doesn't want to read the yaml with out being wrapped first by a dictionary
        s['status'] = []
        self.importModules(s)

        # consider deleting .jobstatus before messing with it
        try:
            os.remove('.jobstatus')
        except:
            pass

        for funcDict in self.yaml['Order']:

            s['status'].append("%s.%s" % (funcDict['Module'], funcDict['Function']) ) # add current place to array
            YAMLwrite(s, '.jobstatus')
            try:
                # temporarily redirect io if necesary
                if 'Log' in funcDict:
                    logFile = funcDict['Log']
                    _stdout = sys.stdout
                    _stderr = sys.stderr
                    # NOTE: this might need to be append but untested thus far
                    sys.stdout = UnbufferedOutput( open(logFile, 'w') ) 
                    sys.stderr = sys.stdout

                funcName = funcDict['Function']
                modName = funcDict['Module']
                
                # convert modName into object and then get function from object
                func = getattr( eval(modName) , funcName)
                
                if 'Inputs' in funcDict:
                    for varDict in funcDict['Inputs']: # iterating a list
                        k, v = varDict.items()[0]
                        if k in locals():
                            if v: #make sure v is not None (since this is reassignment)
                                try:
                                    ev = eval(v)
                                    if type(ev) is list:
                                        exec( "%s=%s" % (k, ev) )
                                    else:
                                        exec( "%s='%s'" % (k, ev) )
                                except:
                                    exec( "%s=%s" % (k, repr(v)) )
                        else:
                            try:
                                ev = eval(v)
                                if type(ev) is list:
                                    exec( "%s=%s" % (k, ev) )
                                else:
                                    exec( "%s='%s'" % (k, ev) )
                            except:
                                exec( "%s=%s" % (k, repr(v)) )

                    inputs = listDict2str(funcDict['Inputs'])
                    funcStr = "func(%s)" % inputs
                else:
                    funcStr = "func()"
                
                if 'Outputs' in funcDict:
                    outputs = list2str(funcDict['Outputs'])
                    funcStr = "%s = %s" % (outputs, funcStr) # this may need a repr... for multivariable output

                # run function
                exec(funcStr)
                
                # undo any redirect
                if 'Log' in funcDict:
                    sys.stdout = _stdout
                    sys.stderr = _stderr
                    del _stdout
                    del _stderr
            except:
                s['status'].append('Exception')
                YAMLwrite(s, '.jobstatus')
                traceback.print_exc()
                raise #re-raise the issue (this will not be seen by RemoteConnection)

        s['status'].append('Done')
        YAMLwrite(s, '.jobstatus')
        return 0

    def isFinished(self):
        jobStatFile = '.jobstatus'
        # append assumed path
        jobStatFile = os.path.join(self.workingDir,jobStatFile)
        print jobStatFile
        if os.path.exists(jobStatFile):
            d = YAMLload(jobStatFile)
            if d['status'][-1] == 'Done':
                return 1 # Finished with no error
            elif d['status'][-1] == 'Exception':
                return 2 # Finished with exception
        # else the remote job is still spinning
        return 0

if __name__ == "__main__":
    desc = '''\
           Command line wrapper of Job class using argparse.
           Job Class framework works in tandem with job specific yaml file.'''
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,
                                description=textwrap.dedent(desc))
    parser.add_argument('yamlConfig', type=str, help="Path to yaml config.")
    parser.add_argument('-log', type=str, default='job.log', help="Path to yaml config.")
    #parser.add_argument('-autoFunc', type=str, default=None, help="Path to yaml config.")
    args = parser.parse_args()
    
    #j = Job(args.yamlConfig, logFile=args.log, autoFunc=args.autoFunc).run()
    j = Job(args.yamlConfig, logFile=args.log)
    j.start()
