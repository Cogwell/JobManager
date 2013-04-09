import sys, os, time 
from Daemon import Daemon

class PipedDaemon(Daemon):
    """
    A generic daemon class.

    Usage: subclass the Daemon class and override the run() method.
    """

    def __init__(self, pidName, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null', pipeFile=None):
            super(PipedDaemon, self).__init__(pidName, stdin, stdout, stderr)

            if pipeFile:
                self.pipeFile = pipeFile
                if not os.path.exists(self.pipeFile):
                    os.mkfifo(self.pipeFile, 0777) # currently no file cleanup
                
                self.pipeReader = os.open(self.pipeFile, os.O_RDONLY|os.O_NONBLOCK) 

    def queue(self, data, delimeter='\n'):
        if self.pipeFile: # some kind of validation
            with open(self.pipeFile, 'w+') as writer:
                writer.write(data + delimeter)
            return 0
        return 1

    def dequeue(self): # if other delimeters are used we are in trouble
        if self.pipeFile: # some kind of validation
            while True:
                # there needs to be some kind of try catch here in case data is writing
                try:
                    with os.fdopen(self.pipeReader, "r") as fo:
                        data = fo.readlines()
                    for i, d in enumerate(data):
                        data[i] = d.strip()
                        
                    return data # this won't work if delimeter changes.
                except OSError: # error 11
                    time.sleep(0.1) # tenth of a second
                    continue
        return None
