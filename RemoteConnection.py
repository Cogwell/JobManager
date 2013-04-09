import sys, os, paramiko, time, YAML, socket
from getpass import getpass

class RemoteConnection:
    """ We assume the connection will be held open for the duration of the object """
    def __init__(self, configFile=None, configDict=None, username=None, hostname=None, key=None, port=22, onDemand=True, cwd=None, testSSH=False):
        if configFile:
            yamlDict = YAML.load(configFile)
            self.__dictInit(yamlDict)
        elif configDict:
            self.__dictInit(configDict)
        else:
            self.__varInit(username, hostname, key=key, port=port, onDemand=onDemand)

        self.cwd = cwd

        # test connection
        if testSSH:
            self.open()
            self.close()

        #self.ssh=None
        # set ssh based on connection type
        """
        if self.onDemand:
            self.ssh=None
        else:
            self.open()
        """

    def __dictInit(self, yamlDict, port=22, onDemand=True):
        self.username = yamlDict['Username']
        self.hostname = yamlDict['Hostname']
        if not self.hostname:
            sys.exit("No hostname provided.")
        
        self.port = port # set default port
        if 'Port' in yamlDict: # if there is a specified port, use it
            self.port = yamlDict['Port']

        self.key = None
        if 'Key' in yamlDict: # if there is a key, first do some kind of conversion
            key = os.path.expanduser( yamlDict['Key'] ) 
            if os.path.exists(key):
                self.key = key
            else:
                sys.exit("Key specified which does not exists. Check '%s'." % yamlDict['Key'])
        else:
            self.password = None

        self.onDemand = onDemand
        if 'onDemand' in yamlDict:
            self.onDemand = yamlDict['onDemand']

    def __varInit(self, username, hostname,  key=None, port=22, onDemand=True):
        if not username:
            sys.exit("No username provided.")
        self.username = username
        self.hostname = hostname
        if not self.hostname:
            sys.exit("No hostname provided.")
        self.port = port
        
        self.key = None
        if key: # if there is a key, first do some kind of conversion
            self.key = os.path.expanduser(key)
        else:
            self.password = None
        
        self.onDemand = onDemand
    
    def __enter__(self):
        # open persistent connection
        self.onDemand = False
        self.open()
        return self
    
    def __exit__(self, type, value, traceback):
        self.close()
        self.onDemand = True # this isn't the best way to do this

    def open(self):
        self.__createSSHConnection()

    def __createSSHConnection(self, tries=10):
        """
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        """

        if not self.key and not self.password: 
            self.password = getpass("Please enter password for %s@%s: " % (self.username, self.hostname) )
        
        attempts = 0
        while attempts < tries:
            try:
                #print (self.hostname,self.port)
                self.transport = paramiko.Transport((self.hostname,self.port))
                #self.transport.start_client()
                if hasattr(self, 'key'):
                    pkey = paramiko.RSAKey.from_private_key_file(self.key)
                    #self.transport.auth_publickey(self.username, pkey)
                    self.transport.connect(username=self.username, pkey=pkey)
                    #client.connect(self.hostname, self.port, self.username, pkey=key)
                else:
                    self.transport.connect(username=self.username, password=self.password)
                    #client.connect(self.hostname, self.port, self.username, self.password)
                return
            except paramiko.BadAuthenticationType:
                print "Authentication Error. Please retry password."
                self.password = getpass("Please enter password for %s@%s: " % (self.username, self.hostname) )
            except paramiko.SSHException, sshFail:
                if "Connection timed out" in str(sshFail):
                    attempts = attempts + 1
                    time.sleep(15)
                    continue
                else:
                    raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    def close(self):
        self.transport.close()
        """
        if self.ssh:
            self.ssh.close()
            self.ssh = None
        """

    def chdir(self, d):
        self.cwd = d

    def __execute(self, cmd, blocking):
        #chan = self.ssh.get_transport().open_session()
        chan = self.transport.open_session()
        if self.cwd:
            cmd = "cd %s;%s" % (self.cwd, cmd)
        chan.exec_command(cmd)
        if blocking: # this means we wait for output
            stdout = chan.recv(4096) # number of bytes to get back
            stderr = chan.recv_stderr(4096)
            return (chan.recv_exit_status(), stdout, stderr)
        return 0

    def execute(self, cmd, blocking=True):
        if self.onDemand:
            self.open()
            status = self.__execute(cmd, blocking)
            self.close()
        else:
            status = self.__execute(cmd, blocking)
        return status

    # this should be a remotePath
    def __put(self, localFile, remoteDir):
        # create a full path for the remote file.
        remoteFile = os.path.join(remoteDir, os.path.basename(localFile))
        
        #sftp = self.ssh.open_sftp()
        sftp = self.transport.open_sftp_client()
        if self.cwd:
            sftp.chdir(self.cwd)
        sftp.put(localFile, remoteFile)
        sftp.close()
        return remoteFile

    def put(self, localFile, remoteDir):
        if self.onDemand:
            self.open()
            remoteFile = self.__put(localFile, remoteDir)
            self.close()
        else:
            remoteFile = self.__put(localFile, remoteDir)
        return remoteFile

    def __get(self, remoteFile, localDir):
        # create a full path for the local file.
        localFile = os.path.join(localDir, os.path.basename(remoteFile))

        #sftp = self.ssh.open_sftp()
        sftp = self.transport.open_sftp_client()
        sftp.get(remoteFile, localFile)
        sftp.close()
    
    def get(self, remoteFile, localDir):
        if self.onDemand:
            self.open()
            self.__get(remoteFile, localDir)
            self.close()
        else:
            self.__get(remoteFile, localDir)

    def __exists(self, path):
        """ os.path.exists for paramiko's SCP object """
        val = False
        #sftp = self.ssh.open_sftp()
        sftp = self.transport.open_sftp_client()
        try:
            sftp.stat(path)
        except IOError, e:
            if e[0] == 2:
                val = False
            else:
                raise
        else:
            val = True
        sftp.close()
        return val

    def exists(self, path):
        if self.onDemand:
            self.open()
            val = self.__exists(path)
            self.close()
        else:
            val = self.__exists(path)
        return val

if __name__ == "__main__":
    #rc = RemoteConnection(hostname='wd74401', username='d3y034', key='/home/d3y034/.ssh/id_rsa')
    rc = RemoteConnection(configFile='./examples/RemoteConnection/conn.yaml')
    rc.execute("hostname > junk")
    rc.get("junk", '.')
    rc.execute("rm junk")
    rc.close() # this isn't necessiary since onDemand but since it generate an error, we will fix this.
