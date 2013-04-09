#!/usr/bin/python
import time, sys, argparse, textwrap, YAML
import boto.ec2 as EC2

class BotoConnection(object):

    def __init__(self, configDict, userId=778499506662):
        if type(configDict) is not dict:
            sys.exit("Error with BotoConnection initilization.\nMake sure the input data is dictionary type.")
        self.paramDict = configDict
        self.userId = userId

    def __createEC2Connection(self):
        regionName = self.paramDict['regionName']
        awsId = self.paramDict['awsIDs'][0]
        awsKey = self.paramDict['awsIDs'][1]
        return EC2.connect_to_region(regionName, aws_access_key_id=awsId, aws_secret_access_key=awsKey)

    # NOTE: TODO: rework method here
    def isInstanceAvailable(self, ec2conn=None):
        if ec2conn == None:
            ec2conn = self.__createEC2Connection()

        runningFilter = {'instance-state-name':'running'} # only running states
        running = ec2conn.get_all_instances(filters=runningFilter)
        if len(running) < self.paramDict['ec2MaxInstances']:
            return True
        return False

    def areInstancesRunning(self, ec2conn=None):
        if ec2conn == None:
            ec2conn = self.__createEC2Connection()

        runningFilter = {'instance-state-name':'running'} # only running states
        running = ec2conn.get_all_instances(filters=runningFilter)
        if len(running) == 0:
            return True
        return False
    
    def getDefaultingValue(self, key, instDict):
        if instDict and key in instDict:
            return instDict[key]
        try: # this is the else case
            return self.paramDict[key]
        except:
            sys.exit("Neither the config yaml nor the job yaml have a value for the '%s' key" % key)

    def getInstance(self, instDict=None, ec2conn=None):
        if ec2conn == None:
            ec2conn = self.__createEC2Connection()

        ami = ec2conn.get_image( self.getDefaultingValue('amiID', instDict) )

        sshKeyName = self.getDefaultingValue('sshKeyName', instDict)
        securityGroups = self.getDefaultingValue('securityGroups', instDict)
        instanceSize = self.getDefaultingValue('instanceSize', instDict)
        
        # NOTE: need to catch "boto.exception.EC2ResponseError: EC2ResponseError: 400 Bad Request"
        reservation = ami.run(key_name=sshKeyName, security_groups=securityGroups, instance_type=instanceSize)
        curInstance = reservation.instances[-1]

        while curInstance.state != 'running':
            # if pending, sleep then update
            if str(curInstance.state) == 'pending':
                time.sleep(self.paramDict['InstPollPeriod'])
                curInstance.update()
            #else issue
        
        # set the instance so that shutting it down will terminate the instance
        curInstance.modify_attribute("instanceInitiatedShutdownBehavior","terminate")
        
        return str(curInstance.id), str(curInstance.public_dns_name)

    # NOTE consider non-blocking version
    def isInstanceReady(self, instanceId, ec2conn=None):
        if ec2conn == None:
            ec2conn = self.__createEC2Connection()

        instanceFilter = { 'instance-id' : instanceId } # only running states
        response = ec2conn.get_all_instances(filters=instanceFilter)

        while response == None: # cloud says who?
            time.sleep(5) # wait and ask again
            response = ec2conn.get_all_instances(filters=instanceFilter)
        
        # get last instance in reservation
        inst = response[0].instances[-1]

        while inst.state == 'pending': 
            time.sleep(5) # wait and check state again
            inst = ec2conn.get_all_instances(filters=instanceFilter)[0].instances[-1]

        return

    def terminateInstance(self, instanceId, stopOnly=False, ec2conn=None):
        if ec2conn == None:
            ec2conn = self.__createEC2Connection()
        if stopOnly:
            ec2conn.stop_instances([instanceId])
        else:
            ec2conn.terminate_instances([instanceId])

        self.cleanUpVolumes(ec2conn)

    def cleanUpVolumes(self, ec2conn):
        try:
            for v in ec2conn.get_all_volumes():
                if str(v.status) == 'available':
                    v.delete()
        except:
            print 'Error Cleaning Up Volumes'

    def createImage(self, instanceId, name, desc="Created by Linnaeus", ec2conn=None):
        """ Creates an image of the instance blocking until img ready """
        if ec2conn == None:
            ec2conn = self.__createEC2Connection()
        
        newAmi = ec2conn.create_image(instanceId, name, desc)
        # get img info and wait till it is ready
        img = ec2conn.get_image(newAmi)
        while img.state == 'pending':
            time.sleep(15)
            img = ec2conn.get_image(newAmi) #img.update()?
        return newAmi

    def listImages(self, ec2conn=None):
        """ Creates an image of the instance blocking until img ready """
        if ec2conn == None:
            ec2conn = self.__createEC2Connection()

        for img in ec2conn.get_all_images(owners=[self.userId]):
            print "%s: %s" % (img.name, img.id)

    def deregisterImage(self, ami, ec2conn=None, deleteSnapshot=True): 
        """ Creates an image of the instance blocking until img ready """
        if ec2conn == None:
            ec2conn = self.__createEC2Connection()

        try:
            ec2conn.deregister_image(ami, delete_snapshot=deleteSnapshot)
            # look for other snapshtos associate with ami
            for snap in ec2conn.get_all_snapshots(owner=str(self.userId)):
                if ami in snap.description:
                    snap.delete()
        except AttributeError:
            sys.exit("Could not deregister the given AMI.\nCheck value and retry.")

def listImages(args):
    yamlConfig = YAML.load(args.yaml)
    botoConn = BotoConnection(yamlConfig)
    botoConn.listImages()

def deregisterAMI(args):
    yamlConfig = YAML.load(args.yaml)
    botoConn = BotoConnection(yamlConfig)
    botoConn.deregisterImage(args.ami)

def startUpInstance(args):
    yamlConfig = YAML.load(args.yaml)
    botoConn = BotoConnection(yamlConfig)
    if args.ami:
        botoConn.getInstance({'amiID':args.ami})
    else:
        botoConn.getInstance()

def createImage(args):
    yamlConfig = YAML.load(args.yaml)
    botoConn = BotoConnection(yamlConfig)
    newAMI = botoConn.createImage(args.instId, args.name)
    print "Image created. The new ami follows: %s" % newAMI

if __name__ == "__main__":
    desc = '''\
           Wrapper for BotoConnection.'''
    parser = argparse.ArgumentParser( formatter_class=argparse.RawTextHelpFormatter,
                                      description=textwrap.dedent(desc) )
    subparser = parser.add_subparsers() #(help='sub-command help')
    # NOTE: Since not meant to be stand alone, dep on JobManager's config yaml ?
    default_yaml_config = 'config/JobManager.yaml'

    list_parser = subparser.add_parser('list', help="List EC2 images")
    list_parser.add_argument('-yaml', type=str, default=default_yaml_config, help="YAML file used to configure manager.")
    list_parser.set_defaults(func=listImages)

    deregister_parser = subparser.add_parser('deregister', help="Deregister EC2 image")
    deregister_parser.add_argument('-yaml', type=str, default=default_yaml_config, help="YAML file used to configure manager.")
    deregister_parser.add_argument('ami', type=str, help="Image AMI being deregistered")
    deregister_parser.set_defaults(func=deregisterAMI)
    
    start_parser = subparser.add_parser('start', help="Start EC2 image")
    start_parser.add_argument('-yaml', type=str, default=default_yaml_config, help="YAML file used to configure manager.")
    start_parser.add_argument('-ami', type=str, default=None, help="Image AMI being deregistered")
    start_parser.set_defaults(func=startUpInstance)
    
    create_parser = subparser.add_parser('createImage', help="Create EC2 image")
    create_parser.add_argument('-yaml', type=str, default=default_yaml_config, help="YAML file used to configure manager.")
    create_parser.add_argument('instId', type=str, help="Instance Id for instance being used as template for image")
    create_parser.add_argument('name', type=str, help="Name for image")
    create_parser.set_defaults(func=createImage)
    
    args = parser.parse_args()
    args.func(args)
