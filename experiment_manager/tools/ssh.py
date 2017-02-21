# based on https://gist.githubusercontent.com/johnfink8/2190472/raw/e4f3df6dde23309d3228986d0a1cc39c0a6ed9ac/ssh.py

import paramiko
import errno
import socket
import os
from stat import S_ISDIR
import getpass
from Crypto.PublicKey import RSA
import socket
import time
from scp import SCPClient
#import shlex
import subprocess
import tarfile
import StringIO
import shutil
import glob
import uuid

class SSHSession(object):
    def __init__(self, hostname, username=None, port = 22, password=None, key_file=None):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.put_wait = []
        self.get_wait = []
        self.port = port
        home = os.environ['HOME']
        if key_file is None:
            self.key_file = '{}/.ssh/id_rsa'.format(home)
        elif key_file[0] == '/':
            self.key_file = key_file
        elif os.path.isfile('{}/.ssh/{}/id_rsa'.format(home,key_file)):
            self.key_file = '{}/.ssh/{}/id_rsa'.format(home,key_file)
        elif os.path.isfile(key_file):
            self.key_file = key_file
        else:
            self.key_file = '{}/.ssh/id_rsa'.format(home)

        self.client = paramiko.SSHClient()
        self.connect()

    def connect(self):
        home = os.environ['HOME']
        self.client.load_system_host_keys()
        try:
            self.client.connect(hostname=self.hostname, username=self.username, port=self.port, password=self.password, key_filename=self.key_file)
        except:
            time.sleep(1)
            retry = True
            while retry:
                try:
                    self.client.connect(hostname=self.hostname, username=self.username, port=self.port, password=self.password, key_filename=self.key_file)
                except:
                    a = raw_input('Connection failed. Retry? Y/N/catch')
                    if a == 'catch':
                        raise
                    elif a not in ['y','Y']:
                        retry = False
            if not retry:
                temp_password = getpass.getpass('SSH Password:')
                self.client.connect(hostname=self.hostname, username=self.username, port=self.port, password=temp_password, key_filename=None)
                question = raw_input('Install SSH key? Y/N')
                if question == 'Y' or question == 'y':
                    where = raw_input('Where? default (~/.ssh/id_rsa) / key_file (<key_file>) / key_file_name (~/.ssh/<key_file>/id_rsa) / <path>')
                    if where == 'default':
                        where = '{}/.ssh/id_rsa'.format(home)
                    elif where == 'key_file_name':
                        where = '{}/.ssh/{}/id_rsa'.format(home,self.key_file)
                    elif where == 'key_file':
                        where = self.key_file
                    self.key_file = where
                    self.install_ssh_key()
                    self.close()
                    self.client.connect(hostname=self.hostname, username=self.username, port=self.port, password=self.password, key_filename=self.key_file)
        #self.transport = self.client._transport
        #self.transport.window_size = 2147483647
        #self.transport.packetizer.REKEY_BYTES = pow(2, 40)
        #self.transport.packetizer.REKEY_PACKETS = pow(2, 40)
        #self.client._transport = self.transport
        self.scp = SCPClient(self.client.get_transport())
        self.sftp = self.client.open_sftp()

    def reconnect(self):
        self.close()
        self.connect()

    def path_exists(self, path):
        try:
            self.sftp.stat(path)
        except IOError, e:
            if e.errno == errno.ENOENT:
                return False
            raise e
        else:
            return True

    def remove(self,remotefilepath):#TODO: add rmdir
        if self.path_exists(remotefilepath):
            self.sftp.remove(remotefilepath)

    def mkdir_p(self, path):
        if path in ['/','','.','~']:
            return None
        else:
            try:
                self.sftp.stat(path)
            except IOError:
                dirname, basename = os.path.split(path.rstrip('/'))
                self.mkdir_p(dirname)
                self.sftp.mkdir(path)

    def create_path(self, path):
        self.mkdir_p(path)
        #self.command_output("mkdir -p {}".format(path))

    def command(self,cmd):
        return self.client.exec_command(cmd)

    def command_output(self,cmd):
        std_in, std_out, std_err = self.client.exec_command(cmd)
        return std_out.read()

    def put(self,localfile,remotefile):
        #if not self.path_exists(os.path.dirname(remotefile)):
        #    self.create_path(os.path.dirname(remotefile))
        try:
            self.sftp.put(localfile,remotefile)
            #self.scp.put(localfile,remotefile)
        except IOError:
            self.create_path(os.path.dirname(remotefile))
            self.sftp.put(localfile,remotefile)
            #self.scp.put(localfile,remotefile)

    def batch_put(self,localfile,remotefile):
        self.put_wait.append({'localname':os.path.basename(localfile),
                              'localdir':os.path.dirname(localfile),
                              'remotename':os.path.basename(remotefile),
                              'remotedir':os.path.dirname(remotefile),
                                })

    def put_dir(self,localdir,remotedir, max_depth=10):
        if max_depth<0:
            raise Exception('Directory too deep!')
        files = os.listdir(localdir)
        for f in files:
            if os.path.isfile(os.path.join(localdir,f)):
                #if not self.path_exists(remotedir):
                #    self.create_path(remotedir)
                self.put(os.path.join(localdir,f),os.path.join(remotedir,f))
            else:
                self.put_dir(os.path.join(localdir,f),os.path.join(remotedir,f),max_depth=max_depth-1)

    def get(self,remotefile,localfile):
        self.sftp.get(remotefile,localfile)
        #self.scp.get(remotefile,localfile)

    def batch_get(self,remotefile,localfile):
        self.get_wait.append({'localname':os.path.basename(localfile),
                              'localdir':os.path.dirname(localfile),
                              'remotename':os.path.basename(remotefile),
                              'remotedir':os.path.dirname(remotefile),
                                })

    def get_dir(self,remotedir,localdir,max_depth=10):
        if max_depth<0:
            raise Exception('Directory too deep!')
        files = self.sftp.listdir(remotedir)
        for f in files:
            f = str(f)
            if not self.isdir(os.path.join(remotedir,f)):
                self.get(os.path.join(remotedir,f),os.path.join(localdir,f))
            else:
                if not os.path.exists(os.path.join(localdir,f)):
                    os.makedirs(os.path.join(localdir,f))
                self.get_dir(os.path.join(remotedir,f),os.path.join(localdir,f),max_depth=max_depth-1)

    def batch_send(self,localtardir='',tar_name=None,remotetardir='',command_send_func=None):
        if len(self.put_wait):
            if tar_name is None:
                tar_name = str(uuid.uuid1())
            tar_name_ext = tar_name+'.tar'
            if not os.path.isdir(localtardir):
                os.makedirs(localtardir)
            with tarfile.open(os.path.join(localtardir,tar_name_ext), 'w') as tar:
                for i in range(len(self.put_wait)):
                    f = self.put_wait[i]
                    tar.add(os.path.join(f['localdir'],f['localname']),arcname=str(i)) #if folder structure not respected, maybe add a with pathpy.Path and just tar add localname
            self.put(os.path.join(localtardir,tar_name_ext),os.path.join(remotetardir,tar_name_ext))
            mkdir_command = 'mkdir -p ' + os.path.join(remotetardir,tar_name)
            tar_command = 'tar xf '+ os.path.join(remotetardir,tar_name_ext) +' -C '+os.path.join(remotetardir,tar_name)
            cp_command = ' && '.join(['cp -R {remotetarpath_i} {path_i}'.format(remotetarpath_i=os.path.join(remotetardir,tar_name,str(i)),path_i=os.path.join(self.put_wait[i]['remotedir'],self.put_wait[i]['remotename'])) for i in range(len(self.put_wait))])
            rm_command = 'rm -R ' + os.path.join(remotetardir,tar_name) + '*'
            final_command = ' && '.join([mkdir_command,tar_command, cp_command, rm_command])
            if command_send_func is None:
                output = self.command_output(final_command)
            else:
                output = command_send_func(final_command)
        self.put_wait = []
        return output
            #command = ''#'{'
            #for f in self.put_wait:
            #    command += os.path.join(f['localdir'],f['localname'])+','
            #command = command[:-1]# + '}'
            #command = ['tar','cf','-',command]
            ##chan = self.client.invoke_shell()
            #buff = StringIO.StringIO()
            #process = subprocess.Popen(command,
            #           shell=False,
            #           stdout=subprocess.PIPE,#self.client.exec_command("tar xf -")[0],#ssh_stdin,
            #           stdin=subprocess.PIPE,
            #           stderr=subprocess.PIPE)
            #ssh_stdin, ssh_stdout, ssh_stderr = self.client.exec_command("tar xf - /scratch/wschueller/")
            #ssh_stdin.write(process.stdout.read())
            #ssh_stdin.flush()
            #print ssh_stderr.read(),ssh_stdout.read()
            #print process.stderr.read(),process.stdout.read()
            #process.
            #self.exec_command()

            #create move command
            #run distant command move (+option to create dir if not exists?)
            #clean
            #remotetempdir? and use it

    def batch_receive(self,localtardir='',tar_name=None,remotetardir='',command_send_func=None):
        if len(self.get_wait):
            if tar_name is None:
                tar_name = str(uuid.uuid1())
            tar_name_ext = tar_name+'.tar'
            mkdir_command = 'mkdir -p '+os.path.join(remotetardir,tar_name)
            cp_command = ' && '.join(['cp -R {path_i} {remotetarpath_i}'.format(remotetarpath_i=os.path.join(remotetardir,tar_name,str(i)),path_i=os.path.join(self.get_wait[i]['remotedir'],self.get_wait[i]['remotename'])) for i in range(len(self.get_wait))])
            tar_command = 'tar cf '+os.path.join(remotetardir, tar_name_ext)+' -C '+ os.path.join(remotetardir,tar_name) + ' ' +' '.join([str(i) for i in range(len(self.get_wait))])
            rm_command = 'rm -R '+os.path.join(remotetardir,tar_name)
            final_command = ' && '.join([mkdir_command, cp_command, tar_command, rm_command])
            if command_send_func is None:
                output = self.command_output(final_command)
            else:
                output = command_send_func(final_command)
            if not os.path.isdir(localtardir):
                os.makedirs(localtardir)
            self.get(os.path.join(remotetardir,tar_name_ext),os.path.join(localtardir,tar_name_ext))
            with tarfile.open(os.path.join(localtardir,tar_name_ext), 'r') as tar:
                tar.extractall(path=os.path.join(localtardir,tar_name))
            for i in range(len(self.get_wait)):
                if os.path.isfile(os.path.join(localtardir,tar_name,str(i))):
                    shutil.copy(os.path.join(localtardir,tar_name,str(i)),os.path.join(self.get_wait[i]['localdir'],self.get_wait[i]['localname']))
                else:
                    shutil.copytree(os.path.join(localtardir,tar_name,str(i)),os.path.join(self.get_wait[i]['localdir'],self.get_wait[i]['localname']))
            shutil.rmtree(os.path.join(localtardir,tar_name))
            os.remove(os.path.join(localtardir,tar_name_ext))
            if not glob.glob(os.path.join(localtardir,'*')):
                shutil.rmtree(localtardir)
        self.get_wait = []
        return output

    def rm(self, path):
        self.command('rm -R '+path)

    def isdir(self, path):
      try:
        return S_ISDIR(self.sftp.stat(path).st_mode)
      except IOError:
        #Path does not exist, so by definition not a directory
        return False

    def close(self):
        self.scp.close()
        self.sftp.close()
        self.client.close()

    def install_ssh_key(self):
        path = os.path.dirname(self.key_file)
        if not os.path.exists(path):
            os.makedirs(path)
        if os.path.isfile(self.key_file) or os.path.isfile(self.key_file+'.pub'):
            raise Exception('Keys already exist!')
        key = RSA.generate(2048)
        with open(self.key_file, 'w') as content_file:
            os.chmod(self.key_file, 0600)
            content_file.write(key.exportKey('PEM'))
        pubkey = key.publickey()
        pubkey_string = pubkey.exportKey('OpenSSH') + ' {}@{}'.format(os.environ['USER'], socket.gethostname())
        with open(self.key_file+'.pub', 'w') as content_file:
            content_file.write(pubkey_string)
        self.command('echo -e "{}" >> /home/{}/.ssh/authorized_keys'.format(pubkey_string, self.username))


