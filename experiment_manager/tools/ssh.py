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
import subprocess
import tarfile
import shutil
import glob
import uuid

from builtins import input, bytes, chr


class SSHSession(object):
    def __init__(self, hostname, auto_connect=True, username=None, port = 22, password=None, key_file=None, auto_accept=False, prefix_command=None):
        self.connected = False
        self.hostname = hostname
        self.username = username
        self.password = password
        self.auto_accept = auto_accept
        self.prefix_command = prefix_command
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
        if auto_connect:
            self.connect()

    def get_username(self):
        if self.username is not None:
            return self.username
        else:
            return self.command_output('echo "$USER"')

    def connect(self):
        home = os.environ['HOME']
        self.client.load_system_host_keys()
        cfg = paramiko.config.SSHConfig()
        sshconfigfile = '{}/.ssh/config'.format(home)
        if os.path.isfile(sshconfigfile):
            with open(sshconfigfile,'r') as f:
                cfg.parse(f)
        if self.hostname in cfg.get_hostnames():
            final_cfg = {}
            for k,v in list(cfg.lookup(self.hostname).items()):
                if k == 'hostname':
                    final_cfg['hostname'] = v
                elif k == 'port':
                    final_cfg['port'] = int(v)
                elif k == 'forwardagent':
                    if v.lower() == 'no':
                        bool_val = False
                    else:
                        bool_val = True
                    final_cfg['allow_agent'] = bool_val
                elif k == 'user':
                    final_cfg['username'] = v
                elif k == 'identityfile':
                    final_cfg['key_filename'] = v[0]
                elif k == 'proxycommand':
                    final_cfg['sock'] = paramiko.proxy.ProxyCommand(v)
            #if 'sock' in final_cfg.keys():
            #    final_cfg['hostname'] += ' (<no hostip for proxy command>)'
            try:
                self.client.connect(**final_cfg)
            except paramiko.SSHException:
                print("unknown host, if present in ECDSA, upgrade your version of paramiko")
                if hasattr(self,'auto_accept') and self.auto_accept:
                    if 'sock' in list(final_cfg.keys()):
                        final_cfg['sock'] = paramiko.proxy.ProxyCommand(cfg.lookup(self.hostname)['proxycommand'])
                    self.client.set_missing_host_key_policy(paramiko.client.WarningPolicy())
                    self.client.connect(**final_cfg)
                    self.client.set_missing_host_key_policy(paramiko.client.RejectPolicy())
                else:
                    raise
        else:
            try:
                    self.client.connect(hostname=self.hostname, username=self.username, port=self.port, password=self.password, key_filename=self.key_file)
            except:
                time.sleep(1)
                retry = True
                while retry:
                    try:
                        self.client.connect(hostname=self.hostname, username=self.username, port=self.port, password=self.password, key_filename=self.key_file)
                    except:
                        a = input('Connection failed. Retry? Y/N/catch')
                        if a == 'catch':
                            raise
                        elif a not in ['y','Y']:
                            retry = False
                if not retry:
                    temp_password = getpass.getpass('SSH Password:')
                    self.client.connect(hostname=self.hostname, username=self.username, port=self.port, password=temp_password, key_filename=None)
                    question = input('Install SSH key? Y/N')
                    if question == 'Y' or question == 'y':
                        where = input('Where? default (~/.ssh/id_rsa) / key_file (<key_file>) / key_file_name (~/.ssh/<key_file>/id_rsa) / <path>')
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
        self.connected = True

    def reconnect(self):
        self.close()
        self.connect()

    def path_exists(self, path):
        try:
            self.sftp.stat(path)
        except IOError as e:
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

    def command(self,cmd,bashrc=False):
        if hasattr(self,'prefix_command') and self.prefix_command is not None:
            cmd = self.prefix_command + cmd
        if bashrc:
            cmd2 = 'mkdir -p ~/.tmp'
            cmd2 += ' && '
            command_file = '~/.tmp/'+str(uuid.uuid1())+'.sh'
            cmd2 += 'echo "#!/bin/bash -i\n'+cmd+'" >> '+command_file
            cmd2 += ' && '
            cmd2 += ' chmod +x '+ command_file
            cmd2 += ' && '
            cmd2 += command_file
            cmd2 += ' && '
            cmd2 += ' rm '+command_file
            return self.client.exec_command(cmd2)
        else:
            return self.client.exec_command(cmd)

    def command_output(self,cmd,bashrc=False,check_exit_code=True):
        std_in, std_out, std_err = self.command(cmd,bashrc=bashrc)
        exit_code = std_out.channel.recv_exit_status()
        if check_exit_code and exit_code != 0:
            raise ValueError('Non-zero exit code ('+str(exit_code)+') for cmd '+cmd+'\n\noutput:'+std_out.read().decode()+'\nerror:'+std_err.read().decode())
        return std_out.read().decode()

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

    def batch_send(self,localtardir='',tar_name=None,remotetardir='',command_send_func=None,untar_basedir='.',limit_min=15,limit_max=500,limit_concurrent_processes=10):
        output = ''
        if len(untar_basedir)>1 and untar_basedir[-1] == '/':
            untar_basedir = untar_basedir[:-1]

        if len(self.put_wait)>0:
            if len(self.put_wait) <limit_min:
                for pw in self.put_wait:
                    if os.path.isdir(os.path.join(pw['localdir'],pw['localname'])):
                        self.put_dir(os.path.join(pw['localdir'],pw['localname']),os.path.join(pw['remotedir'],pw['remotename']))
                    else:
                        self.put(os.path.join(pw['localdir'],pw['localname']),os.path.join(pw['remotedir'],pw['remotename']))
            elif limit_max is not None and len(self.put_wait) > limit_max:
                side_list = self.put_wait[limit_max:]
                self.put_wait = self.put_wait[:limit_max]
                output += self.batch_send(localtardir=localtardir,tar_name=tar_name,remotetardir=remotetardir,command_send_func=command_send_func,untar_basedir=untar_basedir,limit_min=limit_min,limit_max=limit_max)
                self.put_wait = side_list
                return output + self.batch_send(localtardir=localtardir,tar_name=tar_name,remotetardir=remotetardir,command_send_func=command_send_func,untar_basedir=untar_basedir,limit_min=limit_min,limit_max=limit_max)
            else:
                if tar_name is None:
                    tar_name = str(uuid.uuid1())
                tar_name_ext = tar_name+'.tar'
                if not os.path.isdir(localtardir):
                    os.makedirs(localtardir)
                for pw in self.put_wait:
                    untar_len = len(untar_basedir)
                    rmt_dir = pw['remotedir']
                    if len(rmt_dir)>=untar_len and rmt_dir[:untar_len] == untar_basedir:# and rmt_dir[0] == '/'
                        pw['remotedir'] = rmt_dir[untar_len:]
                    if len(pw['remotedir'])>0 and pw['remotedir'][0] == '/':
                        pw['remotedir'] = pw['remotedir'][1:]
                with tarfile.open(os.path.join(localtardir,tar_name_ext), 'w') as tar:
                    for i in range(len(self.put_wait)):
                        f = self.put_wait[i]
                        if os.path.exists(os.path.join(f['localdir'],f['localname'])):
                            tar.add(os.path.join(f['localdir'],f['localname']),arcname=os.path.join(self.put_wait[i]['remotedir'],self.put_wait[i]['remotename']))#,arcname=str(i)) #if folder structure not respected, maybe add a with pathpy.Path and just tar add localname
                self.put(os.path.join(localtardir,tar_name_ext),os.path.join(remotetardir,tar_name_ext))
                #mkdir_command = 'mkdir -p ' + os.path.join(remotetardir,tar_name)
                tar_command = 'tar xf '+ os.path.join(remotetardir,tar_name_ext) +' -C '+untar_basedir
                #mkdir_command2 = 'mkdir -p ' + ' '.join([pw['remotedir'] for pw in self.put_wait])
                #cp_command = '; '.join(['cp -R {remotetarpath_i} {path_i}'.format(remotetarpath_i=os.path.join(remotetardir,tar_name,str(i)),path_i=os.path.join(self.put_wait[i]['remotedir'],self.put_wait[i]['remotename'])) for i in range(len(self.put_wait))])
                #rm_command = 'rm -R ' + os.path.join(remotetardir,tar_name)
                rm_command2 = 'rm -R ' + os.path.join(remotetardir,tar_name_ext)
                final_command = '; '.join([tar_command, rm_command2])
                if command_send_func is None:
                    output = self.command_output(final_command)
                    #move os remove cmd here
                else:
                    #queue command
                    #modify counter
                    #if limit>=counter or end of list
                    # send batch of commands (threading?)
                    # get outputs
                    # concatenate outputs
                    # rm files
                    # empty queues, cmd list etc
                    output = command_send_func(final_command)
                os.remove(os.path.join(localtardir,tar_name_ext))
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
            #print(ssh_stderr.read(),ssh_stdout.read())
            #print(process.stderr.read(),process.stdout.read())
            #process.
            #self.exec_command()

            #create move command
            #run distant command move (+option to create dir if not exists?)
            #clean
            #remotetempdir? and use it

    def batch_receive(self,untar_basedir='',localtardir='',tar_name=None,remotetardir='',command_send_func=None,limit_min=15,limit_max=500):
        output = ''
        if len(untar_basedir)>1 and untar_basedir[-1] == '/':
            untar_basedir = untar_basedir[:-1]
        if len(self.get_wait):
            if len(self.get_wait) <limit_min:
                for gw in self.get_wait:
                    if self.isdir(os.path.join(gw['remotedir'],gw['remotename'])):
                        self.get_dir(os.path.join(gw['remotedir'],gw['remotename']),os.path.join(gw['localdir'],gw['localname']))
                    else:
                        try:
                            self.get(os.path.join(gw['remotedir'],gw['remotename']),os.path.join(gw['localdir'],gw['localname']))
                        except:
                            raise IOError(os.path.join(gw['remotedir'],gw['remotename']),os.path.join(gw['localdir'],gw['localname']))
            elif limit_max is not None and len(self.get_wait) > limit_max:
                side_list = self.get_wait[limit_max:]
                self.get_wait = self.get_wait[:limit_max]
                output += self.batch_receive(localtardir=localtardir,tar_name=tar_name,remotetardir=remotetardir,command_send_func=command_send_func,untar_basedir=untar_basedir,limit_min=limit_min,limit_max=limit_max)
                self.get_wait = side_list
                return output + self.batch_receive(localtardir=localtardir,tar_name=tar_name,remotetardir=remotetardir,command_send_func=command_send_func,untar_basedir=untar_basedir,limit_min=limit_min,limit_max=limit_max)
            else:
                if tar_name is None:
                    tar_name = str(uuid.uuid1())
                tar_name_ext = tar_name+'.tar'
                for gw in self.get_wait:
                    untar_len = len(untar_basedir)
                    lcl_dir = gw['localdir']
                    if len(lcl_dir)>=untar_len and lcl_dir[:untar_len] == untar_basedir:
                        gw['localdir'] = lcl_dir[untar_len:]
                    if len(gw['localdir'])>0 and gw['localdir'][0] == '/':
                        gw['localdir'] = gw['localdir'][1:]
                gw_str = str(self.get_wait)
                python_script = """import tarfile
import os
import sys

remotetardir = "{remotetardir}"
localtardir = "{localtardir}"
get_wait = {gw_str}
untar_basedir = "{untar_basedir}"
tar_name_ext = "{tar_name_ext}"


with tarfile.open(os.path.join(remotetardir,tar_name_ext), 'w') as tar:
    for i in range(len(get_wait)):
        f = get_wait[i]
        if os.path.exists(os.path.join(f['remotedir'],f['remotename'])):
            tar.add(os.path.join(f['remotedir'],f['remotename']),arcname=os.path.join(get_wait[i]['localdir'],get_wait[i]['localname']))

sys.exit(0)
            """.format(remotetardir=remotetardir,localtardir=localtardir,gw_str=gw_str,untar_basedir=untar_basedir, tar_name_ext=tar_name_ext)
                if not os.path.isdir(localtardir):
                    os.makedirs(localtardir)
                python_file = os.path.join(localtardir,tar_name+'.py')
                remote_python_file = os.path.join(remotetardir,tar_name+'.py')
                with open(python_file,'w') as f:
                    f.write(python_script)
                self.mkdir_p(remotetardir)
                self.put(python_file,remote_python_file)
                if command_send_func is None:
                    self.command_output('python '+remote_python_file)
                else:
                    command_send_func('python '+remote_python_file)
                self.get(os.path.join(remotetardir,tar_name_ext),os.path.join(localtardir,tar_name_ext))
                command = 'rm {pythonfile}; rm {tarfile}'.format(pythonfile=remote_python_file,tarfile=os.path.join(remotetardir,tar_name_ext))
                self.command_output(command)
                with tarfile.open(os.path.join(localtardir,tar_name_ext), 'r') as tar:
                    def is_within_directory(directory, target):
                        
                        abs_directory = os.path.abspath(directory)
                        abs_target = os.path.abspath(target)
                    
                        prefix = os.path.commonprefix([abs_directory, abs_target])
                        
                        return prefix == abs_directory
                    
                    def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
                    
                        for member in tar.getmembers():
                            member_path = os.path.join(path, member.name)
                            if not is_within_directory(path, member_path):
                                raise Exception("Attempted Path Traversal in Tar File")
                    
                        tar.extractall(path, members, numeric_owner=numeric_owner) 
                        
                    
                    safe_extract(tar, path=untar_basedir)
                os.remove(os.path.join(localtardir,tar_name_ext))
                os.remove(python_file)
        self.get_wait = []
        return output

#            mkdir_command = 'mkdir -p '+os.path.join(remotetardir,tar_name)
#            cp_command = '; '.join(['cp -R {path_i} {remotetarpath_i}'.format(remotetarpath_i=os.path.join(remotetardir,tar_name,str(i)),path_i=os.path.join(self.get_wait[i]['remotedir'],self.get_wait[i]['remotename'])) for i in range(len(self.get_wait))])
#            tar_command = 'tar cf '+os.path.join(remotetardir, tar_name_ext)+' -C '+ os.path.join(remotetardir,tar_name) + ' ' +' '.join([str(i) for i in range(len(self.get_wait))])
#            rm_command = 'rm -R '+os.path.join(remotetardir,tar_name)
#            final_command = '; '.join([mkdir_command, cp_command, tar_command, rm_command])
#            if command_send_func is None:
#                output = self.command_output(final_command)
#            else:
#                output = command_send_func(final_command)
#            if not os.path.isdir(localtardir):
#                os.makedirs(localtardir)
#            self.get(os.path.join(remotetardir,tar_name_ext),os.path.join(localtardir,tar_name_ext))
#            with tarfile.open(os.path.join(localtardir,tar_name_ext), 'r') as tar:
#                tar.extractall(path=os.path.join(localtardir,tar_name))
#            for i in range(len(self.get_wait)):
#                if os.path.isfile(os.path.join(localtardir,tar_name,str(i))):
#                    shutil.copy(os.path.join(localtardir,tar_name,str(i)),os.path.join(self.get_wait[i]['localdir'],self.get_wait[i]['localname']))
#                else:
#                    shutil.copytree(os.path.join(localtardir,tar_name,str(i)),os.path.join(self.get_wait[i]['localdir'],self.get_wait[i]['localname']))
#            shutil.rmtree(os.path.join(localtardir,tar_name))
#            os.remove(os.path.join(localtardir,tar_name_ext))
#            if not glob.glob(os.path.join(localtardir,'*')):
#                shutil.rmtree(localtardir)

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
        self.connected = False

    def install_ssh_key(self):
        path = os.path.dirname(self.key_file)
        if not os.path.exists(path):
            os.makedirs(path)
        if os.path.isfile(self.key_file) or os.path.isfile(self.key_file+'.pub'):
            raise Exception('Keys already exist!')
        key = RSA.generate(2048)
        with open(self.key_file, 'w') as content_file:
            os.chmod(self.key_file, 0o600)
            content_file.write(key.exportKey('PEM'))
        pubkey = key.publickey()
        pubkey_string = pubkey.exportKey('OpenSSH') + ' {}@{}'.format(os.environ['USER'], socket.gethostname())
        with open(self.key_file+'.pub', 'w') as content_file:
            content_file.write(pubkey_string)
        self.command('echo -e "{}" >> /home/{}/.ssh/authorized_keys'.format(pubkey_string, self.username))


def get_username_from_hostname(hostname):
    home = os.environ['HOME']
    cfg = paramiko.config.SSHConfig()
    sshconfigfile = '{}/.ssh/config'.format(home)
    if os.path.isfile(sshconfigfile):
        with open(sshconfigfile,'r') as f:
            cfg.parse(f)
    if hostname in cfg.get_hostnames():
        return cfg.lookup(hostname)['user']
    else:
        raise ValueError('Hostname '+hostname+' not found. Add it to your .ssh/config, or provide directly username.')

def check_hostname(hostname):
    home = os.environ['HOME']
    cfg = paramiko.config.SSHConfig()
    sshconfigfile = '{}/.ssh/config'.format(home)
    if os.path.isfile(sshconfigfile):
        with open(sshconfigfile,'r') as f:
            cfg.parse(f)
    return (hostname in cfg.get_hostnames())
