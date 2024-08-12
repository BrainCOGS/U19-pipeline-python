
import os
import subprocess
import sys

import psutil
from paramiko import AutoAddPolicy, RSAKey, SSHClient
from paramiko.auth_handler import AuthenticationException, SSHException
from scp import SCPClient

from u19_pipeline.automatic_job.clusters_paths_and_transfers import (
    public_key_location as public_key_location,
)

#Steps on windows machine
#   https://thesysadminchannel.com/solved-add-windowscapability-failed-error-code-0x800f0954-rsat-fix/
#   PowerShell
#     Add-WindowsCapability -Online -Name OpenSSH.Server~~~~0.0.1.0
#     Start-Service sshd
#     Set-Service -Name sshd -StartupType 'Automatic'
# Copy ssh key pub to ~\.ssh\authorized_keys
# Modify C:\ProgramData\ssh\sshd_config
#   Uncomment
#       PasswordAuthentication no
#       StrictModes no
#       AuthorizedKeysFile	.ssh/authorized_keys
#   Comment
#       Match Group administrators
#           AuthorizedKeysFile __PROGRAMDATA__/ssh/administrators_authorized_keys
#   PowerShell
#         restart-service sshd

class RemoteClient:
    """Client to interact with a remote host via SSH & SCP."""

    def __init__(self, host, user, ssh_key_filepath, remote_path):
        self.host = host
        self.user = user
        self.ssh_key_filepath = ssh_key_filepath
        self.remote_path = remote_path

    def _get_ssh_key(self):
        """ Fetch locally stored SSH key."""
        try:
            self.ssh_key = RSAKey.from_private_key_file(
                self.ssh_key_filepath
            )
            print(
                f"Found SSH key at self {self.ssh_key_filepath}"
            )
            return self.ssh_key
        except SSHException as e:
            print(e)

    @property
    def connection(self):
        """Open connection to remote host. """
        try:
            client = SSHClient()
            client.load_system_host_keys()
            client.set_missing_host_key_policy(AutoAddPolicy())
            client.connect(
                self.host,
                username=self.user,
                key_filename=self.ssh_key_filepath,
                timeout=50000,
            )
            return client
        except AuthenticationException as e:
            print(
                f"Authentication failed: did you remember to create an SSH key? {e}"
            )
            raise e

    @property
    def scp(self) -> SCPClient:
        conn = self.connection
        self.client = conn
        return SCPClient(self.client.get_transport())

    def disconnect(self):
        """Close ssh connection."""
        if self.client:
            self.client.close()
        if self.scp:
            self.scp.close()

    def download_folder(self, remote_path: str, local_path: str):
        """Download folder from remote host."""
        self.scp.get(remote_path, local_path=local_path, recursive=True)


def transfer_scp(host=None, username=None, remote_path=None, local_path=None):
    rc = RemoteClient(host, username, public_key_location, remote_path)
    print(host)
    print(username)
    print(public_key_location)
    print(remote_path)
    print(local_path)
    rc._get_ssh_key()
    rc.scp
    rc.download_folder(remote_path=remote_path, local_path=local_path)
    rc.disconnect()

def call_scp_background(ip_address=None, system_user=None, recording_system_directory=None, data_directory=None):

    print(ip_address, system_user, data_directory, recording_system_directory)
    #transfer_scp(rec_series['ip_address'], rec_series['system_user'], rec_series['local_directory'], full_remote_path)

    this_file = os.path.realpath(__file__)


    p = subprocess.Popen(["nohup", "python", this_file, ip_address, system_user, recording_system_directory, data_directory, "&"])

    # To test without nohup
    #p = subprocess.run(["python", this_file, ip_address, system_user, recording_system_directory, data_directory], capture_output=True)
    #print('stderr', p.stderr.decode('UTF-8'))
    #print('stdout', p.stdout.decode('UTF-8'))
    #print('p.returncode', p.returncode)

    return True,p.pid




def check_scp_transfer(pid):

    finished = False
    exit_code = -1

    if psutil.pid_exists(pid):
        pr = psutil.Process(pid=pid)
        print(pr)
        gone, _ = psutil.wait_procs([pr], timeout=3)
        print(gone)

        if len(gone) > 0:
            finished = True
            if gone[0].returncode == 0:
                exit_code = 0

    else:
        finished = True

    return finished, exit_code

def check_directory_copied_correctly():
    pass
    # diff -r -q /path/to/dir1 /path/to/dir2


if __name__ == "__main__":
    args = sys.argv[1:]
    print(args)

    transfer_scp(host=args[0], username=args[1], remote_path=args[2], local_path=args[3])



