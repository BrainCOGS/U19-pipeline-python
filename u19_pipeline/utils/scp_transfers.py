
import os
import psutil
import subprocess
import sys
from paramiko import SSHClient, AutoAddPolicy, RSAKey
from paramiko.auth_handler import AuthenticationException, SSHException
from scp import SCPClient, SCPException

#Steps on windows machine
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

public_key_location = '/Users/alvaros/.ssh/id_rsa_copyfiles'

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


def transfer_scp(host, username, remote_path, local_path):
    rc = RemoteClient(host, username, public_key_location, remote_path)
    rc._get_ssh_key()
    rc.scp
    rc.download_folder(remote_path=remote_path, local_path=local_path)
    rc.disconnect()


if __name__ == "__main__":
    args = sys.argv[1:]
    print(args)
    transfer_scp(args[0], args[1], args[2], args[3])


def call_scp_background(rec_series, full_remote_path):

    print(rec_series['ip_address'], rec_series['system_user'], rec_series['local_directory'], full_remote_path)
    #transfer_scp(rec_series['ip_address'], rec_series['system_user'], rec_series['local_directory'], full_remote_path)

    this_file = os.path.realpath(__file__)

    p = subprocess.Popen(["nohup", "python", this_file,
    rec_series['ip_address'], rec_series['system_user'], rec_series['local_directory'], full_remote_path,  "&"])

    return True,p.pid

def check_scp_transfer(pid):

    finished = False
    exit_code = -1

    if psutil.pid_exists(pid):
        pr = psutil.Process(pid=pid)
        gone, _ = psutil.wait_procs([pr], timeout=3)

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

