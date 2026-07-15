"""MATLAB batch runner via SSH.

Uses the Airflow SSH provider (``apache-airflow-providers-ssh``) to connect
to the designated MATLAB host and run a MATLAB batch command.

The SSH connection is configured in the Airflow Connections UI / environment
variable under the key ``conn_id`` (default: ``"matlab_host"``).
"""

from __future__ import annotations


def run_matlab_batch(host: str, step: str, conn_id: str = "matlab_host") -> None:
    """SSH to *host* and run a MATLAB batch step.

    Executes the following command on the remote host::

        matlab -batch "run('scripts/startup_virtual_machine.m'); <step>"

    where ``<step>`` is a MATLAB expression such as
    ``"populate_TowersSession()"`` or a function call defined in the startup
    path.

    Parameters
    ----------
    host:
        Hostname or IP of the MATLAB virtual machine (e.g. ``"braincogs00.pni.princeton.edu"``).
        This must match the ``host`` field of the Airflow SSH connection ``conn_id``.
    step:
        MATLAB expression to execute after the startup script, e.g.
        ``"populate_BehaviorSession()"`` or ``"run_alert_system()"``·
    conn_id:
        Airflow SSH connection ID configured in the Connections UI. Defaults
        to ``"matlab_host"``.  The connection must supply username, host, and
        either password or private key.

    Notes
    -----
    Uses ``airflow.providers.ssh.hooks.ssh.SSHHook`` to open the connection
    and ``SSHOperator`` (or its hook directly) to run the command.

    Full command template::

        matlab -batch "run('scripts/startup_virtual_machine.m'); {step}"

    Callers inside DAG tasks should instantiate ``SSHOperator`` directly
    (preferred for operator-level retries) or call this helper from a
    ``@task``-decorated function via ``SSHHook.get_conn()``.
    """
    # TODO: implement via SSHHook / SSHOperator
    #   from airflow.providers.ssh.hooks.ssh import SSHHook
    #   hook = SSHHook(ssh_conn_id=conn_id)
    #   with hook.get_conn() as client:
    #       cmd = f"matlab -batch \"run('scripts/startup_virtual_machine.m'); {step}\""
    #       stdin, stdout, stderr = client.exec_command(cmd)
    #       exit_status = stdout.channel.recv_exit_status()
    #       if exit_status != 0:
    #           raise RuntimeError(f"MATLAB step '{step}' failed: {stderr.read().decode()}")
    raise NotImplementedError("run_matlab_batch is a scaffold stub")
