import re
import shlex
import signal
import subprocess

# Fields queried from sacct to explain why a job ended (one row per job and per job step)
sacct_format = [
    "JobID",
    "State",
    "ExitCode",
    "DerivedExitCode",
    "Reason",
    "Elapsed",
    "Timelimit",
    "MaxRSS",
    "NodeList",
]

memory_units = {
    "": 1,
    "K": 1024,
    "M": 1024**2,
    "G": 1024**3,
    "T": 1024**4,
    "P": 1024**5,
}
memory_regex = re.compile(r"^\s*([\d\.]+)\s*([KMGTP]?)\s*$", re.IGNORECASE)


def parse_memory(memory_str):
    """
    Bytes of a sacct memory field ('31457280K', '1.5G', ...), 0 if empty or unknown
    """
    match = memory_regex.match(memory_str or "")
    if not match:
        return 0
    try:
        return float(match.group(1)) * memory_units[match.group(2).upper()]
    except ValueError:
        return 0


def format_memory(memory_bytes):
    """
    Human readable memory, in G when above 1G, in M otherwise
    """
    if memory_bytes >= memory_units["G"]:
        return "%.1fG" % (memory_bytes / memory_units["G"])
    return "%.0fM" % (memory_bytes / memory_units["M"])


def parse_sacct_output(stdout, jobid):
    """
    Parse the output of sacct -P --format=<sacct_format>
    Input:
    stdout (str) = output of sacct (with or without header)
    jobid  (str) = slurm job id queried
    Returns:
    (dict) = fields of the job (main row), with the max MaxRSS of all its steps,
             None if the job is not listed
    """
    if not stdout:
        return None

    rows = []
    for line in stdout.splitlines():
        fields = line.strip().split("|")
        if len(fields) != len(sacct_format) or fields[0] == sacct_format[0]:
            continue
        rows.append(dict(zip(sacct_format, fields, strict=True)))

    if not rows:
        return None

    job_rows = [x for x in rows if x["JobID"] == str(jobid)]
    job_info = dict(job_rows[0] if job_rows else rows[0])

    # Memory is only accounted in the job steps (batch, extern, srun)
    max_memory_row = max(rows, key=lambda x: parse_memory(x["MaxRSS"]))
    job_info["MaxRSS"] = max_memory_row["MaxRSS"]

    return job_info


def describe_exit_code(exit_code):
    """
    <code>:<signal> exit code of sacct, with the name of the signal if any
    """
    code, _, signal_num = exit_code.partition(":")
    if signal_num.isdigit() and int(signal_num) > 0:
        try:
            return exit_code + " (" + signal.Signals(int(signal_num)).name + ")"
        except ValueError:
            pass
    return exit_code


def describe_slurm_job(job_info, max_length=150):
    """
    One line description of how a slurm job ended, e.g.:
    OUT_OF_MEMORY (ExitCode 0:125, node spock-g3, elapsed 01:02:03 of 1-00:00:00 limit, MaxRSS 30.0G)
    Input:
    job_info (dict) = output of parse_sacct_output
    Returns:
    (str) = description ('' if no info)
    """
    if not job_info or not job_info.get("State"):
        return ""

    # sacct reports "CANCELLED by <uid>"
    state = job_info["State"].strip()
    state = re.sub(r"^CANCELLED by (\d+)$", r"CANCELLED by uid \1", state)

    no_exit = ("", "0:0")
    details = []
    exit_code = job_info.get("ExitCode", "")
    derived_exit_code = job_info.get("DerivedExitCode", "")
    if exit_code not in no_exit:
        details.append("ExitCode " + describe_exit_code(exit_code))
    if derived_exit_code not in no_exit and derived_exit_code != exit_code:
        details.append("DerivedExitCode " + describe_exit_code(derived_exit_code))

    if job_info.get("Reason", "") not in ("", "None"):
        details.append("reason " + job_info["Reason"])

    node = job_info.get("NodeList", "")
    if node and node != "None assigned":
        if len(node) > 40:
            node = node[:37] + "..."
        details.append("node " + node)

    elapsed = job_info.get("Elapsed", "")
    if elapsed and elapsed != "00:00:00":
        if job_info.get("Timelimit", "") not in ("", "UNLIMITED", "Partition_Limit"):
            elapsed += " of " + job_info["Timelimit"] + " limit"
        details.append("elapsed " + elapsed)

    max_memory = parse_memory(job_info.get("MaxRSS", ""))
    if max_memory:
        details.append("MaxRSS " + format_memory(max_memory))

    description = state
    if details:
        description += " (" + ", ".join(details) + ")"

    if len(description) > max_length:
        description = description[: max_length - 4] + "...)"

    return description


def update_repository_command(repository_dir, git_url, ssh_target=None):
    """
    Command that fast-forwards a checkout to the default branch of git_url, checks out the
    submodule commits it pins (sorters like Kilosort3) and prints its new commit
    Pulling from the URL (not origin) works even when origin is an ssh remote the user has no key for
    """
    quoted_dir = shlex.quote(repository_dir)
    remote_command = (
        f"git -C {quoted_dir} pull --ff-only --quiet {shlex.quote(git_url)} HEAD"
        f" && git -C {quoted_dir} submodule update --init --recursive --quiet"
        f" && git -C {quoted_dir} rev-parse --short HEAD"
    )
    if ssh_target:
        return ["ssh", "-o", "BatchMode=yes", ssh_target, remote_command]
    return ["bash", "-c", remote_command]


def update_repository(repository_dir, git_url, ssh_target=None, timeout=120):
    """
    Fast-forward a processing repository checkout (through ssh if ssh_target is given)
    Returns:
    updated (bool) = True if the checkout is now at the latest commit of git_url
    message (str)  = new commit if updated, otherwise why it could not be updated
    """
    command = update_repository_command(repository_dir, git_url, ssh_target)
    try:
        p = subprocess.run(
            command, capture_output=True, text=True, timeout=timeout, check=False
        )
    except subprocess.TimeoutExpired:
        return False, "git pull did not finish in " + str(timeout) + " s"
    except OSError as e:
        return False, str(e)

    if p.returncode != 0:
        output = (p.stderr.strip() or p.stdout.strip())[-1000:]
        return False, output or "git pull exited with code " + str(p.returncode)
    return True, p.stdout.strip()


def repository_update_warning(repository, repository_dir, where, job_id, message):
    """
    Slack message for a processing repository that could not be updated before queuing a job
    """
    return {
        "text": (
            f":warning: Could not update *{repository}* (`{repository_dir}` on {where})"
            f" before queuing job {job_id}. The job will run on the code already checked out."
            f"\n```{message}```"
        )
    }


# Jobs the processing code queues are named job_id_<id> and carry the checkout they run
# from as their comment (sbatch --comment). Jobs queued before that have no comment
# ("(null)" in squeue) and could be running from any checkout.
processing_job_prefix = "job_id_"
untagged_job_comments = ("", "(null)")


def running_jobs_command(user, ssh_target=None):
    """
    Command that lists the running jobs of user as "name|comment" lines
    """
    command = ["squeue", "-h", "-u", user, "-t", "RUNNING", "-o", "%j|%k"]
    if ssh_target:
        return ["ssh", "-o", "BatchMode=yes", ssh_target, shlex.join(command)]
    return command


def parse_running_jobs(stdout):
    """
    (name, comment) of each job in the output of running_jobs_command
    """
    jobs = []
    for line in stdout.splitlines():
        name, _, comment = line.strip().partition("|")
        if name:
            jobs.append((name, comment.strip()))
    return jobs


def running_jobs(user, ssh_target=None, timeout=60):
    """
    Running jobs of user (through ssh if ssh_target is given)
    Returns:
    ok   (bool)       = True if squeue answered
    jobs (list | str) = (name, comment) of each running job, otherwise why squeue failed
    """
    command = running_jobs_command(user, ssh_target)
    try:
        p = subprocess.run(
            command, capture_output=True, text=True, timeout=timeout, check=False
        )
    except subprocess.TimeoutExpired:
        return False, "squeue did not finish in " + str(timeout) + " s"
    except OSError as e:
        return False, str(e)

    if p.returncode != 0:
        output = (p.stderr.strip() or p.stdout.strip())[-1000:]
        return False, output or "squeue exited with code " + str(p.returncode)
    return True, parse_running_jobs(p.stdout)


def jobs_using_checkout(jobs, repository_dir):
    """
    Names of the processing jobs (from running_jobs) that may be running from repository_dir
    """
    return [
        name
        for name, comment in jobs
        if name.startswith(processing_job_prefix)
        and (comment == repository_dir or comment in untagged_job_comments)
    ]


def checkout_behind_command(repository_dir, git_url, ssh_target=None):
    """
    Command that prints the commit of a checkout and then the latest commit of git_url
    (only looks: nothing in the checkout changes)
    """
    remote_command = (
        f"git -C {shlex.quote(repository_dir)} rev-parse HEAD"
        f" && git ls-remote {shlex.quote(git_url)} HEAD"
    )
    if ssh_target:
        return ["ssh", "-o", "BatchMode=yes", ssh_target, remote_command]
    return ["bash", "-c", remote_command]


def checkout_behind(repository_dir, git_url, ssh_target=None, timeout=60):
    """
    Whether a checkout is not at the latest commit of git_url (a pull would change it)
    Returns:
    behind  (bool) = True unless the checkout is at the latest commit (also if it can't tell)
    message (str)  = the commit if it is up to date, otherwise both commits or why it can't tell
    """
    command = checkout_behind_command(repository_dir, git_url, ssh_target)
    try:
        p = subprocess.run(
            command, capture_output=True, text=True, timeout=timeout, check=False
        )
    except subprocess.TimeoutExpired:
        return True, "git ls-remote did not finish in " + str(timeout) + " s"
    except OSError as e:
        return True, str(e)

    lines = p.stdout.split()
    if p.returncode != 0 or len(lines) < 2:
        output = (p.stderr.strip() or p.stdout.strip())[-1000:]
        return True, output or "git exited with code " + str(p.returncode)

    local, latest = lines[0], lines[1]
    if local == latest:
        return False, local[:7]
    return True, local[:7] + " is behind " + latest[:7]


def repository_skip_note(repository, repository_dir, where, hours, jobs):
    """
    Slack message for a processing repository not pulled for long because jobs kept running from it
    """
    return {
        "text": (
            f":hourglass: *{repository}* (`{repository_dir}` on {where}) has not been"
            f" updated for {hours:.0f} h: there were always jobs running from it"
            f" (now {', '.join(jobs)}). It will be pulled before the first job queued"
            " while none of them runs."
        )
    }
