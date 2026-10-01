import re
import signal

# Fields queried from sacct to explain why a job ended (one row per job and per job step)
sacct_format = ['JobID', 'State', 'ExitCode', 'DerivedExitCode', 'Reason', 'Elapsed', 'Timelimit', 'MaxRSS', 'NodeList']

memory_units = {'': 1, 'K': 1024, 'M': 1024**2, 'G': 1024**3, 'T': 1024**4, 'P': 1024**5}
memory_regex = re.compile(r'^\s*([\d\.]+)\s*([KMGTP]?)\s*$', re.IGNORECASE)


def parse_memory(memory_str):
    '''
    Bytes of a sacct memory field ('31457280K', '1.5G', ...), 0 if empty or unknown
    '''
    match = memory_regex.match(memory_str or '')
    if not match:
        return 0
    try:
        return float(match.group(1)) * memory_units[match.group(2).upper()]
    except ValueError:
        return 0


def format_memory(memory_bytes):
    '''
    Human readable memory, in G when above 1G, in M otherwise
    '''
    if memory_bytes >= memory_units['G']:
        return '%.1fG' % (memory_bytes / memory_units['G'])
    return '%.0fM' % (memory_bytes / memory_units['M'])


def parse_sacct_output(stdout, jobid):
    '''
    Parse the output of sacct -P --format=<sacct_format>
    Input:
    stdout (str) = output of sacct (with or without header)
    jobid  (str) = slurm job id queried
    Returns:
    (dict) = fields of the job (main row), with the max MaxRSS of all its steps,
             None if the job is not listed
    '''
    if not stdout:
        return None

    rows = []
    for line in stdout.splitlines():
        fields = line.strip().split('|')
        if len(fields) != len(sacct_format) or fields[0] == sacct_format[0]:
            continue
        rows.append(dict(zip(sacct_format, fields, strict=True)))

    if not rows:
        return None

    job_rows = [x for x in rows if x['JobID'] == str(jobid)]
    job_info = dict(job_rows[0] if job_rows else rows[0])

    # Memory is only accounted in the job steps (batch, extern, srun)
    max_memory_row = max(rows, key=lambda x: parse_memory(x['MaxRSS']))
    job_info['MaxRSS'] = max_memory_row['MaxRSS']

    return job_info


def describe_exit_code(exit_code):
    '''
    <code>:<signal> exit code of sacct, with the name of the signal if any
    '''
    code, _, signal_num = exit_code.partition(':')
    if signal_num.isdigit() and int(signal_num) > 0:
        try:
            return exit_code + ' (' + signal.Signals(int(signal_num)).name + ')'
        except ValueError:
            pass
    return exit_code


def describe_slurm_job(job_info, max_length=150):
    '''
    One line description of how a slurm job ended, e.g.:
    OUT_OF_MEMORY (ExitCode 0:125, node spock-g3, elapsed 01:02:03 of 1-00:00:00 limit, MaxRSS 30.0G)
    Input:
    job_info (dict) = output of parse_sacct_output
    Returns:
    (str) = description ('' if no info)
    '''
    if not job_info or not job_info.get('State'):
        return ''

    # sacct reports "CANCELLED by <uid>"
    state = job_info['State'].strip()
    state = re.sub(r'^CANCELLED by (\d+)$', r'CANCELLED by uid \1', state)

    no_exit = ('', '0:0')
    details = []
    exit_code = job_info.get('ExitCode', '')
    derived_exit_code = job_info.get('DerivedExitCode', '')
    if exit_code not in no_exit:
        details.append('ExitCode ' + describe_exit_code(exit_code))
    if derived_exit_code not in no_exit and derived_exit_code != exit_code:
        details.append('DerivedExitCode ' + describe_exit_code(derived_exit_code))

    if job_info.get('Reason', '') not in ('', 'None'):
        details.append('reason ' + job_info['Reason'])

    node = job_info.get('NodeList', '')
    if node and node != 'None assigned':
        if len(node) > 40:
            node = node[:37] + '...'
        details.append('node ' + node)

    elapsed = job_info.get('Elapsed', '')
    if elapsed and elapsed != '00:00:00':
        if job_info.get('Timelimit', '') not in ('', 'UNLIMITED', 'Partition_Limit'):
            elapsed += ' of ' + job_info['Timelimit'] + ' limit'
        details.append('elapsed ' + elapsed)

    max_memory = parse_memory(job_info.get('MaxRSS', ''))
    if max_memory:
        details.append('MaxRSS ' + format_memory(max_memory))

    description = state
    if details:
        description += ' (' + ', '.join(details) + ')'

    if len(description) > max_length:
        description = description[:max_length-4] + '...)'

    return description
