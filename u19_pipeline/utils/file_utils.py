import os

def write_file(path, text):

    os.umask(0)
    descriptor = os.open(
    path=path,
    flags=(
        os.O_WRONLY  # access mode: write only
        | os.O_CREAT  # create if not exists
        | os.O_TRUNC  # truncate the file to zero
    ),
    mode=0o664
    )

    with open(descriptor, 'w') as fh:
        fh.write(text)