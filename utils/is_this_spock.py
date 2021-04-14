
import sys
import os

def is_this_spock():
    """
    Check if current system is spock or scotty
    """
    local_os = sys.platform
    local_os = local_os[:(min(3, len(local_os)))]

    path = os.getcwd()
    in_smb = path.find('smb') == -1
    in_usr_people = path.find('usr/people') == -1
    in_jukebox = path.find('jukebox') == -1

    isSpock = ((in_smb or in_usr_people or in_jukebox) and
        (not local_os.lower() == 'win') and
        (not local_os.lower() == 'dar'))

    return isSpock
