import os


def sid_subdir_path(sid):
    """
    Format subdir path to limit the number directories in any given
    subdirectory to 100.

    The number in each directory is designed to support at least 100000
    equities.

    Parameters
    ----------
    sid : int
        Asset identifier.

    Returns
    -------
    out : string
        A path for the bcolz rootdir, including subdirectory prefixes based on
        the padded string representation of the given sid.

        e.g. 1 is formatted as 00/00/000001.bcolz

    """
    padded_sid = format(sid, '06')
    return os.path.join(
        # subdir 2 00/XX
        padded_sid[0:2],
        # subdir 2 XXX/0
        padded_sid[2:4],
        "{0}.bcolz".format(str(padded_sid))
    )
