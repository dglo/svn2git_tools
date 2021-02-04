#!/usr/bin/env python

from __future__ import print_function

import cProfile
import os
import pstats

from functools import wraps


def profile(output_file=None, lines_to_print=None, save_stats=False,
            sort_by='cumulative', strip_dirs=False):
    """A time profiler decorator.
    Borrowed from Ehsan Khodabandeh (and modified):
    https://towardsdatascience.com/how-to-profile-your-code-in-python-e70c834fad89

    Inspired by and modified the profile decorator of Giampaolo Rodola:
    http://code.activestate.com/recipes/577817-profile-decorator/

    Args:
        output_file: str or None. Default is None
            Path of the output file. If only name of the file is given, it's
            saved in the current directory.
            If it's None, the name of the decorated function is used.
        lines_to_print: int or None
            Number of lines to print. Default (None) is for all the lines.
            This is useful in reducing the size of the printout, especially
            that sorting by 'cumulative', the time consuming operations
            are printed toward the top of the file.
        save_stats: False or True
            If True, save the statistics alongside 'output_file', using
            the '.stats' extension (e.g. if 'output_file is '/tmp/foo.profile',
            the statstics will be saved to '/tmp/foo.stats')
        sort_by: str or SortKey enum or tuple/list of str/SortKey enum
            Sorting criteria for the Stats object.
            For a list of valid string and SortKey refer to:
            https://docs.python.org/3/library/profile.html#pstats.Stats.sort_stats
        strip_dirs: bool
            Whether to remove the leading path info from file names.
            This is also useful in reducing the size of the printout

    Returns:
        Profile of the decorated function

    Usage:
        @profile(output_file="/tmp/profile.out", strip_dirs=True)
        def main():
            ...
    """

    def inner(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            _output_file = output_file or func.__name__ + '.prof'

            prof = cProfile.Profile()
            prof.enable()
            try:
                retval = func(*args, **kwargs)
            finally:
                prof.disable()
                if save_stats:
                    _stats_file = os.path.splitext(_output_file)[0] + ".stats"
                    prof.dump_stats(_stats_file)

                with open(_output_file, 'w') as out:
                    stats = pstats.Stats(prof, stream=out)
                    if strip_dirs:
                        stats.strip_dirs()
                    if isinstance(sort_by, (tuple, list)):
                        stats.sort_stats(*sort_by)
                    else:
                        stats.sort_stats(sort_by)
                    stats.print_stats(lines_to_print)
            return retval

        return wrapper

    return inner
