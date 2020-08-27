#!/usr/bin/env python3

from __future__ import print_function

import argparse
import sys

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

from cmdrunner import CommandException
from svn import SVNMetadata, svn_get_externals, svn_log
from svndb import SVNEntry, SVNRepositoryDB
from pdaqdb import SVNProject


def add_arguments(parser):
    "Add command-line arguments"

    parser.add_argument("-v", "--verbose", dest="verbose",
                        action="store_true", default=False,
                        help="Print details")
    parser.add_argument("-x", "--debug", dest="debug",
                        action="store_true", default=False,
                        help="Print debugging messages")
    parser.add_argument(dest="svn_project", default=None,
                        help=("Subversion/Mantis project name"))


def main():
    parser = argparse.ArgumentParser()
    add_arguments(parser)
    args = parser.parse_args()

    daq_svn = "http://code.icecube.wisc.edu/daq"

    mantis_projects = None
    if args.svn_project is None or args.svn_project == "pdaq":
        url = daq_svn + "/meta-projects/pdaq/trunk"
    elif args.svn_project.find("/") < 0:
        url = daq_svn + "/projects/" + args.svn_project
    else:
        upieces = urlparse.urlparse(args.svn_project)
        upath = upieces.path.split(os.sep)

        if upath[-1] == "trunk":
            del upath[-1]

        url = urlparse.urlunparse((upieces.scheme, upieces.netloc,
                                   os.sep.join(upath), upieces.params,
                                   upieces.query, upieces.fragment))

    # pDAQ used 'releases' instead of 'tags'
    SVNMetadata.set_layout(SVNMetadata.DIRTYPE_TAGS, "releases")

    # load log entries from all URLs
    project = SVNProject(url, debug=args.debug, verbose=args.verbose)
    project.load(ignore_tag=ignore_tag, debug=args.debug, verbose=args.verbose)
    project.save(debug=debug, verbose=verbose)


if __name__ == "__main__":
    main()
