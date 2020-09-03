#!/usr/bin/env python3

from __future__ import print_function

import argparse
import os

from pdaqdb import PDAQManager
from svn import SVNMetadata

from svn_and_mantis_to_github import convert_project, \
     load_subversion_project


def add_arguments(parser):
    "Add command-line arguments"

    parser.add_argument("-A", "--author-file", dest="author_file",
                        default="svn-authors",
                        help="File containing a dictionary-style map of"
                        " Subversion usernames to Git authors"
                        " (e.g. \"abc: Abe Beecee <abc@foo.com>\")")
    parser.add_argument("-S", "--do-not-skip-existing",
                        dest="skip_existing",
                        action="store_false", default=True,
                        help="Do not skip projects with existing directories")
    #parser.add_argument("-G", "--github", dest="use_github",
    #                    action="store_true", default=False,
    #                    help="Create the repository on GitHub")
    #parser.add_argument("-M", "--mantis-dump", dest="mantis_dump",
    #                    default=None,
    #                    help="MySQL dump file of WIPAC Mantis repository")
    #parser.add_argument("-g", "--github-repo", dest="github_repo",
    #                    default=None, help="Github repo name")
    #parser.add_argument("-m", "--mantis-project", dest="mantis_project",
    #                    default=None,
    #                    help="Mantis project name")
    #parser.add_argument("-s", "--sleep-seconds", dest="sleep_seconds",
    #                    type=int, default=1,
    #                    help="Number of seconds to sleep after GitHub"
    #                         " issue operations")
    parser.add_argument("-v", "--verbose", dest="verbose",
                        action="store_true", default=False,
                        help="Print details")
    parser.add_argument("-x", "--debug", dest="debug",
                        action="store_true", default=False,
                        help="Print debugging messages")

    parser.add_argument("--local-repo", dest="local_repo",
                        default="git-repo",
                        help="Specify the local directory where Git repos"
                             " should be created (default is 'git-repo')")
    parser.add_argument("--pause-before-finish", dest="pause_before_finish",
                        action="store_true", default=False,
                        help="Pause for input before exiting program")


def main():
    parser = argparse.ArgumentParser()
    add_arguments(parser)
    args = parser.parse_args()

    # pDAQ used 'releases' instead of 'tags'
    SVNMetadata.set_layout(SVNMetadata.DIRTYPE_TAGS, "releases")

    PDAQManager.load_authors(args.author_file, verbose=args.verbose)

    proj_url = "http://code.icecube.wisc.edu/daq/projects"

    known_projects = ("PyDOM", "cluster-config", "config", "config-scripts",
                      "daq-common", "daq-integration-test", "daq-io",
                      "daq-log", "daq-moni-tool", "daq-pom-config",
                      "daq-request-filler", "dash", "eventBuilder-prod",
                      "fabric-common", "icebucket", "juggler", "payload",
                      "payload-generator", "pdaq-user", "secondaryBuilders",
                      "splicer", "StringHub", "oldtrigger", "trigger",
                      "trigger-common", "trigger-testbed", "pdaq")
    #known_projects = ("daq-log", "icebucket", )

    for pkg in known_projects:
        print("=== %s" % (pkg, ))

        repo_dir = os.path.join(args.local_repo, pkg)
        if args.skip_existing and os.path.isdir(repo_dir):
            print("((Skipping existing %s repo))" % (pkg, ))
            continue

        # get the SVNProject data for the requested project
        svnprj = load_subversion_project(pkg, debug=args.debug,
                                         verbose=args.verbose)

        ghutil = None
        mantis_issues = None
        description = None

        convert_project(svnprj, ghutil, mantis_issues, description,
                        convert_externals=True,
                        ignore_bad_externals=True,
                        local_repo=args.local_repo,
                        pause_before_finish=args.pause_before_finish,
                        debug=args.debug, verbose=args.verbose)


if __name__ == "__main__":
    main()