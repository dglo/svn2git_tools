#!/usr/bin/env python

from __future__ import print_function

import argparse
import os
import shutil
import sys

from git import git_clone, git_commit, git_submodule_add
from svn import SVNNonexistentException, svn_get_externals, svn_list
from svndb import SVNRepositoryDB


def add_arguments(parser):
    "Add command-line arguments"

    parser.add_argument("-v", "--verbose", dest="verbose",
                        action="store_true", default=False,
                        help="Print details")
    parser.add_argument("-x", "--debug", dest="debug",
                        action="store_true", default=False,
                        help="Print debugging messages")


def __build_svn_url(project):
    daq_url = "http://code.icecube.wisc.edu/daq/projects"
    prj_url = "http://code.icecube.wisc.edu/svn/projects"

    if project in ("fabric-common", ):
        return os.path.join(prj_url, project)

    return os.path.join(daq_url, project)


def __check_externals(known_projects, dbdict, svn_url, debug=False,
                      verbose=False):
    missing = {}
    for revision, url, subdir in svn_get_externals(svn_url, debug=debug,
                                                   verbose=verbose):
        if subdir not in known_projects:
            missing[subdir] = 1
            if subdir not in missing:
                print("ERROR: \"%s\" is not a known project (from %s)" %
                      (subdir, relname), file=sys.stderr)
                continue

        if subdir not in dbdict:
            print("ERROR: \"%s\" is not a valid project" % (subdir, ))
            missing[subdir] = 2
            continue

        svndb = dbdict[subdir]

        found_rev, git_branch, git_hash = svndb.find_revision(revision)
        if found_rev is None or git_branch is None or git_hash is None:
            missing[subdir] = 3
            print("ERROR: Cannot find Git hash for %s revision %s" %
                  (subdir, revision))
            continue

        if verbose:
            if found_rev == revision:
                rstr = "-r%s " % (revision, )
            else:
                rstr = "-r%s (expected rev %s)" % (found_rev, revision)

            gstr = "  ## %s:%s" % (git_branch, git_hash)

            print("%s: %s%s%s" % (subdir, rstr, url, gstr))
        else:
            print("%s: %s hash %s" % (subdir, git_branch, git_hash))

    return missing.keys()


def __initialize_git_trunk(repo_path, scratch_path, known_projects, debug=False,
                       verbose=False):
    "Clone the git repo and subprojects into the scratch directory"

    # verify that Git repo exists
    if not os.path.exists(repo_path):
        raise SystemExit("Cannot find Git repo \"%s\"" % (repo_path, ))

    # get rid of old workspace
    if os.path.exists(scratch_path):
        shutil.rmtree(scratch_path)

    # build the URL for the metaproject
    pdaq_repo = git_url(repo_path, "pdaq")

    # create scratch space and clone the metaproject there
    os.mkdir(scratch_path)
    git_clone(pdaq_repo, sandbox_dir=scratch_path, debug=debug,
              verbose=verbose)

    # create all submodules
    for proj in known_projects:
        proj_path = os.path.join(repo_path, proj)
        if not os.path.isdir(proj_path):
            raise SystemExit("Cannot find Git project \"%s\"" % (proj, ))

        git_submodule_add(git_url(repo_path, proj),
                          sandbox_dir=os.path.join(scratch_path, "pdaq"),
                          debug=debug, verbose=verbose)

    # commit the trunk
    details = git_commit(pdaq_repo, commit_message="Add add submodules",
                         commit_all=True, debug=debug, verbose=verbose)


def __open_databases(known_projects, svn_url, debug=False, verbose=False):
    dbdict = {}
    for revision, url, subdir in svn_get_externals(svn_url, debug=debug,
                                                   verbose=verbose):
        if subdir not in known_projects:
            missing[subdir] = 1
            if subdir not in missing:
                print("ERROR: \"%s\" is not a known project (from %s)" %
                      (subdir, relname), file=sys.stderr)
                continue

        try:
            dbdict[subdir] = SVNRepositoryDB(__build_svn_url(subdir),
                                             allow_create=False)
        except SVNNonexistentException as nex:
            raise Exception("\"%s\" is not a valid project" % (subdir, ))

    return dbdict


def git_url(repo_path, project):
    return "file://" + os.path.join(repo_path, project)


def process_pdaq(pdaq_url, known_projects, debug=False, verbose=False):
    trunk_url = os.path.join(pdaq_url, "trunk")
    releases_url = os.path.join(pdaq_url, "releases")

    print("=== TRUNK")

    dbdict = __open_databases(known_projects, trunk_url, debug=debug,
                              verbose=verbose)

    _ = __check_externals(known_projects, dbdict, trunk_url, debug=debug,
                          verbose=verbose)

    return
    releases = {}
    for entry in svn_list(releases_url, list_verbose=True, debug=debug,
                          verbose=verbose):
        size, user, date, filename = entry

        if filename.find("_debug") > 0 or filename.find("_rc") > 0 or \
          filename.find("-RC") > 0 or filename.endswith("dbg"):
            # ignore debugging versions and release candidates
            continue

        if not filename.endswith("/"):
            print("Ignoring non-directory \"%s\"" % (filename, ))
            continue

        # map release name to release date
        releases[filename[:-1]] = date

    for relname, reldate in sorted(releases.items(),
                                   key=lambda x: (x[1], x[0])):
        print("==> %s (%s)" % (relname, reldate))

        this_url = os.path.join(releases_url, relname)

        missing = __check_externals(known_projects, dbdict, this_url,
                                    debug=debug, verbose=verbose)
        if len(missing) > 0:
            print("!! %s requires %s" %
                  (relname, ", ".join(missing)), file=sys.stderr)


def process_submodules(pdaq_url, known_projects, debug=False, verbose=False):
    trunk_url = os.path.join(pdaq_url, "trunk")
    releases_url = os.path.join(pdaq_url, "releases")

    print("=== TRUNK")

    dbdict = {}

    releases = {}
    for entry in svn_list(releases_url, list_verbose=True, debug=debug,
                          verbose=verbose):
        size, user, date, filename = entry

        if filename.find("_debug") > 0 or filename.find("_rc") > 0 or \
          filename.find("-RC") > 0 or filename.endswith("dbg"):
            # ignore debugging versions and release candidates
            continue

        if not filename.endswith("/"):
            print("Ignoring non-directory \"%s\"" % (filename, ))
            continue

        # map release name to release date
        releases[filename[:-1]] = date

    for relname, reldate in sorted(releases.items(),
                                   key=lambda x: (x[1], x[0])):
        print("==> %s (%s)" % (relname, reldate))

        this_url = os.path.join(releases_url, relname)

        missing = __check_externals(known_projects, dbdict, this_url,
                                    debug=debug, verbose=verbose)
        if len(missing) > 0:
            print("!! %s requires %s" %
                  (relname, ", ".join(missing)), file=sys.stderr)


def main():
    parser = argparse.ArgumentParser()
    add_arguments(parser)
    args = parser.parse_args()

    known_projects = ("PyDOM", "cluster-config", "config", "config-scripts",
                      "daq-common", "daq-integration-test", "daq-io",
                      "daq-log", "daq-moni-tool", "daq-pom-config",
                      "daq-request-filler", "dash", "eventBuilder-prod",
                      "fabric-common", "icebucket", "juggler", "payload",
                      "payload-generator", "pdaq-user", "secondaryBuilders",
                      "splicer", "StringHub", "oldtrigger", "trigger",
                      "trigger-common", "trigger-testbed")

    pdaq_url = "http://code.icecube.wisc.edu/daq/meta-projects/pdaq"

    # create Git repo with subprojects
    workspace = os.getcwd()

    repo_path = os.path.join(workspace, "git-repo")
    scratch_path = os.path.join(workspace, "scratch")

    __initialize_git_trunk(repo_path, scratch_path, known_projects,
                           debug=args.debug, verbose=args.verbose)

    process_pdaq(pdaq_url, known_projects, debug=args.debug,
                 verbose=args.verbose)


if __name__ == "__main__":
   main()
