#!/usr/bin/env python

from __future__ import print_function

import argparse
import os
import shutil
import sys

from git import git_clone, git_commit, git_submodule_add, git_submodule_status
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


def __check_hashes(dbdict, svn_url, git_sandbox, debug=False, verbose=False):
    projects = {}
    for revision, url, subdir in svn_get_externals(svn_url, debug=debug,
                                                   verbose=verbose):
        if subdir not in dbdict:
            raise Exception("\"%s\" is not a valid project" % (subdir, ))

        svndb = dbdict[subdir]

        found_rev, git_branch, git_hash = svndb.find_revision(revision)
        if found_rev is None or git_branch is None or git_hash is None:
            raise Exception("Cannot find Git hash for %s revision %s" %
                            (subdir, revision))

        projects[subdir] = (found_rev, git_branch, git_hash)

    print("Sandbox: %s" % (git_sandbox, ))
    num_proj = 0
    for line in git_submodule_status(git_sandbox, debug=debug,
                                     verbose=verbose):

        print("RAWSTAT>> %s<%s>" % (line, type(line)))
        full_hash, project, branch = line.rstrip().split(' ', 2)

        print("%s -> %s :: %s" % (project, full_hash, branch))
        if project not in projects:
            print("WARNING: Unknown project \"%s\"" % (project, ),
                  file=sys.stderr)
            continue

        found_rev, git_branch, git_hash = projects[project]
        del projects[project]
        if not full_hash.startswith(git_hash):
            print("ERROR: \"%s\" hash mismatch; expected %s, not %s" %
                  (project, git_hash, full_hash), file=sys.stderr)
            continue

        num_proj += 1

    if len(projects) != 0:
        raise Exception("Did not validate %d projects: %s" %
                        (len(projects), ", ".join(projects.keys())))
    print("Validated %d projects" % (num_proj, ))

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

    pdaq_path = os.path.join(scratch_path, "pdaq")

    # create all submodules
    for proj in known_projects:
        proj_path = os.path.join(repo_path, proj)
        if not os.path.isdir(proj_path):
            raise SystemExit("Cannot find Git project \"%s\"" % (proj, ))

        git_submodule_add(git_url(repo_path, proj), sandbox_dir=pdaq_path,
                          debug=debug, verbose=verbose)

    # commit the trunk
    details = git_commit(pdaq_repo, commit_message="Add add submodules",
                         commit_all=True, debug=debug, verbose=verbose)

    return pdaq_path


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


def process_pdaq(known_projects, top_svn_url, git_sandbox, debug=False,
                 verbose=False):
    trunk_url = os.path.join(top_svn_url, "trunk")

    print("=== TRUNK")

    dbdict = __open_databases(known_projects, trunk_url, debug=debug,
                              verbose=verbose)

    _ = __check_hashes(dbdict, trunk_url, git_sandbox, debug=debug,
                          verbose=verbose)
    return

    releases_url = os.path.join(top_svn_url, "releases")

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

    top_svn_url = "http://code.icecube.wisc.edu/daq/meta-projects/pdaq"

    # create Git repo with subprojects
    workspace = os.getcwd()

    repo_path = os.path.join(workspace, "git-repo")
    scratch_path = os.path.join(workspace, "scratch")

    git_sandbox = __initialize_git_trunk(repo_path, scratch_path,
                                         known_projects, debug=args.debug,
                                         verbose=args.verbose)

    process_pdaq(known_projects, top_svn_url, git_sandbox, debug=args.debug,
                 verbose=args.verbose)


if __name__ == "__main__":
   main()
