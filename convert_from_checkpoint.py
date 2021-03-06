#!/usr/bin/env python

from __future__ import print_function

import argparse
import os
import shutil
import sys
import tarfile
import traceback

from cmdrunner import set_always_print_command
from i3helper import read_input
from pdaqdb import PDAQManager
from project_db import AuthorDB
from svn import SVNMetadata

from convert_svn_to_git import GitRepoManager, IGNORED_REVISIONS, \
     convert_revision, get_pdaq_project, load_mantis_issues, rewrite_pdaq, \
     save_checkpoint_files


TOPDIR = "/home/dglo/prj/pdaq-git/svn_tools"
WORKSPACE = os.path.join(TOPDIR, "debug_submod")
GIT_REPO = os.path.join(TOPDIR, "git-repo")


BASE_GIT_URL = "file://" + GIT_REPO
BASE_SVN_URL = "http://code.icecube.wisc.edu/daq/projects"


def add_arguments(parser):
    "Add command-line arguments"

    parser.add_argument("-B", "--branch", dest="test_branch",
                        default="trunk",
                        help="Project branch being tested")
    parser.add_argument("-C", "--checkpoint", dest="checkpoint",
                        action="store_true", default=False,
                        help="Save sandbox/repo to tar files before each rev")
    parser.add_argument("-G", "--github", dest="use_github",
                        action="store_true", default=False,
                        help="Create the repository on GitHub")
    parser.add_argument("-R", "--revision", dest="test_revision",
                        type=int, default=12322,
                        help="Project revision being tested")
    parser.add_argument("-Z", "--always-print-command", dest="print_command",
                        action="store_true", default=False,
                        help="Always print external commands before running")
    parser.add_argument("-v", "--verbose", dest="verbose",
                        action="store_true", default=False,
                        help="Print details")
    parser.add_argument("-x", "--debug", dest="debug",
                        action="store_true", default=False,
                        help="Print debugging messages")

    parser.add_argument("--load-from-database", dest="load_from_log",
                        action="store_false", default=True,
                        help="Instead of parsing the Subversion log entries,"
                             " load them from the database")

    parser.add_argument(dest="svn_project", default=None,
                        help="Subversion/Mantis project name")


def __replace_repo(project_name, srcdir, repodir, tarname):
    tarpath = os.path.join(srcdir, "repo_" + tarname + ".tgz")
    if not os.path.isfile(tarpath):
        raise Exception("Cannot find %s repo tarfile %s" %
                        (project_name, tarpath, ))

    # throw away previous repo
    oldrepo = os.path.join(repodir, project_name + ".git")
    if os.path.exists(oldrepo):
        shutil.rmtree(oldrepo)

    # extract files into repo subdirectory
    tar_in = tarfile.open(tarpath, "r")
    tar_in.extractall(repodir)


def __replace_workspace(srcdir, workspace, tarname):
    tarpath = os.path.join(srcdir, tarname + ".tgz")
    if not os.path.isfile(tarpath):
        raise Exception("Cannot find workspace tarfile %s" % (tarpath, ))

    # throw away previous workspace
    if os.path.exists(workspace):
        shutil.rmtree(workspace)

    # create the new workspace directory
    os.makedirs(workspace, mode=0o755)

    # extract sandbox files into workspace subdirectory
    tar_in = tarfile.open(tarpath, "r")
    tar_in.extractall(workspace)


def do_all_the_things(project, gitmgr, mantis_issues, test_branch,
                      test_revision, checkpoint=False, workspace=None,
                      debug=False, verbose=False):
    print("Loading %s database" % (project.name, ))
    database = project.database

    sandbox_dir = os.path.join(workspace, database.name)
    if not os.path.exists(sandbox_dir):
        raise Exception("Sandbox %s does not exist" % (sandbox_dir, ))

    prev_checkpoint_list = None
    for top_url, first_revision, first_date in database.all_urls_by_date:
        _, project_name, branch_path = SVNMetadata.split_url(top_url)

        if project_name != project.name:
            print("WARNING: Found URL for \"%s\", not \"%s\"\n    (URL %s)" %
                  (project_name, project.name, top_url), file=sys.stderr)

        # extract the branch name from the branch path
        #  (e.g. "tags/foo" -> "foo")
        short_branch = branch_path.rsplit("/")[-1]

        # if we're looking for a starting branch...
        if test_branch is not None:
            # ...and this isn't that branch...
            if test_branch != short_branch:
                # ..then skip this branch
                continue

            # we made it to the requested branch, stop looking
            test_branch = None

        # derive the Git remote name from the SVN branch name
        if branch_path == SVNMetadata.TRUNK_NAME:
            git_remote = "master"
        else:
            git_remote = short_branch
            if git_remote in ("HEAD", "master"):
                raise Exception("Questionable branch name \"%s\"" %
                                (git_remote, ))

        print("%s branch %s first_rev %s (%s)\n\t%s" %
              (database.name, branch_path, first_revision, first_date, top_url))
        prev_saved = None
        for count, entry in enumerate(database.entries(branch_path)):
            if test_revision is not None:
                if entry.revision < test_revision:
                    continue
                test_revision = None

            if database.name in IGNORED_REVISIONS and \
              entry.revision in IGNORED_REVISIONS[database.name]:
                print("Ignoring %s rev %s" % (database.name, entry.revision))
                continue

            if checkpoint:
                tarpaths = save_checkpoint_files(sandbox_dir, database.name,
                                                 branch_path, entry.revision,
                                                 gitmgr.local_repo_path)

                if prev_checkpoint_list is not None:
                    for path in prev_checkpoint_list:
                        if path is not None and os.path.exists(path):
                            os.unlink(path)

                prev_checkpoint_list = tarpaths

            print("Convert %s" % str(entry))
            try:
                if convert_revision(database, gitmgr, mantis_issues, count,
                                    top_url, git_remote, entry,
                                    first_commit=False,
                                    rewrite_proc=rewrite_pdaq,
                                    sandbox_dir=sandbox_dir, debug=debug,
                                    verbose=verbose):
                    if prev_saved is not None:
                        entry.set_previous(prev_saved)
                        database.update_previous_in_database(entry)
                    prev_saved = entry
            except:
                traceback.print_exc()
                print("Failed while converting %s rev %s" %
                      (top_url, entry.revision))
                read_input("%s %% Hit Return to exit: " % os.getcwd())
                return

    # clean up unneeded checkpoint files
    if prev_checkpoint_list is not None:
        for path in prev_checkpoint_list:
            if path is not None and os.path.exists(path):
                os.unlink(path)


def init_from_checkpoint(srcdir, repodir, workspace, project, svn_branch,
                         revision, debug=False, verbose=False):
    # make sure the tarfile exists
    tarname = "%s_%s_r%d" % (project.name, svn_branch, revision)

    __replace_repo(project.name, srcdir, repodir, tarname)
    __replace_workspace(srcdir, workspace, tarname)


def list_directory(topdir, title=None):
    if not os.path.isdir(topdir):
        if title is None:
            tstr = ""
        else:
            tstr = " (%s)" % (title, )
        print("!!! %s does not exist%s!!!" % (topdir, tstr), file=sys.stderr)
        return

    if title is None:
        title = topdir

    print("/// %s \\\\\\" % (title, ))
    subdirs = []
    for entry in sorted(os.listdir(topdir)):
        path = os.path.join(topdir, entry)
        if os.path.isdir(path):
            subdirs.append(path)
            continue
        print("\t%s" % (path, ))

    for subdir in sorted(subdirs):
        print("\t%s/" % (subdir, ))


#from profile_code import profile
#@profile(output_file="/tmp/profile.out", strip_dirs=True)
def main():
    parser = argparse.ArgumentParser()
    add_arguments(parser)
    args = parser.parse_args()

    if args.print_command:
        set_always_print_command(True)

    SVNMetadata.set_layout(SVNMetadata.DIRTYPE_TAGS, "releases")

    PDAQManager.set_home_directory()
    AuthorDB.load_authors("svn-authors", verbose=args.verbose)

    gitmgr = GitRepoManager(use_github=False, local_repo_path=GIT_REPO,
                            sleep_seconds=1)

    print("Fetching %s" % (args.svn_project, ))
    project = get_pdaq_project(args.svn_project, clear_tables=False,
                               preload_from_log=args.load_from_log,
                               debug=args.debug, verbose=args.verbose)

    init_from_checkpoint(TOPDIR, GIT_REPO, WORKSPACE, project,
                         args.test_branch, args.test_revision,
                         debug=args.debug, verbose=args.verbose)

    # verify that we're ready to start
    subdir = os.path.join(WORKSPACE, args.svn_project)
    if not os.path.exists(subdir):
        raise SystemExit("Checkpoint file did not contain '%s'" %
                         (args.svn_project, ))

    if args.debug:
        list_directory(subdir, title="Initial")

    # if uploading to GitHub and we have a Mantis SQL dump file, load issues
    mantis_issues = None
    if args.use_github and args.mantis_dump is not None:
        # get the Github or local repo object
        gitrepo = gitmgr.get_repo(args.svn_project,
                                  organization=args.organization,
                                  destroy_old_repo=True,
                                  make_public=args.make_public,
                                  debug=args.debug, verbose=args.verbose)

        if gitrepo.has_issue_tracker:
            mantis_issues = load_mantis_issues(project.database, gitrepo,
                                               args.mantis_dump,
                                               close_resolved=\
                                               args.close_resolved,
                                               preserve_all_status=\
                                               args.preserve_all_status,
                                               preserve_resolved_status=\
                                               args.preserve_resolved_status,
                                               verbose=args.verbose)

    print("Testing %s" % (args.svn_project, ))
    do_all_the_things(project, gitmgr, mantis_issues, args.test_branch,
                      args.test_revision, checkpoint=args.checkpoint,
                      workspace=WORKSPACE, debug=args.debug,
                      verbose=args.verbose)


if __name__ == "__main__":
    main()
