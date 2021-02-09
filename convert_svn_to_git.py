#!/usr/bin/env python

from __future__ import print_function

import argparse
import getpass
import os
import shutil
import sys
import tarfile
import time
import traceback

from datetime import datetime

from cmdrunner import CommandException, set_always_print_command
from github_util import GithubUtil
from git import git_add, git_autocrlf, git_checkout, git_commit, git_config, \
     git_fetch, git_init, git_push, git_remote_add, git_remove, git_reset, \
     git_show_hash, git_status, git_submodule_add, git_submodule_remove, \
     git_submodule_status, git_submodule_update
from i3helper import TemporaryDirectory, read_input
from mantis_converter import MantisConverter
from pdaqdb import PDAQManager
from project_db import AuthorDB
from svn import SVNConnectException, SVNException, SVNMetadata, \
     SVNNonexistentException, svn_checkout, svn_get_externals, svn_propget, \
     svn_switch


# dictionary which maps projects to their older names
FORKED_PROJECTS = {
    "oldtrigger": "trigger",
    "pdaq-user": "pdaq-icecube",
}


# ignore some 'pdaq' revisions
IGNORED_REVISIONS = {
    "pdaq": (14044, 14045, 14046, 14047, 14048, 14049, 14050, 14051)
}


def add_arguments(parser):
    "Add command-line arguments"

    parser.add_argument("-C", "--checkpoint", dest="checkpoint",
                        action="store_true", default=False,
                        help="Save sandbox to a tar file before each commit")
    parser.add_argument("-G", "--github", dest="use_github",
                        action="store_true", default=False,
                        help="Create the repository on GitHub")
    parser.add_argument("-O", "--organization", dest="organization",
                        default=None,
                        help="GitHub organization to use when creating the"
                        " repository")
    parser.add_argument("-M", "--mantis-dump", dest="mantis_dump",
                        default=None,
                        help="MySQL dump file of WIPAC Mantis repository")
    parser.add_argument("-P", "--private", dest="make_public",
                        action="store_false", default=True,
                        help="GitHub repository should be private")
    parser.add_argument("-Z", "--always-print-command", dest="print_command",
                        action="store_true", default=False,
                        help="Always print external commands before running")
    parser.add_argument("-s", "--sleep-seconds", dest="sleep_seconds",
                        type=int, default=1,
                        help="Number of seconds to sleep after GitHub"
                             " issue operations")
    parser.add_argument("-v", "--verbose", dest="verbose",
                        action="store_true", default=False,
                        help="Print details")
    parser.add_argument("-x", "--debug", dest="debug",
                        action="store_true", default=False,
                        help="Print debugging messages")

    parser.add_argument("--close-resolved", dest="close_resolved",
                        action="store_true", default=False,
                        help="Close GitHub issues which are marked as"
                             "'resolved' in Mantis")
    parser.add_argument("--load-from-log", dest="load_from_log",
                        action="store_true", default=False,
                        help="Read in Subversion log entries and save them"
                             " to the database before converting everything")
    parser.add_argument("--local-repo", dest="local_repo_path",
                        default=None,
                        help="Specify the local directory where Git repos"
                             " should be created; if not specified, a"
                             " temporary repo will be created and thrown away"
                             " on exit")
    parser.add_argument("--no-pause", dest="pause",
                        action="store_false", default=True,
                        help="Do not pause after an error before exiting")
    parser.add_argument("--preserve-all-status", dest="preserve_all_status",
                        action="store_true", default=False,
                        help="Preserve status of all Mantis issues")
    parser.add_argument("--preserve-resolved", dest="preserve_resolved_status",
                        action="store_true", default=False,
                        help="Preserve status of resolved Mantis issues")

    parser.add_argument(dest="svn_project", default=None,
                        help="Subversion/Mantis project name")


class GitRepoManager(object):
    __GIT_REPO_DICT = {}

    def __init__(self, use_github=False, local_repo_path=None,
                 sleep_seconds=None):
        if not use_github:
            if local_repo_path is None:
                raise Exception("Please specify the local directory where Git"
                                " repositories are stored")
            if os.path.exists(local_repo_path) and \
              not os.path.isdir(local_repo_path):
                raise Exception("Local repo \"%s\" exists and is not"
                                " a directory" % (local_repo_path, ))

        self.__use_github = use_github
        self.__local_repo_path = local_repo_path
        self.__sleep_seconds = sleep_seconds

    def __str__(self):
        return "GitRepoManager[%s,path=%s,sleep=%s]" % \
          ("GitHub" if self.__use_github else "LocalRepo",
           self.__local_repo_path, self.__sleep_seconds)

    @classmethod
    def __add_repo_to_cache(cls, project_name, git_repo):
        if project_name in cls.__GIT_REPO_DICT:
            raise Exception("Found existing cached repo for \"%s\"" %
                            (project_name, ))

        cls.__GIT_REPO_DICT[project_name] = git_repo

    @classmethod
    def __get_cached_repo(cls, project_name):

        return None if project_name not in cls.__GIT_REPO_DICT \
          else cls.__GIT_REPO_DICT[project_name]

    @classmethod
    def get_github_util(cls, project_name, organization, new_project_name,
                        make_public=False, sleep_seconds=None):
        # if the organization name was not specified,
        #  assume it is this user's name
        if organization is None:
            organization = getpass.getuser()

        # if requested, use a different repository name
        if new_project_name is None:
            repo_name = project_name
        else:
            repo_name = new_project_name

        ghutil = GithubUtil(organization, repo_name)
        ghutil.make_new_repo_public = make_public
        ghutil.sleep_seconds = sleep_seconds

        return ghutil

    def get_repo(self, project_name, organization=None, new_project_name=None,
                 description=None, destroy_old_repo=False, make_public=None,
                 debug=False, verbose=False):
        cached = self.__get_cached_repo(project_name)
        if cached is not None:
            return cached

        # if we're writing to a local repository...
        if not self.__use_github:
            if not os.path.exists(self.__local_repo_path):
                # create the top-level Git repository directory
                os.makedirs(self.__local_repo_path, mode=0o755)

            # create and return the local repository
            return GithubUtil.create_local_repo(self.__local_repo_path,
                                                project_name,
                                                destroy_existing=\
                                                destroy_old_repo,
                                                debug=debug, verbose=verbose)

        # connect to GitHub
        ghutil = self.get_github_util(project_name, organization,
                                      new_project_name,
                                      make_public=make_public,
                                      sleep_seconds=self.__sleep_seconds)

        # if description was not specified, build a default value
        # XXX add a more general solution here
        if description is None:
            description = "WIPAC's %s project" % (project_name, )

        return ghutil.get_github_repo(description=description,
                                      create_repo=destroy_old_repo,
                                      destroy_existing=destroy_old_repo,
                                      debug=debug, verbose=verbose)

    @property
    def local_repo_path(self):
        return self.__local_repo_path


def __build_externs_dict(rewrite_proc=None, sandbox_dir=None, debug=False,
                         verbose=False):
    "Build a dictionary combining SVN external projects with Git submodules"

    externs = {}
    for flds in svn_get_externals(sandbox_dir=sandbox_dir, debug=debug,
                                  verbose=verbose):
        sub_rev, sub_url, sub_dir = flds

        # fix any naming or URL problems
        if rewrite_proc is not None:
            sub_dir, sub_url, sub_rev = \
              rewrite_proc(sub_dir, sub_url, sub_rev, verbose=verbose)

        externs[sub_dir] = ExternMap(sub_dir, sub_url, sub_rev)

    for flds in git_submodule_status(sandbox_dir=sandbox_dir, debug=debug,
                                     verbose=verbose):
        sub_name, _, sub_hash, sub_branch = flds
        if sub_name not in externs:
            raise Exception("Found Git submodule \"%s\" but no SVN external" %
                            (sub_name, ))
        externs[sub_name].add_git(sub_hash, sub_branch)

    return externs


def __commit_to_git(project_name, entry, github_issues=None, allow_empty=False,
                    sandbox_dir=None, debug=False, verbose=False):
    """
    Commit an SVN change to git, return a tuple containing:
    (git_branch, git_hash, number_changed, number_inserted, number_deleted)
    """
    # insert the GitHub message ID if it was specified
    if github_issues is None:
        message = entry.log_message
    else:
        if len(github_issues) == 1:
            plural = ""
        else:
            plural = "s"
        message = "Issue%s %s: %s" % \
          (plural, ", ".join(str(x.number) for x in github_issues),
           entry.log_message)

    #read_input("%s %% Hit Return to commit: " % os.getcwd())
    try:
        flds = git_commit(author=AuthorDB.get_author(entry.author),
                          commit_message=message,
                          date_string=entry.date.isoformat(),
                          filelist=None, allow_empty=allow_empty,
                          commit_all=False, sandbox_dir=sandbox_dir,
                          debug=debug, verbose=verbose)
    except CommandException:
        print("ERROR: Cannot commit %s SVN rev %d (%s)" %
              (project_name, entry.revision, message), file=sys.stderr)
        read_input("%s %% Hit Return to exit: " % os.getcwd())
        raise

    return flds


def __create_gitignore(ignorelist, include_python=False, include_java=False,
                       sandbox_dir=None, debug=False, verbose=False):
    "Initialize .gitignore file using list from SVN's svn:ignore property"
    if sandbox_dir is None:
        path = ".gitignore"
    else:
        path = os.path.join(sandbox_dir, ".gitignore")

    with open(path, "w") as fout:
        if ignorelist is not None:
            for entry in ignorelist:
                print("%s" % str(entry), file=fout)
        print("# Ignore Subversion directory during Git transition\n.svn",
              file=fout)
        if include_python:
            print("\n# Java stuff\n*.class\ntarget", file=fout)
        if include_java:
            print("\n# Python stuff\n*.pyc\n__pycache__", file=fout)

    git_add(".gitignore", sandbox_dir=sandbox_dir, debug=debug,
            verbose=verbose)


def __gather_modifications(sandbox_dir=None, debug=False, verbose=False):
    additions = None
    deletions = None
    modifications = None
    staged = None

    for line in git_status(porcelain=True, sandbox_dir=sandbox_dir,
                           debug=debug, verbose=verbose):
        line = line.rstrip()
        if line == "":
            continue

        if len(line) < 4:
            raise Exception("Short procelain status line \"%s\"" % (line, ))

        if line[2] != " " and line[2] != "M":
            raise Exception("Bad porcelain status line \"%s\"" % (line, ))

        if line[1] == " ":
            # file is staged for commit
            if staged is None:
                staged = []
            staged.append(line[3:])
            continue

        if line[0] == "?" and line[1] == "?":
            # add unknown file
            if additions is None:
                additions = []
            additions.append(line[3:])
            continue

        if line[0] == " " or line[0] == "A" or line[0] == "M":
            if line[1] == "A":
                # file has been added
                if additions is None:
                    additions = []
                additions.append(line[3:])
                continue
            if line[1] == "D":
                # file has been deleted
                if deletions is None:
                    deletions = []
                deletions.append(line[3:])
                continue
            if line[1] == "M":
                # file has been modified
                if modifications is None:
                    modifications = []
                modifications.append(line[3:])
                continue

        raise Exception("Unknown porcelain line \"%s\"" % str(line))

    return additions, deletions, modifications, staged


def __get_mantis_projects(project_name):
    if project_name == "pdaq":
        return ("pDAQ", "dash", "pdaq-config", "pdaq-user")

    return (project_name, )


def __initialize_git_workspace(project_name, git_url, svn_url, revision,
                               create_empty_repo=False, rename_limit=None,
                               sandbox_dir=None, debug=False, verbose=False):
    # initialize the directory as a git repository
    git_init(sandbox_dir=sandbox_dir, debug=debug, verbose=verbose)

    # handle projects with large numbers of files
    if rename_limit is not None:
        git_config("diff.renameLimit", rename_limit, sandbox_dir=sandbox_dir,
                   debug=debug, verbose=verbose)

    if create_empty_repo:
        # allow old files with Windows-style line endings to be committed
        git_autocrlf(sandbox_dir=sandbox_dir, debug=debug, verbose=verbose)

        # get list of ignored entries from SVN
        ignorelist = __load_svn_ignore(svn_url, revision=revision, debug=debug,
                                       verbose=verbose)

        # create a .gitconfig file which ignores .svn as well as anything
        #  else which is already being ignored
        __create_gitignore(ignorelist, sandbox_dir=sandbox_dir,
                           debug=debug, verbose=verbose)

    # point the new git sandbox at the Github/local repo
    try:
        for _ in git_remote_add("origin", git_url, sandbox_dir=sandbox_dir,
                                debug=debug, verbose=verbose):
            pass
    except:
        read_input("%s %% Hit Return to exit: " % os.getcwd())
        raise

    if not create_empty_repo:
        git_fetch(fetch_all=True, sandbox_dir=sandbox_dir, debug=debug,
                  verbose=verbose)


def __initialize_svn_workspace(project_name, svn_url, revision,
                               sandbox_dir=None, debug=False, verbose=False):
    if debug:
        if sandbox_dir is None:
            sandbox_dir = os.path.join(os.getcwd(), project_name)
        print("Checkout %s rev %s in %s" % (svn_url, revision, sandbox_dir))

    svn_checkout(svn_url, revision, target_dir=sandbox_dir, debug=debug,
                 verbose=verbose)

    if debug:
        print("=== After checkout of %s ===" % svn_url)
        for dentry in os.listdir("."):
            print("\t%s" % str(dentry))

    # verify that project subdirectory was created
    if not os.path.exists(sandbox_dir):
        raise CommandException("Cannot find project subdirectory \"%s\""
                               " after checkout" % (project_name, ))


def __load_svn_ignore(svn_url, revision=None, debug=False, verbose=False):
    # get the list of ignored files from Subversion
    ignored = []
    try:
        for line in svn_propget(svn_url, "svn:ignore", revision=revision,
                                debug=debug, verbose=verbose):
            if line.startswith(".git") or line.find("/.git") >= 0:
                continue
            ignored.append(line)
    except CommandException as cex:
        errmsg = str(cex)
        # ignore error message about missing 'svn:ignore' property
        if errmsg.find("E200017") == 0 and errmsg.find("W200017") == 0:
            raise

    if len(ignored) == 0:
        ignored = None

    return ignored


def __progress_reporter(count, total, name, value_name, value):
    spaces = " "*30
    unspaces = "\b"*27  # leave a few spaces to separate error msgs

    print("\r #%d (of %d): %s %s %s%s%s" %
          (count, total, name, value_name, value, spaces, unspaces), end="")


def __push_to_remote_git_repo(git_remote, sandbox_dir=None, debug=False):
    failed = True
    err_exc = None
    err_buffer = []
    for attempt in (0, 1, 2):
        if attempt > 0:
            print("Retrying failed GIT PUSH")

        err_buffer = []
        try:
            for line in git_push(remote_name=git_remote, upstream="origin",
                                 sandbox_dir=sandbox_dir, debug=debug,
                                 verbose=debug):
                err_buffer.append(line)
            failed = False
            break
        except CommandException:
            err_exc = cex

    if failed:
        print(str(err_exc))
        for line in err_buffer:
            print("?? " + str(line))
        read_input("%s %% Hit Return to exit: " % os.getcwd())
        raise


def __diff_strings(str1, str2):
    """
    Compare two strings and return the substrings where they differ
    (e.g. "ABC/def" and "ABC/ddd" would return "ef" and "dd")
    """
    len1 = len(str1)
    len2 = len(str2)
    minlen = min(len1, len2)

    diff = None
    for idx in range(minlen):
        if str1[idx] != str2[idx]:
            diff = idx
            break

    if diff is not None:
        return str1[diff-1:], str2[diff-1:]

    if len1 == len2:
        return "", ""

    return str1[minlen:], str2[minlen:]


def __revert_forked_url(orig_url):
    global FORKED_PROJECTS

    for fork_prj, orig_prj in FORKED_PROJECTS.items():
        idx = orig_url.find(fork_prj)
        if idx < 0:
            continue

        flen = len(fork_prj)
        if orig_url[idx-1] != "/" or orig_url[idx+flen] != "/":
            continue

        return orig_url[:idx] + orig_prj + orig_url[idx+flen:]

    return None


def rewrite_pdaq(project_name, svn_url, revision, verbose=False):
    "Fix broken SVN url and/or revision"
    orig_name, orig_url, orig_rev = (project_name, svn_url, revision)  # XXX
    if project_name == "fabric_common":
        project_name = "fabric-common"

    if project_name == "PyDOM":
        if revision >= 14379 and revision < 17161:
            revision = 14222

    elif project_name == "StringHub":
        if revision == 13771:
            revision = 13770
        elif revision == 14379:
            revision = 14355
        elif revision == 14388:
            revision = 14384

    elif project_name == "cluster-config":
        if svn_url.endswith("/trunk") and revision == 1156:
            svn_url = svn_url[:-6] + "/releases/V10-00-02"
            revision = 1156

    elif project_name == "daq-common":
        if revision == 1362:
            revision = 1326
        elif revision == 14431:
            revision = 14168
        elif revision == 16087:
            revision = 16080

    elif project_name == "daq-integration-test":
        if revision == 14431:
            revision = 13886

    elif project_name == "daq-io":
        if revision == 14431:
            revision = 14365
        elif revision == 17421:
            revision = 17420

    elif project_name == "daq-log":
        if svn_url.endswith("/trunk") and revision == 875:
            svn_url = svn_url[:-6] + "/releases/V10-00-00"
            revision = 877
        elif revision >= 14379 and revision < 14519:
            revision = 14108

    elif project_name == "daq-moni-tool":
        if revision in (14379, 14388):
            revision = 14361
        elif revision == 14431:
            revision = 14403

    elif project_name == "daq-pom-config":
        if revision in (14379, 14388, 14431):
            revision = 12772

    elif project_name == "daq-request-filler":
        if revision >= 14379 and revision <= 14431:
            revision = 14293

    elif project_name == "dash":
        if revision == 2216:
            revision = 2218
        elif revision == 4836:
            revision = 4830
        elif revision == 14388:
            revision = 14380
        elif revision == 14431:
            revision = 14412
        elif svn_url.endswith("branches/Potosi") and revision == 16575:
            svn_url = svn_url[:-15] + "trunk"
            revision = 16561

    elif project_name == "eventBuilder-prod":
        if revision >= 14379 and revision <= 14431:
            revision = 14293
        elif revision == 16087:
            revision = 16079

    elif project_name == "juggler":
        if revision == 14431:
            revision = 14365

    elif project_name == "payload":
        if revision == 4815:
            revision = 4811
        elif revision == 14388:
            revision = 14386
        elif revision == 14431:
            revision = 14293

    elif project_name == "pdaq":
        if svn_url.endswith("releases/Karben4"):
            if revision == 15436:
                svn_url += "_rc1"
        elif svn_url.endswith("releases/Urban_Harvest7"):
            if revision == 17535:
                svn_url += "_rc1"

    if project_name == "secondaryBuilders":
        if revision >= 14379 and revision < 14431:
            revision = 13964
        elif revision == 16087:
            revision = 16079

    if project_name == "splicer":
        if revision == 14431:
            revision = 14156

    if project_name == "trigger":
        if revision == 13624:
            revision = 13622
        elif revision == 14431:
            revision = 14372

    if verbose:
        changed = False
        if orig_name == project_name:
            nstr = "name \"%s\"" % (project_name, )
        else:
            nstr = "name \"%s\"->\"%s\"" % (orig_name, project_name)
            changed = True
        if orig_url == svn_url:
            ustr = "url %s" % (orig_url, )
        else:
            orig_piece, svn_piece = __diff_strings(orig_url, svn_url)
            if orig_piece == svn_piece:
                ustr = "url %s(??)" % (orig_url, )
            else:
                ustr = " url \"%s\"->\"%s\"" % (orig_piece, svn_piece)
                changed = True
        if orig_rev == revision:
            rstr = "rev %s" % (revision, )
        else:
            rstr = "rev %s->%s" % (orig_rev, revision)
            changed = True

        if changed:
            print("\nXXX Rewrite %s %s %s" % (nstr, ustr, rstr))

    return project_name, svn_url, revision


def __stage_modifications(sandbox_dir=None, debug=False, verbose=False):
    """
    Revert any changes in the sandbox.
    Return True if there's nothing to commit, False otherwise
    """
    additions, deletions, modifications, _ = \
      __gather_modifications(sandbox_dir=sandbox_dir, debug=debug,
                             verbose=verbose)

    if debug:
        for pair in (("Additions", additions), ("Deletions", deletions),
                     ("Modifications", modifications)):
            if pair[1] is not None:
                print("=== %s" % pair[0])
                for fnm in pair[1]:
                    print("  %s" % str(fnm))

    # add/remove files to commit
    changed = False
    if deletions is not None:
        git_remove(filelist=deletions, sandbox_dir=sandbox_dir, debug=debug,
                   verbose=verbose)
        changed = True
    if additions is not None:
        if len(additions) > 0:
            git_add(filelist=additions, sandbox_dir=sandbox_dir, debug=debug,
                    verbose=verbose)
        changed = True
    if modifications is not None:
        git_add(filelist=modifications, sandbox_dir=sandbox_dir, debug=debug,
                verbose=verbose)
        changed = True

    return changed


def __switch_project(project_name, top_url, revision, ignore_externals=False,
                     sandbox_dir=None, debug=False, verbose=False):
    tmp_url = top_url
    switch_exc = None
    for _ in (0, 1, 2):
        try:
            for _ in svn_switch(tmp_url, revision=revision,
                                ignore_externals=ignore_externals,
                                sandbox_dir=sandbox_dir, debug=debug,
                                verbose=verbose):
                pass
            switch_exc = None
            break
        except SVNConnectException as exc:
            # if we couldn't connect to the SVN server, try again
            switch_exc = exc
            continue
        except SVNNonexistentException as exc2:
            # if we haven't used an alternate URL yet...
            switch_exc = exc2
            if tmp_url == top_url:
                tmp_url = __revert_forked_url(top_url)
                if tmp_url is not None:
                    # we found an alternate URL, try that
                    continue
            raise

    if switch_exc is not None:
        raise SVNException("Could not switch %s to rev %s after 3 attempts"
                           "\n\t(url %s)\n\t(%s)" %
                           (project_name, revision, top_url, exc))


def __update_both_sandboxes(project_name, gitmgr, sandbox_dir, svn_url,
                            svn_rev, git_branch, git_hash, debug=False,
                            verbose=False):

    if not os.path.exists(sandbox_dir):
        svn_checkout(svn_url, revision=svn_rev, target_dir=sandbox_dir,
                     debug=debug, verbose=verbose)
    else:
        __switch_project(project_name, svn_url, revision=svn_rev,
                         ignore_externals=True, sandbox_dir=sandbox_dir,
                         debug=debug, verbose=verbose)

    git_metadir = os.path.join(sandbox_dir, ".git")
    if not os.path.exists(git_metadir):
        # get the Github or local repo object
        gitrepo = gitmgr.get_repo(project_name, debug=debug, verbose=verbose)

        # initialize the local Git workspace/sandbox
        __initialize_git_workspace(project_name, gitrepo.ssh_url, svn_url,
                                   svn_rev, create_empty_repo=False,
                                   sandbox_dir=sandbox_dir, debug=debug,
                                   verbose=verbose)

    if not os.path.isdir(git_metadir):
        git_checkout(branch_name=git_branch, start_point=git_hash,
                     sandbox_dir=sandbox_dir, debug=debug, verbose=verbose)
    else:
        git_reset(start_point=git_hash, hard=True, sandbox_dir=sandbox_dir,
                  debug=debug, verbose=verbose)


def convert_revision(database, gitmgr, mantis_issues, count, top_url,
                     git_remote, entry, first_commit=False, rewrite_proc=None,
                     sandbox_dir=None, debug=False, verbose=False):
    # assume that the database name is the project name
    project_name = database.name

    # don't report progress if printing verbose/debugging messages
    if debug or verbose:
        progress_reporter = None
    else:
        progress_reporter = __progress_reporter

    # fix any naming or URL problems
    if rewrite_proc is None:
        revision = entry.revision
    else:
        new_name, top_url, revision = \
          rewrite_proc(database.name, top_url, entry.revision, verbose=verbose)
        if new_name != database.name:
            raise Exception("Cannot rewrite %s to %s" %
                            (database.name, new_name))

    if not first_commit:
        switch_and_update_externals(database, gitmgr, top_url, revision,
                                    entry.date_string,
                                    rewrite_proc=rewrite_pdaq,
                                    sandbox_dir=sandbox_dir, debug=debug,
                                    verbose=verbose)

        if count == 0:
            git_checkout(git_remote, new_branch=True, sandbox_dir=sandbox_dir,
                         debug=debug, verbose=verbose)

    # fetch the cached Git repository object
    gitrepo = gitmgr.get_repo(project_name, debug=debug, verbose=verbose)

    if mantis_issues is None or not gitrepo.has_issue_tracker:
        # don't open issues if we don't have any Mantis issues or
        # if we're not writing to a repo with an issue tracker
        github_issues = None
    else:
        # open/reopen GitHub issues
        github_issues = mantis_issues.open_github_issues(revision,
                                                         report_progress=\
                                                         progress_reporter)

    changed = __stage_modifications(sandbox_dir=sandbox_dir, debug=debug,
                                    verbose=verbose)
    if not changed and not first_commit:
        return False

    commit_result = __commit_to_git(project_name, entry, None,
                                    allow_empty=count == 0,
                                    sandbox_dir=sandbox_dir,
                                    debug=debug, verbose=verbose)

    # break tuple of results into separate values
    (git_branch, short_hash, changed, inserted, deleted) = \
      commit_result

    # get the full hash string for the commit
    full_hash = git_show_hash(sandbox_dir=sandbox_dir, debug=debug,
                              verbose=verbose)
    if not full_hash.startswith(short_hash):
        raise Exception("Expected %s hash %s to start with %s" %
                        (sandbox_dir, full_hash, short_hash))

    # write branch/hash info for this revision to database
    database.save_revision(revision, git_branch, full_hash)

    # if we opened one or more issues, close them now
    if github_issues is not None:
        if commit_result is None:
            message = "Nothing commited to git repo!"
        else:
            if changed is None or inserted is None or deleted is None:
                (changed, inserted, deleted) = (0, 0, 0)
            message = "[%s %s] %d changed, %d inserted, %d deleted" % \
              (git_branch, short_hash, changed, inserted, deleted)

        for github_issue in github_issues:
            mantis_issues.close_github_issue(github_issue, message)

    __push_to_remote_git_repo(git_remote, sandbox_dir=sandbox_dir, debug=debug)

    return True

def convert_svn_to_git(project, gitmgr, mantis_issues, git_url,
                       checkpoint=False, pause_interval=1800, pause_seconds=60,
                       rewrite_proc=None, debug=False, verbose=False):
    database = project.database

    # read in the Subversion log entries from the SVN server
    if database.has_unknown_authors:
        raise SystemExit("Please add missing author(s) before continuing")

    # we'll use the project name as the workspace directory name
    sandbox_dir = project.name

    initialized = False
    prev_checkpoint_list = None
    need_newline = False
    for branch_path, top_url, _ in database.project_urls(project.project_url):
        if branch_path is None:
            branch_path = SVNMetadata.TRUNK_NAME

        if branch_path == SVNMetadata.TRUNK_NAME:
            git_remote = "master"
        else:
            git_remote = branch_path.rsplit("/")[-1]
            if git_remote in ("HEAD", "master"):
                raise Exception("Questionable branch name \"%s\"" %
                                (git_remote, ))

        first_commit = False
        if not initialized:
            first_revision = database.find_first_revision(branch_path)
            if first_revision is None:
                raise Exception("Cannot find first revision for %s:%s" %
                                (database.name, branch_path))
            __initialize_svn_workspace(project.name, top_url, first_revision,
                                       sandbox_dir=sandbox_dir, debug=debug,
                                       verbose=verbose)

            # XXX turn this hack into something more generally useful
            if project.name == "config":
                rename_limit = 7000
            else:
                rename_limit = None

            __initialize_git_workspace(project.name, git_url, top_url,
                                       first_revision, create_empty_repo=True,
                                       rename_limit=rename_limit,
                                       sandbox_dir=sandbox_dir, debug=debug,
                                       verbose=verbose)
            initialized = True

            # if this is the first commit, the workspace is ready to commit
            first_commit = True

        # cache the last saved entry so we can link it to the next entry
        prev_saved = None

        start_time = datetime.now()
        num_entries = database.num_entries(branch_path)
        for count, entry in enumerate(database.entries(branch_path)):
            __progress_reporter(count + 1, num_entries, branch_path, "rev",
                                entry.revision)
            need_newline = True

            if database.name in IGNORED_REVISIONS and \
              entry.revision in IGNORED_REVISIONS[database.name]:
                print("Ignoring %s rev %s" % (database.name, entry.revision))
                continue

            if checkpoint:
                tarpaths = save_checkpoint_files(sandbox_dir, project.name,
                                                 branch_path, entry.revision,
                                                 gitmgr.local_repo_path)

                if prev_checkpoint_list is not None:
                    for path in prev_checkpoint_list:
                        if path is not None and os.path.exists(path):
                            os.unlink(path)

                prev_checkpoint_list = tarpaths

            if convert_revision(database, gitmgr, mantis_issues, count,
                                top_url, git_remote, entry,
                                first_commit=first_commit,
                                rewrite_proc=rewrite_proc,
                                sandbox_dir=sandbox_dir, debug=debug,
                                verbose=verbose):
                if prev_saved is not None:
                    entry.set_previous(prev_saved)
                    database.update_previous_in_database(entry)
                prev_saved = entry
                first_commit = False

                if mantis_issues is not None and \
                  pause_seconds is not None and \
                  pause_seconds > 0:
                    now_time = datetime.now()
                    elapsed = now_time - start_time
                    if elapsed.seconds > pause_interval:
                        print("\nPausing for %d seconds" % (pause_seconds, ))
                        time.sleep(pause_seconds)
                        start_time = now_time

        # if we printed any status lines, end on a new line
        if need_newline:
            print("")

    # add all remaining issues to GitHub
    if mantis_issues is not None and mantis_issues.has_issue_tracker:
        mantis_issues.add_issues(report_progress=__progress_reporter)

    # clean up unneeded checkpoint files
    if prev_checkpoint_list is not None:
        for path in prev_checkpoint_list:
            if path is not None and os.path.exists(path):
                os.unlink(path)


def get_pdaq_project(name, clear_tables=False, preload_from_log=False,
                     shallow=False, debug=False, verbose=False):
    try:
        project = PDAQManager.get(name)
        if project is None:
            raise Exception("Cannot find SVN project \"%s\"" % (name, ))
    except SVNNonexistentException:
        return None

    database = project.database
    if database.name != project.name:
        raise Exception("Expected database for \"%s\", not \"%s\"" %
                        (project.name, database.name))

    if not project.is_loaded:
        if not preload_from_log:
            project.load_from_db(shallow=shallow)

        if clear_tables:
            # remove old entries from database
            database.trim()

        if project.total_entries == 0:
            # close the database to clear any cached info
            project.close_db()

            # load log entries from all URLs
            #   and save any new entries to the database
            project.load_from_log(save_to_db=False, debug=debug,
                                  verbose=verbose)

    return project


def load_mantis_issues(database, gitrepo, mantis_dump, close_resolved=False,
                       preserve_all_status=False,
                       preserve_resolved_status=False, verbose=False):
    mantis_projects = __get_mantis_projects(database.name)
    if not verbose:
        print("Loading Mantis issues for %s" % ", ".join(mantis_projects))

    mantis_issues = MantisConverter(mantis_dump, database, gitrepo,
                                    mantis_projects, verbose=verbose)
    mantis_issues.close_resolved = close_resolved
    mantis_issues.preserve_all_status = preserve_all_status
    mantis_issues.preserve_resolved_status = preserve_resolved_status
    return mantis_issues


def save_checkpoint_files(workspace, project_name, branch_path, revision,
                          local_repo_path):
    tardir = "/tmp"
    suffix = ".tgz"

    base_name = "%s_%s_r%s" % \
      (project_name, branch_path.rsplit("/")[-1], revision)
    fullpath = os.path.join(tardir, base_name + suffix)

    curdir = os.getcwd()
    try:
        os.chdir(workspace)
        try:
            with tarfile.open(fullpath, mode="w:gz") as tar:
                tar.add(".", arcname=project_name)
        except KeyboardInterrupt:
            raise
        except:
            traceback.print_exc()
            print("Deleting failed workspace checkpoint file \"%s\"" %
                  (fullpath, ))
            os.unlink(fullpath)
            fullpath = None

        os.chdir(local_repo_path)
        path2 = os.path.join(tardir, "repo_" + base_name + suffix)
        try:
            with tarfile.open(path2, mode="w:gz") as tar:
                tar.add(project_name + ".git")
        except:
            traceback.print_stack()
            print("Deleting failed Git repo checkpoint file \"%s\"" %
                  (path2, ))
            os.unlink(path2)
            path2 = None

        return (fullpath, path2)
    finally:
        os.chdir(curdir)


class ExternMap(object):
    def __init__(self, name, svn_url, revision):
        self.__name = name
        self.__svn_url = svn_url
        self.__revision = revision

        self.__git_branch = None
        self.__git_hash = None

        self.__added = False

    def add_git(self, git_hash, git_branch):
        self.__git_branch = git_branch
        self.__git_hash = git_hash

    @property
    def has_git(self):
        return self.__git_branch is not None and self.__git_hash is not None

    @property
    def is_added(self):
        return self.__added

    def set_added(self, value):
        if value not in (True, False):
            raise Exception("Bad boolean value \"%s\"" % (value, ))
        self.__added = value


def switch_and_update_externals(database, gitmgr, top_url, revision,
                                date_string, rewrite_proc=None,
                                sandbox_dir=None, debug=False, verbose=False):
    # fix any naming or URL problems
    if rewrite_proc is not None:
        new_name, top_url, revision = \
          rewrite_proc(database.name, top_url, revision, verbose=verbose)
        if new_name != database.name:
            raise Exception("Cannot rewrite %s to %s" %
                            (database.name, new_name))

    externs = __build_externs_dict(rewrite_proc=rewrite_proc,
                                   sandbox_dir=sandbox_dir, debug=debug,
                                   verbose=verbose)

    try:
        __switch_project(database.name, top_url, revision=revision,
                         ignore_externals=True, sandbox_dir=sandbox_dir,
                         debug=debug, verbose=verbose)
    except SVNException as sex:
        if database.project_name == "cluster-config":
            sstr = str(sex)
            if sstr.find("E195012: ") and top_url.find("/retired") > 0:
                return
        raise

    # update all externals
    for flds in svn_get_externals(svn_url=top_url, revision=revision,
                                  sandbox_dir=sandbox_dir, debug=debug,
                                  verbose=verbose):
        # unpack the fields
        sub_rev, sub_url, sub_dir = flds

        if rewrite_proc is not None:
            # fix any naming or URL problems
            sub_dir, sub_url, sub_rev = \
              rewrite_proc(sub_dir, sub_url, sub_rev, verbose=verbose)

        # extract the project name and branch info from the URL
        _, sub_name, sub_branch = SVNMetadata.split_url(sub_url)
        if sub_name != sub_dir:
            print("ERROR: Expected %s, not %s in %s" %
                  (sub_dir, sub_name, sub_url))

        # get the SVNProject for this subproject
        sub_proj = get_pdaq_project(sub_name, shallow=True, debug=debug,
                                    verbose=verbose)

        # if no revision was specified in the svn:externals entry,
        #  find the last revision made to this subproject before the
        #  parent commit
        if sub_rev is None:
            if sub_proj is None or sub_proj.database is None:
                if sub_name != "anvil":
                    print("ERROR: Cannot fetch %s database" % (sub_name, ))
                continue

            sub_rev = sub_proj.database.find_revision_from_date(sub_branch,
                                                                date_string)
            if sub_rev is None:
                raise Exception("Cannot find %s revision for %s on %s" %
                                (sub_proj.name, sub_branch, date_string))

        # get the SVNEntry for this revision
        sub_entry = sub_proj.get_cached_entry(sub_rev)
        if sub_entry is None:
            if sub_branch == SVNMetadata.TRUNK_NAME:
                bstr = ""
            else:
                bstr = " (branch %s)" % (sub_branch, )
            raise Exception("Cannot find %s rev %s%s" %
                            (sub_proj.name, sub_rev, bstr))

        if sub_branch != sub_entry.branch_name:
            sub_branch = sub_entry.branch_name

        # find the previous SVN branch/revision and Git branch/hash
        prev_branch, prev_rev, git_branch, git_hash = \
          sub_proj.database.find_previous_revision(sub_branch, sub_entry)

        # build the full path to the subproject
        if sandbox_dir is None:
            sub_path = sub_dir
        else:
            sub_path = os.path.join(sandbox_dir, sub_dir)

        # build the URL for the previous entry and update everything
        prev_url = sub_proj.create_project_url(prev_branch)
        __update_both_sandboxes(sub_name, gitmgr, sub_path, prev_url, prev_rev,
                                git_branch, git_hash, debug=debug,
                                verbose=verbose)

        # find the hash which matches the current revision
        flds = \
          sub_proj.database.find_hash_from_revision(sub_branch, sub_rev,
                                                    with_git_hash=True)
        if flds is not None:
            new_git_branch, new_hash, new_svn_branch, new_rev = flds
            if sub_branch != new_svn_branch or sub_rev != new_rev:
                print("\n\t(%s falling back from %s rev %s to %s rev %s)" %
                      (sub_name, sub_branch, sub_rev, new_svn_branch, new_rev))
        else:
            new_svn_branch, new_rev, new_git_branch, new_hash = \
              sub_branch, sub_rev, git_branch, git_hash

        # update the SVN URL if necessary
        if sub_url.endswith(new_svn_branch):
            new_url = sub_url
        else:
            new_url = sub_proj.create_project_url(new_svn_branch)

        # update to the "real" revision
        __update_both_sandboxes(sub_name, gitmgr, sub_path, new_url, new_rev,
                                new_git_branch, new_hash, debug=debug,
                                verbose=verbose)

        # get the Github or local repo object
        subrepo = gitmgr.get_repo(sub_name, debug=debug, verbose=verbose)

        if sub_name not in externs:
            externs[sub_name] = ExternMap(sub_name, sub_url, sub_rev)
        if not externs[sub_name].has_git:
            git_submodule_add(subrepo.ssh_url, sandbox_dir=sandbox_dir,
                              debug=debug, verbose=verbose)
        else:
            git_submodule_update(sub_name, new_hash, sandbox_dir=sandbox_dir,
                                 debug=debug, verbose=verbose)

        externs[sub_name].set_added(True)

    for ext_dir, ext_map in sorted(externs.items(), key=lambda x: x[0]):
        if ext_map.is_added:
            continue

        if sandbox_dir is None:
            ext_path = ext_dir
        else:
            ext_path = os.path.join(sandbox_dir, ext_dir)

        git_submodule_remove(ext_dir, sandbox_dir=sandbox_dir, debug=debug,
                             verbose=verbose)

        if os.path.exists(ext_path):
            shutil.rmtree(ext_path)


#from profile_code import profile
#@profile(output_file="/tmp/profile.out", strip_dirs=True)
def main():
    parser = argparse.ArgumentParser()
    add_arguments(parser)
    args = parser.parse_args()

    if args.print_command:
        set_always_print_command(True)

    # the pDAQ projects store tags under the 'releases' subdirectory
    SVNMetadata.set_layout(SVNMetadata.DIRTYPE_TAGS, "releases")

    # set the directory where SVN project databases are created/found
    PDAQManager.set_home_directory()

    # load the map of SVN usernames to Git authors
    AuthorDB.load_authors("svn-authors", verbose=args.verbose)

    # force 'pdaq-user' to be a private Github project for security reasons
    make_public = args.make_public
    if args.use_github and make_public and args.svn_project == "pdaq-user":
        print("WARNING: Forcing 'pdaq-user' to be a private GitHub repository"
              " to protect included passwords and/or SSH keys",
              file=sys.stderr)
        make_public = False

    gitmgr = GitRepoManager(use_github=args.use_github,
                            local_repo_path=args.local_repo_path,
                            sleep_seconds=args.sleep_seconds)

    # fetch this project's info
    project = get_pdaq_project(args.svn_project, clear_tables=True,
                               preload_from_log=args.load_from_log,
                               debug=args.debug, verbose=args.verbose)

    # get the Github or local repo object
    gitrepo = gitmgr.get_repo(args.svn_project, organization=args.organization,
                              destroy_old_repo=True, make_public=make_public,
                              debug=args.debug, verbose=args.verbose)

    # if uploading to GitHub and we have a Mantis SQL dump file, load issues
    mantis_issues = None
    if args.use_github and gitrepo.has_issue_tracker and \
      args.mantis_dump is not None:
        mantis_issues = load_mantis_issues(project.database, gitrepo,
                                           args.mantis_dump,
                                           close_resolved=args.close_resolved,
                                           preserve_all_status=\
                                           args.preserve_all_status,
                                           preserve_resolved_status=\
                                           args.preserve_resolved_status,
                                           verbose=args.verbose)

    # execute everything in a temporary directory which will be erased on exit
    with TemporaryDirectory():
        print("Converting %s repo" % (args.svn_project, ))
        try:
            convert_svn_to_git(project, gitmgr, mantis_issues, gitrepo.ssh_url,
                               checkpoint=args.checkpoint, debug=args.debug,
                               verbose=args.verbose)
        except:
            if args.pause:
                read_input("%s %% Hit Return to abort: " % os.getcwd())
            raise


if __name__ == "__main__":
    main()
