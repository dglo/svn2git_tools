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

from cmdrunner import CommandException, run_command, set_always_print_command
from github_util import GitRepoManager
from git import GitAddIgnoredException, GitException, git_add, git_autocrlf, \
     git_checkout, git_commit, git_config, git_fetch, git_init, git_pull, \
     git_push, git_remote_add, git_remove, git_reset, git_rev_parse, \
     git_show_hash, git_status, git_submodule_add, git_submodule_remove, \
     git_submodule_status, git_submodule_update
from i3helper import TemporaryDirectory, read_input
from mantis_converter import MantisConverter
from pdaqdb import PDAQManager
from project_db import AuthorDB
from svn import AcceptType, SVNBadAncestryException, SVNConnectException, \
     SVNException, SVNMergeConflictException, SVNMetadata, \
     SVNNonexistentException, svn_checkout, svn_get_externals, svn_info, \
     svn_propget, svn_revert, svn_switch


# dictionary which maps projects to their older names
FORKED_PROJECTS = {
    "oldtrigger": "trigger",
    "pdaq-user": "pdaq-icecube",
}

# dictionary which maps Git projects to their Subversion names
RENAMED_PROJECTS = {
    "eventBuilder": "eventBuilder-prod",
    "stf-gen1": "stf",
}

# ignore some 'pdaq' revisions
IGNORED_REVISIONS = {
    "pdaq": (14044, 14045, 14046, 14047, 14048, 14049, 14050, 14051)
}

# name used for main branch (the branch formerly known as "master")
GITHUB_MAIN_BRANCH = "main"
# name used for trunk when it is replaced by a branch
GITHUB_DEMOTED_BRANCH = "not_trunk"


def add_arguments(parser):
    "Add command-line arguments"

    parser.add_argument("-B", "--trunk-branch", dest="trunk_branch",
                        default=None,
                        help="Name of branch which should be used as the"
                        "main branch")
    parser.add_argument("-C", "--checkpoint", dest="checkpoint",
                        action="store_true", default=False,
                        help="Save sandbox to a tar file before each commit")
    parser.add_argument("-E", "--early-exit", dest="early_exit",
                        type=int, default=None,
                        help="Maximum revision number to convert"
                             " (useful while debugging")
    parser.add_argument("-G", "--github", dest="use_github",
                        action="store_true", default=False,
                        help="Create the repository on GitHub")
    parser.add_argument("-M", "--mantis-dump", dest="mantis_dump",
                        default=None,
                        help="MySQL dump file of WIPAC Mantis repository")
    parser.add_argument("-O", "--organization", dest="organization",
                        default=None,
                        help="GitHub organization to use when creating the"
                        " repository")
    parser.add_argument("-P", "--private", dest="make_public",
                        action="store_false", default=True,
                        help="GitHub repository should be private")
    parser.add_argument("-S", "--svn-project", dest="svn_project",
                        default=None,
                        help="Name of project in SVN repository (if different"
                             " from Git project name)")
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
    parser.add_argument("--noisy", dest="noisy",
                        action="store_true", default=False,
                        help="Print new hash after each commit")
    parser.add_argument("--preserve-all-status", dest="preserve_all_status",
                        action="store_true", default=False,
                        help="Preserve status of all Mantis issues")
    parser.add_argument("--preserve-resolved", dest="preserve_resolved_status",
                        action="store_true", default=False,
                        help="Preserve status of resolved Mantis issues")

    parser.add_argument(dest="svn_project", default=None,
                        help="Subversion/Mantis project name")


class CompareSandboxes(object):
    @classmethod
    def __compare_hashes(cls, project_name, release, revision, git_branch,
                         git_hash):
        try:
            project = PDAQManager.get(project_name, renamed=RENAMED_PROJECTS)
            if project is None:
                raise Exception("Cannot find SVN project \"%s\"" %
                                (project_name, ))
        except SVNNonexistentException:
            raise Exception("Cannot find %s database" % (project_name, ))

        flds = project.database.find_log_entry(project_name, revision=revision)
        if flds is None:
            print("ERROR: %s rev %s not found for %s (git %s:%s)" %
                  (release, revision, project_name, git_branch, git_hash),
                  file=sys.stderr)
        else:
            (svn_branch, svn_revision, prev_revision, tmp_branch, tmp_hash,
             date, message) = flds

            if tmp_hash == git_hash:
                print("== %s: %s rev %s => %s" %
                      (project_name, svn_branch, svn_revision, git_hash))
            else:
                flds = project.database.find_log_entry(project_name,
                                                       git_hash=git_hash,
                                                       svn_branch=release)
                if flds is None:
                    flds = ("unknown", "???", None, None, None, None, None)
                (new_branch, new_revision, _, _, _, _, _) = flds

                print("!! %s mismatch" % (project_name, ))
                print("   SVN: %s rev %s (prev %s), git %s:%s" %
                      (svn_branch, svn_revision, prev_revision, tmp_branch,
                       tmp_hash[:7]))
                print("   Git: %s rev %s, git %s/%s" %
                      (new_branch, new_revision, git_branch, git_hash[:7]))

    @classmethod
    def __prune_url(cls, url, project_name):
        idx = url.find("projects/")
        if idx < 0:
            return url

        flds = url[idx+9:].split("/", 1)
        if len(flds) == 1 and flds[0] == project_name:
            return SVNMetadata.TRUNK_NAME

        if len(flds) != 2:
            print("WARNING: Bad split of \"%s\" into %s" % (url[idx+9:], flds))
            return url

        name, branch = flds
        if project_name != name:
            if project_name not in RENAMED_PROJECTS or \
              RENAMED_PROJECTS[project_name] != name:
                print("WARNING: Expected \"%s\", not \"%s\" from %s" %
                      (project_name, name, url), file=sys.stderr)
            return url

        return branch

    @classmethod
    def compare(cls, project_name, svn_sandbox, git_sandbox, debug=False,
                verbose=False):
        # get Git hash for top directory
        top_branch = git_rev_parse("HEAD", abbrev_ref=True,
                                   sandbox_dir=git_sandbox, debug=debug,
                                   verbose=verbose)
        top_hash = git_rev_parse("HEAD", sandbox_dir=git_sandbox, debug=debug,
                                 verbose=verbose)

        # get Git hashes for all submodules
        hashdict = {}
        for flds in git_submodule_status(sandbox_dir=git_sandbox, debug=debug,
                                         verbose=verbose):
            (subname, _, subhash, subbranch) = flds
            hashdict[subname] = (subbranch, subhash)

        # get information about the top-level SVN project
        infodict = svn_info(svn_sandbox, debug=debug, verbose=verbose)

        # get the name of this project
        idx = infodict.url.find("projects/")
        if idx < 0:
            raise Exception("Cannot extract project name from \"%s\"" %
                            (infodict.url, ))
        info_name = infodict.url[idx+9:].split("/", 1)[0]
        if project_name != info_name:
            if project_name not in RENAMED_PROJECTS or \
              RENAMED_PROJECTS[project_name] != info_name:
                print("WARNING: Expected %s url while comparing, not <%s>" %
                      (project_name, infodict.url), file=sys.stderr)

        # extract SVN release/revision from top-level 'svn info'
        top_release = cls.__prune_url(infodict.url, project_name)
        top_revision = infodict.last_changed_rev

        revdict = {}
        for flds in svn_get_externals(sandbox_dir=svn_sandbox, debug=debug,
                                      verbose=verbose):
            # unpack the fields
            sub_rev, sub_url, sub_dir = flds

            # get Subversion info for the subproject
            try:
                infodict = svn_info(os.path.join(svn_sandbox, sub_dir),
                                    debug=debug, verbose=verbose)
            except SVNNonexistentException as exc:
                print("WARNING: Ignoring non-SVN subproject \"%s\": %s" %
                      (sub_dir, exc), file=sys.stderr)
                continue

            info_rev = int(infodict.last_changed_rev)
            if sub_rev is None:
                sub_rev = info_rev
            elif sub_rev != info_rev:
                print("WARNING: Expected %s rev %s, but subproject is rev %s" %
                      (sub_dir, sub_rev, info_rev), file=sys.stderr)

            if sub_dir not in hashdict:
                git_branch, git_hash = (None, None)
            else:
                git_branch, git_hash = hashdict[sub_dir]

            revdict[sub_dir] = (cls.__prune_url(sub_url, sub_dir),
                                None if sub_rev is None else int(sub_rev),
                                cls.__prune_url(infodict.url, sub_dir),
                                None if infodict.last_changed_rev is None
                                else int(infodict.last_changed_rev),
                                git_branch, git_hash)

        cls.__compare_hashes(project_name, top_release, top_revision,
                             top_branch, top_hash)

        for subname, flds in sorted(revdict.items(), key=lambda x: x[0]):
            oldrelease, oldrevision, subrelease, subrevision, subbranch, \
              subhash = flds
            cls.__compare_hashes(subname, subrelease, subrevision, subbranch,
                                 subhash)


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


def __categorize_files(topdir, filetypes=None):
    if filetypes is None:
        filetypes = {}

    subdirs = []
    for entry in os.listdir(topdir):
        subdir = os.path.join(topdir, entry)
        if os.path.isdir(subdir):
            # omit a few metadirectories
            if subdir.endswith("/.git") or subdir.endswith("/.hg") or \
              subdir.endswith("/.svn"):
                subdirs.append(subdir)
        elif entry.endswith(".py"):
            filetypes["python"] = True
        elif entry.endswith(".java"):
            filetypes["java"] = True

    for subdir in subdirs:
        __categorize_files(subdir, filetypes=filetypes)

    return filetypes


def __commit_to_git(project_name, revision, author, commit_date, log_message,
                    github_issues=None, allow_empty=False, sandbox_dir=None,
                    debug=False, verbose=False):
    """
    Commit an SVN change to git, return a tuple containing:
    (git_branch, git_hash, number_changed, number_inserted, number_deleted)
    """
    # insert the GitHub message ID if it was specified
    if github_issues is None:
        full_message = log_message
    else:
        full_message = "Issue%s %s%s" % \
          ("" if len(github_issues) == 1 else "s",
           ", ".join(str(x.number) for x in github_issues),
           "" if log_message is None else ": %s" % str(log_message))

    #read_input("%s %% Hit Return to commit: " % os.getcwd())
    try:
        flds = git_commit(author=AuthorDB.get_author(author),
                          commit_message=full_message,
                          date_string=commit_date.isoformat(),
                          filelist=None, allow_empty=allow_empty,
                          commit_all=False, sandbox_dir=sandbox_dir,
                          debug=debug, verbose=verbose)
    except CommandException:
        if full_message is None:
            mstr = ""
        else:
            mstr = " (%s)" % str(full_message)[:30]
        print("ERROR: Cannot commit %s SVN rev %d%s" %
              (project_name, revision, mstr), file=sys.stderr)
        read_input("%s %% Hit Return to exit: " % os.getcwd())
        raise

    return flds


def __create_git_repo(sandbox_dir=None, debug=False, verbose=False):
    initdir = os.path.abspath(sandbox_dir)
    with TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir.name, "HEAD"), "w") as fout:
            print("ref: refs/heads/%s" % (GITHUB_MAIN_BRANCH, ), file=fout)
        git_init(template=tmpdir.name, sandbox_dir=initdir, debug=debug,
                 verbose=verbose)

def __create_gitignore(ignorelist=None, include_python=False,
                       include_java=False, sandbox_dir=None):
    "Initialize .gitignore file"

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
        if include_java:
            print("\n# Java stuff\n*.class\ntarget", file=fout)
        if include_python:
            print("\n# Python stuff\n*.pyc\n__pycache__", file=fout)


def __delete_untracked(git_sandbox, debug=False, verbose=False):
    untracked = False
    for line in git_status(sandbox_dir=git_sandbox, debug=debug,
                           verbose=verbose):
        if line.startswith("#"):
            if len(line) <= 1 or not line[1].isspace():
                line = line[1:]
            else:
                line = line[2:]

        if not untracked:
            if line.startswith("Untracked files:"):
                untracked = True
            continue

        filename = line.strip()
        if len(filename) == 0 or filename.find("use \"git add") >= 0:
            continue

        if filename.endswith("/"):
            shutil.rmtree(os.path.join(git_sandbox, filename[:-1]))
        else:
            os.remove(os.path.join(git_sandbox, filename))


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


def __fix_gitignore_conflict(sandbox_dir, debug=False, verbose=False):
    "Restore SVN copy of .gitignore, then add our comment and '.svn'"
    path = os.path.join(sandbox_dir, ".gitignore")

    if verbose:
        print("=== Before revert")
        with open(path, "r") as fin:
            for line in fin:
                print(line.rstrip())

    # restore the SVN copy of .gitignore
    svn_revert(".gitignore", sandbox_dir=sandbox_dir, debug=debug,
               verbose=verbose)

    if verbose:
        if not os.path.exists(path):
            print("'revert' deleted .gitignore")
        else:
            print("=== After revert")
            with open(path, "r") as fin:
                for line in fin:
                    print(line.rstrip())

    found_svn = False
    ignorelist = []

    # get list of ignored files, and check that '.svn' is on the list
    if os.path.exists(path):
        with open(path, "r") as fin:
            for line in fin:
                line = line.rstrip()
                if line == ".svn":
                    found_svn = True
                ignorelist.append(line)

    if not found_svn:
        # find all the filetypes in this project
        filetypes = __categorize_files(sandbox_dir)

        # SVN version isn't ignoring '.svn', add it now
        __create_gitignore(ignorelist=ignorelist,
                           include_python="python" in filetypes,
                           include_java="java" in filetypes,
                           sandbox_dir=sandbox_dir)

        if verbose:
            print("=== After fix")
            with open(path, "r") as fin:
                for line in fin:
                    print(line.rstrip())

def __fix_status_filename(filename):
    if len(filename) >= 2 and filename[0] == filename[-1] and \
      (filename[0] == '"' or filename[0] == "'"):
        return filename[1:-1]

    return filename


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
            raise Exception("Short porcelain status line \"%s\"" % (line, ))

        if line[2] != " " and line[2] != "M":
            raise Exception("Bad porcelain status line \"%s\"" % (line, ))

        if line[1] == " ":
            # file is staged for commit
            if staged is None:
                staged = []
            staged.append(__fix_status_filename(line[3:]))
            continue

        if line[0] == "?" and line[1] == "?":
            # add unknown file
            if additions is None:
                additions = []
            additions.append(__fix_status_filename(line[3:]))
            continue

        if line[0] == " " or line[0] == "A" or line[0] == "M":
            if line[1] == "A":
                # file has been added
                if additions is None:
                    additions = []
                additions.append(__fix_status_filename(line[3:]))
                continue
            if line[1] == "D":
                # file has been deleted
                if deletions is None:
                    deletions = []
                deletions.append(__fix_status_filename(line[3:]))
                continue
            if line[1] == "M" or line[1] == "T":
                # file has been modified
                if modifications is None:
                    modifications = []
                modifications.append(__fix_status_filename(line[3:]))
                continue

        raise Exception("Unknown porcelain line \"%s\"" % str(line))

    return additions, deletions, modifications, staged


def __get_mantis_projects(project_name):
    if project_name == "pdaq":
        return ("pDAQ", "dash", "pdaq-config", "pdaq-user")
    elif project_name == "eventBuilder":
        return ("eventBuilder-prod", )
    elif project_name == "stf-gen1":
        return ("stf-prod", )
    elif project_name == "trigger":
        return ("globalTrig", "globalTrig-prod", "icetopTrig")

    return (project_name, )


def __initialize_git_workspace(git_url, svn_url, revision,
                               create_empty_repo=False, rename_limit=None,
                               sandbox_dir=None, debug=False, verbose=False):
    # initialize Git repo
    __create_git_repo(sandbox_dir=sandbox_dir, debug=debug, verbose=debug)

    # handle projects with large numbers of files
    if rename_limit is not None:
        git_config("diff.renameLimit", value=rename_limit,
                   sandbox_dir=sandbox_dir, debug=debug, verbose=verbose)

    if create_empty_repo:
        # allow old files with Windows-style line endings to be committed
        git_autocrlf(sandbox_dir=sandbox_dir, debug=debug, verbose=verbose)

        # get list of ignored entries from SVN
        ignorelist = __load_svn_ignore(svn_url, revision=revision, debug=debug,
                                       verbose=verbose)

        # find all the filetypes in this project
        filetypes = __categorize_files(sandbox_dir)

        # create a .gitignore file which ignores .svn as well as anything
        #  else which is already being ignored
        __create_gitignore(ignorelist=ignorelist,
                           include_python="python" in filetypes,
                           include_java="java" in filetypes,
                           sandbox_dir=sandbox_dir)

        # add the new .gitignore to the Git project
        git_add(".gitignore", sandbox_dir=sandbox_dir, debug=debug,
                verbose=verbose)



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

    msg = " #%d (of %d): %s %s %s" % (count, total, name, value_name, value)
    spacelen = 77 - len(msg)

    spaces = " "*spacelen
    unspaces = "\b"*(spacelen-3)
    print("\r%s%s%s" % (msg, spaces, unspaces), end="")
    sys.stdout.flush()


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
        except CommandException as cex:
            err_exc = cex
            # wait a couple of seconds and maybe the problem wil go away
            time.sleep(2)

    if failed:
        print(str(err_exc))
        for line in err_buffer:
            print("?? " + str(line))
        read_input("%s %% Hit Return to exit: " % os.getcwd())
        raise err_exc


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


def __stage_modifications(sandbox_dir=None, debug=False, verbose=False):
    """
    Revert any changes in the sandbox.
    Return True if there's nothing to commit, False otherwise
    """
    additions, deletions, modifications, staged = \
      __gather_modifications(sandbox_dir=sandbox_dir, debug=debug,
                             verbose=verbose)

    if debug:
        for pair in (("Additions", additions), ("Deletions", deletions),
                     ("Modifications", modifications), ("Pre-Staged", staged)):
            if pair[1] is not None:
                print("=== %s" % pair[0])
                for fnm in pair[1]:
                    print("  %s" % str(fnm))

    # set 'changed' to True if changes have already been staged
    changed = staged is not None

    # add/remove files to commit (and update 'changed' to True)
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
        for _ in (0, 1):
            try:
                git_add(filelist=modifications, sandbox_dir=sandbox_dir,
                        debug=debug, verbose=verbose)
                changed = True
                break
            except GitAddIgnoredException as aex:
                deleted = []
                for entry in aex.files:
                    for mod in modifications:
                        if mod.startswith(entry) and \
                          (entry == mod or mod[len(entry)] == os.sep):
                            deleted.append(mod)

                ignored = 0
                for path in deleted:
                    try:
                        del modifications[modifications.index(path)]
                        ignored += 1
                    except ValueError:
                        # maybe we already deleted it?
                        continue
                print("WARNING: Retrying git_add with modified files"
                      " (%d ignored): %s" %
                      (ignored, ", ".join(modifications)), file=sys.stderr)

    # return True if we found changes
    return changed


def __switch_project(project_name, top_url, revision, ignore_externals=False,
                     sandbox_dir=None, debug=False, verbose=False):
    tmp_url = top_url
    switch_exc = None
    ignore_ancestry = False
    merge_conflict = False
    for _ in (0, 1, 2):
        conflicts = []
        try:
            for line in svn_switch(tmp_url, revision=revision,
                                   accept_type=AcceptType.WORKING,
                                   ignore_ancestry=ignore_ancestry,
                                   ignore_externals=ignore_externals,
                                   sandbox_dir=sandbox_dir, debug=debug,
                                   verbose=verbose):
                xline = line.strip()
                if xline.startswith("C "):
                    badname = xline.split()[1]
                    conflicts.append(badname)
            switch_exc = None
            break
        except SVNBadAncestryException as exc:
            switch_exc = exc
            ignore_ancestry = True
        except SVNConnectException as exc2:
            # if we couldn't connect to the SVN server, try again
            switch_exc = exc2
        except SVNNonexistentException as exc3:
            # if we haven't used an alternate URL yet...
            switch_exc = exc3
            if tmp_url == top_url:
                # if this project was forked/renamed, try the alternate URL
                tmp_url = __revert_forked_url(top_url)
                if tmp_url is not None:
                    # we found an alternate URL, try that
                    continue
            raise
        except SVNMergeConflictException:
            traceback.print_exc()
            raise SystemExit(1)

    if len(conflicts) > 0:
        if len(conflicts) != 1 or conflicts[0] != ".gitignore":
            raise SVNMergeConflictException("Found unexpected merge"
                                            " conflicts: %s" %
                                            ", ".join(conflicts))
        __fix_gitignore_conflict(sandbox_dir)

    if switch_exc is not None:
        raise SVNException("Could not switch %s to rev %s after 3 attempts"
                           "\n\t(url %s)\n\t(%s)" %
                           (project_name, revision, top_url, switch_exc))


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
        __initialize_git_workspace(gitrepo.ssh_url, svn_url, svn_rev,
                                   create_empty_repo=False,
                                   sandbox_dir=sandbox_dir, debug=debug,
                                   verbose=verbose)

    if not gitmgr.has_branch(project_name, git_branch):
        git_checkout(branch_name=git_branch, start_point=git_hash,
                     sandbox_dir=sandbox_dir, debug=debug, verbose=verbose)
        gitmgr.add_branch(project_name, git_branch)
    else:
        try:
            git_reset(start_point=git_hash, hard=True, sandbox_dir=sandbox_dir,
                      debug=debug, verbose=verbose)
        except CommandException as cex:
            raise CommandException("Cannot reset %s to hash %s (rev %s): %s" %
                                   (os.path.abspath(sandbox_dir),
                                    git_hash[:20], svn_rev, cex))

    __delete_untracked(sandbox_dir, debug=debug, verbose=verbose)


def convert_revision(database, gitmgr, mantis_issues, count, top_url,
                     git_remote, entry, first_commit=False,
                     issue_count=None, issue_pause=None, noisy=False,
                     pause_before_commit=False, rewrite_proc=None,
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
        if revision is None:
            return False

    if not first_commit:
        switch_and_update_externals(database, gitmgr, top_url, revision,
                                    entry.date_string,
                                    rewrite_proc=rewrite_pdaq,
                                    sandbox_dir=sandbox_dir, debug=debug,
                                    verbose=verbose)

    if not gitmgr.has_branch(database.name, git_remote):
        git_checkout(branch_name=git_remote, new_branch=True,
                     sandbox_dir=sandbox_dir, debug=debug, verbose=verbose)
        gitmgr.add_branch(database.name, git_remote)

    # fetch the cached Git repository object
    gitrepo = gitmgr.get_repo(project_name, debug=debug, verbose=verbose)

    if mantis_issues is None or not gitrepo.has_issue_tracker:
        # don't open issues if we don't have any Mantis issues or
        # if we're not writing to a repo with an issue tracker
        github_issues = None
    else:
        # open/reopen GitHub issues
        github_issues = mantis_issues.open_github_issues(revision,
                                                         database=database,
                                                         pause_count=\
                                                         issue_count,
                                                         pause_seconds=\
                                                         issue_pause,
                                                         report_progress=\
                                                         progress_reporter)

    changed = __stage_modifications(sandbox_dir=sandbox_dir, debug=debug,
                                    verbose=verbose)
    if not changed and not first_commit:
        return False

    if pause_before_commit:
        read_input("%s %% Hit Return to commit: " % os.getcwd())

    commit_result = __commit_to_git(project_name, entry.revision, entry.author,
                                    entry.date, entry.log_message,
                                    allow_empty=count == 0,
                                    sandbox_dir=sandbox_dir,
                                    debug=debug, verbose=verbose)

    # break tuple of results into separate values
    (git_branch, short_hash, changed, inserted, deleted) = \
      commit_result
    if noisy:
        print("  >>%s:%s(m%s i%s d%s)" % (git_branch, short_hash, changed,
                                          inserted, deleted), end="")
    sys.stdout.flush()

    # get the full hash string for the commit
    full_hash = git_show_hash(sandbox_dir=sandbox_dir, debug=debug,
                              verbose=verbose)
    if not full_hash.startswith(short_hash):
        raise Exception("Expected %s hash %s to start with %s" %
                        (sandbox_dir, full_hash, short_hash))

    # write branch/hash info for this revision to database
    database.save_revision(revision, git_branch, full_hash)

    print()
    CompareSandboxes.compare(database.name, sandbox_dir, sandbox_dir,
                             debug=debug, verbose=verbose)

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
                       checkpoint=False, early_exit=None, issue_count=10,
                       issue_pause=5, noisy=False, pause_interval=900,
                       pause_seconds=60, rewrite_proc=None,
                       trunk_branch=None, debug=False, verbose=False):
    database = project.database

    # read in the Subversion log entries from the SVN server
    if database.has_unknown_authors:
        raise SystemExit("Please add missing author(s) before continuing")

    # we'll use the project name as the workspace directory name
    sandbox_dir = project.name

    initialized = False
    prev_checkpoint_list = None
    need_newline = False
    for branch_path, top_url, _ in database.project_urls(project.name,
                                                         project.project_url):
        if branch_path is None:
            branch_path = SVNMetadata.TRUNK_NAME

        # determine Git branch to use
        git_remote = None
        if trunk_branch is not None:
            if branch_path.endswith(trunk_branch):
                git_remote = GITHUB_MAIN_BRANCH
            elif branch_path == SVNMetadata.TRUNK_NAME:
                git_remote = GITHUB_DEMOTED_BRANCH
        elif branch_path == SVNMetadata.TRUNK_NAME:
            git_remote = GITHUB_MAIN_BRANCH

        # if we haven't got a Git branch yet, construct one from the SVN branch
        if git_remote is None:
            git_remote = branch_path.rsplit("/")[-1]
            if git_remote in ("HEAD", GITHUB_MAIN_BRANCH):
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
            if project.name == "config" or project.name == "dom-fpga":
                rename_limit = 7000
            else:
                rename_limit = None

            __initialize_git_workspace(git_url, top_url, first_revision,
                                       create_empty_repo=True,
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
            if early_exit is not None and entry.revision > early_exit:
                # we're debugging and want to exit after a specified revision
                print("-- Early exit for %s at rev %s" %
                      (branch_path, entry.revision))
                break

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
                                                 gitmgr.local_repo_path,
                                                 gitmgr.is_local)

                if prev_checkpoint_list is not None:
                    for path in prev_checkpoint_list:
                        if path is not None and os.path.exists(path):
                            os.unlink(path)

                prev_checkpoint_list = tarpaths

            # set 'pause_before_commit' to True when you'd like to inspect
            #  the sandbox before committing to Git
            pause_before_commit = False

            if convert_revision(database, gitmgr, mantis_issues, count,
                                top_url, git_remote, entry,
                                first_commit=first_commit,
                                issue_count=issue_count,
                                issue_pause=issue_pause, noisy=noisy,
                                pause_before_commit=pause_before_commit,
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
                        print("\nPausing %s for %d seconds" %
                              (database.name, pause_seconds, ))
                        time.sleep(pause_seconds)
                        start_time = now_time

        # if we printed any status lines, end on a new line
        if need_newline:
            print()
            need_newline = False

    # add all remaining issues to GitHub
    #  (but don't add them if we exited early for debugging)
    if early_exit is None and mantis_issues is not None and \
      mantis_issues.has_issue_tracker:
        mantis_issues.add_issues(database=database,
                                 report_progress=__progress_reporter,
                                 verbose=verbose)

    # make a final commit with everything brought up to HEAD
    final_commit(database.name, sandbox_dir, debug=debug, verbose=verbose)

    # clean up unneeded checkpoint files
    if prev_checkpoint_list is not None:
        for path in prev_checkpoint_list:
            if path is not None and os.path.exists(path):
                os.unlink(path)


def final_commit(project_name, sandbox_dir, debug=False, dry_run=False,
                 verbose=False):
    """
    Update main project (and any subprojects) to HEAD and, if anything was
    modified, make one final commit
    """

    git_pull("origin", GITHUB_MAIN_BRANCH, sandbox_dir=sandbox_dir, debug=debug,
             verbose=verbose)

    # update all submodules to the latest version
    try:
        git_submodule_update(merge=True, remote=True, sandbox_dir=sandbox_dir,
                             debug=debug, verbose=verbose)
    except GitException as gex:
        #
        gexstr = str(gex)
        if gexstr.find("Usage: ") < 0:
            raise

        cmd_args = ("git", "submodule", "foreach", "git", "pull", "origin",
                    GITHUB_MAIN_BRANCH)

        run_command(cmd_args, "GIT SUBMODULE PULL_ALL",
                    working_directory=sandbox_dir,
                    stderr_handler=final_stderr, debug=debug,
                    dry_run=dry_run, verbose=verbose)

    changed = __stage_modifications(sandbox_dir=sandbox_dir, debug=debug,
                                    verbose=verbose)
    if changed:
        commit_result = __commit_to_git(project_name, "Final commit",
                                        getpass.getuser(), datetime.now(),
                                        "Update all subprojects to"
                                        " latest revision",
                                        sandbox_dir=sandbox_dir,
                                        debug=debug, verbose=verbose)

        # break tuple of results into separate values
        (git_branch, short_hash, changed, inserted, deleted) = \
          commit_result
        print("Final commit made to %s:%s (+%s -%s ~%s)" %
              (git_branch, short_hash, inserted, deleted, changed))

        __push_to_remote_git_repo(GITHUB_MAIN_BRANCH, sandbox_dir=sandbox_dir,
                                  debug=debug)


def final_stderr(cmdname, line, verbose=False):
    print(">>%s>> %s" % (cmdname, line, ))


def get_pdaq_project(name, clear_tables=False, preload_from_log=False,
                     shallow=False, debug=False, verbose=False):
    try:
        project = PDAQManager.get(name, renamed=RENAMED_PROJECTS)
        if project is None:
            raise Exception("Cannot find SVN project \"%s\"" % (name, ))
    except SVNNonexistentException:
        return None

    database = project.database
    if database.name != project.name:
        raise Exception("Expected database for \"%s\", not \"%s\"" %
                        (project.name, database.name))

    if not project.is_loaded:
        # don't initialize from the log, load cached database entries
        if not preload_from_log:
            project.load_from_db(shallow=shallow)

        if clear_tables:
            # remove old entries from database
            database.trim()

        # if we don't have any log entries yet (DB was empty or not loaded)
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
            if revision <= 15469:
                svn_url += "_rc1"
        elif svn_url.endswith("releases/Urban_Harvest7"):
            if revision <= 17562:
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


def save_checkpoint_files(workspace, project_name, branch_path, revision,
                          local_repo_path, is_local_repo):
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
        git_repo_name = project_name + ".git"
        if not is_local_repo:
            path2 = None
        else:
            path2 = os.path.join(tardir, "repo_" + base_name + suffix)
            try:
                with tarfile.open(path2, mode="w:gz") as tar:
                    tar.add(git_repo_name)
            except:
                traceback.print_exc()
                print("Deleting failed Git repo checkpoint file \"%s\"" %
                      (path2, ))
                os.unlink(path2)
                path2 = None

        return (fullpath, path2)
    finally:
        os.chdir(curdir)


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
        if revision is None:
            return

    externs = __build_externs_dict(rewrite_proc=rewrite_proc,
                                   sandbox_dir=sandbox_dir, debug=debug,
                                   verbose=verbose)

    try:
        __switch_project(database.name, top_url, revision=revision,
                         ignore_externals=True, sandbox_dir=sandbox_dir,
                         debug=debug, verbose=verbose)
    except SVNException as sex:
        if database.name == "cluster-config":
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

        # attempt to find the log entry for this revision
        sub_entry = None if sub_rev is None else \
          sub_proj.get_cached_entry(sub_rev)

        # if no revision was specified in the svn:externals entry,
        #  find the last revision made to this subproject before the
        #  parent commit
        if sub_rev is None or sub_entry is None:
            if sub_proj is None or sub_proj.database is None:
                if sub_name != "anvil":
                    print("ERROR: Cannot fetch %s database" % (sub_name, ))
                continue

            # find the first revision checked in before the parent revision
            sub_rev = sub_proj.database.find_revision_from_date(sub_branch,
                                                                date_string)

            # if we didn't find a revision on the branch, check trunk
            if sub_rev is None and sub_branch != SVNMetadata.TRUNK_NAME:
                sub_rev = sub_proj.database.\
                  find_revision_from_date(SVNMetadata.TRUNK_NAME, date_string)

            # if we didn't find it on trunk or the branch, give up
            if sub_rev is None:
                raise Exception("Cannot find %s revision for %s on %s" %
                                (sub_proj.name, sub_branch, date_string))

            # attempt to find the log entry for the updated revision
            sub_entry = sub_proj.get_cached_entry(sub_rev)

        # if we didn't find a log entry, give up
        if sub_entry is None:
            if sub_branch == SVNMetadata.TRUNK_NAME:
                bstr = ""
            else:
                bstr = " (branch %s)" % (sub_branch, )
            raise Exception("Cannot find %s rev %s%s" %
                            (sub_proj.name, sub_rev, bstr))

        # if the branch changed, update our cached version
        if sub_branch != sub_entry.branch_name:
            sub_branch = sub_entry.branch_name

        # find the previous SVN branch/revision and Git branch/hash
        prev_entry = \
          sub_proj.database.find_previous_revision(sub_branch, sub_entry)

        # build the full path to the subproject
        if sandbox_dir is None:
            sub_path = sub_dir
        else:
            sub_path = os.path.join(sandbox_dir, sub_dir)

        # build the URL for the previous entry and update everything
        prev_url = sub_proj.create_project_url(prev_entry.branch_name)
        __update_both_sandboxes(sub_name, gitmgr, sub_path, prev_url,
                                prev_entry.revision, prev_entry.git_branch,
                                prev_entry.git_hash, debug=debug,
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
              sub_branch, sub_rev, prev_entry.git_branch, prev_entry.git_hash

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


def validate_trunk_branch(project, trunk_branch):
    """
    Throw an exception if --trunk-branch specifies a branch which is not
    found in the project.
    Warn about projects which contain a 'rel-4xx' branch but have not specified
    a --trunk-branch argument.
    """
    database = project.database

    found_rel4xx = False
    found_new_trunk = trunk_branch is None
    for branch_path, top_url, _ in database.project_urls(project.name,
                                                         project.project_url):
        if branch_path == "branches/rel-4xx":
            found_rel4xx = True
        if trunk_branch is not None and branch_path.endswith(trunk_branch):
            found_new_trunk = True

        if found_rel4xx and found_new_trunk:
            break

    if found_rel4xx and trunk_branch is None:
        print("WARNING: Found rel-4xx branch, but no trunk-branch is specified")
    if not found_new_trunk:
        raise Exception("Project %s does not contain a \"%s\" branch" %
                        (project.name, trunk_branch))


#from profile_code import profile
#@profile(output_file="/tmp/profile.out", strip_dirs=True, save_stats=True)
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
    if project is None:
        raise Exception("Cannot find project \"%s\"" % args.svn_project)

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

    # throw exception for projects which specify a nonexistent trunk_branch
    # warn if project has a rel-4xx branch not specified by trunk_branch
    validate_trunk_branch(project, args.trunk_branch)

    # execute everything in a temporary directory which will be erased on exit
    with TemporaryDirectory():
        print("Converting %s repo" % (args.svn_project, ))
        try:
            convert_svn_to_git(project, gitmgr, mantis_issues, gitrepo.ssh_url,
                               checkpoint=args.checkpoint,
                               early_exit=args.early_exit, noisy=args.noisy,
                               trunk_branch=args.trunk_branch,
                               debug=args.debug, verbose=args.verbose)
        except:
            if args.pause:
                read_input("%s %% Hit Return to abort: " % os.getcwd())
            raise


if __name__ == "__main__":
    main()
