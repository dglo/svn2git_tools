#!/usr/bin/env python

from __future__ import print_function

import argparse
import getpass
import os
import shutil
import sys
import tarfile
import traceback

from cmdrunner import CommandException, set_always_print_command
from github_util import GithubUtil
from git import GitUntrackedException, git_add, git_autocrlf, git_checkout, \
     git_commit, git_config, git_init, git_pull, git_push, git_remote_add, \
     git_remove, git_reset, git_show_hash, git_status, git_submodule_add, \
     git_submodule_remove, git_submodule_status, git_submodule_update
from i3helper import TemporaryDirectory, read_input
from pdaqdb import PDAQManager
from svn import SVNConnectException, SVNException, SVNMetadata, \
     SVNNonexistentException, svn_checkout, svn_get_externals, svn_info, \
     svn_propget, svn_status, svn_switch


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
    parser.add_argument("-P", "--private", dest="make_public",
                        action="store_false", default=True,
                        help="GitHub repository should be private")
    parser.add_argument("-Z", "--always-print-command", dest="print_command",
                        action="store_true", default=False,
                        help="Always print external commands before running")
    parser.add_argument("-v", "--verbose", dest="verbose",
                        action="store_true", default=False,
                        help="Print details")
    parser.add_argument("-x", "--debug", dest="debug",
                        action="store_true", default=False,
                        help="Print debugging messages")

    parser.add_argument("--local-repo", dest="local_repo_path",
                        default=None,
                        help="Specify the local directory where Git repos"
                             " should be created; if not specified, a"
                             " temporary repo will be created and thrown away"
                             " on exit")
    parser.add_argument("-s", "--sleep-seconds", dest="sleep_seconds",
                        type=int, default=1,
                        help="Number of seconds to sleep after GitHub"
                             " issue operations")

    parser.add_argument(dest="svn_project", default=None,
                        help="Subversion/Mantis project name")


def __check_metadirs(sandbox_dir):
    need_list = False
    svn_dir = os.path.join(sandbox_dir, ".svn")
    if not os.path.isdir(svn_dir):
        print("!!!!!!!!!!! No SVN metadir in %s !!!!!!!!!!!" % (sandbox_dir, ))
        need_list = True
    git_dir = os.path.join(sandbox_dir, ".git")
    if not os.path.exists(git_dir):
        print("!!!!!!!!!!! No GIT metadir in %s !!!!!!!!!!!" % (sandbox_dir, ))
        need_list = True
    if need_list:
        list_directory(sandbox_dir, title="CheckMeta %s" % (sandbox_dir, ))


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
        flds = git_commit(author=PDAQManager.get_author(entry.author),
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


def __fix_external(flds):
    sub_rev, sub_url, sub_dir = flds

    # XXX hack for a renamed project
    if sub_dir == "fabric_common":
        sub_dir = "fabric-common"

    return sub_rev, sub_url, sub_dir


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


def __get_pdaq_project(name, shallow=False, debug=False, verbose=False):
    try:
        project = PDAQManager.get(name)
        if project is None:
            raise Exception("Cannot find SVN project \"%s\"" % (name, ))
    except SVNNonexistentException:
        return None

    if not project.is_loaded:
        project.load_from_db(shallow=shallow)
        if project.total_entries == 0:
            # close the database to clear any cached info
            project.close_db()

            # load log entries from all URLs
            #   and save any new entries to the database
            project.load_from_log(debug=debug, verbose=verbose)


    return project


class GitRepoManager(object):
    GIT_REPO_DICT = {}

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

    @classmethod
    def __add_repo_to_cache(cls, project_name, git_repo):
        if project_name in cls.GIT_REPO_DICT:
            raise Exception("Found existing cached repo for \"%s\"" %
                            (project_name, ))

        cls.GIT_REPO_DICT[project_name] = git_repo

    @classmethod
    def __get_cached_repo(cls, project_name):

        return None if project_name not in cls.GIT_REPO_DICT \
          else cls.GIT_REPO_DICT[project_name]

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

    @property
    def local_repo_path(self):
        return self.__local_repo_path

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
        if description is None:
            description = "WIPAC's %s project" % (project_name, )

        return ghutil.get_github_repo(description=description,
                                      create_repo=destroy_old_repo,
                                      destroy_existing=destroy_old_repo,
                                      debug=debug, verbose=verbose)


def __initialize_git_workspace(project_name, gitmgr, svn_url, revision,
                               create_empty_repo=False, make_public=False,
                               organization=None, rename_limit=None,
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

    # get the Github or local repo object
    gitrepo = gitmgr.get_repo(project_name, organization=organization,
                              destroy_old_repo=create_empty_repo,
                              make_public=make_public,
                              debug=debug, verbose=verbose)

    # point the new git sandbox at the Github/local repo
    try:
        for _ in git_remote_add("origin", gitrepo.ssh_url,
                                sandbox_dir=sandbox_dir, debug=debug,
                                verbose=verbose):
            pass
    except:
        read_input("%s %% Hit Return to exit: " % os.getcwd())
        raise

    if not create_empty_repo:
        #read_input("%s %% Hit Return to pull: " % os.getcwd())
        branches = git_pull_and_clean(project_name, sandbox_dir=sandbox_dir,
                                      debug=debug, verbose=verbose)
        print("!!XXX PostPull %s" % str(branches, ))


def __initialize_svn_workspace(project_name, svn_url, revision,
                               sandbox_dir=None, debug=False, verbose=False):
    if debug:
        if sandbox_dir is None:
            sandbox_dir = os.path.join(os.getcwd(), project_name)
        print("Checkout %s rev %d in %s" % (svn_url, revision, sandbox_dir))

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


def __monitor_git_status(sandbox_dir=None, debug=False, verbose=False):
    success = True
    # git_branch = None
    hash7 = None

    state = 0
    for line in git_status(sandbox_dir=sandbox_dir, debug=debug,
                           verbose=verbose):
        if not success:
            continue

        if line == "":
            continue

        if state == 0:
            if line.startswith("On branch "):
                state = 1
                # git_branch = line[10:]
                continue

            if line.startswith("HEAD detached "):
                state = 2
                hash7 = line[-7:]
                continue

            print("??GIT STATUS#0?? %s" % (line, ), file=sys.stderr)
            success = False
            continue

        if state == 1:
            if line.startswith("Your branch is up to date with '"):
                state = 2
                continue

        if state == 2:
            if line == "nothing to commit, working tree clean":
                state = 3
                continue

            print("??GIT STATUS#1?? %s" % (line, ), file=sys.stderr)
            success = False
            continue

        if state == 3:
            print("??GIT STATUS#2?? %s" % (line, ), file=sys.stderr)
            success = False
            continue

    return success, hash7


def __monitor_status(title, sandbox_dir=None, debug=False, verbose=False):
    if sandbox_dir is not None and not os.path.exists(sandbox_dir):
        print("ERROR: %s has not been checked out" % (sandbox_dir, ),
              file=sys.stderr)
        return True

    success, hash7 = __monitor_git_status(sandbox_dir=sandbox_dir, debug=debug,
                                          verbose=verbose)
    if not success:
        return False

    if not __monitor_svn_status(sandbox_dir=sandbox_dir, debug=debug,
                                verbose=verbose):
        return False

    infodict = svn_info(sandbox_dir=sandbox_dir, debug=debug, verbose=verbose)
    if "relative_url" not in infodict:
        repo_branch = "??Unknown Branch??"
    else:
        repo_branch = infodict["relative_url"]
        idx = repo_branch.find("projects/")
        if idx >= 0:
            repo_branch = repo_branch[idx+9:]

    if "revision" not in infodict:
        repo_rev = "??Unknown Revision??"
    else:
        repo_rev = infodict["revision"]

    if verbose:
        print("Validated %s :: %s rev %s -> %s" %
              (title, repo_branch, repo_rev, hash7))

    return True


def __monitor_svn_status(sandbox_dir=None, debug=False, verbose=False):
    if sandbox_dir is None:
        top_dir = "."
    else:
        top_dir = sandbox_dir

    success = True
    for line in svn_status(sandbox_dir=top_dir, debug=debug, verbose=verbose):
        if not success:
            continue

        if line == "":
            continue

        if line[0] == "?":
            filename = line[1:].strip()
            if filename not in (".git", ".gitignore"):
                print("ERROR: Found unexpected non-SVN file \"%s\"" %
                      (filename, ), file=sys.stderr)
                success = False
            continue

        if line[0] == "!":
            # assume these are empty directories which were deleted by Git
            continue

        print("ERROR: Unexpected SVN status: %s" % (line, ))
        success = False
        continue

    return success


def __print_status(title, sandbox_dir=None, debug=False, verbose=False):
    if title is not None:
        print("### %s ###" % (title, ))

    if sandbox_dir is not None and not os.path.exists(sandbox_dir):
        print("      %s has not been checked out" % (sandbox_dir, ))
        return

    if sandbox_dir is None:
        sandbox_dir = "."

    if not os.path.isdir(os.path.join(sandbox_dir, ".git")):
        print("!! No Git subdirectory")
    else:
        for line in git_status(sandbox_dir=sandbox_dir, debug=debug,
                               verbose=verbose):
            print("GS >> %s" % (line, ))

    if not os.path.isdir(os.path.join(sandbox_dir, ".svn")):
        print("!! No SVN subdirectory")
    else:
        infodict = svn_info(sandbox_dir=sandbox_dir, debug=debug,
                            verbose=verbose)
        if "relative_url" not in infodict:
            repo_branch = "??Unknown Branch??"
        else:
            repo_branch = infodict["relative_url"]
            idx = repo_branch.find("projects/")
            if idx >= 0:
                repo_branch = repo_branch[idx+9:]

        if "revision" not in infodict:
            repo_rev = "??Unknown Revision??"
        else:
            repo_rev = infodict["revision"]
        print("SI >> %s rev %s" % (repo_branch, repo_rev))

        for line in svn_status(sandbox_dir=sandbox_dir, debug=debug,
                               verbose=verbose):
            print("SS >> %s" % (line, ))


def __push_to_remote_git_repo(git_remote, sandbox_dir=None, debug=False,
                              verbose=False):
    try:
        err_buffer = []
        for line in git_push(remote_name=git_remote, upstream="origin",
                             sandbox_dir=sandbox_dir, debug=debug,
                             verbose=debug):
            err_buffer.append(line)
    except CommandException as cex:
        print(str(cex))
        for line in err_buffer:
            print("?? " + str(line))
        read_input("%s %% Hit Return to exit: " % os.getcwd())
        raise


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
                     sandbox_dir=None, debug=False, verbose=False,
                     extra_verbose=False):
    tmp_url = top_url
    switched = False
    for _ in (0, 1, 2):
        try:
            for line in svn_switch(tmp_url, revision=revision,
                                   ignore_externals=ignore_externals,
                                   sandbox_dir=sandbox_dir, debug=debug,
                                   verbose=verbose):
                if extra_verbose:
                    print("-- SWITCH>> %s" % (line, ))
            switched = True
            break
        except SVNConnectException:
            # if we couldn't connect to the SVN server, try again
            continue
        except SVNNonexistentException:
            # if we haven't used an alternate URL yet...
            if tmp_url == top_url:
                tmp_url = __revert_forked_url(top_url)
                if tmp_url is not None:
                    # we found an alternate URL, try that
                    continue
            raise

    if not switched:
        raise SVNException("Could not switch %s to rev %s after 3 attempts"
                           "\n\t(url %s)" % (project_name, revision, top_url))


def __update_both_sandboxes(project_name, gitmgr, sandbox_dir, svn_url,
                            svn_rev, git_branch, git_hash, debug=False,
                            verbose=False):
    __check_metadirs(sandbox_dir)
    print("    XXX UpdBoth %s rev %s -> %s hash %s" %
          (os.path.basename(sandbox_dir), svn_rev, git_branch, git_hash[:7]))

    extra_verbose = False
    if not os.path.exists(sandbox_dir):
        if extra_verbose:
            print("  SUBCHKOUT %s@%s" % (svn_url, svn_rev))
        svn_checkout(svn_url, revision=svn_rev, target_dir=sandbox_dir,
                     debug=debug, verbose=verbose)
    else:
        if extra_verbose:
            print("  SUBSWITCH to %s@%s (in %s)" %
                  (svn_url, svn_rev, sandbox_dir))

        __switch_project(project_name, svn_url, revision=svn_rev,
                         ignore_externals=True, sandbox_dir=sandbox_dir,
                         debug=debug, verbose=verbose,
                         extra_verbose=extra_verbose)

    git_metadir = os.path.join(sandbox_dir, ".git")
    if not os.path.exists(git_metadir):
        #print("XXX %s InitGitWrkspc %s" % (project_name, sandbox_dir, ))
        __initialize_git_workspace(project_name, gitmgr, svn_url, svn_rev,
                                   create_empty_repo=False,
                                   sandbox_dir=sandbox_dir, debug=debug,
                                   verbose=verbose)
    #else: print("XXX %s CreGitWrkspc %s" % (project_name, sandbox_dir, ))

    __check_metadirs(sandbox_dir)
    if not os.path.isdir(git_metadir):
        git_checkout(branch_name=git_branch, start_point=git_hash,
                     sandbox_dir=sandbox_dir, debug=debug, verbose=verbose)
    else:
        git_reset(start_point=git_hash, hard=True, sandbox_dir=sandbox_dir,
                  debug=debug, verbose=verbose)
    __check_metadirs(sandbox_dir)


def convert_revision(database, gitmgr, count, top_url, git_remote, entry,
                     first_commit=False, sandbox_dir=None, debug=False,
                     verbose=False):
    project_name = database.name

    if not first_commit:
        switch_and_update_externals(database, gitmgr, top_url, entry.revision,
                                    entry.date_string, sandbox_dir=sandbox_dir,
                                    debug=debug, verbose=verbose)

        if count == 0:
            git_checkout(git_remote, new_branch=True, sandbox_dir=sandbox_dir,
                         debug=debug, verbose=verbose)

        changed = __stage_modifications(sandbox_dir=sandbox_dir, debug=debug,
                                        verbose=verbose)
        if changed or first_commit:
            flds = __commit_to_git(project_name, entry, None,
                                   allow_empty=count == 0,
                                   sandbox_dir=sandbox_dir, debug=debug,
                                   verbose=verbose)

            # break tuple of results into separate values
            (git_branch, short_hash, _, _, _) = flds

            # get the full hash string for the commit
            full_hash = git_show_hash(sandbox_dir=sandbox_dir, debug=debug,
                                      verbose=verbose)
            if not full_hash.startswith(short_hash):
                raise Exception("Expected %s hash %s to start with %s" %
                                (sandbox_dir, full_hash, short_hash))

            database.save_revision(entry.revision, git_branch, full_hash)

        __push_to_remote_git_repo(git_remote, sandbox_dir=sandbox_dir,
                                  debug=debug, verbose=verbose)

        if not __monitor_status("Final %s" % (project_name, ),
                                sandbox_dir=sandbox_dir, debug=debug,
                                verbose=verbose):
            title = "Final %s status for %s rev %s, Git %s hash %s" % \
              (project_name, entry.branch_name, entry.revision,
               entry.git_branch, entry.git_hash)
            __print_status(title, sandbox_dir=sandbox_dir, debug=debug,
                           verbose=verbose)


def convert_svn_to_git(project_name, gitmgr, checkpoint=False,
                       destroy_existing_repo=False, make_public=False,
                       organization=None, debug=False, verbose=False):
    pdb = __get_pdaq_project(project_name, debug=debug, verbose=verbose)
    database = pdb.database
    if database.name != project_name:
        raise Exception("Expected database for \"%s\", not \"%s\"" %
                        (project_name, database.name))

    sandbox_dir = project_name

    initialized = False
    prev_checkpoint_list = None
    for top_url, first_revision, first_date in database.all_urls_by_date:
        _, project_name, branch_name = SVNMetadata.split_url(top_url)

        if branch_name == SVNMetadata.TRUNK_NAME:
            git_remote = "master"
        else:
            git_remote = branch_name.rsplit("/")[-1]
            if git_remote in ("HEAD", "master"):
                raise Exception("Questionable branch name \"%s\"" %
                                (git_remote, ))

        first_commit = False
        if not initialized:
            __initialize_svn_workspace(project_name, top_url, first_revision,
                                       sandbox_dir=sandbox_dir, debug=debug,
                                       verbose=verbose)

            # XXX turn this hack into something more generally useful
            if project_name == "config":
                rename_limit = 7000
            else:
                rename_limit = None

            __initialize_git_workspace(project_name, gitmgr, top_url,
                                       first_revision,
                                       create_empty_repo=destroy_existing_repo,
                                       make_public=make_public,
                                       organization=organization,
                                       rename_limit=rename_limit,
                                       sandbox_dir=sandbox_dir, debug=debug,
                                       verbose=verbose)
            initialized = True

            # if this is the first commit, the workspace is ready to commit
            first_commit = True

        num_entries = database.num_entries(branch_name)
        for count, entry in enumerate(database.entries(branch_name)):
            spaces = " "*30
            unspaces = "\b"*27  # leave a few spaces to separate error msgs

            print("\r #%d (of %d): %s rev %s%s%s" %
                  (count, num_entries, branch_name, entry.revision, spaces,
                   unspaces), end="")

            if database.name in IGNORED_REVISIONS and \
              entry.revision in IGNORED_REVISIONS[database.name]:
                print("Ignoring %s rev %s" % (database.name, entry.revision))
                continue

            if checkpoint:
                tarpaths = save_checkpoint_files(sandbox_dir, project_name,
                                                 branch_name, entry.revision,
                                                 gitmgr)

                if prev_checkpoint_list is not None:
                    for path in prev_checkpoint_list:
                        if os.path.exists(path):
                            os.unlink(path)

                prev_checkpoint_list = tarpaths

            convert_revision(database, gitmgr, count, top_url, git_remote,
                             entry, first_commit=first_commit,
                             sandbox_dir=sandbox_dir, debug=debug,
                             verbose=verbose)
            first_commit = False


def save_checkpoint_files(workspace, project_name, branch_name, revision,
                          gitmgr):
    tardir = "/tmp"
    suffix = ".tgz"

    base_name = "%s_%s_r%s" % \
      (project_name, branch_name.rsplit("/")[-1], revision)
    fullpath = os.path.join(tardir, base_name + suffix)

    curdir = os.getcwd()
    try:
        os.chdir(workspace)
        try:
            with tarfile.open(fullpath, mode="w:gz") as tar:
                tar.add(".", arcname=project_name)
        except:
            traceback.print_exc()
            print("Deleting failed workspace checkpoint file \"%s\"" %
                  (fullpath, ))
            os.unlink(fullpath)
            fullpath = None

        os.chdir(gitmgr.local_repo_path)
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


def git_pull_and_clean(project_name, remote="origin", branch="master",
                       sandbox_dir=None, debug=False, verbose=False):
    # attempt this twice, so we have a chance tp clean untracked files
    pulled = False
    cleaned = False
    for _ in (0, 1, 3):
        try:
            _ = git_pull(remote, branch, sandbox_dir=sandbox_dir, debug=debug,
                         verbose=verbose)
            pulled = True
            break
        except GitUntrackedException as gux:
            missing = None
            for name in gux.files:
                path = os.path.join(sandbox_dir, name)
                if not os.path.exists(path):
                    if missing is None:
                        missing = []
                    missing.append(path)
                    continue

                os.unlink(path)
                cleaned = True

            if missing is not None:
                raise Exception("Cannot remove untracked files: " +
                                ", ".join(missing))

    if not pulled:
        raise Exception("Failed to pull %s from Git repo" % (project_name, ))

    return cleaned


def list_directory(topdir, title=None):
    if topdir is None:
        topdir = "."

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
        self.__added = (value == True)


def switch_and_update_externals(database, gitmgr, top_url, revision,
                                date_string, sandbox_dir=None, debug=False,
                                verbose=False):
    if False:
        print("\n\n%s\n" % ("-"*70, ))
    extra_verbose = False
    # remember the current externals
    externs = {}
    for flds in svn_get_externals(sandbox_dir=sandbox_dir, debug=debug,
                                  verbose=verbose):
        # fix any naming or URL problems
        flds = __fix_external(flds)

        sub_rev, sub_url, sub_dir = flds
        externs[sub_dir] = ExternMap(sub_dir, sub_url, sub_rev)
        print("++ SVNExtern %s rev %s" % (sub_dir, sub_rev))  # XXX debugging

    for flds in git_submodule_status(sandbox_dir=sandbox_dir, debug=debug,
                                     verbose=verbose):
        sub_name, sub_stat, sub_hash, sub_branch = flds
        if sub_name not in externs:
            raise Exception("Found Git submodule \"%s\" but no SVN external" %
                            (sub_name, ))
        externs[sub_name].add_git(sub_hash, sub_branch)

        print("++ GitExtern %s hash %s branch %s stat \"%s\"" %
              (sub_name, "???????" if sub_hash is None else sub_hash[:7],
               sub_branch, sub_stat))

    if extra_verbose:
        print("SWITCH to %s@%s" % (top_url, revision))
    try:
        __switch_project(database.name, top_url, revision=revision,
                         ignore_externals=True, sandbox_dir=sandbox_dir,
                         debug=debug, verbose=verbose,
                         extra_verbose=extra_verbose)
    except SVNException as sex:
        if database.project_name == "cluster-config":
            sstr = str(sex)
            if sstr.find("E195012: ") and top_url.find("/retired") > 0:
                return
        raise

    # get the generator for SVN externals
    if database.name == "pdaq":   # XXX debugging
        print("XXX GetExterns %s r%s" % (database.name, revision))
    extern_gen = svn_get_externals(svn_url=top_url, revision=revision,
                                   sandbox_dir=sandbox_dir, debug=debug,
                                   verbose=verbose)

    # update all externals
    for count, flds in enumerate(extern_gen):
        # fix any naming or URL problems
        flds = __fix_external(flds)

        # unpack the fields
        sub_rev, sub_url, sub_dir = flds
        print("XXX EXTERN %s r%s -> %s" % (sub_dir, sub_rev, sub_url))

        # extract the project name and branch info from the URL
        _, sub_name, sub_branch = SVNMetadata.split_url(sub_url)
        if sub_name != sub_dir:
            print("ERROR: Expected %s, not %s in %s" %
                  (sub_dir, sub_name, sub_url))

        # get the SVNProject for this subproject
        sub_proj = __get_pdaq_project(sub_name, shallow=True, debug=debug,
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
        print("*** FindHash project %s branch %s rev %s" %
              (sub_proj.name, sub_branch, sub_rev))
        prev_branch, prev_rev, git_branch, git_hash = \
          sub_proj.database.find_previous_revision(sub_branch, sub_entry)
        print("\tFoundHash prev %s branch %s rev %s -> %s hash %s (%d chars)" %
              (sub_proj.name, prev_branch, prev_rev, git_branch,
               "???????" if git_hash is None else git_hash[:7],
               -1 if git_hash is None else len(git_hash)))

        # build the full path to the subproject
        if sandbox_dir is None:
            sub_path = sub_dir
        else:
            sub_path = os.path.join(sandbox_dir, sub_dir)

        if not __monitor_status("PreUpd %s" % str(sub_name),
                                sandbox_dir=sub_path, debug=debug,
                                verbose=verbose):
            __print_status("Update %s to prev SVN %s rev %s, Git %s hash %s" %
                           (sub_name, prev_branch, prev_rev, git_branch,
                            git_hash[:7]), sandbox_dir=sub_path, debug=debug,
                           verbose=verbose)
            raise SystemExit("Failed(PreUpdate)")

        #list_directory(sandbox_dir, title="PreUpdate %s" % (sandbox_dir, ))  # XXX

        # build the URL for the previous entry and update everything
        prev_url = sub_proj.project_url + "/" + prev_branch
        #print("XXX PreUpdBoth path %s url %s" % (sub_path, prev_url))
        __update_both_sandboxes(sub_name, gitmgr, sub_path, prev_url, prev_rev,
                                git_branch, git_hash, debug=debug,
                                verbose=verbose)

        # find the hash which matches the current revision
        flds = \
          sub_proj.database.find_hash_from_revision(sub_branch, sub_rev,
                                                    with_git_hash=True)
        if flds is not None:
            new_git_branch, new_hash, new_svn_branch, new_rev = flds
            print("\tXXX %s NewHash %s (branch %s)" %
                  (sub_name, new_hash, new_git_branch))
            if sub_branch != new_svn_branch or sub_rev != new_rev:
                print("!! %s falling back from %s rev %s to %s rev %s" %
                      (sub_name, sub_branch, sub_rev, new_svn_branch, new_rev))
        else:
            new_svn_branch, new_rev, new_git_branch, new_hash = \
              sub_branch, sub_rev, git_branch, git_hash
            print("\tXXX %s SameHash %s (branch %s)" %
                  (sub_name, new_hash, new_git_branch))

        # update the SVN URL if necessary
        if sub_url.endswith(new_svn_branch):
            new_url = sub_url
        else:
            new_url = sub_proj.project_url + "/" + new_svn_branch

        #print("XXX PreUpdBoth2 %s%s new %s rev %s -> %s hash %s\n\tNewURL %s" %
        #      (sub_proj.name, "" if sub_name == sub_proj.name
        #       else " (%s)" % sub_name, new_svn_branch, new_rev,
        #       new_git_branch, new_hash[:7], new_url))
        if not __monitor_status("PostUpd %s" % str(sub_name),
                                sandbox_dir=sub_path, debug=debug,
                                verbose=verbose):
            __print_status("Update %s to new SVN %s rev %s, Git %s hash %s" %
                           (sub_name, new_svn_branch, new_rev, new_git_branch,
                            new_hash[:7]), sandbox_dir=sub_path, debug=debug,
                           verbose=verbose)
            raise SystemExit("Failed(PostUpdate)")

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
        print("## UpdExt %s added %s" % (database.project_name, sub_name))

    for ext_dir, ext_map in sorted(externs.items(), key=lambda x: x[0]):
        if ext_map.is_added:
            continue

        if sandbox_dir is None:
            ext_path = ext_dir
        else:
            ext_path = os.path.join(sandbox_dir, ext_dir)

        git_submodule_remove(ext_dir, sandbox_dir=sandbox_dir, debug=debug,
                             verbose=verbose)

        if not os.path.exists(ext_path):
            print("## UpdExt %s removed %s" %
                  (database.project_name, ext_dir))
        else:
            if extra_verbose or True:
                print("  SUBREMOVE %s" % (ext_dir, ))
                list_directory(ext_path, title=" Remove %s" % (ext_dir, ))
            shutil.rmtree(ext_path)
            print("## UpdExt %s removed %s )and directory)" %
                  (database.project_name, ext_dir))


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

    PDAQManager.set_home_directory()
    PDAQManager.load_authors("svn-authors", verbose=args.verbose)

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

    # execute everything in a temporary directory which will be erased on exit
    with TemporaryDirectory():
        print("Converting %s repo" % (args.svn_project, ))
        try:
            convert_svn_to_git(args.svn_project, gitmgr,
                               checkpoint=args.checkpoint,
                               destroy_existing_repo=True,
                               make_public=make_public,
                               organization=args.organization,
                               debug=args.debug, verbose=args.verbose)
        except:
            read_input("%s %% Hit Return to abort: " % os.getcwd())
            raise


if __name__ == "__main__":
    main()
