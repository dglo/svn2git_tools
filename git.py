#!/usr/bin/env python

from __future__ import print_function

import os
import re
import shutil
import subprocess
import sys
import tempfile

from cmdrunner import default_returncode_handler, run_command, run_generator


COMMIT_TOP_PAT = None
COMMIT_CHG_PAT = None
COMMIT_INS_PAT = None
COMMIT_DEL_PAT = None
ISSUE_OPEN_PAT = None


class GitException(Exception):
    "General Git exception"


def git_add(filelist, sandbox_dir=None, debug=False, dry_run=False,
            verbose=False):
    "Add the specified files/directories to the GIT commit index"

    if isinstance(filelist, (tuple, list)):
        cmd_args = ["git", "add"] + filelist
    else:
        cmd_args = ("git", "add", str(filelist))

    run_command(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
                working_directory=sandbox_dir, debug=debug, dry_run=dry_run,
                verbose=verbose)


def git_autocrlf(sandbox_dir=None, debug=False, dry_run=False, verbose=False):
    "Hack around overeager Git line policing"

    cmd_args = ("git", "config", "--global", "core.autocrlf", "false")

    run_command(cmd_args, cmdname="GIT AUTOCRLF",
                working_directory=sandbox_dir, debug=debug, dry_run=dry_run,
                verbose=verbose)


def __handle_checkout_stderr(cmdname, line, verbose=False):
    if line.startswith("Switched to a new branch"):
        return
    if line.startswith("Switched to branch "):
        return
    if line.startswith("Already on "):
        return

    raise GitException("%s failed: %s" % (cmdname, line))


def git_checkout(branch_name, start_point=None, new_branch=False,
                 sandbox_dir=None, debug=False, dry_run=False, verbose=False):
    "Check out a branch (or 'master) of the Git repository"

    cmd_args = ["git", "checkout"]

    if new_branch:
        cmd_args.append("-b")

    cmd_args.append(branch_name)

    if start_point is not None:
        cmd_args.append(str(start_point))

    run_command(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
                working_directory=sandbox_dir,
                stderr_handler=__handle_checkout_stderr, debug=debug,
                dry_run=dry_run, verbose=verbose)


def __handle_clone_stderr(cmdname, line, verbose=False):
    if line.startswith("Cloning into "):
        return
    if line.find("You appear to have cloned") >= 0:
        return
    if line.startswith("Updating files: "):
        return

    raise GitException("%s failed: %s" % (cmdname, line))


def git_clone(url, sandbox_dir=None, debug=False, dry_run=False,
              verbose=False):
    "Clone a Git repository"

    cmd_args = ("git", "clone", url)

    run_command(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
                working_directory=sandbox_dir,
                stderr_handler=__handle_clone_stderr, debug=debug,
                dry_run=dry_run, verbose=verbose)


class CommitHandler(object):
    "Retry 'svn commit' command if it times out"

    COMMIT_TOP_PAT = None
    COMMIT_CHG_PAT = None
    COMMIT_INS_PAT = None
    COMMIT_DEL_PAT = None

    def __init__(self, sandbox_dir=None, author=None, commit_message=None,
                 date_string=None, filelist=None, commit_all=False,
                 debug=False, dry_run=False, verbose=False):

        self.__init_regexps()

        if sandbox_dir is None:
            self.__sandbox_dir = "."
        else:
            self.__sandbox_dir = sandbox_dir

        self.__extra_args = []
        if author is not None:
            self.__extra_args.append("--author=%s" % author)

        if date_string is not None:
            os.environ["GIT_COMMITTER_DATE"] = date_string
            self.__extra_args.append("--date=%s" % date_string)

        if filelist is not None:
            self.__extra_args += filelist
        elif commit_all:
            self.__extra_args.append("-a")

        #self.__author = author
        #self.__date_string = date_string
        #self.__filelist = filelist
        #self.__commit_all = commit_all

        self.__commit_message = commit_message
        self.__debug = debug
        self.__dry_run = dry_run
        self.__verbose = verbose

        self.__saw_error = False
        self.__auto_pack_err = False

        self.__branch = None
        self.__hash_id = None
        self.__changed = None
        self.__inserted = None
        self.__deleted = None

    @classmethod
    def __init_regexps(cls):
        if cls.COMMIT_TOP_PAT is None:
            cls.COMMIT_TOP_PAT = re.compile(r"^\s*\[(\S+)\s+"
                                            r"(?:\(root-commit\)\s+)?(\S+)\]"
                                            r" (.*)\s*$")
        if cls.COMMIT_CHG_PAT is None:
            cls.COMMIT_CHG_PAT = re.compile(r"^\s*(\d+) files? changed(.*)$")
        if cls.COMMIT_INS_PAT is None:
            cls.COMMIT_INS_PAT = re.compile(r" (\d+) insertion")
        if cls.COMMIT_DEL_PAT is None:
            cls.COMMIT_DEL_PAT = re.compile(r" (\d+) deletion")

    def __hndl_rtncd(self, cmdname, rtncode, lines, verbose=False):
        if not self.__saw_error:
            default_returncode_handler(cmdname, rtncode, lines,
                                       verbose=verbose)

    def handle_stderr(self, cmdname, line, verbose=False):
        if self.__verbose:
            print("COMMIT!! %s" % (line, ))

        if line.find("Auto packing the repository") >= 0:
            self.__auto_pack_err = True
        elif self.__auto_pack_err:
            if line.find("for manual housekeeping") < 0:
                print("!!AutoPack!! %s" % (line, ), file=sys.stderr)
        else:
            self.__saw_error = True
            raise GitException("Commit failed: %s" % line)

    def __process_line(self, line):
        # check for initial commit line with Git branch/hash info
        if self.__branch is None and self.__hash_id is None:
            mtch = self.COMMIT_TOP_PAT.match(line)
            if mtch is not None:
                self.__branch = mtch.group(1)
                self.__hash_id = mtch.group(2)
                return

            raise GitException("Bad first line of commit: %s" % line)

        # check for changed/inserted/deleted line
        if self.__changed is None or self.__inserted is None or \
          self.__deleted is None:
            mtch = self.COMMIT_CHG_PAT.match(line)
            if mtch is not None:
                self.__changed = int(mtch.group(1))
                stuff = mtch.group(2)

                srch = self.COMMIT_INS_PAT.search(stuff)
                if srch is not None:
                    self.__inserted = int(mtch.group(1))
                else:
                    self.__inserted = 0

                srch = self.COMMIT_DEL_PAT.search(stuff)
                if srch is not None:
                    self.__deleted = int(mtch.group(1))
                else:
                    self.__deleted = 0

            return

        if line.find("delete mode ") >= 0:
            return

        if self.__verbose:
            print("COMMIT IGNORED>> %s" % (line, ))

    def run(self):
        logfile = tempfile.NamedTemporaryFile(mode="w", delete=False)
        try:
            # write log message to a temporary file
            if self.__commit_message is None:
                commit_file = os.devnull
            else:
                print("%s" % self.__commit_message, file=logfile, end="")
                logfile.close()
                commit_file = logfile.name

            cmd_args = ["git", "commit", "-F", commit_file] + self.__extra_args
            cmdname = " ".join(cmd_args[:2]).upper()

            while True:
                for line in run_generator(cmd_args, cmdname=cmdname,
                                          returncode_handler=self.__hndl_rtncd,
                                          stderr_handler=self.handle_stderr,
                                          debug=self.__debug,
                                          dry_run=self.__dry_run,
                                          verbose=self.__verbose):
                    self.__process_line(line)

                # no errors seen, we're done
                if not self.__saw_error:
                    break

                # reset flags and try again
                self.__saw_error = False
                self.__auto_pack_error = False
        finally:
            os.unlink(logfile.name)

    @property
    def tuple(self):
        return (self.__branch, self.__hash_id, self.__changed,
                self.__inserted, self.__deleted)


def git_commit(sandbox_dir=None, author=None, commit_message=None,
               date_string=None, filelist=None, commit_all=False,
               debug=False, dry_run=False, verbose=False):
    """
    Commit all changes to the local repository

    Return a tuple containing:
    (branch_name, hash_id, number_changed, number_inserted, number_deleted)
    """

    handler = CommitHandler(sandbox_dir, author, commit_message, date_string,
                            filelist, commit_all=commit_all, debug=debug,
                            dry_run=dry_run, verbose=verbose)
    handler.run()
    return handler.tuple


def git_init(sandbox_dir=None, debug=False, dry_run=False, verbose=False):
    "Add the specified file/directory to the SVN commit"

    cmd_args = ("git", "init")

    run_command(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
                working_directory=sandbox_dir, debug=debug, dry_run=dry_run,
                verbose=verbose)


def git_log(sandbox_dir=None, debug=False, dry_run=False, verbose=False):
    "Return the log entries for the sandbox"

    cmd_args = ("git", "log")

    for line in run_generator(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
                              working_directory=sandbox_dir, debug=debug,
                              dry_run=dry_run, verbose=verbose):
        yield line


def __handle_push_stderr(cmdname, line, verbose=False):
    #if not line.startswith("Switched to a new branch"):
    #    raise GitException("%s failed: %s" % (cmdname, line))
    if verbose:
        print(">> %s" % line)


def git_push(remote_name=None, upstream=None, sandbox_dir=None, debug=False,
             dry_run=False, verbose=False):
    "Add the specified file/directory to the SVN commit"

    cmd_args = ["git", "push"]

    if upstream is not None:
        cmd_args += ("-u", upstream)

    if remote_name is not None:
        cmd_args.append(remote_name)

    for line in run_generator(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
                              working_directory=sandbox_dir,
                              stderr_handler=__handle_push_stderr, debug=debug,
                              dry_run=dry_run, verbose=verbose):
        yield line


def git_remote_add(remote_name, url, sandbox_dir=None, debug=False,
                   dry_run=False, verbose=False):
    "Add a new remote to the Git sandbox"

    cmd_args = ("git", "remote", "add", remote_name, url)

    for line in run_generator(cmd_args, cmdname="GIT RMTADD",
                              working_directory=sandbox_dir, debug=debug,
                              dry_run=dry_run, verbose=verbose):
        yield line


class RemoveHandler(object):
    def __init__(self):
        self.__expect_error = False

    def handle_rtncode(self, cmdname, rtncode, lines, verbose=False):
        if not self.__expect_error:
            default_returncode_handler(cmdname, rtncode, lines,
                                       verbose=verbose)

    def handle_stderr(self, cmdname, line, verbose=False):
        if verbose:
            print("!!REMOVE!! %s" % (line, ))

        if not line.startswith("fatal: pathspec") or \
          line.find("did not match any files") < 0:
            raise GitException("Remove failed: %s" %
                               line.strip().decode("utf-8"))
        self.__expect_error = True


def git_remove(filelist, sandbox_dir=None, debug=False, dry_run=False,
               verbose=False):
    "Remove the specified files/directories from the GIT commit index"

    if isinstance(filelist, (tuple, list)):
        cmd_args = ["git", "rm", "-r"] + filelist
    else:
        cmd_args = ("git", "rm", "-r", str(filelist))

    handler = RemoveHandler()
    run_command(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
                working_directory=sandbox_dir,
                returncode_handler=handler.handle_rtncode,
                stderr_handler=handler.handle_stderr, debug=debug,
                dry_run=dry_run, verbose=verbose)


def handle_reset_stderr(cmdname, line, verbose=False):
    print("!!RESET!! %s" % (line, ), file=sys.stderr)


def git_reset(start_point, hard=False, sandbox_dir=None, debug=False,
              dry_run=False, verbose=False):
    "Add the specified file/directory to the SVN commit"

    cmd_args = ["git", "reset"]

    if hard is not None:
        cmd_args.append("--hard")

    cmd_args.append(start_point)

    run_command(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
                stderr_handler=handle_reset_stderr,
                working_directory=sandbox_dir, debug=debug, dry_run=dry_run,
                verbose=verbose)


def git_status(sandbox_dir=None, porcelain=False, debug=False, dry_run=False,
               verbose=False):
    "Return the lines describing the status of the Git sandbox"

    cmd_args = ["git", "status"]

    if porcelain:
        cmd_args.append("--porcelain")

    for line in run_generator(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
                              working_directory=sandbox_dir, debug=debug,
                              dry_run=dry_run, verbose=verbose):
        yield line


def __handle_sub_add_stderr(cmdname, line, verbose=False):
    # 'submodule add' does an implicit clone, so ignore all those errors
    __handle_clone_stderr(cmdname, line, verbose=verbose)


def git_submodule_add(url, git_hash=None, sandbox_dir=None, debug=False,
                      dry_run=False, verbose=False):
    "Add a Git submodule"

    cmd_args = ("git", "submodule", "add", url)

    run_command(cmd_args, cmdname=" ".join(cmd_args[:3]).upper(),
                working_directory=sandbox_dir,
                stderr_handler=__handle_sub_add_stderr, debug=debug,
                dry_run=dry_run, verbose=verbose)

    if git_hash is not None:
        _, name = url.rsplit(os.sep, 1)
        update_args = ("git", "update-index", "--cacheinfo", "160000",
                       str(git_hash), name)

        run_command(update_args, cmdname=" ".join(update_args[:3]).upper(),
                    working_directory=sandbox_dir, debug=debug,
                    dry_run=dry_run, verbose=verbose)

def git_submodule_remove(name, sandbox_dir=None, debug=False, dry_run=False,
                         verbose=False):
    "Remove a Git submodule"

    cmd_args = ("git", "rm", name)

    run_command(cmd_args, cmdname=" ".join(cmd_args[:3]).upper(),
                working_directory=sandbox_dir,
                stderr_handler=__handle_sub_add_stderr, debug=debug,
                dry_run=dry_run, verbose=verbose)

    # if necessary, remove the cached repository information
    if sandbox_dir is not None:
        topdir = sandbox_dir
    else:
        topdir = os.getcwd()
    subpath = os.path.join(topdir, ".git", "modules", name)
    if os.path.exists(subpath):
        if debug:
            print("RMTREE %s" % subpath)
        shutil.rmtree(subpath)
    else:
        print("WARNING: Cannot removed cached submodule %s" % (subpath, ),
              file=sys.stderr)

def git_submodule_status(sandbox_dir=None, debug=False, dry_run=False,
                         verbose=False):
    """
    Return lines describing the status of this Git project's submodules
    """

    cmd_args = ["git", "submodule", "status"]

    for line in run_generator(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
                              working_directory=sandbox_dir, debug=debug,
                              dry_run=dry_run, verbose=verbose):
        yield line
