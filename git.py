#!/usr/bin/env python

from __future__ import print_function

import os
import re
import shutil
import sys
import tempfile

from cmdrunner import CommandException, default_returncode_handler, \
     run_command, run_generator


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


def git_branch(remote_name=None, upstream=None, sandbox_dir=None, debug=False,
               dry_run=False, verbose=False):
    "Add the specified file/directory to the SVN commit"

    cmd_args = ["git", "push"]

    if upstream is not None:
        cmd_args += ("-u", upstream)

    if remote_name is not None:
        cmd_args.append(remote_name)

    for line in run_generator(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
                              working_directory=sandbox_dir,
                              stderr_handler=__handle_stderr, debug=debug,
                              dry_run=dry_run, verbose=verbose):
        yield line


def __handle_checkout_stderr(cmdname, line, verbose=False):
    if verbose:
        print("%s!! %s" % (cmdname, line), file=sys.stderr)

    if line.startswith("Switched to a new branch"):
        return
    if line.startswith("Switched to branch "):
        return
    if line.startswith("Already on "):
        return
    if line.find("unable to rmdir "):
        if verbose:
            print("%s" % (line, ), file=sys.stderr)
        return

    raise GitException("%s failed: %s" % (cmdname, line))


def git_checkout(branch_name=None, start_point=None, new_branch=False,
                 recurse_submodules=False, sandbox_dir=None, debug=False,
                 dry_run=False, verbose=False):
    "Check out a branch (or 'master) of the Git repository"

    cmd_args = ["git", "checkout"]

    if new_branch:
        cmd_args.append("-b")
    if branch_name is not None:
        cmd_args.append(branch_name)
    if recurse_submodules:
        cmd_args.append("--recurse-submodules")
    if start_point is not None:
        cmd_args.append(str(start_point))

    run_command(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
                working_directory=sandbox_dir,
                stderr_handler=__handle_checkout_stderr, debug=debug,
                dry_run=dry_run, verbose=verbose)


class CloneHandler(object):
    RECURSE_SUPPORTED = True

    def __init__(self):
        self.__recurse_error = False

    @classmethod
    def __disable_recurse(cls):
        cls.RECURSE_SUPPORTED = False

    def clear_recurse_error(self):
        self.__recurse_error = False

    @classmethod
    def handle_clone_stderr(cls, cmdname, line, verbose=False):
        if verbose:
            print("%s!! %s" % (cmdname, line), file=sys.stderr)

        if line.startswith("Cloning into "):
            return
        if line.find("You appear to have cloned") >= 0:
            return
        if line.startswith("Updating files: "):
            return
        if line.startswith("Submodule ") and \
          line.find(" registered for path ") > 0:
            return

        raise GitException("%s failed: %s" % (cmdname, line))

    def handle_rtncode(self, cmdname, rtncode, lines, verbose=False):
        if not self.__recurse_error:
            default_returncode_handler(cmdname, rtncode, lines,
                                       verbose=verbose)

    def handle_stderr(self, cmdname, line, verbose=False):
        if verbose:
            print("%s!! %s" % (cmdname, line), file=sys.stderr)

        if self.__recurse_error:
            # ignore all errors after a 'recurse-submodules' error
            return

        if line.startswith("error: unknown option") and \
          line.find("recurse-submodules") > 0:
            self.__recurse_error = True
            self.__disable_recurse()
            return

        self.handle_clone_stderr(cmdname, line, verbose=False)

    @property
    def saw_recurse_error(self):
        return self.__recurse_error


def git_clone(url, recurse_submodules=False, sandbox_dir=None, target_dir=None,
              debug=False, dry_run=False, verbose=False):
    """
    Clone a Git repository
    sandbox_dir - if specified, create the cloned directory under `sandbox_dir`
    target_dir = if specified, use `target_dir` as the name of the cloned
                 directory
    """

    handler = CloneHandler()
    for new_recurse in CloneHandler.RECURSE_SUPPORTED, False:
        cmd_args = ["git", "clone"]
        if recurse_submodules:
            cmd_args.append("--recurse-submodules" if new_recurse
                            else "--recursive")
        cmd_args.append(url)
        if target_dir is not None:
            cmd_args.append(target_dir)

        handler.clear_recurse_error()

        run_command(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
                    working_directory=sandbox_dir,
                    returncode_handler=handler.handle_rtncode,
                    stderr_handler=handler.handle_stderr, debug=debug,
                    dry_run=dry_run, verbose=verbose)

        if not handler.saw_recurse_error:
            break


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
            print("%s!! %s" % (cmdname, line), file=sys.stderr)

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

            if line.startswith("On branch "):
                self.__branch = line[10:]
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

    def run_handler(self):
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
                self.__auto_pack_err = False
        finally:
            os.unlink(logfile.name)

        return self.tuple

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
    return handler.run_handler()


def git_diff(unified=False, sandbox_dir=None, debug=False, dry_run=False,
             verbose=False):
    "Return a list of changes to all files"

    cmd_args = ["git", "diff"]

    if unified:
        cmd_args.append("-U")

    for line in run_generator(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
                              working_directory=sandbox_dir,
                              stderr_handler=__handle_stderr, debug=debug,
                              dry_run=dry_run, verbose=verbose):
        yield line


def git_init(sandbox_dir=None, bare=False, debug=False, dry_run=False,
             verbose=False):
    "Add the specified file/directory to the SVN commit"

    cmd_args = ["git", "init"]
    if bare:
        cmd_args.append("--bare")
        sandbox_dir, project = sandbox_dir.rsplit("/", 1)
        if not project.endswith(".git"):
            project += ".git"
        cmd_args.append(project)

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


def __handle_stderr(cmdname, line, verbose=False):
    #if not line.startswith("Switched to a new branch"):
    #    raise GitException("%s failed: %s" % (cmdname, line))
    if verbose:
        print("%s!! %s" % (cmdname, line), file=sys.stderr)


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
                              stderr_handler=__handle_stderr, debug=debug,
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
            print("%s!! %s" % (cmdname, line, ), file=sys.stderr)

        if not line.startswith("fatal: pathspec") or \
          line.find("did not match any files") < 0:
            raise GitException("Remove failed: %s" % line.strip())
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
    print("%s!! %s" % (cmdname, line, ), file=sys.stderr)


def git_reset(start_point, hard=False, sandbox_dir=None, debug=False,
              dry_run=False, verbose=False):
    "Reset the current HEAD to the specified state"

    cmd_args = ["git", "reset"]

    if hard is not None:
        cmd_args.append("--hard")

    cmd_args.append(start_point)

    run_command(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
                stderr_handler=handle_reset_stderr,
                working_directory=sandbox_dir, debug=debug, dry_run=dry_run,
                verbose=verbose)


class ShowHashHandler(object):
    NO_PATCH_SUPPORTED = True

    def __init__(self):
        self.__no_patch_error = False

    @classmethod
    def disable_no_patch(cls):
        cls.NO_PATCH_SUPPORTED = False

    def clear_no_patch_error(self):
        self.__no_patch_error = False

    def handle_rtncode(self, cmdname, rtncode, lines, verbose=False):
        if not self.__no_patch_error:
            default_returncode_handler(cmdname, rtncode, lines,
                                       verbose=verbose)

    def handle_stderr(self, cmdname, line, verbose=False):
        if verbose:
            print("%s!! %s" % (cmdname, line, ), file=sys.stderr)

        if not line.startswith("fatal: unrecognized") or \
          line.find("--no-patch") < 0:
            raise GitException("ShowHash failed: %s" % line.strip())
        self.__no_patch_error = True
        self.disable_no_patch()

    @property
    def saw_no_patch_error(self):
        return self.__no_patch_error


# TODO: Make this a more comprehensive implementation of 'git show'
def git_show_hash(sandbox_dir=None, debug=False, dry_run=False, verbose=False):
    "Return the full hash of the current Git sandbox"

    handler = ShowHashHandler()
    for no_patch in True, False:
        cmd_args = ["git", "show", "--format=%H"]
        if no_patch and ShowHashHandler.NO_PATCH_SUPPORTED:
            cmd_args.append("--no-patch")

        handler.clear_no_patch_error()

        full_hash = None
        for line in run_generator(cmd_args,
                                  cmdname=" ".join(cmd_args[:2]).upper(),
                                  working_directory=sandbox_dir,
                                  returncode_handler=handler.handle_rtncode,
                                  stderr_handler=handler.handle_stderr,
                                  debug=debug, dry_run=dry_run,
                                  verbose=verbose):
            line = line.rstrip()
            if line == "":
                continue

            if line.startswith("fatal: ") and line.find("--no-patch") > 0:
                break

            if full_hash is None:
                full_hash = line.rstrip()
                continue

            if line.startswith("diff "):
                break

            raise GitException("Found multiple lines:\n%s\n%s" %
                               (full_hash, line.rstrip()))

        if full_hash is not None:
            break

    if full_hash is None:
        raise GitException("Cannot find full hash from 'git show %s'" %\
                           (sandbox_dir, ))

    return full_hash


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
    # 'submodule add' does an implicit clone, so ignore those errors as well
    CloneHandler.handle_clone_stderr(cmdname, line, verbose=verbose)


def git_submodule_add(url, git_hash=None, force=False, sandbox_dir=None,
                      debug=False,
                      dry_run=False, verbose=False):
    "Add a Git submodule"

    cmd_args = ["git", "submodule", "add"]
    if force:
        cmd_args.append("--force")
    cmd_args.append(url)

    run_command(cmd_args, cmdname=" ".join(cmd_args[:3]).upper(),
                working_directory=sandbox_dir,
                stderr_handler=__handle_sub_add_stderr, debug=debug,
                dry_run=dry_run, verbose=verbose)

    if git_hash is not None:
        _, name = url.rsplit(os.sep, 1)
        if name.endswith(".git"):
            name = name[:-4]

        git_submodule_update(name, git_hash, sandbox_dir=sandbox_dir,
                             initialize=True, debug=debug, verbose=verbose)

def git_submodule_init(url=None, sandbox_dir=None, debug=False, dry_run=False,
                       verbose=False):
    "Initialize Git submodules"

    cmd_args = ["git", "submodule", "init"]
    if url is not None:
        cmd_args.append(url)

    run_command(cmd_args, cmdname=" ".join(cmd_args[:3]).upper(),
                working_directory=sandbox_dir,
                stderr_handler=__handle_sub_add_stderr, debug=debug,
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
    Return tuples describing the status of this Git project's submodules.
    Each tuple contains (name, status, sha1, branchname)
    """

    cmd_args = ["git", "submodule", "status"]

    stat_pat = re.compile(r"^(.)(\S+)\s+([^(]+)(?:\s+\((.*)\))?\s*$")
    for line in run_generator(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
                              working_directory=sandbox_dir, debug=debug,
                              dry_run=dry_run, verbose=verbose):
        mtch = stat_pat.match(line)
        if mtch is None:
            print("WARNING: Ignoring unknown SUBMODULE STATUS line %s" %
                  (line, ), file=sys.stderr)
            continue

        # unpack the groups into named variables and return them in a
        # slightly shuffled order
        (status, sha1, name, branchname) = mtch.groups()
        yield (name, status, sha1, branchname)


def git_submodule_update(name=None, git_hash=None, initialize=False,
                         sandbox_dir=None, debug=False, dry_run=False,
                         verbose=False):
    "Update a single Git submodule"

    if git_hash is not None:
        update_args = ("git", "update-index", "--cacheinfo",
                       "160000", str(git_hash), str(name))

        try:
            run_command(update_args, cmdname=" ".join(update_args[:3]).upper(),
                        working_directory=sandbox_dir, debug=debug,
                        dry_run=dry_run, verbose=verbose)
        except CommandException as cex:
            raise GitException("Cannot update %s index to hash %s: %s" %
                               (name, git_hash, cex))

    cmd_args = ["git", "submodule", "update"]
    if initialize:
        cmd_args.append("--init")
    if name is not None:
        cmd_args.append(name)

    run_command(cmd_args, cmdname=" ".join(cmd_args[:3]).upper(),
                working_directory=sandbox_dir,
                stderr_handler=__handle_sub_add_stderr, debug=debug,
                dry_run=dry_run, verbose=verbose)
