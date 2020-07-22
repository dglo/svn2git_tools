#!/usr/bin/env python

import os
import re
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
    if not line.startswith("Switched to a new branch"):
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
    if not line.startswith("Cloning into ") and \
      line.find("You appear to have cloned") < 0:
        raise GitException("%s failed: %s" % (cmdname, line))


def git_clone(url, sandbox_dir=None, debug=False, dry_run=False,
              verbose=False):
    "Clone a Git repository"

    cmd_args = ("git", "clone", url)

    run_command(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
                working_directory=sandbox_dir,
                stderr_handler=__handle_clone_stderr, debug=debug,
                dry_run=dry_run, verbose=verbose)


def git_commit(sandbox_dir=None, author=None, commit_message=None,
               date_string=None, filelist=None, commit_all=False,
               debug=False, dry_run=False, verbose=False):
    """
    Commit all changes to the local repository

    Return a tuple containing:
    (branch_name, hash_id, number_changed, number_inserted, number_deleted)
    """

    global COMMIT_TOP_PAT, COMMIT_CHG_PAT
    global COMMIT_INS_PAT
    global COMMIT_DEL_PAT

    if COMMIT_TOP_PAT is None or COMMIT_CHG_PAT is None:
        COMMIT_TOP_PAT = re.compile(r"^\s*\[(\S+)\s+"
                                    r"(?:\(root-commit\)\s+)?(\S+)\] (.*)\s*$")
        COMMIT_CHG_PAT = re.compile(r"^\s*(\d+) files? changed(.*)$")
        COMMIT_INS_PAT = re.compile(r" (\d+) insertion")
        COMMIT_DEL_PAT = re.compile(r" (\d+) deletion")

    logfile = tempfile.NamedTemporaryFile(mode="w", delete=False)
    try:
        # write log message to a temporary file
        if commit_message is None:
            commit_file = os.devnull
        else:
            print(commit_message, file=logfile, end="")
            logfile.close()
            commit_file = logfile.name

        cmd_args = ["git", "commit", "-F", commit_file]

        if author is not None:
            cmd_args.append("--author=%s" % author)

        if date_string is not None:
            os.environ["GIT_COMMITTER_DATE"] = date_string
            cmd_args.append("--date=%s" % date_string)

        if filelist is not None:
            cmd_args += filelist
        elif commit_all:
            cmd_args.append("-a")

        if dry_run:
            print("GIT COMMIT " + " ".join(cmd_args[2:]))
            return None

        # commit everything
        if debug:
            print("CMD: %s" % " ".join(cmd_args))
        proc = subprocess.Popen(cmd_args, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, close_fds=True,
                                cwd=sandbox_dir)

        branch = None
        hash_id = None
        changed = None
        inserted = None
        deleted = None

        auto_pack_err = False
        for line in proc.stderr:
            line = line.rstrip().decode("utf-8")

            if verbose:
                print("COMMIT!! %s" % (line, ))

            if line.find("Auto packing the repository") >= 0:
                auto_pack_err = True
                continue
            if auto_pack_err:
                if line.find("for manual housekeeping") < 0:
                    print("!!AutoPack!! %s" % line, file=sys.stderr)
                continue
            raise GitException("Commit failed: %s" % line)

        cache = []
        for line in proc.stdout:
            line = line.rstrip().decode("utf-8")

            if verbose:
                print("COMMIT>> %s" % (line, ))
            else:
                cache.append(line)

            if branch is None and hash_id is None:
                mtch = COMMIT_TOP_PAT.match(line)
                if mtch is None:
                    raise GitException("Bad first line of commit: %s" % line)

                branch = mtch.group(1)
                hash_id = mtch.group(2)

                continue

            if changed is None or inserted is None or deleted is None:

                mtch = COMMIT_CHG_PAT.match(line)
                if mtch is not None:
                    changed = int(mtch.group(1))
                    stuff = mtch.group(2)

                    srch = COMMIT_INS_PAT.search(stuff)
                    if srch is not None:
                        inserted = int(mtch.group(1))
                    else:
                        inserted = 0

                    srch = COMMIT_DEL_PAT.search(stuff)
                    if srch is not None:
                        deleted = int(mtch.group(1))
                    else:
                        deleted = 0

                    continue

        # wait for subprocess to finish
        proc.wait()

        if proc.returncode != 0:
            if not verbose:
                print("Output from '%s'" % " ".join(cmd_args[:2]),
                      file=sys.stderr)
                for line in cache:
                    print(">> %s" % line, file=sys.stderr)
            raise GitException("Commit failed with returncode %d" %
                               proc.returncode)

        if changed is None or inserted is None or deleted is None:
            raise GitException("Changed/inserted/modified line not found")

        return (branch, hash_id, changed, inserted, deleted)
    finally:
        os.unlink(logfile.name)


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
            print("REMOVE!! %s" % (line, ))

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
                stderr_handler=handler.handle_stderr,
                returncode_handler=handler.handle_rtncode, debug=debug,
                dry_run=dry_run, verbose=verbose)


def git_reset(start_point, hard=False, sandbox_dir=None, debug=False,
              dry_run=False, verbose=False):
    "Add the specified file/directory to the SVN commit"

    cmd_args = ["git", "reset"]

    if hard is not None:
        cmd_args.append("--hard")

    cmd_args.append(start_point)

    run_command(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
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
