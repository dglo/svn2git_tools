#!/usr/bin/env python

from __future__ import print_function

import argparse
import os
import re
import shutil
import sys

from datetime import datetime

from cmdrunner import default_returncode_handler, run_generator
from i3helper import TemporaryDirectory, read_input
from pdaqdb import PDAQManager
from repostatus import DatabaseCollection, RepoStatus
from svn import SVNConnectException, SVNDate, SVNNonexistentException, \
     svn_checkout, svn_get_externals, svn_info, svn_switch, svn_update
from git import git_checkout, git_clone, git_status, git_submodule_update


def add_arguments(parser):
    "Add command-line arguments"

    parser.add_argument("-D", "--compare-sandboxes", dest="compare_sandboxes",
                        action="store_true", default=False,
                        help="Perform 'diff' on SVN & Git sandboxes")
    parser.add_argument("-P", "--pause", dest="pause",
                        action="store_true", default=False,
                        help="Pause for input after each step")
    parser.add_argument("-S", "--skip-project", dest="skip_projects",
                        action="append",
                        help="Skip one or more projects in the list")
    parser.add_argument("-v", "--verbose", dest="verbose",
                        action="store_true", default=False,
                        help="Print details")
    parser.add_argument("-x", "--debug", dest="debug",
                        action="store_true", default=False,
                        help="Print debugging messages")

    parser.add_argument(dest="projects", nargs="*", default=None,
                        help="Subversion/Mantis project name(s)")


class DiffErrorHandler(object):
    def __init__(self):
        self.__no_deref_error = False
        self.__ignore_rtncode = False

    def clear_errors(self):
        self.__no_deref_error = False
        self.__ignore_rtncode = True

    def handle_rtncode(self, cmdname, rtncode, lines, verbose=False):
        if not self.__no_deref_error and not self.__ignore_rtncode:
            default_returncode_handler(cmdname, rtncode, lines,
                                       verbose=verbose)

    def handle_stderr(self, cmdname, line, verbose=False):
        if verbose:
            print("%s!! %s" % (cmdname, line, ), file=sys.stderr)

        if self.__no_deref_error:
            return

        if not line.startswith("diff: unrecognized") or \
          line.find("--no-dereference") < 0:
            raise Exception("Diff failed: %s" % line.strip())

        self.__no_deref_error = True

    @property
    def ignore_rtncode(self):
        return self.__ignore_rtncode

    @ignore_rtncode.setter
    def ignore_rtncode(self, val):
        self.__ignore_rtncode = (val == True)

    @property
    def saw_no_deref_error(self):
        return self.__no_deref_error


class Crosscheck(DatabaseCollection):
    SVN_URL_FMT = "http://code.icecube.wisc.edu/daq/%s/%s"
    GIT_URL_FMT = "file:///home/dglo/prj/pdaq-git/svn_tools/git-repo/%s.git"

    SVN_SANDBOX_FMT = "%s-svn"
    GIT_SANDBOX_FMT = "%s-git"

    SPECIAL_FILES = (".git", ".gitignore", ".gitmodules", ".svn")

    DATE_PAT = re.compile(r"(\d+-\d+-\d+)\s+(\d+:\d+:\d+\.\d+)"
                          r"\s+([\-\+])(\d+)\s+\(.*\)\s*$")

    def __init__(self, project):
        self.__project = project

    def __compare_sandboxes(self, title, git_sandbox, svn_sandbox,
                            sandbox_dir=None, debug=False, verbose=False):
        sandboxes = (svn_sandbox, git_sandbox)

        handler = DiffErrorHandler()

        for deref in True, False:
            cmd_args = ["diff", "-ru"]
            for ign in self.SPECIAL_FILES:
                cmd_args += ("-x", ign)
            if deref:
                cmd_args.append("--no-dereference")
            cmd_args += sandboxes


            handler.clear_errors()

            printed = False
            rtncode = 0
            for line in run_generator(cmd_args, cmdname=cmd_args[0],
                                      returncode_handler=\
                                      handler.handle_rtncode,
                                      working_directory=sandbox_dir,
                                      stderr_handler=handler.handle_stderr,
                                      debug=debug, verbose=False):
                if line.startswith("Only in "):
                    mid = line.find(": ")
                    if mid <= 0:
                        raise Exception("Bad diff %s line: %s" %
                                        ("<=>".join(sandboxes), line))
                    path = os.path.join(line[8:mid], line[mid+2:].rstrip())
                    if not self.__is_empty_directory(path):
                        rtncode -= 1
                    elif verbose:
                        handler.ignore_rtncode = True
                        print("Ignoring empty SVN directory %s" % (path, ))
                    continue

                if not printed:
                    print("=== %s" % (title, ))
                    printed = True

                print("%s" % (line, ))
                rtncode -= 1

            if not handler.saw_no_deref_error:
                break

        if verbose and not printed:
            if rtncode == 0:
                rstr = ""
            else:
                rstr = " (rtncode %d)" % (rtncode, )
            print("--- No differences for %s%s" % (title, rstr))

        return rtncode

    def __create_sandboxes(self, git_sandbox, svn_sandbox, pause=False,
                           debug=False, verbose=False):
        if not verbose:
            print("Checking out '%s'" % (svn_sandbox, ))
        checked_out = False
        for topname in ("meta-projects", "projects"):
            for subname in ("trunk", None):
                svn_url = self.SVN_URL_FMT % (topname, self.__project, )
                if subname is not None:
                    svn_url = os.path.join(svn_url, subname)
                try:
                    svn_checkout(svn_url, target_dir=svn_sandbox, debug=debug,
                                 verbose=verbose)
                    checked_out = True
                    break
                except SVNNonexistentException:
                    continue
            if checked_out:
                break
        if not checked_out:
            svn_url = self.SVN_URL_FMT % ("meta-projects", self.__project, )
            raise SVNNonexistentException(svn_url)

        if not verbose:
            print("Cloning '%s'" % (git_sandbox, ))
        git_clone(self.GIT_URL_FMT % (self.__project, ),
                  recurse_submodules=True, target_dir=git_sandbox, debug=debug,
                  verbose=verbose)

        if pause:
            read_input("%s %% Hit Return after checkout: " % os.getcwd())

        return svn_url

    def __delete_untracked(self, git_sandbox, debug=False, verbose=False):
        untracked = False
        directories = []
        for line in git_status(sandbox_dir=git_sandbox, debug=debug,
                               verbose=verbose):
            if not untracked:
                if line.startswith("Untracked files:"):
                    untracked = True
                continue

            filename = line.strip()
            if len(filename) == 0 or filename.find("use \"git add") >= 0:
                continue

            if filename.endswith("/"):
                shutil.rmtree(os.path.join(git_sandbox, filename[:-1]))
                directories.append(filename[:-1])
            else:
                os.remove(os.path.join(git_sandbox, filename))

    def __fix_svn_externals(self, svn_sandbox, branch, revision, pause=False,
                            debug=False, verbose=False):
        externals = []
        for subrev, _, subdir in svn_get_externals(svn_sandbox,
                                                   revision=revision,
                                                   debug=debug,
                                                   verbose=verbose):
            externals.append((subrev, subdir))

        # extract "Last Changed Date" from 'svn info'
        info = svn_info(svn_sandbox, debug=debug, verbose=verbose)
        svn_date = SVNDate(info["last_changed_date"])

        if debug:
            print("*** %s rev %s: Found %d externals, last changed %s" %
                  (branch, revision, len(externals), svn_date))

        # update all subprojects to specified date
        for subrev, subdir in externals:
            if subrev is None:
                _, subrev = self.__get_revision_from_date(svn_date.string)

            subpath = os.path.join(svn_sandbox, subdir)
            for line in svn_update(svn_url=subpath,
                                   revision=subrev, debug=debug,
                                   verbose=verbose):
                if True or verbose:
                    if line.startswith("Updating ") or \
                      line.startswith("Updated "):
                        print("[%s] -> %s" % (subrev, line, ))

        if pause:
            read_input("%s %% Hit Return after fixing %s rev %s: " %
                       (os.path.join(os.getcwd(), svn_sandbox), branch,
                        revision))

    def __get_revision_from_date(self, date_string):
        conn = self.db_connection(self.__project)

        branch, revision = (None, None)
        with conn:
            cursor = conn.cursor()

            cursor.execute("select branch, revision from svn_log where date<=?"
                           " order by date desc limit 1", (date_string, ))
            for row in cursor:
                branch = row[0]
                revision = int(row[1])
                break

            if branch is None and revision is None:
                cursor.execute("select branch, revision from svn_log"
                               " order by revision asc limit 1")

                for row in cursor:
                    branch = row[0]
                    revision = int(row[1])
                    break

        return branch, revision

    @classmethod
    def __is_empty_directory(cls, path):
        for _, dirs, files in os.walk(path):
            dirs[:] = [entry for entry in dirs
                       if entry not in cls.SPECIAL_FILES]
            if len(files) > 0:
                for entry in files:
                    return False
        return True

    def __show_git_status(self, title, repo_status):
        print("=== Git status :: %s" % (title, ))
        for name, rstat in sorted(repo_status.items(), key=lambda x: x[0]):
            if rstat.git_status is None:
                sstr = "[?]"
            elif rstat.git_status != " ":
                sstr = "[%s]" % (rstat.git_status, )
            else:
                sstr = ""

            print("%s: %s/%s%s -> %s rev %s" %
                  (name, rstat.git_branch, rstat.git_hash[12:], sstr,
                   rstat.svn_branch, rstat.svn_revision))

    def __show_svn_status(self, title, repo_status):
        print("=== SVN status :: %s" % (title, ))
        for name, rstat in sorted(repo_status.items(), key=lambda x: x[0]):
            print("%s: %s rev %s (%s) -> %s/%s" %
                  (name, rstat.svn_branch, rstat.svn_revision, rstat.svn_date,
                   rstat.git_branch, rstat.git_hash))

    def __update_to_next(self, svn_sandbox, base_url, branch, prev_branch,
                         revision, verbose=False, debug=False):
        if branch == prev_branch:
            for line in svn_update(sandbox_dir=svn_sandbox,
                                   revision=revision,
                                   ignore_bad_externals=True,
                                   debug=debug, verbose=debug):
                if verbose:
                    print("%s" % (line, ))
        else:
            for line in svn_switch(os.path.join(base_url, branch),
                                   revision=revision,
                                   ignore_bad_externals=True,
                                   sandbox_dir=svn_sandbox,
                                   debug=debug, verbose=debug):
                if verbose:
                    print("%s" % (line, ))

    @property
    def all_entries(self):
        conn = self.db_connection(self.__project)

        with conn:
            cursor = conn.cursor()

            cursor.execute("select branch, revision, git_branch, git_hash"
                           " from svn_log order by revision")

            for row in cursor:
                yield row[0], int(row[1]), row[2], row[3]

    def compare(self, compare_sandboxes=False, pause=False,
                debug=False, verbose=False):
        svn_sandbox = self.SVN_SANDBOX_FMT % (self.__project, )
        git_sandbox = self.GIT_SANDBOX_FMT % (self.__project, )

        svn_url = self.__create_sandboxes(git_sandbox, svn_sandbox,
                                          debug=debug, verbose=verbose)
        if not svn_url.endswith("/trunk"):
            base_url = svn_url
        else:
            base_url = svn_url[:-6]

        entries = 0
        checked = 0
        skipped = 0

        prev_branch = "trunk"
        for branch, revision, git_branch, git_hash in self.all_entries:
            entries += 1
            if verbose:
                print("*** %s rev %s -> %s/%s" %
                      (branch, revision, git_branch, git_hash))
            elif not debug:
                tmpline = "*** %s rev %s -> %s/%s%s" % \
                  (branch, revision, git_branch, git_hash, " "*80)
                print("\r%s\r" % tmpline[:79], end="")

            if git_branch is None or git_hash is None:
                if not verbose and not debug:
                    print()
                print("WARNING: Skipping %s rev %d; no Git branch/hash" %
                      (branch, revision), file=sys.stderr)
                skipped += 1
                continue

            # update SVN sandbox to this branch/release
            success = False
            missing_rev = False
            for attempt in (0, 1, 2):
                try:
                    self.__update_to_next(svn_sandbox, base_url, branch,
                                          prev_branch, revision,
                                          verbose=verbose, debug=debug)
                    prev_branch = branch
                    success = True
                    break
                except SVNConnectException as sex:
                    print("Retrying update to %s rev %s: %s" %
                          (branch, revision, sex), file=sys.stderr)
                except SVNNonexistentException as nex:
                    nexstr = str(nex)
                    if nexstr.find("Bad Subversion URL ") >= 0:
                        missing_rev = True
                        break

                    # raise unknown exception
                    raise

            if missing_rev:
                longline = "\rWARNING: Ignoring missing %s rev %s for %s%s" % \
                      (branch, revision, self.__project, " "*80)
                print(longline[:79], file=sys.stderr)
                continue

            if not success:
                raise SVNConnectException("Failed to update %s to %s rev %s" %
                                          (self.__project, branch, revision))

            self.__fix_svn_externals(svn_sandbox, branch, revision,
                                     pause=pause, debug=debug, verbose=verbose)

            # update Git sandbox to this release
            try:
                if verbose or debug:
                    tmp_repo = RepoStatus.from_git(self.__project, git_sandbox,
                                                   debug=debug)
                    self.__show_git_status("Before checkout", tmp_repo)
                git_checkout(start_point=git_hash, sandbox_dir=git_sandbox,
                             debug=debug, verbose=verbose)
                if verbose or debug:
                    tmp_repo = RepoStatus.from_git(self.__project, git_sandbox,
                                                   debug=debug)
                    self.__show_git_status("Before submodule update", tmp_repo)
                git_submodule_update(sandbox_dir=git_sandbox, debug=debug,
                                     verbose=verbose)
                self.__delete_untracked(git_sandbox, debug=debug,
                                        verbose=verbose)
                if verbose or debug:
                    tmp_repo = RepoStatus.from_git(self.__project, git_sandbox,
                                                   debug=debug)
                    self.__show_git_status("Git sandbox is ready", tmp_repo)
            except:
                import traceback; traceback.print_exc()
                if pause:
                    read_input("%s %% Hit Return to exit: " % os.getcwd())
                raise

            svn_repo = RepoStatus.from_svn(self.__project, svn_sandbox,
                                           debug=debug, verbose=verbose)
            git_repo = RepoStatus.from_git(self.__project, git_sandbox,
                                           debug=debug)
            RepoStatus.compare(self.__project, svn_repo, git_repo)
            checked += 1

            if compare_sandboxes:
                found_diff = self.__compare_sandboxes("%s rev %s" %
                                                      (branch, revision),
                                                      git_sandbox, svn_sandbox,
                                                      debug=debug,
                                                      verbose=verbose)
                if found_diff:
                    read_input("%s %% Hit Return after diff: " % os.getcwd())
            elif pause:
                read_input("%s %% Hit Return to unpause: " % os.getcwd())

        if checked == entries:
            longline = "All %d entries matched%s" % (entries, " "*80)
            print("%s" % (longline[:79], ))
        else:
            print("\n")
            print("WARNING: Checked %d and skipped %d entries (total=%d)" %
                  (checked, skipped, entries), file=sys.stderr)


def main():
    parser = argparse.ArgumentParser()
    add_arguments(parser)
    args = parser.parse_args()

    if args.projects is not None and len(args.projects) > 0:
        projects = args.projects
    else:
        projects = PDAQManager.PROJECT_NAMES

    if args.skip_projects is not None and len(args.skip_projects) > 0:
        skipped = args.skip_projects
    else:
        skipped = []

    with TemporaryDirectory() as tmpdir:
        RepoStatus.set_database_home(tmpdir.original)
        Crosscheck.set_database_home(tmpdir.original)

        for project in projects:
            if project in skipped:
                continue

            start_time = datetime.now()
            cmps2g = Crosscheck(project)
            try:
                cmps2g.compare(compare_sandboxes=args.compare_sandboxes,
                               pause=args.pause, debug=args.debug,
                               verbose=args.verbose)
            except:
                import traceback; traceback.print_exc()
                read_input("%s %% Hit Return to continue: " % os.getcwd())

            elapsed = datetime.now() - start_time
            seconds = float(elapsed.seconds) + \
              (float(elapsed.microseconds) / 1000000.0)
            print("%s took %.03f seconds" % (project, seconds))


if __name__ == "__main__":
    main()
