#!/usr/bin/env python

from __future__ import print_function

import argparse
import os
import re
import shutil
import sys
import tempfile

from cmdrunner import run_generator
from i3helper import read_input
from pdaqdb import PDAQManager
from repostatus import DatabaseCollection, RepoStatus
from svn import SVNDate, SVNNonexistentException, svn_checkout, \
     svn_get_externals, svn_info, svn_switch, svn_update
from git import git_checkout, git_clone, git_status, git_submodule_update


def add_arguments(parser):
    "Add command-line arguments"

    parser.add_argument("-D", "--compare-sandboxes", dest="compare_sandboxes",
                        action="store_true", default=False,
                        help="Perform 'diff' on SVN & Git sandboxes")
    parser.add_argument("-P", "--pause", dest="pause",
                        action="store_true", default=False,
                        help="Pause for input after each step")
    parser.add_argument("-v", "--verbose", dest="verbose",
                        action="store_true", default=False,
                        help="Print details")
    parser.add_argument("-x", "--debug", dest="debug",
                        action="store_true", default=False,
                        help="Print debugging messages")

    parser.add_argument(dest="projects", nargs="*", default=None,
                        help="Subversion/Mantis project name(s)")


class Crosscheck(DatabaseCollection):
    SVN_URL_FMT = "http://code.icecube.wisc.edu/daq/%s/%s"
    GIT_URL_FMT = "file:///home/dglo/prj/pdaq-git/svn_tools/git-repo/%s.git"

    SVN_SANDBOX_FMT = "%s-svn"
    GIT_SANDBOX_FMT = "%s-git"

    DATE_PAT = re.compile(r"(\d+-\d+-\d+)\s+(\d+:\d+:\d+\.\d+)"
                          r"\s+([\-\+])(\d+)\s+\(.*\)\s*$")

    def __init__(self, project):
        self.__project = project
        self.__diff_rtncode = None

    def __compare_sandboxes(self, title, git_sandbox, svn_sandbox,
                            sandbox_dir=None, debug=False, verbose=False):
        ignored = (".git", ".gitignore", ".gitmodules", ".svn")

        sandboxes = (svn_sandbox, git_sandbox)

        cmd_args = ["diff", "-ru"]
        for ign in ignored:
            cmd_args += ("-x", ign)
        cmd_args.append("--no-dereference")
        cmd_args += sandboxes

        # reset the return code
        self.__diff_rtncode = None

        printed = False
        rtncode = 0
        for line in run_generator(cmd_args, cmdname=cmd_args[0],
                                  returncode_handler=\
                                  self.__handle_diff_rtncode,
                                  working_directory=sandbox_dir, debug=debug,
                                  verbose=False):
            if line.startswith("Only in "):
                mid = line.find(": ")
                if mid <= 0:
                    raise Exception("Bad diff %s line: %s" %
                                    ("<=>".join(sandboxes), line))
                path = os.path.join(line[8:mid], line[mid+2:].rstrip())
                if not self.__is_empty_directory(path):
                    if verbose:
                        print("Ignoring empty SVN directory %s" % (path, ))
                    rtncode -= 1
                continue

            if not printed:
                print("=== %s" % (title, ))
                printed = True

            print("%s" % (line, ))
            rtncode -= 1

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
                except SVNNonexistentException as sex:
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
                _, subrev = self.__get_revision_from_date(subdir,
                                                          svn_date.string)

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

    def __handle_diff_rtncode(self, cmdname, rtncode, lines, verbose=False):
        self.__diff_rtncode = rtncode

    @classmethod
    def __is_empty_directory(cls, path):
        for root, dirs, files in os.walk(path):
            if len(files) > 0:
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
                print("*** %s rev %s -> %s/%s%s" %
                      (branch, revision, git_branch, git_hash))
            elif not debug:
                tmpline = "*** %s rev %s -> %s/%s%s" % \
                  (branch, revision, git_branch, git_hash, " "*80)
                print("\r%s\r" % tmpline[:79], end="")

            if git_branch is None or git_hash is None:
                print("WARNING: Skipping %s rev %d; no Git branch/hash" %
                      (branch, revision), file=sys.stderr)
                skipped += 1
                continue

            # update SVN sandbox to this branch/release
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

                prev_branch = branch

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
                    read_input("%s %% Hit Return to continue: " % os.getcwd())
            elif pause:
                read_input("%s %% Hit Return to continue: " % os.getcwd())

        if checked == entries:
            print("All %d entries matched" % (entries, ))
        print("WARNING: Checked %d and skipped %d entries (total=%d)" %
              (checked, skipped, entries))

def main():
    parser = argparse.ArgumentParser()
    add_arguments(parser)
    args = parser.parse_args()

    if args.projects is not None and len(args.projects) > 0:
        projects = args.projects
    else:
        projects = PDAQManager.PROJECT_NAMES

    origdir = os.getcwd()
    scratchdir = tempfile.mkdtemp()
    try:
        RepoStatus.set_database_home(origdir)
        Crosscheck.set_database_home(origdir)

        os.chdir(scratchdir)
        for project in projects:
            cmps2g = Crosscheck(project)
            try:
                cmps2g.compare(compare_sandboxes=args.compare_sandboxes,
                               pause=args.pause, debug=args.debug,
                               verbose=args.verbose)
            except:
                import traceback; traceback.print_exc()
                read_input("%s %s %% Hit Return to exit: " %
                           (project, os.getcwd()))
                raise SystemExit(1)

    finally:
        os.chdir(origdir)
        shutil.rmtree(scratchdir)


if __name__ == "__main__":
    main()
