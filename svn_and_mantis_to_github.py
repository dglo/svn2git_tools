#!/usr/bin/env python3

from __future__ import print_function

import argparse
import getpass
import os
import re
import shutil
import sys
import traceback

from cmdrunner import CommandException
from git import GitException, git_add, git_autocrlf, git_checkout, \
     git_commit, git_diff, git_init, git_push, git_remote_add, git_remove, \
     git_reset, git_show_hash, git_status, git_submodule_add, \
     git_submodule_remove, git_submodule_status, git_submodule_update
from github_util import GithubUtil, LocalRepository
from i3helper import TemporaryDirectory, read_input
from mantis_converter import MantisConverter
from pdaqdb import PDAQManager
from repostatus import RepoStatus
from svn import AcceptType, SVNConnectException, SVNMetadata, \
     SVNNonexistentException, svn_checkout, svn_get_externals, svn_propget, \
     svn_revert, svn_status, svn_switch, svn_update


class Submodule(object):
    def __init__(self, name, revision, url):
        self.name = name
        self.revision = revision
        self.url = url

        self.__project = None
        self.__gitrepo = None
        self.__got_repo = False

    def __str__(self):
        if self.revision is None:
            rstr = ""
        else:
            rstr = "@%s" % str(self.revision)
        return "[%s%s]%s" % (self.name, rstr, self.url)

    def get_git_hash(self, branch_name, revision):
        if revision is None:
            raise Exception("Cannot fetch unknown %s revision" % (self.name, ))

        if self.__project is None:
            self.__project = PDAQManager.get(self.name)

        _, git_branch, git_hash = \
          self.__project.database.find_revision(branch_name, revision)

        _, svn_rev = \
          self.__project.database.find_revision_from_hash(git_hash)

        return git_branch, git_hash, svn_rev

    def get_revision_from_date(self, svn_branch, svn_date):
        if svn_date is None:
            raise Exception("Cannot fetch unknown %s SVN date" % (self.name, ))

        if self.__project is None:
            self.__project = PDAQManager.get(self.name)

        return self.__project.database.find_revision_from_date(svn_branch,
                                                               svn_date)


def add_arguments(parser):
    "Add command-line arguments"

    parser.add_argument("-A", "--author-file", dest="author_file",
                        default=None, required=True,
                        help="File containing a dictionary-style map of"
                        " Subversion usernames to Git authors"
                        " (e.g. \"abc: Abe Beecee <abc@foo.com>\")")
    parser.add_argument("-B", "--ignore-bad-externals",
                        dest="ignore_bad_externals",
                        action="store_true", default=False,
                        help="Ignore bad URLs in svn:externals")
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
    parser.add_argument("-R", "--use-releases-directory", dest="use_releases",
                        action="store_true", default=False,
                        help="SVN project uses 'releases' instead of 'tags'")
    parser.add_argument("-X", "--convert-externals",
                        dest="convert_externals",
                        action="store_true", default=False,
                        help="Convert Subversion externals to Git submodules")
    parser.add_argument("-d", "--description", dest="description",
                        default=None,
                        help="GitHub project description")
    parser.add_argument("-g", "--github-repo", dest="github_repo",
                        default=None, help="Github repo name")
    parser.add_argument("-m", "--mantis-project", dest="mantis_project",
                        default=None,
                        help="Mantis project name")
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
    parser.add_argument("--destroy-old-repo", dest="destroy_old",
                        action="store_true", default=False,
                        help="If this repository exists on GitHub,"
                        " destroy and recreating the repository")
    parser.add_argument("--load-from-database", dest="load_from_db",
                        action="store_true", default=False,
                        help="Instead of parsing the Subversion log entries,"
                             " load them from the database")
    parser.add_argument("--local-repo", dest="local_repo",
                        default=None,
                        help="Specify the local directory where Git repos"
                             " should be created; if not specified, a"
                             " temporary repo will be created and thrown away"
                             " on exit")
    parser.add_argument("--pause-before-finish", dest="pause_before_finish",
                        action="store_true", default=False,
                        help="Pause for input before exiting program")
    parser.add_argument("--preserve-all-status", dest="preserve_all_status",
                        action="store_true", default=False,
                        help="Preserve status of all Mantis issues")
    parser.add_argument("--preserve-resolved", dest="preserve_resolved_status",
                        action="store_true", default=False,
                        help="Preserve status of resolved Mantis issues")

    parser.add_argument(dest="svn_project", default=None,
                        help="Subversion/Mantis project name")


class DiffParser(object):
    (ST_UNKNOWN, ST_TOP, ST_INDEX, ST_HDR1, ST_HDR2, ST_BODY) = range(6)
    STATE_NAMES = ("UNKNOWN", "TOP", "INDEX", "HDR1", "HDR2", "BODY")

    DIFF_TOP_PAT = re.compile(r"^diff\s+.*\s+(\S+)\s+(\S+)\s*$")
    DIFF_IDX_PAT = re.compile(r"^index\s+(\S+)\.\.(\S+)\s+(\S+)\s*$")
    DIFF_HDR_PAT = re.compile(r"^([\-\+][\-\+][\-\+])\s(\S+)\s*$")
    DIFF_SECT_PAT = re.compile(r"^@@ -\d+,\d+\s+\+\d+,\d+ @@ .*$")

    def __init__(self, project):
        self.__project = project

        # initialize "empty" internal variables
        self.__filename = None
        self.__added = 0
        self.__removed = 0
        self.__eof_newline = False
        self.__old_mode = None
        self.__new_mode = None
        self.__verbose_header = False

        # parser starts out in UNKNOWN state
        self.__state = self.ST_UNKNOWN

    def __set_file(self, filename, debug=False, verbose=False):
        self.finalize(debug=debug, verbose=verbose)

        if filename is None:
            raise Exception("Cannot set file name to None")

        # remove leading "a" or "b" from 'diff' path
        if filename.startswith("a/") or filename.startswith("b/"):
            filename = filename[2:]

        # clear file-specific attributes
        self.__added = 0
        self.__removed = 0
        self.__eof_newline = False
        self.__old_mode = None
        self.__new_mode = None
        self.__verbose_header = False

        # set current file name
        self.__filename = filename

        # set parser to TOP_OF_FILE
        self.__state = self.ST_TOP

    @property
    def __state_string(self):
        return self.STATE_NAMES[self.__state]

    def finalize(self, debug=False, verbose=False):
        if self.__filename is None:
            return

        revert = False
        if self.__eof_newline and self.__added == 1 and self.__removed == 1:
            revert = True
        elif self.__old_mode is not None and self.__new_mode is not None:
            revert = True

        if revert:
            if verbose:
                print("*** REVERT %s file %s (cwd %s)" %
                      (self.__project, self.__filename, os.getcwd()))
            git_checkout(start_point=self.__filename,
                         sandbox_dir=self.__project, debug=debug,
                         verbose=verbose)
            return

        raise Exception("Unhandled %s file %s: added %d removed %d eof_nl %s"
                        " old/new %s/%s" %
                        (self.__project, self.__filename, self.__added,
                         self.__removed, self.__eof_newline, self.__old_mode,
                         self.__new_mode))

    def parse(self, line, debug=False, verbose=False):
        if line.startswith("diff "):
            mtch = self.DIFF_TOP_PAT.match(line)
            if mtch is not None:
                self.__set_file(mtch.group(1), debug=debug, verbose=verbose)
                return

        if self.__state == self.ST_TOP:
            mtch = self.DIFF_IDX_PAT.match(line)
            if mtch is not None:
                self.__state = self.ST_INDEX
                return

            if line.startswith("old mode "):
                self.__old_mode = line[9:]
                return
            elif line.startswith("new mode "):
                self.__new_mode = line[9:]
                return

        if self.__state == self.ST_INDEX or self.__state == self.ST_HDR1:
            mtch = self.DIFF_HDR_PAT.match(line)
            if mtch is not None:
                if self.__state == self.ST_INDEX:
                    self.__state = self.ST_HDR1
                else:
                    self.__state = self.ST_HDR2
                return

        if self.__state == self.ST_HDR2:
            mtch = self.DIFF_SECT_PAT.match(line)
            if mtch is not None:
                self.__state = self.ST_BODY
                return

        if self.__state == self.ST_BODY:
            if line != "":
                if line[0] == "+":
                    self.__added += 1
                elif line[0] == "-":
                    self.__removed += 1
                elif line[0] == "\\" and line.find("No newline at end") >= 0:
                    self.__eof_newline = True

            if verbose:
                if not self.__verbose_header:
                    self.__verbose_header = True
                    print("=== %s :: %s" % (self.__project, self.__filename),
                          file=sys.stderr)

                print("%s" % (line, ), file=sys.stderr)

            return

        print("??state#%s?? %s" % (self.__state_string, line.rstrip(), ),
              file=sys.stderr)


class Subversion2Git(object):
    def __init__(self, svnprj, ghutil, mantis_issues, repo_description,
                 local_path, convert_externals=False,
                 destroy_existing_repo=False, ignore_bad_externals=False,
                 debug=False, verbose=False):
        self.__svnprj = svnprj
        self.__ghutil = ghutil
        self.__mantis_issues = mantis_issues
        self.__convert_externals = convert_externals
        self.__ignore_bad_externals = ignore_bad_externals

        # if description was not specified, build a default value
        if repo_description is None:
            repo_description = "WIPAC's %s project" % (svnprj.name, )

        # initialize GitHub or local repository object
        if ghutil is not None:
            self.__gitrepo = ghutil.get_github_repo(repo_description,
                                                    create_repo=True,
                                                    destroy_existing=\
                                                    destroy_existing_repo,
                                                    debug=debug,
                                                    verbose=verbose)
        else:
            self.__gitrepo = LocalRepository(local_path, svnprj.name,
                                             create_repo=True,
                                             destroy_existing=\
                                             destroy_existing_repo,
                                             debug=debug, verbose=verbose)

        # dictionary mapping Subversion revision to Git branch and hash
        self.__rev_to_hash = {}

        # dictionary mapping submodule names to submodule revisions
        self.__submodules = {}

        # the hash string from the final Git commit on the master branch
        self.__master_hash = None

        # if True, the GitHub repository has not been fully initialized
        self.__initial_commit = False

    def __add_entry(self, svn_url, entry, entry_count, entry_total,
                    branch_name, progress_reporter=None, debug=False,
                    verbose=False):
        if progress_reporter is not None:
            progress_reporter(entry_count + 1, entry_total, "SVN rev",
                              entry.revision)

        # retry a couple of times in case update fails to connect
        for _ in (0, 1, 2):
            try:
                for _ in svn_update(revision=entry.revision,
                                    ignore_bad_externals=\
                                    self.__ignore_bad_externals,
                                    ignore_externals=\
                                    self.__convert_externals,
                                    debug=debug, verbose=verbose):
                    pass
                break
            except SVNConnectException:
                continue
            except SVNNonexistentException:
                # if this url and/or revision does not exist, we're done
                print("WARNING: Revision %s does not exist for %s" %
                      (entry.revision, svn_url))
                return False

        print_progress = progress_reporter is not None

        if self.__convert_externals:
            try:
                self.__convert_externals_to_submodules(branch_name,
                                                       entry.date_string,
                                                       print_progress=\
                                                       print_progress,
                                                       debug=debug,
                                                       verbose=verbose)
            except SVNNonexistentException as sex:
                if progress_reporter is not None:
                    prefix = "\n"
                else:
                    prefix = ""

                print("%sWARNING: Skipping %s rev %s; cannot load external"
                      " \"%s\"" % (prefix, self.name, entry.revision, sex.url),
                      file=sys.stderr)
                return False
            except:
                traceback.print_exc()
                read_input("%s %% Hit Return to commit: " % os.getcwd())

        if self.__mantis_issues is None or \
          not self.__gitrepo.has_issue_tracker:
            # don't open issues if we don't have any Mantis issues or
            # if we're not writing to a repo with an issue tracker
            github_issues = None
        else:
            # open/reopen GitHub issues
            github_issues = \
              self.__mantis_issues.open_github_issues(self.__gitrepo,
                                                      entry.revision,
                                                      report_progress=\
                                                      progress_reporter)

        fixed = self.__fix_git_sandbox(entry, github_issues,
                                       print_progress=print_progress,
                                       debug=debug, verbose=verbose)
        if fixed is not None:
            if fixed[0] is None and fixed[1] is None:
                return None
            return fixed

        # commit this revision to git
        commit_result = self.__commit_to_git(entry,
                                             github_issues=github_issues,
                                             debug=debug, verbose=verbose)

        # if we opened one or more issues, close them now
        if github_issues is not None:
            if commit_result is None:
                message = "Nothing commited to git repo!"
            else:
                (branch, hash_id, changed, inserted, deleted) = \
                  commit_result
                if changed is None or inserted is None or deleted is None:
                    (changed, inserted, deleted) = (0, 0, 0)
                message = "[%s %s] %d changed, %d inserted, %d deleted" % \
                  (branch, hash_id, changed, inserted, deleted)

            for github_issue in github_issues:
                self.__mantis_issues.close_github_issue(github_issue, message)

        # if something was committed...
        added = False
        if commit_result is not None:
            # save the hash ID for this Git commit
            (branch, hash_id, changed, inserted, deleted) = commit_result
            full_hash = git_show_hash(debug=debug, verbose=verbose)
            if verbose and not full_hash.startswith(hash_id):
                print("WARNING: %s rev %s short hash was %s,"
                      " but full hash is %s" %
                      (branch, entry.revision, hash_id, full_hash),
                      file=sys.stderr)

            if branch == "master" and changed is not None and \
              inserted is not None and deleted is not None:
                self.__master_hash = full_hash

            self.__svnprj.database.add_git_commit(entry.revision, branch,
                                                  full_hash)

            if entry.revision in self.__rev_to_hash:
                obranch, ohash = self.__rev_to_hash[entry.revision]
                raise CommandException("Cannot map r%d to %s:%s, already"
                                       " mapped to %s:%s" %
                                       (entry.revision, branch, full_hash,
                                        obranch, ohash))

            if debug:
                print("Mapping SVN r%d -> branch %s hash %s" %
                      (entry.revision, branch, full_hash))
            self.__rev_to_hash[entry.revision] = (branch, full_hash)

            added = True

        if self.__initial_commit:
            # create GitHub repository and push the first commit
            self.__finish_first_commit(debug=debug, verbose=verbose)

            # remember that we're done with GitHub repo initialization
            self.__initial_commit = False
        else:
            # we've already initialized the Git repo,
            #  push this commit
            if branch_name == SVNMetadata.TRUNK_NAME:
                upstream = None
                remote_name = None
            else:
                upstream = "origin"
                remote_name = branch_name.rsplit("/")[-1]
                if remote_name in ("HEAD", "master"):
                    raise Exception("Questionable branch name \"%s\"" %
                                    (remote_name, ))

            for _ in git_push(remote_name=remote_name, upstream=upstream,
                              debug=debug, verbose=debug):
                pass

        return added

    def __add_or_update_submodule(self, submodule, subhash, debug=False,
                                  verbose=False):
        need_update = True
        initialize = True
        if not os.path.exists(submodule.name):
            git_url = self.__gitrepo.make_url(submodule.name)

            # if this submodule was added previously, force it to be added
            force = os.path.exists(os.path.join(".git", "modules",
                                                submodule.name))

            try:
                git_submodule_add(git_url, subhash, force=force, debug=debug,
                                  verbose=verbose)
                need_update = False
            except GitException as gex:
                xstr = str(gex)
                if xstr.find("already exists in the index") >= 0:
                    need_update = True
                else:
                    if xstr.find("does not appear to be a git repo") >= 0:
                        print("WARNING: Not adding nonexistent"
                              " submodule \"%s\"" % (submodule.name, ),
                              file=sys.stderr)
                    else:
                        print("WARNING: Cannot add \"%s\" (URL %s) to %s: %s" %
                              (submodule.name, git_url, self.name, gex),
                              file=sys.stderr)
                        if not self.__ignore_bad_externals:
                            read_input("%s %% Hit Return to continue: " %
                                       os.getcwd())
                    return False

        if need_update:
            # submodule already exists, update to the correct hash
            try:
                git_submodule_update(submodule.name, subhash,
                                     initialize=initialize, debug=debug,
                                     verbose=verbose)
            except GitException as gex:
                print("Cannot update %s Git submodule %s to %s: %s" %
                      (self.name, submodule.name, subhash, gex))
                raise

        return True

    def __build_commit_message(self, entry, github_issues):
        # build the commit message
        message = None
        for line in entry.loglines:
            if message is None:
                message = line
            else:
                message += "\n" + line

        # insert the GitHub message ID if it was specified
        if github_issues is not None:
            if len(github_issues) == 1:
                plural = ""
            else:
                plural = "s"
            message = "Issue%s %s: %s" % \
              (plural, ", ".join(str(x.number) for x in github_issues),
               message)

        return message

    @classmethod
    def __check_out_svn_project(cls, svn_url, target_dir, revision,
                                debug=False, verbose=False):
        if debug:
            print("Checkout %s rev %d in %s" %
                  (svn_url, revision, os.getcwd()))

        svn_checkout(svn_url, revision, target_dir, debug=debug,
                     verbose=verbose)

        if debug:
            print("=== After checkout of %s ===" % svn_url)
            for dentry in os.listdir("."):
                print("\t%s" % str(dentry))

        # verify that project subdirectory was created
        if not os.path.exists(target_dir):
            raise CommandException("Cannot find project subdirectory \"%s\""
                                   " after checkout" % (target_dir, ))

    @classmethod
    def __clean_svn_sandbox(cls, branch_name, ignore_externals=False,
                            verbose=False):
        submodules = None
        if os.path.exists(".gitsubmodules"):
            for flds in git_submodule_status(verbose=verbose):
                if submodules is None:
                    submodules = []
                submodules.append(flds[0])

        error = False
        for line in svn_status():
            if not line.startswith("?"):
                if not error:
                    print("%s SVN sandbox contains:" % (branch_name, ))
                    error = True
                print("%s" % line)
                continue

            filename = line[1:].strip()
            if filename in (".git", ".gitignore", ".gitmodules"):
                continue

            if submodules is not None and filename in submodules:
                # don't remove submodules
                continue

            if verbose:
                print("Removing stray entry while switching to %s: %s" %
                      (branch_name, filename))
            if os.path.isdir(filename):
                shutil.rmtree(filename)
            else:
                os.remove(filename)

            if error:
                raise CommandException("Found stray files in %s sandbox,"
                                       " cannot continue" % (branch_name, ))

    def __clean_up(self):
        pass

    def __commit_project(self, debug=False, verbose=False):
        # build a list of all trunk/branch/tag URLs for this project
        all_urls = []
        for _, _, svn_url in self.all_urls:
            all_urls.append(svn_url)

        # convert trunk/branches/tags to Git
        in_sandbox = False
        for ucount, svn_url in enumerate(all_urls):
            # extract the branch name from this Subversion URL
            branch_name = self.branch_name(svn_url)

            # values used when reporting progress to user
            num_entries = self.num_entries(branch_name)
            num_added = 0

            print("Converting %d revisions from %s (#%d of %d)" %
                  (num_entries, branch_name, ucount + 1, len(all_urls)))

            # don't report progress if printing verbose/debugging messages
            if debug or verbose:
                progress_reporter = None
            else:
                progress_reporter = self.__progress_reporter

            for bcount, entry in enumerate(self.entries(branch_name)):
                # if this is the first entry for trunk/branch/tag...
                if bcount == 0:
                    if branch_name == SVNMetadata.TRUNK_NAME:
                        # this is the first entry on trunk
                        subdir = self.name

                        # initialize Git and Subversion sandboxes
                        self.__initialize_sandboxes(svn_url, subdir,
                                                    entry.revision,
                                                    debug=debug,
                                                    verbose=verbose)

                        # move into the newly created sandbox
                        os.chdir(subdir)
                        in_sandbox = True

                        # remember to finish GitHub initialization
                        self.__initial_commit = self.__gitrepo is not None
                    else:
                        # this is the first entry on a branch/tag
                        if entry.previous is None:
                            print("Ignoring standalone branch %s" %
                                  (branch_name, ))
                            break

                        self.__switch_to_branch(svn_url, branch_name,
                                                entry.revision, entry.previous,
                                                debug=debug, verbose=verbose)
                elif debug:
                    # this is not the first entry on trunk/branch/tag
                    print("Update %s to rev %d in %s" %
                          (self.name, entry.revision, os.getcwd()))

                if self.__add_entry(svn_url, entry, bcount, num_entries,
                                    branch_name,
                                    progress_reporter=progress_reporter,
                                    debug=debug, verbose=verbose):
                    # if we added an entry, increase the count of git commits
                    num_added += 1

            # add all remaining issues to GitHub
            if self.__mantis_issues is not None and \
              self.__gitrepo.has_issue_tracker:
                self.__mantis_issues.add_issues(self.__gitrepo,
                                                report_progress=\
                                                progress_reporter)

            # clear the status line
            if progress_reporter is not None:
                print("\rAdded %d of %d SVN entries                         " %
                      (num_added, num_entries))

        if in_sandbox:
            # make sure we leave the new repo on the last commit for 'master'
            git_checkout("master", debug=debug, verbose=verbose)
            if self.__master_hash is None:
                self.__master_hash = "HEAD"
            git_reset(start_point=self.__master_hash, hard=True, debug=debug,
                      verbose=verbose)

    def __commit_to_git(self, entry, github_issues=None, debug=False,
                        verbose=False):
        """
        Commit an SVN change to git, return a tuple containing:
        (branch_name, hash_id, number_changed, number_inserted, number_deleted)
        """

        message = self.__build_commit_message(entry, github_issues)

        #read_input("%s %% Hit Return to commit: " % os.getcwd())
        try:
            flds = git_commit(author=PDAQManager.get_author(entry.author),
                              commit_message=message,
                              date_string=entry.date.isoformat(),
                              filelist=None, commit_all=False, debug=debug,
                              verbose=verbose)
        except CommandException:
            print("ERROR: Cannot commit %s SVN rev %d (%s)" %
                  (self.name, entry.revision, message), file=sys.stderr)
            raise

        return flds

    def __convert_externals_to_submodules(self, branch_name, svn_date,
                                          print_progress=False, debug=False,
                                          verbose=False):
        found = {}
        for subrev, url, subdir in svn_get_externals(".", debug=debug,
                                                     verbose=verbose):
            found[subdir] = 1

            # get the Submodule object for this project
            if subdir not in self.__submodules:
                submodule = Submodule(subdir, subrev, url)
            else:
                submodule = self.__submodules[subdir]

            if submodule.name != subdir:
                print("ERROR: Expected submodule name to be %s, not %s" %
                      (subdir, submodule.name), file=sys.stderr)

            if subrev is None:
                # find latest revision before 'date' on this branch
                subrev = submodule.get_revision_from_date(branch_name,
                                                          svn_date)

            # get branch/hash data
            if subrev is None:
                # if there's no revision, they must just want HEAD
                subhash = None
            else:
                _, subhash, newrev = \
                  submodule.get_git_hash(branch_name, subrev)
                subrev = newrev

            if submodule.revision != subrev:
                submodule.revision = subrev

            if submodule.url != url:
                old_str = submodule.url
                new_str = url
                for idx in range(min(len(submodule.url), len(url))):
                    if submodule.url[idx] != url[idx]:
                        old_str = submodule.url[idx:]
                        new_str = url[idx:]
                        break
                submodule.url = url

                if print_progress:
                    prefix = "\n"
                else:
                    prefix = ""

                print("%sWARNING: %s external %s URL changed from %s to %s" %
                      (prefix, self.name, submodule.name, old_str, new_str),
                      file=sys.stderr)

            if verbose:
                print("\t+ %s -> %s" % (submodule, subhash))
            if self.__add_or_update_submodule(submodule, subhash, debug=debug,
                                              verbose=verbose):
                # add Submodule if it's a new entry
                if submodule.name not in self.__submodules:
                    self.__submodules[submodule.name] = submodule

            if not os.path.isdir(submodule.name):
                raise Exception("ERROR: Didn't find newly created %s"
                                " submodule directory" % (submodule.name, ))

            updated = False
            for _ in (0, 1, 2):
                try:
                    self.__update_svn_external(submodule, subrev, debug=debug,
                                               verbose=verbose)
                    updated = True
                except SVNConnectException:
                    pass

            if not updated:
                raise SVNConnectException("Cannot update %s external %s to"
                                          " rev %s" %
                                          (self.name, submodule.name, subrev))

        for proj in self.__submodules:
            if proj not in found:
                if os.path.exists(proj):
                    if verbose:
                        print("\t- %s" % (submodule, ))
                    git_submodule_remove(proj, debug=debug, verbose=verbose)
                elif verbose:
                    print("WARNING: Not removing nonexistent submodule \"%s\""
                          " from %s" % (proj, self.name), file=sys.stderr)

    def __create_gitignore(self, sandbox_dir, include_python=False,
                           include_java=False, debug=False, verbose=False):

        # get list of ignored entries from SVN
        ignorelist = self.__load_svn_ignore()

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

    def __finish_first_commit(self, debug=False, verbose=False):
        for _ in git_remote_add("origin", self.__gitrepo.ssh_url, debug=debug,
                                verbose=verbose):
            pass

        for _ in git_push("master", "origin", debug=debug, verbose=verbose):
            pass

    @classmethod
    def __fix_conflicts(cls, conflicts, debug=False, verbose=False):
        for path in conflicts:
            new_path = path + ".new"
            with open(path, "r") as fin:
                fixed = open(new_path, "w")
                try:
                    cls.__fix_file_conflicts(fin, fixed)
                except:
                    os.unlink(path + ".new")
                    raise
                finally:
                    fixed.close()

            # replace conflicted file with fixed version
            os.unlink(path)
            os.rename(new_path, path)
            if verbose:
                print("*** FIXED CONFLICTS IN \"%s\"" % (path, ))

    @classmethod
    def __fix_file_conflicts(cls, input_handle, output_handle):
        # set up diff states
        (diff_none, diff_body, diff_copy) = (0, 1, 2)

        diff_state = diff_none
        for line in input_handle:
            line = line.rstrip("\n\r")

            if diff_state == diff_none:
                if line.startswith("<<<<<<<"):
                    diff_state = diff_body
                    continue

            elif diff_state == diff_body:
                if line.startswith("======="):
                    diff_state = diff_copy

                # don't copy the 'body' lines
                #  until we're in the last part
                continue
            elif diff_state == diff_copy:
                if line.startswith(">>>>>>>"):
                    diff_state = diff_none
                    continue

            print("%s" % (line, ), file=output_handle)

    def __fix_git_sandbox(self, entry, github_issues, print_progress=False,
                          debug=False, verbose=False):
        additions, deletions, modifications = \
          self.__gather_changes(debug=debug, verbose=verbose)

        if debug:
            for pair in (("Additions", additions), ("Deletions", deletions),
                         ("Modifications", modifications)):
                if pair[1] is not None:
                    print("=== %s" % pair[0])
                    for fnm in pair[1]:
                        print("  %s" % str(fnm))

        # add/remove files to commit
        if deletions is not None:
            git_remove(filelist=deletions, debug=debug, verbose=verbose)
        if additions is not None:
            git_add(filelist=additions, debug=debug, verbose=verbose)
        if modifications is not None:
            git_add(filelist=modifications, debug=debug, verbose=verbose)

        # some SVN commits may not change files (e.g. file property changes)
        untracked = False
        unstaged = None
        for line in git_status(debug=debug, verbose=verbose):
            if untracked:
                print("??? %s" % (line, ), file=sys.stderr)
                continue

            if unstaged is not None:
                modidx = line.find("modified:")
                if modidx < 0:
                    continue

                modend = line.find(" (modified content)")
                if modend < 0:
                    raise Exception("Unknown UNSTAGED line: %s" % (line, ))

                filename = line[modidx+9:modend].strip()
                if not os.path.isdir(filename):
                    raise Exception("Found modified file \"%s/%s\"" %
                                    (self.name, filename))
                unstaged.append(filename)
                continue

            if line.startswith("nothing to commit"):
                if verbose:
                    if print_progress:
                        prefix = "\n"
                    else:
                        prefix = ""
                    print("%sWARNING: No changes found in %s SVN rev %d" %
                          (prefix, self.name, entry.revision), file=sys.stderr)

                    message = self.__build_commit_message(entry, github_issues)
                    if message is not None:
                        print("(Commit message: %s)" % str(message),
                              file=sys.stderr)

                # use the first previous commit with a Git hash
                prev = entry.previous
                while prev is not None:
                    if prev.git_branch is not None and \
                      prev.git_hash is not None:
                        return prev.git_branch, prev.git_hash
                    prev = prev.previous
                return None, None

            if line.startswith("Untracked files:"):
                print("ERROR: Found untracked %s files:" % self.name,
                      file=sys.stderr)
                untracked = True
                continue

            if line.startswith("Changes not staged for commit:"):
                unstaged = []
                continue

        if untracked:
            raise CommandException("Found untracked files for %s SVN rev %d" %
                                   (self.name, entry.revision))
        if unstaged is not None:
            print("UNSTAGED: %s" % (unstaged, ))
            try:
                self.__fix_unstaged(unstaged, debug=debug, verbose=verbose)
            except:
                traceback.print_exc()
                read_input("%s %% Hit Return to exit: " % os.getcwd())
                raise CommandException("Found unhandled changes for %s SVN"
                                       " rev %d" % (self.name, entry.revision))

        return None

    @classmethod
    def __fix_unstaged(cls, projects, debug=False, verbose=False):
        for prj in projects:
            fullpath = os.path.join(os.getcwd(), prj)
            if not os.path.exists(fullpath):
                print("WARNING: Submodule directory \"%s\" does not exist" %
                      (fullpath, ))
                continue

            parser = DiffParser(prj)

            for line in git_diff(unified=True, sandbox_dir=prj,
                                 debug=debug, verbose=verbose):
                parser.parse(line, debug=debug, verbose=verbose)
            parser.finalize(debug=debug, verbose=verbose)

    @classmethod
    def __gather_changes(cls, debug=False, verbose=False):
        additions = None
        deletions = None
        modifications = None

        for line in git_status(porcelain=True, debug=debug, verbose=verbose):
            line = line.rstrip()
            if line == "":
                continue

            if len(line) < 4:
                raise Exception("Short procelain status line \"%s\"" %
                                (line, ))

            if line[2] != " " and line[2] != "M":
                raise Exception("Bad porcelain status line \"%s\"" % (line, ))

            if line[1] == " ":
                # ignore files which have already been staged
                continue

            if line[0] == "?" and line[1] == "?":
                if additions is None:
                    additions = []
                additions.append(line[3:])
                continue

            if line[0] == " " or line[0] == "A" or line[0] == "M":
                if line[1] == "A":
                    if additions is None:
                        additions = []
                    additions.append(line[3:])
                    continue
                if line[1] == "D":
                    if deletions is None:
                        deletions = []
                    deletions.append(line[3:])
                    continue
                if line[1] == "M":
                    if modifications is None:
                        modifications = []
                    modifications.append(line[3:])
                    continue

            raise Exception("Unknown porcelain line \"%s\"" % str(line))

        return additions, deletions, modifications

    def __initialize_sandboxes(self, svn_url, subdir, revision, debug=False,
                               verbose=False):
        # check out the Subversion repo
        self.__check_out_svn_project(svn_url, subdir, revision, debug=debug,
                                     verbose=verbose)
        if debug:
            print("=== Inside newly checked-out %s ===" % (subdir, ))
            for dentry in os.listdir(subdir):
                print("\t%s" % str(dentry))

        # initialize the directory as a git repository
        git_init(sandbox_dir=subdir, verbose=verbose)

        # allow old files with Windows-style line endings to be committed
        git_autocrlf(sandbox_dir=subdir, debug=debug, verbose=verbose)

        # create a .gitconfig file which ignores .svn as well as anything
        #  else which is already being ignored
        self.__create_gitignore(subdir, debug=debug, verbose=verbose)

    def __load_svn_ignore(self):
        # get the list of ignored files from Subversion
        ignored = []
        try:
            for line in svn_propget(self.__svnprj.trunk_url, "svn:ignore"):
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

    @classmethod
    def __progress_reporter(cls, count, total, name, value):
        # print spaces followed backspaces to erase any stray characters
        spaces = " "*30
        backup = "\b"*30

        print("\r#%d (of %d): %s %d%s%s" % (count, total, name, value, spaces,
                                            backup), end="")

    @classmethod
    def __revert_files(cls, filelist, debug=False, verbose=False):
        not_fixed = []
        for filename, actions in sorted(filelist.items(), key=lambda x: x[0]):
            if "A" in actions and "C" in actions:
                if verbose:
                    print("%%%%%%%%%% Reverting %s %%%%%%%%%%" % (filename, ))
                svn_revert(filename, debug=debug, verbose=verbose)
                continue

            not_fixed.append("%s: %s" % (filename, ", ".join(actions)))

        if len(not_fixed) > 0:
            raise Exception("Cannot fix one or more files:%s" %
                            "\n\t".join(not_fixed))

    def __switch_to_branch(self, branch_url, branch_name, revision,
                           prev_entry, debug=False, verbose=False):
        while prev_entry.revision not in self.__rev_to_hash:
            if prev_entry.previous is None:
                raise Exception("Cannot find committed ancestor for SVN r%d" %
                                (prev_entry.revision, ))
            prev_entry = prev_entry.previous

        prev_branch, prev_hash = self.__rev_to_hash[prev_entry.revision]

        # switch back to trunk (in case we'd switched to a branch)
        for _ in svn_switch(self.__svnprj.trunk_url,
                            revision=prev_entry.revision,
                            ignore_bad_externals=self.__ignore_bad_externals,
                            ignore_externals=self.__convert_externals,
                            debug=debug, verbose=verbose):
            pass

        # revert all modifications
        svn_revert(recursive=True, debug=debug, verbose=verbose)

        # update to fix any weird stuff post-reversion
        for _ in svn_update(revision=prev_entry.revision,
                            ignore_bad_externals=self.__ignore_bad_externals,
                            ignore_externals=self.__convert_externals,
                            debug=debug, verbose=verbose):
            pass

        # revert Git repository to the original branch point
        if debug:
            print("** Reset to rev %s (%s hash %s)" %
                  (prev_entry.revision, prev_branch, prev_hash))
        git_reset(start_point=prev_hash, hard=True, debug=debug,
                  verbose=verbose)

        new_name = branch_name.rsplit("/")[-1]

        # create the new Git branch (via the checkout command)
        git_checkout(new_name, start_point=prev_hash, new_branch=True,
                     debug=debug, verbose=verbose)

        # revert any changes caused by the git checkout
        svn_revert(recursive=True, debug=debug, verbose=verbose)

        # remove any stray files not cleaned up by the 'revert'
        self.__clean_svn_sandbox(branch_name,
                                 ignore_externals=self.__convert_externals,
                                 verbose=verbose)

        # switch sandbox to new revision
        try:
            for _ in svn_switch(branch_url, revision=revision,
                                ignore_bad_externals=\
                                self.__ignore_bad_externals,
                                ignore_externals=self.__convert_externals,
                                debug=debug, verbose=verbose):
                pass
        except SVNNonexistentException:
            print("WARNING: Cannot switch to nonexistent %s rev %s (ignored)" %
                  (branch_url, revision), file=sys.stderr)

    def __update_svn_external(self, submodule, revision, debug=False,
                              verbose=False):
        subsvn = os.path.join(submodule.name, ".svn")
        if not os.path.exists(subsvn):
            svn_checkout(submodule.url, revision=revision,
                         target_dir=submodule.name, force=True, debug=debug,
                         verbose=verbose)
            return

        revert_list = {}
        conflicts = []
        resolved = False
        for line in svn_update(submodule.name, accept_type=AcceptType.WORKING,
                               force=True, revision=revision, debug=debug,
                               verbose=verbose):
            print("%s" % (line, ))
            if len(line) == 0:
                continue

            if line.startswith("Updating ") or \
              line.startswith("Restored ") or \
              line.startswith("Updated to ") or \
              line.startswith("At revision "):
                continue

            if line.startswith("Tree conflict at ") and \
              line.endswith(" marked as resolved."):
                resolved = True
                continue

            if line.startswith("Summary of conflicts:"):
                continue

            if line.startswith("  Tree conflicts: "):
                if not resolved:
                    raise Exception("Found %s rev %s merge conflict!!!" %
                                    (submodule.name, revision))
                continue

            if line.startswith("Merge conflicts in ") and \
              line.endswith(" marked as resolved."):
                conflicts.append(line[20:-21])
                continue

            if line.strip().startswith("Text conflicts: 0 remaining"):
                continue

            if len(line) < 5:
                raise Exception("Bad 'svn update' line for %s rev %s: %s" %
                                (submodule.name, revision, line))

            action = line[3]
            filename = line[5:]

            if action == " ":
                continue

            print("!!PROBLEM!! \"%s\"" % (line, ), file=sys.stderr)
            if filename not in revert_list:
                revert_list[filename] = [action, ]
            else:
                revert_list[filename].append(action)

        if len(revert_list) > 0:
            self.__revert_files(revert_list, debug=debug, verbose=verbose)

        if len(conflicts) > 0:
            self.__fix_conflicts(conflicts, debug=debug, verbose=verbose)

    @property
    def all_urls(self):
        for flds in self.__svnprj.all_urls(ignore=self.__svnprj.ignore_tag):
            yield flds

    def branch_name(self, svn_url):
        return self.__svnprj.branch_name(svn_url)

    def convert(self, pause_before_finish=False, debug=False, verbose=False):
        # let user know that we're starting to do real work
        if not verbose:
            print("Converting %s SVN repository to %s" %
                  (self.name, self.project_type))

        with TemporaryDirectory() as tmpdir:
            try:
                self.__commit_project(debug=debug, verbose=verbose)
            except:
                traceback.print_exc()
                raise
            finally:
                self.__clean_up()
                if pause_before_finish:
                    read_input("%s %% Hit Return to finish: " % os.getcwd())

    def entries(self, branch_name):
        for entry in self.__svnprj.database.entries(branch_name):
            yield entry

    @property
    def name(self):
        "Return project name"
        return self.__svnprj.name

    def num_entries(self, branch_name):
        return self.__svnprj.database.num_entries(branch_name)

    @property
    def project_type(self):
        """
        Return a string describing the project type (GitHub, local repo, or
        temporary repo)
        """
        if self.__ghutil is not None:
            return "GitHub"
        return "local Git repo"


def load_github_data(organization, repo_name, make_public=False,
                     sleep_seconds=1):
    # if the organization name was not specified, assume it's the user's
    #  personal space
    if organization is None:
        organization = getpass.getuser()

    ghutil = GithubUtil(organization, repo_name)
    ghutil.make_new_repo_public = make_public
    ghutil.sleep_seconds = sleep_seconds

    return ghutil


def load_mantis_issues(svnprj, mantis_dump, close_resolved=False,
                       preserve_all_status=False,
                       preserve_resolved_status=False, verbose=False):
    if not verbose:
        print("Loading Mantis issues for %s" %
              ", ".join(svnprj.mantis_projects))

    mantis_issues = MantisConverter(mantis_dump, svnprj.database,
                                    svnprj.mantis_projects,
                                    verbose=verbose)
    mantis_issues.close_resolved = close_resolved
    mantis_issues.preserve_all_status = preserve_all_status
    mantis_issues.preserve_resolved_status = preserve_resolved_status
    return mantis_issues


def load_subversion_project(svn_project, load_from_db=False, debug=False,
                            verbose=False):
    "Load Subversion project log entries and cache them in an SQLite3 database"

    svnprj = PDAQManager.get(svn_project)
    if svnprj is None:
        raise SystemExit("Cannot find SVN project \"%s\"" % (svn_project, ))

    # load log entries from all URLs and save any new entries to the database
    if not verbose:
        print("Loading Subversion log messages for %s" % (svnprj.name, ))
    if not load_from_db:
        svnprj.load_from_log(debug=debug, verbose=verbose)
        svnprj.save_to_db(debug=debug, verbose=verbose)
    else:
        try:
            svnprj.load_from_db(debug=debug, verbose=verbose)
        except:
            raise SystemExit("Cannot find Subversion log messages for %s" %
                             (svnprj.name, ))

    return svnprj


def main():
    parser = argparse.ArgumentParser()
    add_arguments(parser)
    args = parser.parse_args()

    # pDAQ used 'releases' instead of 'tags'
    if args.use_releases:
        SVNMetadata.set_layout(SVNMetadata.DIRTYPE_TAGS, "releases")

    PDAQManager.set_home_directory()
    PDAQManager.load_authors(args.author_file, verbose=args.verbose)

    # get the SVNProject data for the requested project
    svnprj = load_subversion_project(args.svn_project, args.load_from_db,
                                     debug=args.debug, verbose=args.verbose)

    # 'pdaq-user' contains public ssh keys, don't make it public
    if args.svn_project == "pdaq-user":
        make_public = False
    else:
        make_public = args.make_public

    # if saving to GitHub, initialize the GitHub utility data
    if not args.use_github:
        ghutil = None
    else:
        if args.github_repo is not None:
            repo_name = args.github_repo
        else:
            repo_name = svnprj.name

        ghutil = load_github_data(args.organization, repo_name,
                                  make_public=make_public,
                                  sleep_seconds=args.sleep_seconds)

    # if uploading to GitHub and we have a Mantis SQL dump file, load issues
    if not args.use_github or args.mantis_dump is None:
        mantis_issues = None
    else:
        mantis_issues = load_mantis_issues(svnprj, args.mantis_dump,
                                           close_resolved=args.close_resolved,
                                           preserve_all_status=\
                                           args.preserve_all_status,
                                           preserve_resolved_status=\
                                           args.preserve_resolved_status,
                                           verbose=args.verbose)

    RepoStatus.set_database_home(os.getcwd())

    svn2git = Subversion2Git(svnprj, ghutil, mantis_issues,
                             args.description, args.local_repo,
                             convert_externals=args.convert_externals,
                             destroy_existing_repo=args.destroy_old,
                             ignore_bad_externals=args.ignore_bad_externals,
                             debug=args.debug, verbose=args.verbose)

    # do all the things!
    svn2git.convert(pause_before_finish=args.pause_before_finish,
                    debug=args.debug, verbose=args.verbose)


if __name__ == "__main__":
    main()
