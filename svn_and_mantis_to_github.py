#!/usr/bin/env python3

from __future__ import print_function

import argparse
import getpass
import os
import shutil
import sys
import tempfile
import traceback

from cmdrunner import CommandException
from git import GitException, git_add, git_autocrlf, git_checkout, \
     git_commit, git_init, git_push, git_remote_add, git_remove, git_reset, \
     git_show_hash, git_status, git_submodule_add, git_submodule_remove, \
     git_submodule_status, git_submodule_update
from github_util import GithubUtil, LocalRepository
from i3helper import read_input
from mantis_converter import MantisConverter
from pdaqdb import PDAQManager
from svn import SVNConnectException, SVNMetadata, SVNNonexistentException, \
     svn_checkout, svn_get_externals, svn_propget, svn_revert, svn_status, \
     svn_switch, svn_update


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

    def get_git_hash(self, revision):
        if revision is None:
            raise Exception("Cannot fetch unknown %s revision" % (self.name, ))

        if self.__project is None:
            self.__project = PDAQManager.get(self.name)

        _, git_branch, git_hash = \
          self.__project.database.find_revision(revision)

        return git_branch, git_hash


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


class Subversion2Git(object):
    def __init__(self, svnprj, ghutil, mantis_issues, repo_description,
                 repo_path, convert_externals=False,
                 ignore_bad_externals=False, debug=False, verbose=False):
        self.__svnprj = svnprj
        self.__ghutil = ghutil
        self.__mantis_issues = mantis_issues
        self.__convert_externals = convert_externals
        self.__ignore_bad_externals = ignore_bad_externals

        # if description was not specified, build a default value
        if repo_description is None:
            repo_description = "WIPAC's %s project" % (svnprj.name, )

        # the path to the directory which will hold the new repository
        if repo_path is not None:
            # if we're saving the local repo, create it in the repo directory
            self.__repo_path = os.path.abspath(repo_path)
            self.__repo_is_temporary = False
        else:
            self.__repo_path = tempfile.mkdtemp()
            self.__repo_is_temporary = True

        # initialize GitHub or local repository object
        if ghutil is not None:
            self.__gitrepo = ghutil.get_github_repo(repo_description,
                                                    create_repo=True,
                                                    debug=debug,
                                                    verbose=verbose)
        else:
            self.__gitrepo = LocalRepository(self.__repo_path)

        # dictionary mapping Subversion revision to Git branch and hash
        self.__rev_to_hash = {}

        # dictionary mapping submodule names to submodule revisions
        self.__submodules = {}

        # the hash string from the final Git commit on the master branch
        self.__master_hash = None

        # if True, the GitHub repository has not been fully initialized
        self.__initial_commit = False

    def __add_entry(self, svn_url, entry, entry_count, entry_total,
                    branch_name, report_progress=None, debug=False,
                    verbose=False):
        if report_progress is not None:
            report_progress(entry_count + 1, entry_total, "SVN rev",
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

        if self.__convert_externals:
            use_github = self.__ghutil is not None
            self.__convert_externals_to_submodules(entry.revision,
                                                   use_github=use_github,
                                                   debug=debug,
                                                   verbose=verbose)

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
                                                      report_progress)

        # commit this revision to git
        print_progress = report_progress is not None
        commit_result = self.__commit_to_git(entry,
                                             github_issues=github_issues,
                                             print_progress=print_progress,
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
        elif self.__ghutil is not None:
            # we've already initialized the GitHub repo,
            #  push this commit
            if branch_name == SVNMetadata.TRUNK_NAME:
                upstream = None
                remote_name = None
            else:
                upstream = "origin"
                remote_name = branch_name.rsplit("/")[-1]

            for _ in git_push(remote_name=remote_name, upstream=upstream,
                              debug=debug, verbose=debug):
                pass

        return added

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
    def __clean_reverted_svn_sandbox(cls, branch_name, ignore_externals=False,
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
                    print("Reverted %s sandbox contains:" % (branch_name, ))
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

    def __commit_to_git(self, entry, github_issues=None, print_progress=False,
                        debug=False, verbose=False):
        """
        Commit an SVN change to git, return a tuple containing:
        (branch_name, hash_id, number_changed, number_inserted, number_deleted)
        """

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
        added = False
        if deletions is not None:
            git_remove(filelist=deletions, debug=debug, verbose=verbose)
        if additions is not None:
            git_add(filelist=additions, debug=debug, verbose=verbose)
            added = True
        if modifications is not None:
            git_add(filelist=modifications, debug=debug, verbose=verbose)
            added = True

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

        # some SVN commits may not change files (e.g. file property changes)
        for line in git_status(debug=debug, verbose=verbose):
            if line.startswith("nothing to commit"):
                if verbose:
                    if print_progress:
                        # if we're printing progress messages, add a newline
                        print("")
                    print("WARNING: No changes found in %s SVN rev %d" %
                          (self.name, entry.revision), file=sys.stderr)
                    if message is not None:
                        print("(Commit message: %s)" % str(message),
                              file=sys.stderr)

                # use the first previous commit with a Git hash
                prev = entry.previous
                while prev is not None:
                    if prev.git_branch is not None and \
                      prev.git_hash is not None:
                        return prev.git_branch, prev.git_hash, None, None, None
                    prev = prev.previous
                return None

            if line.startswith("Untracked files:"):
                raise CommandException("Found untracked files for %s SVN rev"
                                       " %d" % (self.name, entry.revision))
            if line.startswith("Changes not staged for commit:"):
                raise CommandException("Found unknown changes for %s SVN rev"
                                       " %d" % (self.name, entry.revision))

        # NOTE: We always add .gitignore, so this shouldn't be needed
        # if nothing was added, add a dummy file to the initial commit
        if self.__initial_commit and not added and False:
            # add a dummy file so the initial commit isn't empty
            dummy = "LoremIpsum.md"
            with open(dummy, "w") as dout:
                print("# %s" % (self.name, ), file=dout)
            git_add(dummy, debug=debug, verbose=verbose)

        try:
            flds = git_commit(author=PDAQManager.get_author(entry.author),
                              commit_message=message,
                              date_string=entry.datetime.isoformat(),
                              filelist=None, commit_all=False, debug=debug,
                              verbose=verbose)
        except CommandException:
            print("ERROR: Cannot commit %s SVN rev %d (%s)" %
                  (self.name, entry.revision, message), file=sys.stderr)
            raise

        return flds

    def __convert_externals_to_submodules(self, revision, use_github=False,
                                          debug=False, verbose=False):
        found = {}
        for subrev, url, subdir in svn_get_externals(".", debug=debug,
                                                     verbose=verbose):
            found[subdir] = 1

            # get the Submodule object for this project
            if subdir not in self.__submodules:
                submodule = Submodule(subdir, subrev, url)
            else:
                submodule = self.__submodules[subdir]

                if submodule.url != url:
                    old_str = submodule.url
                    new_str = url
                    for idx in range(min(len(submodule.url), len(url))):
                        if submodule.url[idx] != url[idx]:
                            old_str = submodule.url[idx:]
                            new_str = url[idx:]
                            break

                    print("WARNING: %s external %s URL changed from %s to %s" %
                          (self.name, subdir, old_str, new_str),
                          file=sys.stderr)

            # get branch/hash data
            if subrev is None:
                # if there's no revision, they must just want HEAD
                subbranch, subhash = (None, None)
            else:
                subbranch, subhash = submodule.get_git_hash(subrev)

                if subrev not in self.__rev_to_hash:
                    # if we don't know about this revision and
                    # we have branch/hash data, add it to the dictionary
                    if subbranch is not None and subhash is not None:
                        self.__rev_to_hash[subrev] = (subbranch, subhash)
                else:
                    # validate branch/hash data
                    obranch, ohash = self.__rev_to_hash[subrev]
                    if subbranch is None and subhash is None:
                        subbranch = obranch
                        subhash = ohash
                    elif obranch != subbranch or ohash != subhash:
                        raise SystemExit("Expected %s rev %s to map to %s@%s,"
                                         " not %s@%s" %
                                         (subdir, subrev, subbranch, subhash,
                                          obranch, ohash))

            need_update = True
            initialize = True
            if not os.path.exists(subdir):
                base_url, _ = self.__gitrepo.ssh_url.rsplit("/", 1)
                git_url = "%s/%s.git" % (base_url, subdir)

                # if this submodule was added previously, force it to be added
                force = os.path.exists(os.path.join(".git", "modules", subdir))

                try:
                    git_submodule_add(git_url, subhash, force=force,
                                      debug=debug, verbose=verbose)
                    submodule.revision = subrev
                    submodule.url = url
                    need_update = False
                except GitException as gex:
                    xstr = str(gex)
                    if xstr.find("already exists in the index") >= 0:
                        need_update = True
                    else:
                        if xstr.find("does not appear to be a git repo") >= 0:
                            if verbose:
                                print("WARNING: Not adding nonexistent"
                                      " submodule \"%s\"" % (subdir, ))
                        else:
                            print("WARNING: Cannot add \"%s\" (URL %s) to %s:"
                                  " %s" % (subdir, git_url, self.name, gex),
                                  file=sys.stderr)
                            read_input("%s %% Hit Return to continue: " %
                                       os.getcwd())
                        continue

            if need_update:
                # submodule already exists, recover it
                git_submodule_update(subdir, subhash, initialize=initialize,
                                     debug=debug, verbose=verbose)

            # add Submodule if it's a new entry
            if subdir not in self.__submodules:
                self.__submodules[subdir] = submodule

        for proj in self.__submodules:
            if proj not in found:
                if os.path.exists(proj):
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
        self.__clean_reverted_svn_sandbox(branch_name,
                                          ignore_externals=\
                                          self.__convert_externals,
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

    @property
    def all_urls(self):
        for flds in self.__svnprj.all_urls(ignore=self.__svnprj.ignore_tag):
            yield flds

    def branch_name(self, svn_url):
        return self.__svnprj.branch_name(svn_url)

    def commit_project(self, debug=False, verbose=False):
        # build a list of all trunk/branch/tag URLs for this project
        all_urls = []
        for _, _, svn_url in self.all_urls:
            all_urls.append(svn_url)

        # convert trunk/branches/tags to Git
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
                report_progress = None
            else:
                report_progress = self.__progress_reporter

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

                        # remember to finish GitHub initialization
                        self.__initial_commit = self.__ghutil is not None
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
                                    report_progress=report_progress,
                                    debug=debug, verbose=verbose):
                    # if we added an entry, increase the count of git commits
                    num_added += 1

            # add all remaining issues to GitHub
            if self.__mantis_issues is not None and \
              self.__gitrepo.has_issue_tracker:
                self.__mantis_issues.add_issues(self.__gitrepo,
                                                report_progress=\
                                                report_progress)

            # clear the status line
            if report_progress is not None:
                print("\rAdded %d of %d SVN entries                         " %
                      (num_added, num_entries))

        # make sure we leave the new repo on the last commit for 'master'
        git_checkout("master", debug=debug, verbose=verbose)
        if self.__master_hash is None:
            self.__master_hash = "HEAD"
        git_reset(start_point=self.__master_hash, hard=True, debug=debug,
                  verbose=verbose)

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
        if self.__repo_is_temporary:
            return "temporary Git repo"
        return "local Git repo"

    @property
    def repo_is_temporary(self):
        return self.__repo_is_temporary

    @property
    def repo_path(self):
        return self.__repo_path


def convert_project(svn2git, pause_before_finish=False, debug=False,
                    verbose=False):

    # let user know that we're starting to do real work
    if not verbose:
        print("Converting %s SVN repository to %s" %
              (svn2git.name, svn2git.project_type))

    # remember the current directory
    curdir = os.getcwd()

    try:
        os.chdir(svn2git.repo_path)

        # if an older repo exists, delete it
        if os.path.exists(svn2git.name):
            shutil.rmtree(svn2git.name)
            print("Removed existing %s" % (svn2git.name, ))

        svn2git.commit_project(debug=debug, verbose=verbose)
    except:
        traceback.print_exc()
        raise
    finally:
        if pause_before_finish:
            read_input("%s %% Hit Return to finish: " % os.getcwd())

        os.chdir(curdir)

        # if we created the Git repo in a temporary directory, remove it now
        if svn2git.repo_is_temporary:
            shutil.rmtree(svn2git.repo_path)


def load_github_data(svnprj, organization, repo_name, destroy_old=False,
                     make_public=False, sleep_seconds=1):
    # if the organization name was not specified, assume it's the user's
    #  personal space
    if organization is None:
        organization = getpass.getuser()

    if repo_name is None:
        repo_name = svnprj.name

    ghutil = GithubUtil(organization, repo_name)
    ghutil.destroy_existing_repo = destroy_old
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


def load_subversion_project(svn_project, debug=False, verbose=False):
    "Load Subversion project log entries and cache them in an SQLite3 database"

    svnprj = PDAQManager.get(svn_project)
    if svnprj is None:
        raise SystemExit("Cannot find SVN project \"%s\"" % (svn_project, ))

    # load log entries from all URLs and save any new entries to the database
    if not verbose:
        print("Loading Subversion log messages for %s" % (svnprj.name, ))
    svnprj.load(debug=debug, verbose=verbose)
    svnprj.save_to_db(debug=debug, verbose=verbose)

    return svnprj


def main():
    parser = argparse.ArgumentParser()
    add_arguments(parser)
    args = parser.parse_args()

    # pDAQ used 'releases' instead of 'tags'
    if args.use_releases:
        SVNMetadata.set_layout(SVNMetadata.DIRTYPE_TAGS, "releases")

    PDAQManager.load_authors(args.author_file, verbose=args.verbose)

    # get the SVNProject data for the requested project
    svnprj = load_subversion_project(args.svn_project, debug=args.debug,
                                     verbose=args.verbose)

    # if saving to GitHub, initialize the GitHub utility data
    if not args.use_github:
        ghutil = None
    else:
        ghutil = load_github_data(svnprj, args.organization, args.github_repo,
                                  destroy_old=args.destroy_old,
                                  make_public=args.make_public,
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

    svn2git = Subversion2Git(svnprj, ghutil, mantis_issues,
                             args.description, args.local_repo,
                             convert_externals=args.convert_externals,
                             ignore_bad_externals=args.ignore_bad_externals)

    # do all the things!
    convert_project(svn2git, pause_before_finish=args.pause_before_finish,
                    debug=args.debug, verbose=args.verbose)


if __name__ == "__main__":
    main()
