#!/usr/bin/env python3

from __future__ import print_function

import argparse
import getpass
import os
import re
import shutil
import sys
import tempfile
import traceback

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

from github import GithubException

from cmdrunner import CommandException
from git import git_add, git_autocrlf, git_checkout, git_commit, git_init, \
     git_push, git_remote_add, git_remove, git_reset, git_status
from github_util import GithubUtil
from i3helper import read_input
from mantis_converter import MantisConverter
from svn import SVNConnectException, SVNMetadata, svn_checkout, \
     svn_get_externals, svn_log, svn_propget, svn_revert, svn_status, \
     svn_switch, svn_update
from svndb import MetadataManager, SVNEntry, SVNRepositoryDB


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
    parser.add_argument("-X", "--ignore-externals",
                        dest="ignore_externals",
                        action="store_true", default=False,
                        help="Do not check out external projects")
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


def __build_base_suffix(svn_url, base_url):
    "Build the base URL prefix which is stripped from each file URL"

    # strip base URL from front of full URL
    if not svn_url.startswith(base_url):
        raise CommandException("URL \"%s\" does not start with"
                               " base URL \"%s\"" % (svn_url, base_url))

    prefix = svn_url[len(base_url):]

    # strip leading slash
    if not prefix.startswith("/"):
        raise CommandException("Cannot strip base URL \"%s\" from \"%s\"" %
                               (base_url, svn_url))
    prefix = prefix[1:]

    return prefix


def __check_out_svn_project(svn_url, target_dir, revision=None, debug=False,
                            verbose=False):
    if debug:
        print("Checkout %s rev %d in %s" %
              (svn_url, revision, os.getcwd()))

    svn_checkout(svn_url, revision, target_dir, debug=debug, verbose=verbose)

    if debug:
        print("=== After checkout of %s ===" % svn_url)
        for dentry in os.listdir("."):
            print("\t%s" % str(dentry))

    # verify that project subdirectory was created
    if not os.path.exists(target_dir):
        raise CommandException("Cannot find project subdirectory \"%s\" after"
                               " checkout" % (target_dir, ))


def __create_gitignore(ignorelist=None, include_python=False,
                       include_java=False, debug=False, verbose=False):
    with open(".gitignore", "w") as fout:
        if ignorelist is not None:
            for entry in ignorelist:
                print("%s" % str(entry), file=fout)
        print("# Ignore Subversion directory during Git transition\n.svn",
              file=fout)
        if include_python:
            print("\n# Java stuff\n*.class\ntarget", file=fout)
        if include_java:
            print("\n# Python stuff\n*.pyc\n__pycache__", file=fout)

    git_add(".gitignore", debug=debug, verbose=verbose)


def __commit_project(svndb, authors, ghutil, mantis_issues,
                     description, ignore_bad_externals=False,
                     ignore_externals=False, debug=False, verbose=False):
    svn2git = {}

    trunk_url = svndb.metadata.trunk_url

    # the final commit on the Git master branch
    git_master_hash = None

    for _, _, svn_url in svndb.metadata.all_urls(ignore=ignore_tag):
        # build the base prefix string which is stripped from each file
        svn_file_prefix = __build_base_suffix(svn_url, svndb.metadata.base_url)

        # ensure this is for our project
        if not svn_file_prefix.startswith(svndb.project):
            raise CommandException("SVN file prefix \"%s\" does not start with"
                                   " project name \"%s\"" %
                                   (svn_file_prefix, svndb.project))
        branch_name = svn_file_prefix[len(svndb.project):]
        if branch_name == "":
            branch_name = SVNMetadata.TRUNK_NAME
        elif branch_name[0] == "/":
            branch_name = branch_name[1:]
        else:
            raise CommandException("SVN branch name \"%s\" (from \"%s\")"
                                   " does not start with project name \"%s\"" %
                                   (branch_name, svn_file_prefix,
                                    svndb.project))

        print("Converting %d revisions from %s" %
              (svndb.num_entries, branch_name))

        # SVN file prefix should end with a file separator character
        if not svn_file_prefix.endswith("/"):
            svn_file_prefix += "/"

        # don't report progress if we're printing verbose of debugging messages
        if debug or verbose:
            report_progress = None
        else:
            report_progress = __progress_reporter

        # values used when reporting progress to user
        num_entries = svndb.num_entries
        num_added = 0

        gitrepo = None
        finish_github_init = False
        for count, entry in enumerate(svndb.entries(branch_name)):

            if report_progress is not None:
                report_progress(count, num_entries, "SVN rev", entry.revision)

            if count > 0:
                # update SVN sandbox to this revision
                if debug:
                    print("Update %s to rev %d in %s" %
                          (svndb.project, entry.revision, os.getcwd()))
            elif branch_name == SVNMetadata.TRUNK_NAME:
                # check out the SVN sandbox and initialize Git/GitHub repo
                __initialize_svn_and_git(svndb, svn_url, entry.revision,
                                         debug=debug, verbose=verbose)

                # remember to finish GitHub initialization
                finish_github_init = ghutil is not None
            else:
                if entry.previous is None:
                    print("Ignoring standalone branch %s" % (branch_name, ))
                    continue

                prev_entry = entry.previous
                while prev_entry.revision not in svn2git:
                    prev_entry = prev_entry.previous
                    if prev_entry is None:
                        raise Exception("Cannot find committed ancestor"
                                        " for SVN r%d" %
                                        (entry.previous.revision, ))

                prev_branch, prev_hash = svn2git[prev_entry.revision]

                # switch back to trunk (in case we'd switched to a branch)
                for _ in svn_switch(trunk_url, revision=prev_entry.revision,
                                    ignore_bad_externals=ignore_bad_externals,
                                    debug=debug, verbose=verbose):
                    pass

                # revert all modifications
                svn_revert(recursive=True, debug=debug, verbose=verbose)

                # update to fix any weird stuff post-reversion
                for _ in svn_update(revision=prev_entry.revision,
                                    ignore_bad_externals=\
                                    ignore_bad_externals,
                                    ignore_externals=ignore_externals,
                                    debug=debug, verbose=verbose):
                    pass

                # revert Git repository to the original branch point
                git_reset(start_point=prev_hash, hard=True, debug=debug,
                          verbose=verbose)

                new_name = branch_name.rsplit("/")[-1]

                # create the new Git branch (via the checkout command)
                git_checkout(new_name, start_point=prev_hash,
                             new_branch=True, debug=debug, verbose=verbose)

                # revert any changes caused by the git checkout
                svn_revert(recursive=True, debug=debug, verbose=verbose)

                # remove any stray files not cleaned up by the 'revert'
                __clean_reverted_svn_sandbox(branch_name, verbose=verbose)

                # switch sandbox to new revision
                for _ in svn_switch(svn_url, revision=entry.revision,
                                    ignore_bad_externals=ignore_bad_externals,
                                    debug=debug, verbose=verbose):
                    pass

                # print("*** Prev rev %d -> hash %s" %
                #       (prev_entry.revision, prev_hash))
                # read_input("%s %% branch %s entry %s hash %s: " %
                #            (os.getcwd(), branch_name, entry, prev_hash))


            # if update fails due to connect error, retry a couple of times
            for _ in (0, 1, 2):
                try:
                    for _ in svn_update(revision=entry.revision,
                                        ignore_bad_externals=\
                                        ignore_bad_externals,
                                        ignore_externals=ignore_externals,
                                        debug=debug, verbose=verbose):
                        pass
                    break
                except SVNConnectException:
                    continue

            # open/reopen GitHub issues
            github_issues = \
              __open_issues_for_entry(mantis_issues, gitrepo, ghutil is None,
                                      entry, report_progress=report_progress)

            # commit this revision to git
            commit_result = __commit_to_git(entry, svndb.project, authors,
                                            github_issues=github_issues,
                                            initial_commit=finish_github_init,
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
                    mantis_issues.close_github_issue(github_issue, message)

            # if something was committed...
            if commit_result is not None:
                svndb.add_git_commit(entry.revision, branch, hash_id)

                # save the hash ID for this Git commit
                (branch, hash_id, changed, inserted, deleted) = commit_result
                if branch == "master" and changed is not None and \
                  inserted is not None and deleted is not None:
                    git_master_hash = hash_id

                if entry.revision in svn2git:
                    obranch, ohash = svn2git[entry.revision]
                    raise CommandException("Cannot map r%d to %s:%s, already"
                                           " mapped to %s:%s" %
                                           (entry.revision, branch, hash_id,
                                            obranch, ohash))

                if debug:
                    print("SVN r%d -> branch %s hash %s" %
                          (entry.revision, branch, hash_id))
                svn2git[entry.revision] = (branch, hash_id)

                # increase the number of git commits
                num_added += 1

            if finish_github_init:
                # create GitHub repository and push the first commit
                gitrepo = __finish_first_commit(ghutil, description,
                                                debug=debug, verbose=verbose)
                finish_github_init = False
            elif ghutil is not None:
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

        # add all remaining issues to GitHub
        if mantis_issues is not None:
            mantis_issues.add_issues(gitrepo, report_progress=report_progress)

        # clear the status line
        if report_progress is not None:
            print("\rAdded %d of %d SVN entries                             " %
                  (num_added, num_entries))

    # make sure we leave the new repo on the last commit for 'master'
    git_checkout("master", debug=debug, verbose=verbose)
    if git_master_hash is None:
        git_master_hash = "HEAD"
    git_reset(start_point=git_master_hash, hard=True, debug=debug,
              verbose=verbose)


def __clean_reverted_svn_sandbox(branch_name, verbose=False):
    error = False
    for line in svn_status():
        if not line.startswith("?"):
            if not error:
                print("Reverted %s sandbox contains:" % (branch_name, ))
                error = True
            print("%s" % line)
            continue

        filename = line[1:].strip()
        if filename in (".git", ".gitignore"):
            continue

        if verbose:
            print("Removing stray entry while switching to %s: %s" %
                  (branch_name, filename))
        if os.path.isdir(filename):
            shutil.rmtree(filename)
        else:
            os.remove(filename)

        if error:
            raise CommandException("Found stray files in sandbox,"
                                   " cannot continue")


def __gather_changes(debug=False, verbose=False):
    additions = None
    deletions = None
    modifications = None

    for line in git_status(porcelain=True, debug=debug, verbose=verbose):
        line = line.rstrip()
        if line == "":
            continue

        if len(line) < 4:
            raise Exception("Short procelain status line \"%s\"" % str(line))

        if line[2] != " ":
            raise Exception("Bad porcelain status line \"%s\"" % str(line))

        if line[1] == " ":
            # ignore files which have already been staged
            continue

        if line[0] == "?" and line[1] == "?":
            if additions is None:
                additions = []
            additions.append(line[3:])
            continue

        if line[0] == " ":
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


def __commit_to_git(entry, project, authors, github_issues=None,
                    initial_commit=False, debug=False, verbose=False):
    """
    Commit an SVN change to git, return a tuple containing:
    (branch_name, hash_id, number_changed, number_inserted, number_deleted)
    """

    additions, deletions, modifications = \
      __gather_changes(debug=debug, verbose=verbose)

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
          (plural, ", ".join(str(x.number) for x in github_issues), message)

    # some SVN commits may not change files (e.g. file property changes)
    for line in git_status(debug=debug, verbose=verbose):
        if line.startswith("nothing to commit"):
            print("WARNING: No changes found in %s SVN rev %d" %
                  (project, entry.revision))
            if message is not None:
                print("(Commit message: %s)" % str(message))

            # use the first previous commit with a Git hash
            prev = entry.previous
            while prev is not None:
                if prev.git_branch is not None and prev.git_hash is not None:
                    return prev.git_branch, prev.git_hash, 0, 0, 0
                prev = prev.previous
            return None

        if line.startswith("Untracked files:"):
            raise CommandException("Found untracked files for %s SVN rev %d" %
                                   (project, entry.revision))
        if line.startswith("Changes not staged for commit:"):
            raise CommandException("Found unknown changes for %s SVN rev %d" %
                                   (project, entry.revision))

    # NOTE: We always add .gitignore, so this shouldn't be needed
    # if nothing was added, add a dummy file to the initial commit
    if initial_commit and not added and False:
        # add a dummy file so the initial commit isn't empty
        dummy = "LoremIpsum.md"
        with open(dummy, "w") as dout:
            print("# %s" % project, file=dout)
        git_add(dummy, debug=debug, verbose=verbose)

    try:
        flds = git_commit(author=authors[entry.author], commit_message=message,
                          date_string=entry.datetime.isoformat(),
                          filelist=None, commit_all=False, debug=debug,
                          verbose=verbose)
    except CommandException:
        print("ERROR: Cannot commit %s SVN rev %d (%s)" %
              (project, entry.revision, message), file=sys.stderr)
        raise

    return flds


def __finish_first_commit(ghutil, description, debug=False,
                          verbose=False):
    gitrepo = ghutil.build_git_repo(description, debug=debug, verbose=verbose)

    for _ in git_remote_add("origin", gitrepo.ssh_url, debug=debug,
                            verbose=verbose):
        pass

    for _ in git_push("master", "origin", debug=debug, verbose=verbose):
        pass

    return gitrepo


def __initialize_svn_and_git(svndb, svn_url, revision, debug=False,
                             verbose=False):
    subdir = svndb.project

    # check out the Subversion repo
    __check_out_svn_project(svn_url, subdir, revision=revision, debug=debug,
                            verbose=verbose)
    os.chdir(subdir)
    if debug:
        print("=== Inside newly checked-out %s ===" % subdir)
        for dentry in os.listdir("."):
            print("\t%s" % str(dentry))

    # get list of ignored entries from SVN
    ignorelist = __load_svn_ignore(svndb.trunk_url)

    # initialize the directory as a git repository
    git_init(verbose=verbose)

    # allow old files with Windows-style line endings to be committed
    git_autocrlf(debug=debug, verbose=verbose)

    # create a .gitconfig file so we ignore .svn along with anything
    #  else which is already being ignored
    __create_gitignore(ignorelist=ignorelist, debug=debug, verbose=verbose)


def __load_svn_ignore(trunk_url):
    # get the list of ignored files from Subversion
    ignored = []
    try:
        for line in svn_propget(trunk_url, "svn:ignore"):
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


def __open_issues_for_entry(mantis_issues, gitrepo, use_github, entry,
                            report_progress=False):
    """
    Open/reopen GitHub issues.
    Return list of GitHub issues, or None if there are no issues
    """
    if not use_github or mantis_issues is None:
        # if we're not writing to GitHub, or if there are no Mantis issues
        #  for this entry, return None
        return None

    return mantis_issues.open_github_issues(gitrepo, entry.revision,
                                            report_progress=report_progress)


def __progress_reporter(count, total, name, value):
    # print spaces followed backspaces to erase any stray characters
    spaces = " "*30
    backup = "\b"*30

    print("\r#%d (of %d): %s %d%s%s" % (count, total, name, value, spaces,
                                        backup), end="")


def convert_project(svndb, authors, ghutil, mantis_issues, description,
                    local_repo=None, ignore_bad_externals=False,
                    ignore_externals=False, pause_before_finish=False,
                    debug=False, verbose=False):
    # remember the current directory
    curdir = os.getcwd()

    if local_repo is not None:
        # if we're saving the local repo, create it in the repo directory
        tmpdir = os.path.abspath(local_repo)
    else:
        tmpdir = tempfile.mkdtemp()

    try:
        os.chdir(tmpdir)

        # if an older repo exists, delete it
        if os.path.exists(svndb.project):
            shutil.rmtree(svndb.project)
            print("Removed existing %s" % (svndb.project, ))

        __commit_project(svndb, authors, ghutil, mantis_issues, description,
                         ignore_bad_externals=ignore_bad_externals,
                         ignore_externals=ignore_externals,
                         debug=debug, verbose=verbose)
    except:
        traceback.print_exc()
        raise
    finally:
        if pause_before_finish:
            read_input("%s %% Hit Return to finish: " % os.getcwd())

        if not local_repo:
            shutil.rmtree(tmpdir)

        os.chdir(curdir)


def find_unknown_authors(authors, svndb):
    seen = {}
    unknown = False
    for entry in svndb.all_entries:
        # no need to check authors we've seen before
        if entry.author in seen:
            continue
        seen[entry.author] = True

        if entry.author not in authors:
            print("SVN committer \"%s\" missing from authors file" %
                  str(entry.author), file=sys.stderr)
            unknown = True
    if unknown:
        raise SystemExit("Please add missing authors before continuing")


def get_organization_or_user(github, organization):
    if organization == getpass.getuser():
        return github.get_user()

    try:
        return github.get_organization(organization)
    except GithubException:
        raise Exception("Bad organization \"%s\"" % str(organization))


def ignore_tag(tag_name):
    """
    pDAQ release candidates are named _rc#
    Non-release debugging candidates are named _debug#
    """
    return tag_name.find("_rc") >= 0 or tag_name.find("_debug") >= 0


def load_authors(filename):
    authors = {}

    apat = re.compile(r"(\S+): (\S.*)\s+<(.*)>$")
    with open(filename, "r") as fin:
        for rawline in fin:
            line = rawline.strip()
            if line.startswith("#"):
                # ignore comments
                continue

            mtch = apat.match(line)
            if mtch is None:
                print("ERROR: Bad line in \"%s\": %s" %
                      (filename, rawline.rstrip()), file=sys.stderr)
                continue

            authors[mtch.group(1)] = "%s <%s>" % \
              (mtch.group(2).strip(), mtch.group(3).strip())

    return authors


def read_github_token(filename):
    with open(filename, "r") as fin:
        for line in fin:
            line = line.strip()
            if line.startswith("#"):
                continue

            if line == "":
                continue

            return line


def save_log_to_db(top_url, add_externals=False, debug=False, verbose=False):
    # load log entries from all URLs
    project = SVNProject(top_url)
    for dirtype, _, url in project.metadata.all_urls(ignore=ignore_tag):
        project.load_from_url(url, dirtype, add_externals=add_externals,
                              debug=debug, verbose=verbose)
    print("Loaded %d entries from %s" % (project.num_entries, project.name))

    print("Opening %s repository database" % project.name)
    svndb = SVNRepositoryDB(project.metadata)
    old_entries = {project.make_key(entry.revision): entry
                   for entry in svndb.all_entries}

    print("Saving %d entries to DB" % project.num_entries)
    total = 0
    added = 0
    for key, entry in project.entry_pairs:
        total += 1
        if key not in old_entries:
            svndb.save_entry(entry)
            added += 1
    if added == 0:
        print("No entries added, total is %d" % (total, ))
    elif added == total:
        print("Added %d entries" % (added, ))
    else:
        print("Added %d new entries, total now %d" % (added, total))

    # ugly hack for broken 'pdaq-user' repository
    if project.name == "pdaq-user":
        svndb.trim(12298)


class SVNProject(object):
    def __init__(self, url):
        self.__loaded_trunk = {}
        self.__revision_log = {}

        self.__metadata = SVNMetadata(url)

    def __load_log_entries(self, rel_url, rel_name, revision=None,
                           debug=False):
        "Add all SVN log entries to the internal revision log dictionary"

        # get information about this SVN repository
        try:
            metadata = SVNMetadata(rel_url)
        except CommandException as cex:
            if str(cex).find("W170000") >= 0:
                print("WARNING: Ignoring nonexistent SVN repository %s" %
                      (rel_url, ), file=sys.stderr)
                return
            raise

        prev = None
        for log_entry in svn_log(rel_url, revision="HEAD", end_revision=1,
                                 debug=debug):
            existing = self.get_entry(log_entry.revision)
            if existing is not None:
                existing.check_duplicate(log_entry)
                entry = existing
            else:
                entry = SVNEntry(metadata, rel_name, metadata.branch_name,
                                 log_entry.revision, log_entry.author,
                                 log_entry.date_string, log_entry.num_lines,
                                 log_entry.filedata, log_entry.loglines)
                self.add_entry(entry)

            if prev is not None:
                prev.set_previous(entry)
            prev = entry

            if existing is not None:
                # if we're on a branch and we've reached the trunk, we're done
                break

    @classmethod
    def __build_base_suffix(cls, svn_url, base_url):
        "Build the base URL prefix which is stripped from each file URL"

        if not svn_url.startswith(base_url):
            raise CommandException("URL \"%s\" does not start with"
                                   " base URL \"%s\"" % (svn_url, base_url))

        prefix = svn_url[len(base_url):]
        if not prefix.startswith("/"):
            raise CommandException("Cannot strip base URL \"%s\" from \"%s\"" %
                                   (base_url, svn_url))

        prefix = prefix[1:]
        if not prefix.endswith("/"):
            prefix += "/"

        return prefix

    def load_from_url(self, rel_url, rel_name, add_externals=False,
                      debug=False, verbose=False):
        if verbose:
            print("Loading log entries from %s" % rel_name)

        self.__load_log_entries(rel_url, rel_name, debug=debug)

        if add_externals:
            print("*** ADD_EXTERN")
            # fetch external project URLs
            for revision, url, subdir in svn_get_externals(rel_url):
                # load all log entries for this repository
                try:
                    self.__load_log_entries(url, rel_name, revision=revision,
                                            debug=debug)
                except CommandException as cex:
                    # if the exception did not involve a dead link, reraise it
                    if str(cex).find("E160013") < 0:
                        raise

                    # complain about dead links and continue
                    print("WARNING: Repository %s does not exist" %
                          (url, ), file=sys.stderr)

        if verbose:
            print("After %s, revision log contains %d entries" %
                  (rel_name, len(self.__revision_log)))

    def add_entry(self, entry):
        key = self.make_key(entry.revision)
        if key in self.__revision_log:
            raise Exception("Cannot overwrite <%s>%s with <%s>%s" %
                            (type(self.__revision_log[key]),
                             self.__revision_log[key], type(entry), entry))
        self.__revision_log[key] = entry

    @property
    def entries(self):
        for entry in sorted(self.__revision_log.values(),
                            key=lambda x: x.date_string):
            yield entry

    @property
    def entry_pairs(self):
        for pair in sorted(self.__revision_log.items(),
                           key=lambda x: x[1].date_string):
            yield pair

    def get_entry(self, revision):
        key = self.make_key(revision)
        if key not in self.__revision_log:
            return None
        return self.__revision_log[key]

    def make_key(self, revision):
        return "%s#%d" % (self.__metadata.root_url, revision)

    @property
    def metadata(self):
        return self.__metadata

    @property
    def name(self):
        return self.__metadata.project_name

    @property
    def num_entries(self):
        return len(self.__revision_log)


def main():
    parser = argparse.ArgumentParser()
    add_arguments(parser)
    args = parser.parse_args()

    daq_svn = "http://code.icecube.wisc.edu"
    daq_mantis = ["pDAQ", "dash", "pdaq-config", "pdaq-user"]

    mantis_projects = None
    if args.svn_project is None or args.svn_project == "pdaq":
        url = daq_svn + "/daq/meta-projects/pdaq/trunk"
        svn_project = "pdaq"
        mantis_projects = daq_mantis
    elif args.svn_project.find("/") < 0:
        if args.svn_project == "fabric-common":
            prefix = "svn"
        else:
            prefix = "daq"
        url = os.path.join(daq_svn, prefix, "projects", args.svn_project)
        svn_project = args.svn_project
        mantis_projects = (args.svn_project, )
    else:
        upieces = urlparse.urlparse(args.svn_project)
        upath = upieces.path.split(os.sep)

        if upath[-1] == "trunk":
            del upath[-1]

        url = urlparse.urlunparse((upieces.scheme, upieces.netloc,
                                   os.sep.join(upath), upieces.params,
                                   upieces.query, upieces.fragment))
        svn_project = upath[-1]
        mantis_projects = upath[-1:]

    # pDAQ used 'releases' instead of 'tags'
    if args.use_releases:
        SVNMetadata.set_layout(SVNMetadata.DIRTYPE_TAGS, "releases")

    if verbose:
        print("Loading authors from \"%s\"" % str(args.author_file))
    authors = load_authors(args.author_file)

    save_log_to_db(url, add_externals=False, debug=args.debug,
                   verbose=args.verbose)

    if not args.use_github:
        ghutil = None
    else:
        # if the organization name was not specified, assume it's the user's
        #  personal space
        if args.organization is not None:
            organization = args.organization
        else:
            organization = getpass.getuser()

        if args.github_repo is not None:
            repo_name = args.github_repo
        else:
            repo_name = svn_project

        ghutil = GithubUtil(organization, repo_name)
        ghutil.destroy_existing_repo = args.destroy_old
        ghutil.make_new_repo_public = args.make_public
        ghutil.sleep_seconds = args.sleep_seconds

    if verbose:
        print("Loading entries from DB")
    metadata = SVNMetadata(url)
    svndb = SVNRepositoryDB(MetadataManager.get(metadata))
    if svndb.num_entries == 0:
        raise SystemExit("No data found for %s" % (svndb.project, ))
    if verbose:
        print("Loaded %s from %s" % (svndb.project, svndb.path))

    print("Checking for unknown SVN committers in \"%s\"" %
          (str(args.author_file)))
    find_unknown_authors(authors, svndb)

    # if description was not specified, build a default value
    if args.description is not None:
        description = args.description
    else:
        description = "WIPAC's %s project" % (svndb.project, )

    # if uploading to GitHub and we have a Mantis SQL dump file, load issues
    if not args.use_github or args.mantis_dump is None:
        mantis_issues = None
    else:
        print("Loading Mantis issues for %s" % ", ".join(mantis_projects))
        mantis_issues = MantisConverter(args.mantis_dump, svndb,
                                        mantis_projects, verbose=args.verbose)
        mantis_issues.close_resolved = args.close_resolved
        mantis_issues.preserve_all_status = args.preserve_all_status
        mantis_issues.preserve_resolved_status = args.preserve_resolved_status

    # let user know that we're starting to do real work
    if args.use_github:
        prjtype = "GitHub"
    elif args.local_repo is not None:
        prjtype = "local Git repo"
    else:
        prjtype = "temporary Git repo"
    print("Converting %s SVN repository to %s" % (svndb.project, prjtype))

    # do all the things!
    convert_project(svndb, authors, ghutil, mantis_issues, description,
                    local_repo=args.local_repo,
                    ignore_bad_externals=args.ignore_bad_externals,
                    ignore_externals=args.ignore_externals,
                    pause_before_finish=args.pause_before_finish,
                    debug=args.debug, verbose=args.verbose)


if __name__ == "__main__":
    main()
