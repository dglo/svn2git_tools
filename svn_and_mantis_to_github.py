#!/usr/bin/env python3

from __future__ import print_function

import argparse
import getpass
import os
import shutil
import sys
import tempfile
import traceback

from github import GithubException

from cmdrunner import CommandException
from git import GitException, git_add, git_autocrlf, git_checkout, \
     git_commit, git_init, git_push, git_remote_add, git_remove, git_reset, \
     git_show_hash, git_status, git_submodule_add, git_submodule_remove, \
     git_submodule_update
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

        self.__project = PDAQManager.get(name)

    def __str__(self):
        if self.revision is None:
            rstr = ""
        else:
            rstr = "@%s" % str(self.revision)
        return "[%s%s]%s" % (self.name, rstr, self.url)

    def get_git_hash(self, revision):
        if revision is None:
            raise Exception("Cannot fetch unknown %s revision" % (self.name, ))

        revnum, git_branch, git_hash = \
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


def __clean_reverted_svn_sandbox(branch_name, ignore_externals=False,
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
            raise CommandException("Found stray files in sandbox,"
                                   " cannot continue")


def __create_gitignore(sandbox_dir, ignorelist=None, include_python=False,
                       include_java=False, debug=False, verbose=False):
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


def __commit_project(svnprj, ghutil, gitrepo, mantis_issues, description,
                     convert_externals=False, ignore_bad_externals=False,
                     debug=False, verbose=False):
    svn2git = {}

    trunk_url = svnprj.trunk_url

    # dictionary mapping submodule names to submodule revisions
    subdict = {}

    # the final commit on the Git master branch
    git_master_hash = None

    # build a list of all trunk/branch/tag URLs for this project
    all_urls = []
    for _, _, svn_url in svnprj.all_urls(ignore=svnprj.ignore_tag):
        all_urls.append(svn_url)

    # convert trunk/branches/tags to Git
    for count, svn_url in enumerate(all_urls):
        # build the base prefix string which is stripped from each file
        svn_file_prefix = __build_base_suffix(svn_url, svnprj.base_url)

        # ensure this is for our project
        if not svn_file_prefix.startswith(svnprj.name):
            raise CommandException("SVN file prefix \"%s\" does not start with"
                                   " project name \"%s\"" %
                                   (svn_file_prefix, svnprj.name))
        branch_name = svn_file_prefix[len(svnprj.name):]
        if branch_name == "":
            branch_name = SVNMetadata.TRUNK_NAME
        elif branch_name[0] == "/":
            branch_name = branch_name[1:]
        else:
            raise CommandException("SVN branch name \"%s\" (from \"%s\")"
                                   " does not start with project name \"%s\"" %
                                   (branch_name, svn_file_prefix, svnprj.name))

        print("Converting %d revisions from %s (#%d of %d)" %
              (svnprj.database.num_entries, branch_name, count + 1,
               len(all_urls)))

        # SVN file prefix should end with a file separator character
        if not svn_file_prefix.endswith("/"):
            svn_file_prefix += "/"

        # don't report progress if we're printing verbose of debugging messages
        if debug or verbose:
            report_progress = None
        else:
            report_progress = __progress_reporter

        # values used when reporting progress to user
        num_entries = svnprj.database.num_entries
        num_added = 0

        finish_github_init = False
        for count, entry in enumerate(svnprj.database.entries(branch_name)):

            if report_progress is not None:
                report_progress(count, num_entries, "SVN rev", entry.revision)

            if count > 0:
                # update SVN sandbox to this revision
                if debug:
                    print("Update %s to rev %d in %s" %
                          (svnprj.name, entry.revision, os.getcwd()))
            elif branch_name == SVNMetadata.TRUNK_NAME:
                subdir = svnprj.name

                # initialize Git and Subversion sandboxes
                __initialize_sandboxes(svn_url, svnprj.trunk_url, subdir,
                                       entry.revision, debug=debug,
                                       verbose=verbose)

                # move into the newly created sandbox
                os.chdir(subdir)

                # remember to finish GitHub initialization
                finish_github_init = ghutil is not None
            else:
                __switch_to_branch(trunk_url, svn_url, branch_name, entry,
                                   svn2git,
                                   ignore_bad_externals=ignore_bad_externals,
                                   ignore_externals=convert_externals,
                                   debug=debug, verbose=verbose)

            # retry a couple of times in case update fails to connect
            ignored = False
            for _ in (0, 1, 2):
                try:
                    for _ in svn_update(revision=entry.revision,
                                        ignore_bad_externals=\
                                        ignore_bad_externals,
                                        ignore_externals=convert_externals,
                                        debug=debug, verbose=verbose):
                        pass
                    break
                except SVNConnectException:
                    continue
                except SVNNonexistentException:
                    # if this url and/or revision does not exist, ignore it
                    ignored = True
                    print("WARNING: Revision %s does not exist for %s" %
                          (entry.revision, svn_url))
                    break

            # if we ignored this revision, move onto the next log entry
            if ignored:
                continue

            if convert_externals:
                subdict = __convert_externals_to_submodules(svnprj, gitrepo,
                                                            svn2git,
                                                            entry.revision,
                                                            subdict,
                                                            debug=debug,
                                                            verbose=verbose)

            if mantis_issues is None or not gitrepo.has_issue_tracker:
                # don't open issues if we don't have any Mantis issues or
                # if we're not writing to a repo with an issue tracker
                github_issues = None
            else:
                # open/reopen GitHub issues
                github_issues = \
                  mantis_issues.open_github_issues(gitrepo, entry.revision,
                                                   report_progress=\
                                                   report_progress)

            # commit this revision to git
            commit_rpt = report_progress is not None
            commit_result = __commit_to_git(entry, svnprj,
                                            github_issues=github_issues,
                                            initial_commit=finish_github_init,
                                            report_progress=commit_rpt,
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
                    git_master_hash = full_hash

                svnprj.database.add_git_commit(entry.revision, branch,
                                               full_hash)

                if entry.revision in svn2git:
                    obranch, ohash = svn2git[entry.revision]
                    raise CommandException("Cannot map r%d to %s:%s, already"
                                           " mapped to %s:%s" %
                                           (entry.revision, branch, full_hash,
                                            obranch, ohash))

                if debug:
                    print("Mapping SVN r%d -> branch %s hash %s" %
                          (entry.revision, branch, full_hash))
                svn2git[entry.revision] = (branch, full_hash)

                # increase the number of git commits
                num_added += 1

            if finish_github_init:
                # create GitHub repository and push the first commit
                __finish_first_commit(gitrepo, debug=debug, verbose=verbose)

                # remember that we're done with GitHub repo initialization
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
        if mantis_issues is not None and gitrepo.has_issue_tracker:
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


def __commit_to_git(entry, svnprj, github_issues=None, initial_commit=False,
                    report_progress=False, debug=False, verbose=False):
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
            if verbose:
                if report_progress is not None:
                    # if we're printing progress messages, add a newline
                    print("")
                print("WARNING: No changes found in %s SVN rev %d" %
                      (svnprj.name, entry.revision), file=sys.stderr)
                if message is not None:
                    print("(Commit message: %s)" % str(message),
                          file=sys.stderr)

            # use the first previous commit with a Git hash
            prev = entry.previous
            while prev is not None:
                if prev.git_branch is not None and prev.git_hash is not None:
                    return prev.git_branch, prev.git_hash, None, None, None
                prev = prev.previous
            return None

        if line.startswith("Untracked files:"):
            raise CommandException("Found untracked files for %s SVN rev %d" %
                                   (svnprj.name, entry.revision))
        if line.startswith("Changes not staged for commit:"):
            raise CommandException("Found unknown changes for %s SVN rev %d" %
                                   (svnprj.name, entry.revision))

    # NOTE: We always add .gitignore, so this shouldn't be needed
    # if nothing was added, add a dummy file to the initial commit
    if initial_commit and not added and False:
        # add a dummy file so the initial commit isn't empty
        dummy = "LoremIpsum.md"
        with open(dummy, "w") as dout:
            print("# %s" % (svnprj.name, ), file=dout)
        git_add(dummy, debug=debug, verbose=verbose)

    try:
        flds = git_commit(author=PDAQManager.get_author(entry.author),
                          commit_message=message,
                          date_string=entry.datetime.isoformat(),
                          filelist=None, commit_all=False, debug=debug,
                          verbose=verbose)
    except CommandException:
        print("ERROR: Cannot commit %s SVN rev %d (%s)" %
              (svnprj.name, entry.revision, message), file=sys.stderr)
        raise

    return flds


def __convert_externals_to_submodules(svnprj, gitrepo, svn2git, revision,
                                      subdict, debug=False, verbose=False):
    if subdict is None:
        subdict = {}

    found = {}
    for subrev, url, subdir in svn_get_externals(".", debug=debug,
                                                 verbose=verbose):
        found[subdir] = 1

        # get the Submodule object for this project
        if subdir not in subdict:
            submodule = Submodule(subdir, subrev, url)
        else:
            submodule = subdict[subdir]

            if submodule.url != url:
                old_str = submodule.url
                new_str = url
                for idx in range(min(len(submodule.url), len(url))):
                    if submodule.url[idx] != url[idx]:
                        old_str = submodule.url[idx:]
                        new_str = url[idx:]
                        break

                print("WARNING: %s external %s URL changed from %s to %s" %
                      (svnprj.name, subdir, old_str, new_str), file=sys.stderr)

        git_url = os.path.join(gitrepo.ssh_url, subdir)

        # get branch/hash data
        if subrev is None:
            # if there's no revision, they must just want HEAD
            subbranch, subhash = (None, None)
        else:
            subbranch, subhash = submodule.get_git_hash(subrev)

            if subrev not in svn2git:
                # if we don't know about this revision and
                # we have branch/hash data, add it to the dictionary
                if subbranch is not None and subhash is not None:
                    svn2git[subrev] = (subbranch, subhash)
            else:
                # validate branch/hash data
                obranch, ohash = svn2git[subrev]
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
            git_url = os.path.join(gitrepo.ssh_url, subdir)

            # if this submodule was added previously, force it to be added
            force = os.path.exists(os.path.join(".git", "modules", subdir))

            try:
                git_submodule_add(git_url, subhash, force=force, debug=debug,
                                  verbose=verbose)
                submodule.revision = subrev
                submodule.url = url
                need_update = False
            except GitException as gex:
                gexstr = str(gex)
                if gexstr.find("already exists in the index") >= 0:
                    need_update = True
                else:
                    if gexstr.find("does not appear to be a git repo") >= 0:
                        if verbose:
                            print("WARNING: Not adding nonexistent submodule"
                                  " \"%s\"" % (subdir, ))
                    else:
                        print("WARNING: Cannot add \"%s\" to %s: %s" %
                              (subdir, svnprj.name, gex), file=sys.stderr)
                        read_input("%s %% Hit Return to continue: " %
                                   os.getcwd())
                    continue

        if need_update:
            # submodule already exists, recover it
            git_submodule_update(subdir, subhash, initialize=initialize,
                                 debug=debug, verbose=verbose)

        # add Submodule if it's a new entry
        if subdir not in subdict:
            subdict[subdir] = submodule

    for proj in subdict:
        if proj not in found:
            if os.path.exists(proj):
                git_submodule_remove(proj, debug=debug, verbose=verbose)
            elif verbose:
                print("WARNING: Not removing nonexistent submodule \"%s\""
                      " from %s" % (proj, svnprj.name), file=sys.stderr)

    return subdict


def __finish_first_commit(gitrepo, debug=False, verbose=False):
    for _ in git_remote_add("origin", gitrepo.ssh_url, debug=debug,
                            verbose=verbose):
        pass

    for _ in git_push("master", "origin", debug=debug, verbose=verbose):
        pass


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

        if line[2] != " " and line[2] != "M":
            raise Exception("Bad porcelain status line \"%s\"" % str(line))

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


def __initialize_sandboxes(svn_url, trunk_url, subdir, revision, debug=False,
                           verbose=False):
    # check out the Subversion repo
    __check_out_svn_project(svn_url, subdir, revision=revision, debug=debug,
                            verbose=verbose)
    if debug:
        print("=== Inside newly checked-out %s ===" % (subdir, ))
        for dentry in os.listdir(subdir):
            print("\t%s" % str(dentry))

    # get list of ignored entries from SVN
    ignorelist = __load_svn_ignore(trunk_url)

    # initialize the directory as a git repository
    git_init(sandbox_dir=subdir, verbose=verbose)

    # allow old files with Windows-style line endings to be committed
    git_autocrlf(sandbox_dir=subdir, debug=debug, verbose=verbose)

    # create a .gitconfig file which ignores .svn as well as anything
    #  else which is already being ignored
    __create_gitignore(subdir, ignorelist=ignorelist, debug=debug,
                       verbose=verbose)


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


def __progress_reporter(count, total, name, value):
    # print spaces followed backspaces to erase any stray characters
    spaces = " "*30
    backup = "\b"*30

    print("\r#%d (of %d): %s %d%s%s" % (count, total, name, value, spaces,
                                        backup), end="")


def __switch_to_branch(trunk_url, branch_url, branch_name, entry, svn2git,
                       ignore_bad_externals=False, ignore_externals=False,
                       debug=False, verbose=False):
    if entry.previous is None:
        print("Ignoring standalone branch %s" % (branch_name, ))
        return

    prev_entry = entry.previous
    while prev_entry.revision not in svn2git:
        prev_entry = prev_entry.previous
        if prev_entry is None:
            raise Exception("Cannot find committed ancestor for SVN r%d" %
                            (entry.previous.revision, ))

    prev_branch, prev_hash = svn2git[prev_entry.revision]

    # switch back to trunk (in case we'd switched to a branch)
    for _ in svn_switch(trunk_url, revision=prev_entry.revision,
                        ignore_bad_externals=ignore_bad_externals,
                        ignore_externals=ignore_externals, debug=debug,
                        verbose=verbose):
        pass

    # revert all modifications
    svn_revert(recursive=True, debug=debug, verbose=verbose)

    # update to fix any weird stuff post-reversion
    for _ in svn_update(revision=prev_entry.revision,
                        ignore_bad_externals=ignore_bad_externals,
                        ignore_externals=ignore_externals, debug=debug,
                        verbose=verbose):
        pass

    # revert Git repository to the original branch point
    if debug:
        print("** Reset to rev %s (%s hash %s)" %
              (prev_entry.revision, prev_branch, prev_hash))
    git_reset(start_point=prev_hash, hard=True, debug=debug, verbose=verbose)

    new_name = branch_name.rsplit("/")[-1]

    # create the new Git branch (via the checkout command)
    git_checkout(new_name, start_point=prev_hash, new_branch=True, debug=debug,
                 verbose=verbose)

    # revert any changes caused by the git checkout
    svn_revert(recursive=True, debug=debug, verbose=verbose)

    # remove any stray files not cleaned up by the 'revert'
    __clean_reverted_svn_sandbox(branch_name,
                                 ignore_externals=ignore_externals,
                                 verbose=verbose)

    # switch sandbox to new revision
    try:
        for _ in svn_switch(branch_url, revision=entry.revision,
                            ignore_bad_externals=ignore_bad_externals,
                            ignore_externals=ignore_externals, debug=debug,
                            verbose=verbose):
            pass
    except SVNNonexistentException:
        print("WARNING: Cannot switch to nonexistent %s rev %s (ignored)" %
              (branch_url, entry.revision), file=sys.stderr)

    # print("*** Prev rev %d -> hash %s" %
    #       (prev_entry.revision, prev_hash))
    # read_input("%s %% branch %s entry %s hash %s: " %
    #            (os.getcwd(), branch_name, entry, prev_hash))


def convert_project(svnprj, ghutil, mantis_issues, description,
                    convert_externals=False, ignore_bad_externals=False,
                    local_repo=None, pause_before_finish=False, debug=False,
                    verbose=False):

    # let user know that we're starting to do real work
    if not verbose:
        if ghutil is not None:
            prjtype = "GitHub"
        elif local_repo is not None:
            prjtype = "local Git repo"
        else:
            prjtype = "temporary Git repo"
        print("Converting %s SVN repository to %s" % (svnprj.name, prjtype))

    # if description was not specified, build a default value
    if description is None:
        description = "WIPAC's %s project" % (svnprj.name, )

    # remember the current directory
    curdir = os.getcwd()

    if local_repo is not None:
        # if we're saving the local repo, create it in the repo directory
        tmpdir = os.path.abspath(local_repo)
    else:
        tmpdir = tempfile.mkdtemp()

    if ghutil is not None:
        gitrepo = ghutil.build_github_repo(description, debug=debug,
                                           verbose=verbose)
    else:
        gitrepo = LocalRepository(tmpdir)

    try:
        os.chdir(tmpdir)

        # if an older repo exists, delete it
        if os.path.exists(svnprj.name):
            shutil.rmtree(svnprj.name)
            print("Removed existing %s" % (svnprj.name, ))

        __commit_project(svnprj, ghutil, gitrepo, mantis_issues, description,
                         ignore_bad_externals=ignore_bad_externals,
                         convert_externals=convert_externals,
                         debug=debug, verbose=verbose)
    except:
        traceback.print_exc()
        raise
    finally:
        if pause_before_finish:
            read_input("%s %% Hit Return to finish: " % os.getcwd())

        os.chdir(curdir)

        # if we created the Git repo in a temporary directory, remove it now
        if local_repo is None:
            shutil.rmtree(tmpdir)


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

    # do all the things!
    convert_project(svnprj, ghutil, mantis_issues, args.description,
                    convert_externals=args.convert_externals,
                    ignore_bad_externals=args.ignore_bad_externals,
                    local_repo=args.local_repo,
                    pause_before_finish=args.pause_before_finish,
                    debug=args.debug, verbose=args.verbose)


if __name__ == "__main__":
    main()
