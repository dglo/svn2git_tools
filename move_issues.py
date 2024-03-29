#!/usr/bin/env python

from __future__ import print_function

import argparse
import getpass
import os
import sys

from github import GithubException

from git import git_clone
from github_util import GithubUtil
from i3helper import TemporaryDirectory
from mantis_converter import MantisConverter


def add_arguments(parser):
    "Add command-line arguments"

    parser.add_argument("-A", "--add-after", dest="add_after",
                        action="store_true", default=False,
                        help="If Mantis ID is specified, only add issues"
                        " after this ID")
    parser.add_argument("-M", "--mantis-dump", dest="mantis_dump",
                        default=None, required=True,
                        help="MySQL dump file of WIPAC Mantis repository")
    parser.add_argument("-O", "--organization", dest="organization",
                        default=None,
                        help="GitHub organization to use when creating the"
                        " repository")
    parser.add_argument("-i", "--mantis-id", dest="mantis_id",
                        type=int,
                        help="If specified, only add issues before this ID ")
    parser.add_argument("-m", "--mantis_project", dest="mantis_project",
                        default=None,
                        help="Mantis project name, if different from GitHub"
                             " project")
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
    parser.add_argument("--preserve-all-status", dest="preserve_all_status",
                        action="store_true", default=False,
                        help="Preserve status of all Mantis issues")
    parser.add_argument("--preserve-resolved", dest="preserve_resolved_status",
                        action="store_true", default=False,
                        help="Preserve status of resolved Mantis issues")

    parser.add_argument("github_project", default=None,
                        help="Github project name")


def __progress_reporter(count, total, name, value_name, value):
    # print spaces followed backspaces to erase any stray characters
    spaces = " "*30
    unspaces = "\b"*27  # leave a few spaces to separate error msgs

    print("\r #%d (of %d): %s %s %s%s%s" %
          (count, total, name, value_name, value, spaces, unspaces), end="")
    sys.stdout.flush()


def build_git_repo(github, ghutil, github_project, description=None,
                   destroy_old_repo=False, verbose=False):

    try:
        org = get_organization_or_user(github, ghutil.organization)
    except GithubException:
        raise Exception("Unknown GitHub organization/user \"%s\"" %
                        str(ghutil.organization))

    if destroy_old_repo:
        try:
            repo = org.get_repo(github_project)
        except GithubException:
            repo = None

        if repo is not None:
            repo.delete()
            if verbose:
                print("Deleted existing \"%s/%s\" repository" %
                      (ghutil.organization, github_project, ))

    repo = org.create_repo(github_project, description=description,
                           has_issues=True,
                           private=not ghutil.make_new_repo_public)

    return repo


def get_github_util(project_name, organization=None, new_project_name=None,
                    make_public=False, sleep_seconds=2, debug=False,
                    verbose=False):
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


def get_git_repo(ghutil, description=None, destroy_old_repo=False, debug=False,
                 verbose=False):
    # if description was not specified, build a default value
    # XXX add a more general solution here
    if description is None:
        description = "WIPAC's %s project" % (ghutil.repository, )

    return ghutil.get_github_repo(description=description,
                                  create_repo=destroy_old_repo,
                                  destroy_existing=destroy_old_repo,
                                  debug=debug, verbose=verbose)


def get_organization_or_user(github, organization):
    if organization == getpass.getuser():
        return github.get_user()

    try:
        return github.get_organization(organization)
    except GithubException:
        raise Exception("Bad organization \"%s\"" % str(organization))


def read_github_token(filename):
    with open(filename, "r") as fin:
        for line in fin:
            line = line.strip()
            if line.startswith("#"):
                continue

            if line == "":
                continue

            return line


def move_issues(mantis_issues, gitrepo, project_name, description,
                mantis_id=None, add_after=False, debug=False,
                verbose=False):
    # remember the current directory
    with TemporaryDirectory():
        # clone the GitHub project
        git_clone(gitrepo.ssh_url, debug=debug, verbose=verbose)

        # move into the repo sandbox directory
        os.chdir(project_name)

        # add all issues
        mantis_issues.add_issues(mantis_id=mantis_id, add_after=add_after,
                                 report_progress=__progress_reporter)

    print()


def main():
    parser = argparse.ArgumentParser()
    add_arguments(parser)
    args = parser.parse_args()

    # get connection to GitHub repo
    ghutil = get_github_util(args.github_project,
                             organization=args.organization, make_public=True,
                             debug=args.debug, verbose=args.verbose)
    gitrepo = get_git_repo(ghutil, destroy_old_repo=args.destroy_old,
                           debug=args.debug, verbose=args.verbose)

    if args.mantis_project is not None:
        mantis_project = args.mantis_project
    else:
        mantis_project = args.github_project

    print("Loading Mantis issues for %s" % mantis_project)
    mantis_issues = MantisConverter(args.mantis_dump, None, gitrepo,
                                    (mantis_project, ), verbose=args.verbose)
    mantis_issues.close_resolved = args.close_resolved
    mantis_issues.preserve_all_status = args.preserve_all_status
    mantis_issues.preserve_resolved_status = args.preserve_resolved_status

    if len(mantis_issues) == 0:
        raise SystemExit("No issues found for %s" % mantis_project)

    print("Uploading %d %s issues" % (len(mantis_issues), args.github_project))
    description = "Test repo for %s issues" % str(args.github_project)
    move_issues(mantis_issues, gitrepo, ghutil.repository, description,
                mantis_id=args.mantis_id, add_after=args.add_after,
                debug=args.debug, verbose=args.verbose)


if __name__ == "__main__":
    main()
