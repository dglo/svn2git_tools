#!/usr/bin/env python3

from __future__ import print_function

import argparse
import getpass
import os

from github import Github, GithubException

from git import git_clone
from github_util import GithubUtil
from i3helper import TemporaryDirectory, read_input
from mantis_converter import MantisConverter


def add_arguments(parser):
    "Add command-line arguments"

    parser.add_argument("-M", "--mantis-dump", dest="mantis_dump",
                        default=None,
                        help="MySQL dump file of WIPAC Mantis repository")
    parser.add_argument("-O", "--organization", dest="organization",
                        default=None,
                        help="GitHub organization to use when creating the"
                        " repository")
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


def __progress_reporter(count, total, name, value):
    # print spaces followed backspaces to erase any stray characters
    spaces = " "*30
    backup = "\b"*30

    print("\r#%d (of %d): %s %d%s%s" % (count, total, name, value, spaces,
                                        backup), end="")


def build_git_repo(github, ghutil, github_project, description=None,
                   verbose=False):

    try:
        org = get_organization_or_user(github, ghutil.organization)
    except GithubException:
        raise Exception("Unknown GitHub organization/user \"%s\"" %
                        str(ghutil.organization))

    if ghutil.destroy_existing_repo:
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


def move_issues(mantis_issues, github, ghutil, github_project, description,
                debug=False, verbose=False):
    # remember the current directory
    with TemporaryDirectory() as tmpdir:
        try:
            if ghutil.destroy_existing_repo:
                repo = build_git_repo(github, ghutil, github_project,
                                      description, verbose=False)
            else:
                try:
                    org = get_organization_or_user(github, ghutil.organization)
                except GithubException:
                    raise Exception("Unknown GitHub organization/user \"%s\"" %
                                    str(ghutil.organization))
                try:
                    repo = org.get_repo(github_project)
                except GithubException:
                    raise Exception("Unknown GitHub repository \"%s\" for %s" %
                                    (github_project, org.name))

            # clone the GitHub project
            git_clone(repo.ssh_url, debug=debug, verbose=verbose)

            # move into the repo sandbox directory
            os.chdir(github_project)

            # add all issues
            mantis_issues.add_issues(repo, report_progress=__progress_reporter)
        finally:
            read_input("%s %% Hit Return to finish: " % os.getcwd())


def main():
    parser = argparse.ArgumentParser()
    add_arguments(parser)
    args = parser.parse_args()

    if args.organization is not None:
        organization = args.organization
    else:
        organization = getpass.getuser()

    # open GitHub connection
    token = read_github_token("%s/.github_token" % os.environ["HOME"])
    github = Github(token)

    # build a GitHub object for the organization
    ghutil = GithubUtil(organization, args.github_project)
    ghutil.destroy_existing_repo = args.destroy_old
    ghutil.make_new_repo_public = True
    ghutil.sleep_seconds = args.sleep_seconds

    if args.mantis_project is not None:
        mantis_project = args.mantis_project
    else:
        mantis_project = args.github_project

    print("Loading Mantis issues for %s" % mantis_project)
    mantis_issues = MantisConverter(args.mantis_dump, None, (mantis_project, ),
                                    verbose=args.verbose)
    mantis_issues.close_resolved = args.close_resolved
    mantis_issues.preserve_all_status = args.preserve_all_status
    mantis_issues.preserve_resolved_status = args.preserve_resolved_status

    if len(mantis_issues) == 0:
        raise SystemExit("No issues found for %s" % mantis_project)

    print("Uploading %d %s issues" % (len(mantis_issues), args.github_project))
    description = "Test repo for %s issues" % str(args.github_project)
    move_issues(mantis_issues, github, ghutil, args.github_project,
                description, debug=args.debug, verbose=args.verbose)


if __name__ == "__main__":
    main()
