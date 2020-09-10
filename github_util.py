#!/usr/bin/env python3

from __future__ import print_function

import getpass
import os
import time

from datetime import datetime

from github import Github, GithubException, GithubObject


class GHOptException(Exception):
    "General Git exception"


class MeteredIssue(object):
    def __init__(self, metered_repo, issue):
        self.__metered_repo = metered_repo
        self.__issue = issue

    def create_comment(self, body):
        self.__metered_repo.check_limit()

        return self.__issue.create_comment(body)

    def edit(self, title=GithubObject.NotSet, body=GithubObject.NotSet,
             assignee=GithubObject.NotSet, state=GithubObject.NotSet,
             milestone=GithubObject.NotSet, labels=GithubObject.NotSet,
             assignees=GithubObject.NotSet):
        self.__metered_repo.check_limit()

        self.__issue.edit(title=title, body=body, assignee=assignee,
                          state=state, milestone=milestone, labels=labels,
                          assignees=assignees)

    @property
    def number(self):
        return self.__issue.number


class MeteredRepo(object):
    """
    A metered version of the Github Repository object which monitors
    the GitHub limit and either pauses or aborts once it's reached
    """

    MAX_REMAINING = 5

    def __init__(self, github, repo, sleep_seconds=1, abort_at_limit=False,
                 debug=False, verbose=False):
        self.__github = github
        self.__repo = repo
        self.__sleep_seconds = sleep_seconds
        self.__abort_at_limit = abort_at_limit
        self.__debug = debug
        self.__verbose = verbose

    def check_limit(self, skip_sleep=False):
        """
        If the Github limit is reached, either abort or pause until it is reset
        """
        if not skip_sleep:
            # sleep a bit to avoid GitHub's spam hammer
            if self.__verbose:
                print("Sleeping ...", end="")
            start_time = datetime.now()
            time.sleep(self.__sleep_seconds)
            if self.__verbose:
                print(" slept for %s" % (datetime.now() - start_time))

        remaining, limit = self.__github.rate_limiting
        if self.__debug:
            print("[GitHub limit: %d of %d]" % (remaining, limit))

        if remaining >= self.MAX_REMAINING:
            # we're good, keep going
            return

        secs = self.__reset_seconds
        if remaining == 0 and self.__abort_at_limit:
            raise Exception("Limit reached, reset in %d seconds" % secs)

        print("Limit reached, pausing for %d seconds" % secs)
        time.sleep(secs + 1)

        # recheck the limit
        self.check_limit(skip_sleep=True)

    def __reset_seconds(self):
        "Return the number of seconds remaining before the limit is reset"
        return int(time.time()) - self.__github.rate_limiting_resettime

    def create_issue(self, title, body=GithubObject.NotSet,
                     assignee=GithubObject.NotSet,
                     milestone=GithubObject.NotSet, labels=GithubObject.NotSet,
                     assignees=GithubObject.NotSet):
        self.check_limit()
        issue = self.__repo.create_issue(title, body=body, assignee=assignee,
                                         milestone=milestone, labels=labels,
                                         assignees=assignees)
        return MeteredIssue(self, issue)

    def create_label(self, name, color, description=GithubObject.NotSet):
        self.check_limit()
        return self.__repo.create_label(name, color, description)

    def create_milestone(self, title, state=GithubObject.NotSet,
                         description=GithubObject.NotSet,
                         due_on=GithubObject.NotSet):
        self.check_limit()
        return self.__repo.create_milestone(title, state=state,
                                            description=description,
                                            due_on=due_on)

    def get_labels(self):
        self.check_limit()
        return self.__repo.get_labels()

    def get_milestones(self):
        self.check_limit()
        return self.__repo.get_milestones()

    @property
    def has_issue_tracker(self):
        return True

    @property
    def ssh_url(self):
        return self.__repo.ssh_url


class LocalRepository(object):
    def __init__(self, local_path):
        self.__path = os.path.abspath(local_path)

    @property
    def has_issue_tracker(self):
        return False

    @property
    def ssh_url(self):
        return "file://%s" % str(self.__path)


class GithubUtil(object):
    def __init__(self, organization, repository, token_path=None):
        if organization is None:
            raise GHOptException("Please specify a GitHub user/organization")
        if repository is None:
            raise GHOptException("Please specify a GitHub repository")

        self.__organization = organization
        self.__repository = repository

        self.__destroy_existing_repo = False
        self.__make_public = False
        self.__sleep_seconds = 1

        self.__github = self.__open_connection(token_path)

    @classmethod
    def __open_connection(cls, token_path=None):
        if token_path is not None:
            filename = token_path
        else:
            filename = "%s/.github_token" % os.environ["HOME"]

        token = None
        with open(filename, "r") as fin:
            for line in fin:
                line = line.strip()
                if line.startswith("#"):
                    continue

                if line == "":
                    continue

                token = line
                break

        if token is None:
            raise GHOptException("Cannot find GitHub token in \"%s\"" %
                                 str(filename))

        return Github(token)

    def build_github_repo(self, description=None, debug=False, verbose=False):
        "Return a Github Repository object, creating it on Github if necessary"

        try:
            org = self.get_organization_or_user(self.__github)
        except GithubException:
            raise GHOptException("Unknown GitHub organization/user \"%s\"" %
                                 str(self.__organization))

        if self.__destroy_existing_repo:
            try:
                repo = org.get_repo(self.__repository)
            except GithubException:
                repo = None

            if repo is not None:
                repo.delete()
                if verbose:
                    print("Deleted existing \"%s/%s\" repository" %
                          (self.__organization, self.__repository, ))

        repo = org.create_repo(self.__repository, description=description,
                               has_issues=True, private=not self.__make_public)

        return MeteredRepo(self.__github, repo,
                           sleep_seconds=self.__sleep_seconds, debug=debug,
                           verbose=verbose)

    def get_organization_or_user(self, github):
        """
        Return either this user's AuthenticatedUser object or
        an Organization object
        """
        if self.__organization == getpass.getuser():
            return github.get_user()

        try:
            return github.get_organization(self.__organization)
        except GithubException:
            raise GHOptException("Bad organization \"%s\"" %
                                 str(self.__organization))

    @property
    def destroy_existing_repo(self):
        "Return True if the existing repo will be destroyed and recreated"
        return self.__destroy_existing_repo

    @destroy_existing_repo.setter
    def destroy_existing_repo(self, value):
        """
        Set the boolean value determining if the existing repo will be
        destroyed and recreated
        """
        self.__destroy_existing_repo = value

    @property
    def make_new_repo_public(self):
        "Return True if a created repository is made visible to the public"
        return self.__make_public

    @make_new_repo_public.setter
    def make_new_repo_public(self, value):
        """
        Set the boolean value determining if a created repository is made
        visible to the public
        """
        self.__make_public = value

    @property
    def organization(self):
        """
        Return the name of the organization used to fetch or create the
        repository
        """
        return self.__organization

    @property
    def repository(self):
        "Return the repository name"
        return self.__repository

    @property
    def sleep_seconds(self):
        "Return the number of seconds to sleep between GitHub issue operations"
        return self.__sleep_seconds

    @sleep_seconds.setter
    def sleep_seconds(self, value):
        self.__sleep_seconds = int(value)
