#!/usr/bin/env python3

from __future__ import print_function

import getpass
import os
import shutil
import time

from datetime import datetime
from github import Github, GithubException, GithubObject

from git import git_init


class GithubUtilException(Exception):
    "General GitHub utilities exception"


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

    MAX_REMAINING = 3

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

        # check all limits, set 'throttle' if we've exceeded any
        throttle = False
        all_limits = self.__github.get_rate_limit()
        limit_str = None
        for name, obj in \
          ("core", all_limits.core), \
          ("search", all_limits.search), \
          ("graphql", all_limits.graphql):
            if obj.remaining < self.MAX_REMAINING:
                throttle = True
            elif obj.remaining == 0 and self.__abort_at_limit:
                raise Exception("Limit reached for %s, reset in %s seconds" %
                                (name, self.__reset_seconds, ))


            # if we're debugging, add these limits to the output string
            if self.__debug:
                lstr = "%s(%s of %s)" % (name, obj.remaining, obj.limit)
                if limit_str is None:
                    limit_str = lstr
                else:
                    limit_str += " " + lstr
        if self.__debug:
            print("[GitHub limits: %s%s]" %
                  (limit_str, "  !!THROTTLED!!" if throttle else ""))

        # if no limits have been hit, we're done
        if not throttle:
            return

        # we've reached a limit, pause for a bit to reset the limit
        secs = self.__reset_seconds
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

    def make_url(self, repo_name):
        base_url, _ = self.__repo.ssh_url.rsplit("/", 1)
        return "%s/%s.git" % (base_url, repo_name)

    @property
    def ssh_url(self):
        return self.__repo.ssh_url


class LocalRepository(object):
    def __init__(self, local_path, repo_name, create_repo=False,
                 destroy_existing=False, debug=False, verbose=False):
        if not repo_name.endswith(".git"):
            repo_name += ".git"

        self.__path = os.path.abspath(os.path.join(local_path, repo_name))
        exists = os.path.exists(self.__path)

        if exists and destroy_existing:
            shutil.rmtree(self.__path)
            exists = False

        if not exists:
            if not create_repo:
                raise GithubUtilException("Repository %s does not exist" %
                                          (self.__path, ))

            # initialize the new repository
            git_init(sandbox_dir=self.__path, bare=True, debug=debug,
                     verbose=verbose)

    @property
    def has_issue_tracker(self):
        return False

    def make_url(self, repo_name):
        base_url, _ = self.__path.rsplit("/", 1)
        return "file://%s/%s.git" % (base_url, repo_name)

    @property
    def ssh_url(self):
        return "file://%s" % (self.__path, )


class ProjectRepo(object):
    def __init__(self, project_name, git_repo=None):
        self.name = project_name
        self.git_repo = git_repo

        self.__branches = {}

    def add_branch(self, branch_name):
        if branch_name in self.__branches:
            raise Exception("Manager cannot add existing Git branch \"%s\"" %
                            (branch_name, ))
        self.__branches[branch_name] = True


    def has_branch(self, branch_name):
        return branch_name in self.__branches


class GitRepoManager(object):
    """
    Manage a set of Git repos
    """
    __GIT_REPO_DICT = {}

    def __init__(self, use_github=False, local_repo_path=None,
                 sleep_seconds=None):
        if not use_github:
            if local_repo_path is None:
                raise Exception("Please specify the local directory where Git"
                                " repositories are stored")
            if os.path.exists(local_repo_path) and \
              not os.path.isdir(local_repo_path):
                raise Exception("Local repo \"%s\" exists and is not"
                                " a directory" % (local_repo_path, ))

        self.__use_github = use_github
        self.__local_repo_path = local_repo_path
        self.__sleep_seconds = sleep_seconds

    def __str__(self):
        return "GitRepoManager[%s,path=%s,sleep=%s]" % \
          ("GitHub" if self.__use_github else "LocalRepo",
           self.__local_repo_path, self.__sleep_seconds)

    @classmethod
    def __add_repo_to_cache(cls, project_name, git_repo):
        if project_name in cls.__GIT_REPO_DICT:
            raise Exception("Found existing cached repo for \"%s\"" %
                            (project_name, ))

        cls.__GIT_REPO_DICT[project_name] = ProjectRepo(project_name, git_repo)

    @classmethod
    def __get_cached_repo(cls, project_name):
        if project_name in cls.__GIT_REPO_DICT:
            prjrepo = cls.__GIT_REPO_DICT[project_name]
            if prjrepo.git_repo is not None:
                return prjrepo.git_repo
        return None

    @classmethod
    def add_branch(cls, project_name, git_branch):
        if project_name not in cls.__GIT_REPO_DICT:
            cls.__GIT_REPO_DICT[project_name] = ProjectRepo(project_name)
        cls.__GIT_REPO_DICT[project_name].add_branch(git_branch)

    @classmethod
    def get_github_util(cls, project_name, organization=None,
                        new_project_name=None, make_public=False,
                        sleep_seconds=None):
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

    def get_repo(self, project_name, organization=None, new_project_name=None,
                 description=None, destroy_old_repo=False, make_public=None,
                 debug=False, verbose=False):
        cached = self.__get_cached_repo(project_name)
        if cached is not None:
            return cached

        # if we're writing to a local repository...
        if not self.__use_github:
            if not os.path.exists(self.__local_repo_path):
                # create the top-level Git repository directory
                os.makedirs(self.__local_repo_path, mode=0o755)

            # create and return the local repository
            repo = GithubUtil.create_local_repo(self.__local_repo_path,
                                                project_name,
                                                destroy_existing=\
                                                destroy_old_repo,
                                                debug=debug, verbose=verbose)
        else:
            # connect to GitHub
            ghutil = self.get_github_util(project_name, organization,
                                          new_project_name,
                                          make_public=make_public,
                                          sleep_seconds=self.__sleep_seconds)

            # if description was not specified, build a default value
            # XXX add a more general solution here
            if description is None:
                description = "WIPAC's %s project" % (project_name, )

            repo = ghutil.get_github_repo(description=description,
                                          create_repo=destroy_old_repo,
                                          destroy_existing=destroy_old_repo,
                                          debug=debug, verbose=verbose)

        # cache the new repo and return
        self.__add_repo_to_cache(project_name, repo)
        return repo

    @classmethod
    def has_branch(cls, project_name, git_branch):
        if project_name in cls.__GIT_REPO_DICT:
            prjrepo = cls.__GIT_REPO_DICT[project_name]
            return prjrepo.has_branch(git_branch)
        return False

    @property
    def is_local(self):
        return not self.__use_github

    @property
    def local_repo_path(self):
        return self.__local_repo_path


class GithubUtil(object):
    def __init__(self, organization, repository, token_path=None):
        if organization is None:
            raise GithubUtilException("Please specify a GitHub"
                                      " user/organization")
        if repository is None:
            raise GithubUtilException("Please specify a GitHub repository")

        self.__organization = organization
        self.__repository = repository

        self.__make_public = False
        self.__sleep_seconds = 1

        self.__github = self.__open_connection(token_path)

    @classmethod
    def __open_connection(cls, token_path=None):
        if token_path is not None:
            filename = token_path
        else:
            filename = "%s/.github_token" % os.environ["HOME"]

        if not os.path.exists(filename):
            raise Exception("Please create a GitHub personal access token"
                            " and save it to %s" % (filename, ))

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
            raise GithubUtilException("Cannot find GitHub token in \"%s\"" %
                                      (filename, ))

        return Github(token)

    def get_github_repo(self, description=None, create_repo=False,
                        destroy_existing=False, debug=False, verbose=False):
        "Return a Github Repository object, creating it on Github if necessary"

        try:
            org = self.get_organization_or_user(self.__github)
        except GithubException:
            raise GithubUtilException("Unknown GitHub organization/user"
                                      " \"%s\"" % (self.__organization, ))

        try:
            repo = org.get_repo(self.__repository)
        except GithubException:
            print("ERROR: Could not get %s repo from GitHub" %
                  (self.__repository, ))
            print("       No issues will be added")
            repo = None

        if destroy_existing and repo is not None:
            repo.delete()
            repo = None

            if verbose:
                print("Deleted existing \"%s/%s\" repository" %
                      (self.__organization, self.__repository, ))

        if create_repo and repo is None:
            if verbose:
                print("Created %s GitHub \"%s/%s\" repository" %
                      ("public" if self.__make_public else "private",
                       self.__organization, self.__repository, ))
            repo = org.create_repo(self.__repository, description=description,
                                   has_issues=True,
                                   private=not self.__make_public)

        return MeteredRepo(self.__github, repo,
                           sleep_seconds=self.__sleep_seconds, debug=debug,
                           verbose=verbose)

    @classmethod
    def create_local_repo(cls, local_path, name, destroy_existing=False,
                          debug=False, verbose=False):
        return LocalRepository(local_path, name, create_repo=True,
                               destroy_existing=destroy_existing, debug=debug,
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
            raise GithubUtilException("Bad organization \"%s\"" %
                                      (self.__organization, ))

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
