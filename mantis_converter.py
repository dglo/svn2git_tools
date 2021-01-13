#!/usr/bin/env python

from __future__ import print_function

import sys

from github import GithubException, GithubObject
from issue_finder import IssueFinder
from mantisdump import MantisDump, MantisSchema


class MantisConverter(object):
    LABEL_COLORS = {
        "new": "fcbdbd",
        "feedback": "e3b7eb",
        "acknowledged": "ffcd85",
        "confirmed": "fff494",
        "assigned": "c2dfff",
        "resolved": "d2f5b0",
    }

    def __init__(self, mantis_dump, svndb, gitrepo, project_names=None,
                 verbose=False):
        if project_names is None or len(project_names) == 0:
            raise Exception("Please specify one or more Mantis project names")

        # GitHub or local repo
        if gitrepo is None:
            raise Exception("Please specify a Git repo object")
        self.__gitrepo = gitrepo

        # list of Mantis projects associated with the Subversion project
        self.__project_names = project_names
        self.__close_resolved = False
        self.__preserve_all_status = False
        self.__preserve_resolved = False

        # cached lists of GitHub issue labels and milestones
        #  (loaded when needed)
        self.__labels = None
        self.__milestones = None

        # dictionary mapping Mantis issue numbers to GitHub issue numbers
        self.__mantis2github = {}

        # get the list of all Mantis issues
        self.__all_issues = self.load_issues(mantis_dump)

        # get the list of Mantis issues referenced in SVN log messages
        if svndb is None:
            self.__svn_issues = None
        else:
            self.__svn_issues = self.find_svn_issues(svndb)

        # ordered list of Mantis issues for this project
        self.__project_issue_numbers = \
          self.__create_issue_order(verbose=verbose)
        if verbose:
            print("Found %d issues (out of %d total) for %s" %
                  (len(self.__project_issue_numbers), len(self.__all_issues),
                   ", ".join(self.__project_names)))

    def __len__(self):
        return len(self.__project_issue_numbers)

    def __add_github_label(self, label_name):
        if self.__labels is None:
            tmplist = {}
            for label in self.__gitrepo.get_labels():
                tmplist[label.name] = label
            self.__labels = tmplist

        if label_name in self.__labels:
            return self.__labels[label_name]

        if label_name not in self.LABEL_COLORS:
            raise Exception("No color found for issue status \"%s\"" %
                            label_name)

        color = self.LABEL_COLORS[label_name]
        description = "Mantis status %s" % label_name
        try:
            label = self.__gitrepo.create_label(label_name, color, description)
        except GithubException:
            raise Exception("Cannot create label %s color %s (%s)" %
                            (label_name, color, description))

        self.__labels[label.name] = label
        return label

    def __add_github_milestone(self, milestone_name):
        if self.__milestones is None:
            tmplist = {}
            for milestone in self.__gitrepo.get_milestones():
                tmplist[milestone.title] = milestone
            self.__milestones = tmplist

        if milestone_name in self.__milestones:
            return self.__milestones[milestone_name]

        description = milestone_name
        try:
            milestone = self.__gitrepo.create_milestone(milestone_name,
                                                        state="open",
                                                        description=description)
        except GithubException:
            raise Exception("Cannot create milestone %s (%s)" %
                            (milestone_name, description))

        self.__milestones[milestone.title] = milestone
        return milestone

    def __create_issue_order(self, verbose=False):
        """
        Create an ordered list of Mantis issue numbers for this project
        """
        # find Mantis issues referenced in SVN commits
        references = {}
        if self.__svn_issues is not None:
            for rev, numlist in self.__svn_issues.items():
                for inum in numlist:
                    if inum is None:
                        print("ERROR: Ignoring rev%d issue number set"
                              " to None" % (rev, ))
                    else:
                        references[inum] = 1
            if verbose:
                print("Found %d referenced Mantis issues" % len(references))

        # find Mantis issues for the specified project(s)
        for issue in self.__all_issues.values():
            if issue.project in self.__project_names:
                if issue.id is None:
                    print("ERROR: Found ID set to None in issue %s" %
                          str(issue))
                else:
                    references[issue.id] = 0
        if verbose:
            print("Found %d total Mantis issues" % len(references))

        mantis_ids = list(references.keys())
        mantis_ids.sort()

        return mantis_ids

    @classmethod
    def __mantis_issue_to_strings(cls, issue, foreign_project=None):
        """
        Convert a Mantis issue to a title string and a body string for GitHub
        """

        title_prefix = None
        title = None
        message = None

        for text in (issue.summary, issue.description):
            if text is not None and text != "":
                if title is None:
                    title_prefix = "[%s on %s] " % \
                      (issue.reporter, issue.date_submitted)
                    title = text
                else:
                    message = "\n%s" % str(text)

        if title is None:
            print("WARNING: No summary/description for issue #%d" % issue.id)
            title = "Mantis issue %d" % issue.id

        if foreign_project is not None:
            title = "%s: %s" % (foreign_project, title)

        if title_prefix is not None:
            title = "%s: %s" % (title_prefix, title)

        for fld, text in (("Steps to Reproduce", issue.steps_to_reproduce),
                          ("Additional Information",
                           issue.additional_information)):
            if text is not None and text != "":
                if message is None:
                    message = "%s: %s" % (fld, text)
                else:
                    message += "\n\n%s: %s" % (fld, text)

        return title, message

    @classmethod
    def __mantis_note_to_string(cls, note):
        return "[%s on %s]\n%s" % (note.reporter, note.last_modified,
                                   note.text)

    def __open_issue(self, issue):
        "Open a GitHub issue which copies the Mantis issue"
        if issue.project in self.__project_names:
            foreign_project = None
        else:
            foreign_project = issue.project

        labels = GithubObject.NotSet
        if not issue.is_closed:
            if self.__preserve_all_status or \
              (self.__preserve_resolved and issue.is_resolved):
                label_name = issue.status
                label = self.__add_github_label(label_name)
                labels = (label, )

        ms_name = None
        if issue.fixed_in_version != "":
            ms_name = issue.fixed_in_version
        elif issue.target_version != "":
            ms_name = issue.target_version

        if ms_name is None:
            milestone = GithubObject.NotSet
        else:
            milestone = self.__add_github_milestone(ms_name)

        title, message = \
          self.__mantis_issue_to_strings(issue, foreign_project)

        try:
            gh_issue = self.__gitrepo.create_issue(title, message,
                                                   milestone=milestone,
                                                   labels=labels)
        except GithubException as gex:
            if milestone == GithubObject.NotSet:
                mstr = ""
            else:
                mstr = " milestone %s" % milestone.title

            if labels == GithubObject.NotSet:
                lstr = ""
            else:
                lstr = " labels: %s" % ". ".join(x.name for x in labels)

            raise Exception("Cannot create GitHub issue \"%s\":"
                            " \"%s\"%s%s\n%s" %
                            (title, message, mstr, lstr, gex))

        for note in issue.notes:
            message = self.__mantis_note_to_string(note)
            try:
                gh_issue.create_comment(message)
            except GithubException:
                raise Exception("Cannot create GitHub issue #%d"
                                " comment \"%s\"" % (gh_issue.number, message))

        self.__mantis2github[issue.id] = gh_issue

        return gh_issue

    def add_issues(self, mantis_id=None, report_progress=None):
        """
        Add Mantis issues with numbers less than 'mantis_id'.
        If 'mantis_id' is None, add all issues
        """
        issues = []
        for inum in self.__project_issue_numbers:
            if mantis_id is not None and inum >= mantis_id:
                # we've added all issues before 'mantis_id', exit the loop
                break

            if inum in self.__mantis2github:
                # issue has been added, continue looking
                continue

            if inum not in self.__all_issues:
                if mantis_id is None:
                    extra = ""
                else:
                    extra = " (before adding #%s)" % (mantis_id, )
                print("ERROR: Cannot add missing issue #%s%s" % (inum, extra))
                continue

            issues.append(self.__all_issues[inum])

        for count, issue in enumerate(issues):
            if report_progress is not None:
                report_progress(count, len(issues), "Mantis", "issue",
                                issue.id)

            gh_issue = self.__open_issue(issue)

            if issue.is_closed or (self.__close_resolved and
                                   issue.is_resolved):
                gh_issue.edit(body="No associated GitHub commit",
                              state="closed")

    @classmethod
    def close_github_issue(cls, issue, message):
        issue.edit(body=message, state="closed")

    @property
    def close_resolved(self):
        return self.__close_resolved

    @close_resolved.setter
    def close_resolved(self, value):
        self.__close_resolved = value

    @classmethod
    def find_svn_issues(cls, svndb):
        """
        Return a dictionary mapping Subversion revision numbers to a list of
        Mantis issue numbers
        """
        print("Searching for Mantis issues in SVN log messages")
        svn_issues = {}
        for entry in svndb.all_entries:
            numlist = IssueFinder.find(entry.loglines, fix_lines=True)
            if numlist is None or len(numlist) == 0:
                continue

            numlist.sort()
            svn_issues[entry.revision] = numlist

        return svn_issues

    @property
    def git_repo(self):
        return self.__gitrepo

    @property
    def has_issue_tracker(self):
        return self.__gitrepo.has_issue_tracker

    @property
    def issues(self):
        for inum in self.__project_issue_numbers:
            yield self.__all_issues[inum]

    def open_github_issues(self, svn_revision, report_progress=None):
        if self.__svn_issues is None or svn_revision not in self.__svn_issues:
            return None

        new_issues = None
        for inum in self.__svn_issues[svn_revision]:
            if inum not in self.__all_issues:
                print("WARNING: Unknown Mantis #%d in SVN r%d" %
                      (inum, svn_revision), file=sys.stderr)
                continue

            if inum not in self.__mantis2github:
                self.add_issues(inum, report_progress=report_progress)
                gh_issue = self.__open_issue(self.__all_issues[inum])
            else:
                gh_issue = self.__mantis2github[inum]
                body = "Reopening for SVN rev %d" % svn_revision
                gh_issue.edit(body=body, state="open")

            # save the GitHub issue ID
            if new_issues is None:
                new_issues = [gh_issue, ]
            else:
                new_issues.append(gh_issue)

        return new_issues

    @classmethod
    def load_issues(cls, dump_path, project_names=None):
        """
        Read in a Mantis SQL dump file and return a dictionary mapping issue
        number to a MantisIssue object.  If project_name is specified, return
        issues for that project.  Otherwise, return all issues.
        """

        tables = MantisDump.read_mysqldump(dump_path, MantisSchema.ALL_TABLES)

        data = tables[MantisSchema.MAIN].data_table
        for name, tbl in tables.items():
            if tbl.data_table is not None and \
              tbl.name != MantisSchema.MAIN:
                data.add_table(name, tbl.data_table)

        issues = {}

        for issue in data.rows:
            if project_names is not None and \
              issue.project not in project_names:
                continue

            if issue.id is None:
                print("ERROR: Found ID set to None in issue %s" %
                      str(issue))
            else:
                issues[issue.id] = issue

        return issues

    @property
    def preserve_all_status(self):
        return self.__preserve_all_status

    @preserve_all_status.setter
    def preserve_all_status(self, value):
        self.__preserve_all_status = value

    @property
    def preserve_resolved_status(self):
        return self.__preserve_resolved

    @preserve_resolved_status.setter
    def preserve_resolved_status(self, value):
        self.__preserve_resolved = value
