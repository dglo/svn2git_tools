#!/usr/bin/env python

from __future__ import print_function

import os
import sys
import time
import traceback

from github import GithubException, GithubObject
from issue_finder import CommitFinder, IssueFinder
from mantisdump import MantisDump, MantisSchema

# Python3 redefined 'unicode' to be 'str'
if sys.version_info[0] >= 3:
    unicode = str


def pluralize(number):
    "Trivial method to return an 's' if 'number' is not equal to 1"
    return "" if number == 1 else "s"


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
        # GitHub or local repo
        if gitrepo is None:
            raise Exception("Please specify a Git repo object")
        self.__gitrepo = gitrepo

        if mantis_dump is None:
            raise Exception("Please specify the Mantis dump file")
        if not os.path.exists(mantis_dump):
            raise Exception("Mantis dump file \"%s\" does not exist" %
                            (mantis_dump, ))

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

        # list of missing issues
        self.__missing = []

        # get the list of all Mantis issues
        self.__all_issues = self.load_issues(mantis_dump, verbose=False)

        # get the list of Mantis issues referenced in SVN log messages
        #  and SVN commits referenced in Mantis notes
        if svndb is None:
            self.__svn_issues = None
        else:
            self.__svn_issues = self.find_svn_issues(svndb)

        # ordered list of Mantis issues for this project
        self.__project_issue_numbers = \
          self.__create_issue_order(verbose=verbose)
        if verbose:
            if self.__project_names is None:
                pstr = "all projects"
            else:
                pstr = ", ".join(self.__project_names)
            num_issues = len(self.__project_issue_numbers)
            print("Found %d issue%s (out of %d total) for %s" %
                  (num_issues, pluralize(num_issues),
                   len(self.__all_issues), pstr))

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
                              " to None" % (rev, ), file=sys.stderr)
                    else:
                        references[inum] = 1
            if verbose:
                print("Found %d referenced Mantis issues" % len(references))

        # find Mantis issues for the specified project(s)
        for issue in self.__all_issues.values():
            if self.__project_names is None or \
              issue.project in self.__project_names:
                if issue.id is None:
                    print("ERROR: Found ID set to None in issue %s" %
                          (issue, ), file=sys.stderr)
                else:
                    references[issue.id] = 0
        if verbose:
            num_refs = len(references)
            print("Found %d total Mantis issue%s" %
                  (num_refs, pluralize(num_refs)))

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
            if text is not None and isinstance(text, bytes):
                text = text.decode("utf-8", "ignore")
            if text is not None and text != "":
                if title is None:
                    title_prefix = "[%s on %s] " % \
                      (issue.reporter, issue.date_submitted)
                    title = text
                else:
                    message = "\n" + text

        if title is None:
            print("WARNING: No summary/description for issue #%d" %
                  (issue.id, ), file=sys.stderr)
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
    def __mantis_note_to_string(cls, note, database=None):
        text = "[%s on %s]" % (note.reporter, note.last_modified)
        if not isinstance(note.text, bytes):
            fixed = note.text
        else:
            fixed = note.text.decode("utf-8", "ignore")

        # if there's no SVN commit database,
        #  we can't map SVN revisions to Git hashs
        if database is None:
            return text + "\n" + fixed

        # add Git hashes to all notes which refer to SVN revision numbers
        for line in fixed.split("\n"):
            svn_rev = CommitFinder.find_in_text(line)

            # if we find an SVN revision, link it to the Git hash
            cstr = "\n"
            if svn_rev is not None:
                result = database.find_hash_from_revision(revision=svn_rev)
                if result is not None and result[1] is not None:
                    cstr = "\n[commit %s] " % (result[1], )

            # add this line to the note
            text += cstr + line

        return text

    def __open_issue(self, issue, database=None):
        "Open a GitHub issue which copies the Mantis issue"
        if self.__project_names is None or \
          issue.project in self.__project_names:
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

        retries = 6
        sleep_secs = 60
        while retries > 0:
            try:
                gh_issue = self.__gitrepo.create_issue(title, message,
                                                       milestone=milestone,
                                                       labels=labels)
                break
            except GithubException as gex:
                if gex.status != 403:
                    raise

                retries -= 1
                if retries <= 0:
                    print("WARNING: Failed to open issue for Mantis #%s" %
                          (issue.id, ), file=sys.stderr)
                    return None

                print("\r%s\rMantis #%s sleeping for %s seconds ." %
                      (" "*60, issue.id, sleep_secs), end="")
                sys.stdout.flush()
                time.sleep(sleep_secs)
                sleep_secs += 60
                print(".. retrying \"%s\"\r" % (title, ))
                sys.stdout.flush()
                continue

        for note in issue.notes:
            message = self.__mantis_note_to_string(note, database=database)
            try:
                gh_issue.create_comment(message)
            except GithubException:
                raise Exception("Cannot create GitHub issue #%d"
                                " comment \"%s\"" % (gh_issue.number, message))

        self.__mantis2github[issue.id] = gh_issue

        return gh_issue

    def add_issues(self, mantis_id=None, add_after=False, database=None,
                   pause_count=None, pause_seconds=None, report_progress=None,
                   verbose=False):
        """
        If 'mantis_id' is None, add all issues
        If `add_after` is False, add issues with IDs less than `mantis_id`.
        If `add_after` is True, add issues with IDs greater than `mantis_id`.
        """
        issues = []
        for inum in self.__project_issue_numbers:
            if mantis_id is not None:
                if not add_after and inum >= mantis_id:
                    # we've added all issues before 'mantis_id', exit the loop
                    break
                if add_after and inum <= mantis_id:
                    # we haven't started adding yet, keep looking
                    continue

            if inum in self.__mantis2github:
                # issue has been added, continue looking
                continue

            if inum not in self.__all_issues:
                if inum not in self.__missing:
                    # add to list of missing issues and complain
                    self.__missing.append(inum)

                    if mantis_id is None:
                        extra = ""
                    else:
                        extra = " (before adding #%s)" % (mantis_id, )
                    print("ERROR: Cannot add missing issue #%s%s" %
                          (inum, extra), file=sys.stderr)

                continue

            issues.append(self.__all_issues[inum])

        if verbose:
            print("\nOpening %d issues%s" %
                  (len(issues), "" if mantis_id is None
                   else " preceeding Mantis #%s" % (mantis_id, )))
        else:
            # start the Mantis progress on a new line
            print()

        # attempt to create all the preceding issues
        for count, issue in enumerate(issues):
            if report_progress is not None:
                report_progress(count, len(issues), "Mantis", "issue",
                                issue.id)

            try:
                gh_issue = self.__open_issue(issue, database=database)

                if gh_issue is not None:
                    if issue.is_closed or (self.__close_resolved and
                                           issue.is_resolved):
                        gh_issue.edit(body="No associated GitHub commit",
                                      state="closed")
            except KeyboardInterrupt:
                raise
            except:
                print("Failed to open & close issue #%s (%d of %d)" %
                      (issue.id, count, len(issues)), file=sys.stderr)
                traceback.print_exc()
                gh_issue = None

            # if requested, pause a bit after adding the number of issues
            #  specified by 'pause_count'
            if pause_count is not None and pause_seconds is not None and \
              count > 0 and count % pause_count == 0:
                time.sleep(pause_seconds)

    def add_project(self, project_name):
        if self.__project_names is None:
            self.__project_names = []
        self.__project_names.append(project_name)

    def add_projects(self, namelist):
        for project_name in namelist:
            self.add_project(project_name)

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

        num_found = len(svn_issues)
        num_total = svndb.num_entries()
        print("Found %d issue%s from %d total issue%s" %
              (num_found, pluralize(num_found), num_total,
               pluralize(num_total)))
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

    @classmethod
    def load_data_from_dump(cls, dump_path, verbose=False):
        data = None
        aux_tbls = []
        all_tbls = MantisSchema.ALL_TABLES
        for table in MantisDump.read_mysqldump(dump_path,
                                               include_list=all_tbls):
            if verbose:
                print("-- found %s" % (table.name, ))
            if table.name == MantisSchema.MAIN:
                data = table.data_table
            elif table.data_table is not None:
                aux_tbls.append(table)

        for tbl in aux_tbls:
            data.add_table(tbl.name, tbl.data_table)

        return data

    @classmethod
    def load_issues(cls, dump_path, project_names=None, verbose=False):
        """
        Read in a Mantis SQL dump file and return a dictionary mapping issue
        number to a MantisIssue object.  If project_name is specified, return
        issues for that project.  Otherwise, return all issues.
        """

        data = cls.load_data_from_dump(dump_path, verbose=verbose)

        issues = {}
        for issue in data.rows:
            if project_names is not None and \
              issue.project not in project_names:
                continue

            if issue.id is None:
                print("ERROR: Found ID set to None in issue %s" % (issue, ),
                      file=sys.stderr)
            else:
                issues[issue.id] = issue

        return issues

    def open_github_issues(self, svn_revision, database=None, pause_count=None,
                           pause_seconds=None, report_progress=None):
        if self.__svn_issues is None or svn_revision not in self.__svn_issues:
            return None

        new_issues = None
        for inum in self.__svn_issues[svn_revision]:
            if inum not in self.__all_issues:
                print("WARNING: Unknown Mantis #%d in SVN r%d" %
                      (inum, svn_revision), file=sys.stderr)
                continue

            if inum not in self.__mantis2github:
                # add and close all issues before this one
                self.add_issues(inum, database=database,
                                pause_count=pause_count,
                                pause_seconds=pause_seconds,
                                report_progress=report_progress)
                # open this issue
                try:
                    gh_issue = self.__open_issue(self.__all_issues[inum],
                                                 database=database)
                except KeyboardInterrupt:
                    raise
                except:
                    print("Failed to open new issue #%s" % (inum, ),
                          file=sys.stderr)
                    traceback.print_exc()
                    gh_issue = None
            else:
                # reopen previously closed issue
                gh_issue = self.__mantis2github[inum]
                body = "Reopening for SVN rev %d" % svn_revision
                gh_issue.edit(body=body, state="open")

            # save the GitHub issue ID
            if gh_issue is not None:
                if new_issues is None:
                    new_issues = [gh_issue, ]
                else:
                    new_issues.append(gh_issue)

        return new_issues

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

def main():
    project_name = None
    filename = None

    grab_project = False
    bad_arg = False
    for arg in sys.argv[1:]:
        if grab_project:
            project_name = arg
            grab_project = False
        elif arg == "-p":
            grab_project = True
        elif filename is None:
            filename = arg
        else:
            print("Unknown argument \"%s\"" % (arg, ), file=sys.stderr)
            bad_arg = True
    if bad_arg:
        raise SystemExit(1)
    if filename is None:
        filename = "mantis_db.sql.gz"

    mcvt = MantisConverter(filename, None, "GitRepo")

    projects = {}
    for issue in mcvt.issues:
        if issue.project not in projects:
            projects[issue.project] = []
        projects[issue.project].append(issue)

    for key, entrylist in sorted(projects.items(), key=lambda x: x[0]):
        if project_name is None:
            print("=== %s" % (key, ))
        elif key != project_name:
            continue

        for entry in sorted(entrylist, key=lambda x: x.id):
            text = entry.description
            idx = text.find("\n")
            if idx >= 0:
                text = text[:idx]
            if len(text) > 40:
                text = text[:40]
            try:
                print("Issue %s: %s" % (entry.id, text, ))
            except UnicodeEncodeError:
                print("Could not decode issue #%d description" %
                      (entry.id, ), file=sys.stderr)
                raise


if __name__ == "__main__":
    main()
