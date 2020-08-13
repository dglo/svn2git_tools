#!/usr/bin/env python

from __future__ import print_function

import os
import re
import sys

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

from cmdrunner import CommandException
from svn import SVNMetadata, svn_log
from svndb import MetadataManager, SVNEntry, SVNRepositoryDB


class SVNNotFoundError(CommandException):
    "Error thrown when a Subversion repository does not exist"


class SVNLogEntries(object):
    "Container for all Subversion log entries for a single project"

    def __init__(self, svnprj, ignore_tag=None, debug=False, verbose=False):
        self.__metadata = svnprj.metadata

        self.__revision_log = {}

        for dirtype, dirname, url in svnprj.all_urls(ignore=ignore_tag):
            self.__load_log(svnprj, url, dirname, debug=debug, verbose=verbose)
        if verbose:
            print("Loaded %d entries from %s" % (svnprj.database.num_entries,
                                                 svnprj.name))

    def __load_log(self, svnprj, rel_url, rel_name, debug=False,
                   verbose=False):
        """
        Add all Subversion log entries for a trunk, branch, or tag
        to this object's internal cache
        """

        if verbose:
            print("Loading log entries from %s(%s)" % (rel_name, rel_url))

        prev = None
        for log_entry in svn_log(rel_url, revision="HEAD", end_revision=1,
                                 debug=debug):
            existing = self.get_entry(log_entry.revision)
            if existing is not None:
                existing.check_duplicate(log_entry)
                entry = existing
            else:
                entry = self.add_entry(rel_name, log_entry)

            if prev is not None:
                prev.set_previous(entry)
            prev = entry

            if existing is not None:
                # if we're on a branch and we've reached the trunk, we're done
                break

        if verbose:
            print("After %s, revision log contains %d entries" %
                  (rel_name, len(self.__revision_log)))

    def add_entry(self, rel_name, log_entry):
        "Add a Subversion log entry"
        entry = SVNEntry(self.__metadata, rel_name,
                         self.__metadata.branch_name, log_entry.revision,
                         log_entry.author, log_entry.date_string,
                         log_entry.num_lines, log_entry.filedata,
                         log_entry.loglines)
        key = self.make_key(entry.revision)
        if key in self.__revision_log:
            raise Exception("Cannot overwrite <%s>%s with <%s>%s" %
                            (type(self.__revision_log[key]),
                             self.__revision_log[key], type(entry), entry))
        self.__revision_log[key] = entry
        return entry

    @property
    def entries(self):
        """
        Iterate through the list of all Subversion log entries for this
        project, sorted by date
        """
        for entry in sorted(self.__revision_log.values(),
                            key=lambda x: x.date_string):
            yield entry

    @property
    def entry_pairs(self):
        """
        Iterate through the list of all Subversion log entries for this
        project (sorted by date), yielding tuples containing (key, entry)
        """
        for pair in sorted(self.__revision_log.items(),
                           key=lambda x: x[1].date_string):
            yield pair

    def get_entry(self, revision):
        "Return the entry for this revision, or None if none exists"
        key = self.make_key(revision)
        if key not in self.__revision_log:
            return None
        return self.__revision_log[key]

    def make_key(self, revision):
        "Make a key for this object"
        return "%s#%d" % (self.__metadata.root_url, revision)


class SVNProject(object):
    "Container for all things related to this Subversion project"

    def __init__(self, project_name, url, mantis_projects):
        "Create this object"

        # get information about this SVN repository
        try:
            metadata = SVNMetadata(url)
        except CommandException as cex:
            if str(cex).find("W170000") >= 0:
                raise SVNNotFoundError("SVN repository \"%s\" does not exist" %
                                       (url, ))
            raise

        self.__metadata = MetadataManager.get(metadata)
        if self.__metadata.project_name != project_name:
            print("WARNING: Expected %s, not %s" %
                  (project_name, self.__metadata.project))
        if self.__metadata.project_url != url:
            print("WARNING: Expected %s, not %s" %
                  (url, metadata.project_url))

        self.__mantis_projects = mantis_projects

        self.__database = None
        self.__log_entries = None

    def __str__(self):
        "Return a debugging string describing this object"
        return "%s -> %s" % (self.__metadata, self.__mantis_projects)

    def all_urls(self, ignore=None):
        """
        Iterate through this project's Subversion repository URLs
        (trunk, branches, and tags) and return tuples containing
        (dirtype, dirname, url)
        Note: dirtype is the SVNMetadata directory type: DIRTYPE_TRUNK,
        DIRTYPE_BRANCHES, or DIRTYPE_TAGS
        """
        return self.__metadata.all_urls(ignore=ignore)

    @property
    def base_url(self):
        "Return the base URL for this project's Subversion repository"
        return self.__metadata.base_url

    @property
    def database(self):
        "Return the SVNRepositoryDB object for this project"
        if self.__database is None:
            self.__database = PDAQManager.get_database(self.__metadata)
        return self.__database

    def load_logs(self, ignore_tag=None, debug=False, verbose=False):
        if self.__log_entries is not None:
            print("WARNING: Subversion log entries for %s have already"
                  " been loaded" % (self.name, ), file=sys.stderr)
            return

        if verbose:
            print("Loading entries from %s DB" % (self.name, ))
        if self.database.num_entries == 0:
            raise SystemExit("No data found for %s" % (self.name, ))

        self.__log_entries = SVNLogEntries(self, ignore_tag=ignore_tag,
                                           debug=debug, verbose=verbose)

        if verbose:
            print("Loaded %s from %s" % (self.name, self.__database.path))

    @property
    def log(self):
        "Return the SVNLogEntries object this project"
        if self.__log_entries is None:
            raise Exception("Log entries have not been loaded")

        return self.__log_entries

    @property
    def mantis_projects(self):
        "Return the list of Mantis categories associated with this project"
        return self.__mantis_projects

    @property
    def metadata(self):
        """
        Return this project's Subversion metadata
        (trunk/tags/branches URLS, etc.)
        """
        return self.__metadata

    @property
    def name(self):
        "Return this project's name"
        return self.__metadata.project_name

    @property
    def trunk_url(self):
        "Return the URL for this project's Subversion trunk"
        return self.__metadata.trunk_url

    @property
    def url(self):
        "Return the URL for this project's Subversion repository"
        return self.__metadata.project_url


class PDAQManager(object):
    "Manage all pDAQ SVN data"

    SVN_SERVER_PREFIX = "http://code.icecube.wisc.edu"
    ALL_MANTIS = ["pDAQ", "dash", "pdaq-config", "pdaq-user"]

    PROJECT_NAMES = ("PyDOM", "cluster-config", "config", "config-scripts",
                     "daq-common", "daq-integration-test", "daq-io", "daq-log",
                     "daq-moni-tool", "daq-pom-config", "daq-request-filler",
                     "dash", "eventBuilder-prod", "fabric-common", "icebucket",
                     "juggler", "payload", "payload-generator", "pdaq-user",
                     "secondaryBuilders", "splicer", "StringHub", "oldtrigger",
                     "trigger", "trigger-common", "trigger-testbed")

    __AUTHORS = {}
    __DATABASES = {}
    __PROJECTS = {}

    __AUTHORS_FILENAME = None

    def __init__(self):
        pass

    @classmethod
    def __exit_if_unknown_authors(cls, database):
        if cls.__AUTHORS_FILENAME is None:
            raise Exception("Please load Subversion authors before using"
                            " any databases")

        seen = {}
        unknown = False
        for entry in database.all_entries:
            # no need to check authors we've seen before
            if entry.author in seen:
                continue
            seen[entry.author] = True

            if entry.author not in cls.__AUTHORS:
                print("SVN committer \"%s\" missing from \"%s\"" %
                      (entry.author, cls.__AUTHORS_FILENAME), file=sys.stderr)
                unknown = True
        if unknown:
            raise SystemExit("Please add missing author(s) before continuing")

    @classmethod
    def get(cls, name_or_url):
        """
        Return the object which captures all information about the requested
        Subversion project
        """
        if name_or_url is None or name_or_url == "pdaq":
            url = cls.SVN_SERVER_PREFIX + "/daq/meta-projects/pdaq"
            svn_project = "pdaq"
            mantis_projects = cls.ALL_MANTIS
        elif name_or_url.find("/") < 0:
            if name_or_url == "fabric-common":
                prefix = "svn"
            else:
                prefix = "daq"
            url = os.path.join(cls.SVN_SERVER_PREFIX, prefix, "projects",
                               name_or_url)
            svn_project = name_or_url
            mantis_projects = (name_or_url, )
        else:
            upieces = urlparse.urlparse(name_or_url)
            upath = upieces.path.split(os.sep)

            if upath[-1] == "trunk":
                del upath[-1]
            lastpath = upath[-1]

            url = urlparse.urlunparse((upieces.scheme, upieces.netloc,
                                       os.sep.join(upath), upieces.params,
                                       upieces.query, upieces.fragment))
            svn_project = lastpath
            mantis_projects = (lastpath, )

        if svn_project not in cls.__PROJECTS:
            try:
                cls.__PROJECTS[svn_project] = SVNProject(svn_project, url,
                                                         mantis_projects)
            except SVNNotFoundError:
                return None

        return cls.__PROJECTS[svn_project]

    @classmethod
    def get_author(cls, username):
        """
        Return the Git author associated with this Subversion username
        """
        if cls.__AUTHORS_FILENAME is None:
            raise Exception("Authors have not been loaded")

        if username not in cls.__AUTHORS:
            raise Exception("No author found for Subversion username \"%s\"" %
                            (username, ))

        return cls.__AUTHORS[username]

    @classmethod
    def get_database(cls, metadata):
        """
        Return the repository database which has been loaded from the metadata
        """
        if metadata.project_name not in cls.__DATABASES:
            database = SVNRepositoryDB(metadata)
            cls.__exit_if_unknown_authors(database)
            cls.__DATABASES[metadata.project_name] = database

        return cls.__DATABASES[metadata.project_name]

    @classmethod
    def load_authors(cls, filename, verbose=False):
        """
        Load file which maps Subversion usernames to Git authors
        (e.g. `someuser: Some User <someuser@example.com>`)
        """
        if cls.__AUTHORS_FILENAME is None:

            if verbose:
                print("Checking for unknown SVN committers in \"%s\"" %
                      (filename, ))

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

            cls.__AUTHORS_FILENAME = filename
            cls.__AUTHORS = authors


def main():
    "Main method"
    for proj in PDAQManager.PROJECT_NAMES:
        _ = PDAQManager.get(proj)
        # print("%s :: %s" % (proj, svnprj))


if __name__ == "__main__":
    main()
