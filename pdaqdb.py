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
from svn import SVNMetadata
from svndb import SVNEntry, SVNRepositoryDB


class SVNProject(object):
    def __init__(self, url, mantis_projects=None, debug=False, verbose=False):
        self.__metadata = SVNMetadata(url)
        self.__mantis_projects = mantis_projects

        self.__database = None

    def __str__(self):
        return "SVNProject#%s[%s,%s]" % \
          (len(self.__mantis_projects), self.__metadata, self.__database)

    def add_entry(self, metadata, rel_name, log_entry, save_to_db=False):
        "Add a Subversion log entry"
        entry = SVNEntry(metadata, rel_name, metadata.branch_name,
                         log_entry.revision, log_entry.author,
                         log_entry.date_string, log_entry.num_lines,
                         log_entry.filedata, log_entry.loglines)
        self.database.add_entry(entry, None, save_to_db=save_to_db)
        return entry

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

    def branch_name(self, svn_url):
        # build the base prefix string which is stripped from each file
        svn_file_prefix = self.get_path_prefix(svn_url)

        # ensure this is for our project
        if not svn_file_prefix.startswith(self.name):
            raise CommandException("SVN file prefix \"%s\" does not start with"
                                   " project name \"%s\"" %
                                   (svn_file_prefix, self.name))
        branch_name = svn_file_prefix[len(self.name):]
        if branch_name == "":
            return SVNMetadata.TRUNK_NAME
        if branch_name[0] != "/":
            raise CommandException("SVN branch name \"%s\" (from \"%s\")"
                                   " does not start with project name \"%s\"" %
                                   (branch_name, svn_file_prefix, self.name))

        return branch_name[1:]

    def close_db(self):
        if self.__database is not None:
            PDAQManager.forget_database(self.name)
            self.__database.close()
            self.__database = None

    @property
    def database(self):
        "Return the SVNRepositoryDB object for this project"
        if self.__database is None:
            self.__database = PDAQManager.get_database(self.__metadata,
                                                       allow_create=True)
            if self.__database is None:
                raise Exception("Cannot get database for %s" % (self.name, ))
        return self.__database

    @property
    def entries(self):
        """
        Iterate through the list of all Subversion log entries for this
        project, sorted by date
        """
        for entry in self.database.all_entries_by_date:
            yield entry

    def get_cached_entry(self, revision):
        "Return the entry for this revision, or None if none exists"
        return self.database.get_cached_entry(revision)

    def get_path_prefix(self, svn_url):
        "Return the base filesystem path used by this Subversion URL"

        # cache the base URL
        base_url = self.__metadata.base_url

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

    @classmethod
    def ignore_tag(cls, tag_name):
        """
        pDAQ release candidates are named _rc#
        Non-release debugging candidates are named _debug#
        """
        return tag_name.find("_rc") >= 0 or tag_name.find("_debug") >= 0

    @property
    def is_loaded(self):
        return self.__database is not None and \
          self.__database.is_loaded

    def load_from_db(self):
        if not self.database.is_loaded:
            self.database.load_from_db()

    def load_from_log(self, debug=False, verbose=False):
        self.database.load_from_log(debug=debug, verbose=verbose)

    def make_key(self, revision):
        "Make a key for this object"
        return "%s#%d" % (self.__metadata.root_url, revision)

    @property
    def mantis_projects(self):
        "Return the list of Mantis categories associated with this project"
        return self.__mantis_projects

    @property
    def name(self):
        return self.__metadata.project_name

    @property
    def total_entries(self):
        """
        Return the number of entries in the list of all Subversion log entries
        """
        return self.database.num_entries()

    @property
    def trunk_url(self):
        "Return the URL for this project's Subversion trunk"
        return self.__metadata.trunk_url


def get_pdaq_project_data(name_or_url):
    """
    Translate a pDAQ project name or Subversion URL
    into a tuple containing (url, project_name, mantis_projects)
    """
    pdaq_svn_url_prefix = "http://code.icecube.wisc.edu"

    if name_or_url is None or name_or_url == "pdaq":
        url = pdaq_svn_url_prefix + "/daq/meta-projects/pdaq"
        svn_project = "pdaq"
        mantis_projects = ("pDAQ", "dash", "pdaq-config", "pdaq-user")
    elif name_or_url.find("/") < 0:
        if name_or_url == "fabric-common":
            prefix = "svn"
        else:
            prefix = "daq"
        url = os.path.join(pdaq_svn_url_prefix, prefix, "projects",
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

    return (url, svn_project, mantis_projects)

class PDAQManager(object):
    "Manage all pDAQ SVN data"


    PROJECT_NAMES = ("PyDOM", "cluster-config", "config", "config-scripts",
                     "daq-common", "daq-integration-test", "daq-io", "daq-log",
                     "daq-moni-tool", "daq-pom-config", "daq-request-filler",
                     "daq-testframe", "daq-test-util", "dash",
                     "eventBuilder-prod", "fabric-common", "icebucket",
                     "juggler", "new-dispatch", "obsolete", "payload",
                     "payload-generator", "pdaq-user", "secondaryBuilders",
                     "splicer", "StringHub", "oldtrigger", "trigger",
                     "trigger-common", "trigger-testbed")

    __AUTHORS = {}
    __DATABASES = {}
    __PROJECTS = {}

    __AUTHORS_FILENAME = None

    __HOME_DIRECTORY = None

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
    def forget_database(cls, project_name):
        """
        Remove the cached entry for this project's database
        """
        if project_name in cls.__DATABASES:
            del cls.__DATABASES[project_name]

    @classmethod
    def get(cls, name_or_url, debug=False, verbose=False):
        """
        Return the object which captures all information about the requested
        Subversion project
        """
        url, svn_project, mantis_projects = get_pdaq_project_data(name_or_url)

        if svn_project not in cls.__PROJECTS:
            cls.__PROJECTS[svn_project] = SVNProject(url, mantis_projects,
                                                     debug=debug,
                                                     verbose=verbose)

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
    def get_database(cls, metadata, allow_create=False):
        """
        Return the repository database which has been loaded from the metadata
        """
        if metadata.project_name not in cls.__DATABASES:
            database = SVNRepositoryDB(metadata, allow_create=allow_create,
                                       directory=cls.__HOME_DIRECTORY)
            cls.__exit_if_unknown_authors(database)
            cls.__DATABASES[metadata.project_name] = database

        return cls.__DATABASES[metadata.project_name]

    @classmethod
    def load_authors(cls, filename, verbose=False):
        """
        Load file which maps Subversion usernames to Git authors
        (e.g. `someuser: Some User <someuser@example.com>`)
        """

        if verbose:
            print("Loading authors from \"%s\"" % (filename, ))

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

    @classmethod
    def set_home_directory(cls, directory="."):
        cls.__HOME_DIRECTORY = os.path.abspath(directory)


def main():
    "Main method"
    for proj in PDAQManager.PROJECT_NAMES:
        _ = PDAQManager.get(proj)
        # print("%s :: %s" % (proj, svnprj))


if __name__ == "__main__":
    main()
