#!/usr/bin/env python

from __future__ import print_function

import os

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

from cmdrunner import CommandException
from svn import SVNMetadata
from svndb import SVNEntry, SVNRepositoryDB


class SVNProject(SVNMetadata):
    def __init__(self, url, debug=False, verbose=False):
        super(SVNProject, self).__init__(url)

        self.__database = None

    def __str__(self):
        metastr = str(super(SVNProject, self))
        return "SVNProject[%s,%s]" % (metastr, self.__database)

    def add_entry(self, metadata, rel_name, log_entry, save_to_db=False):
        "Add a Subversion log entry"
        entry = SVNEntry(metadata, rel_name, metadata.branch_name,
                         log_entry.revision, log_entry.author,
                         log_entry.date_string, log_entry.num_lines,
                         log_entry.filedata, log_entry.loglines)
        self.database.add_entry(entry, None, save_to_db=save_to_db)
        return entry

    def close_db(self):
        if self.__database is not None:
            PDAQManager.forget_database(self.name)
            self.__database.close()
            self.__database = None

    @property
    def database(self):
        "Return the SVNRepositoryDB object for this project"
        if self.__database is None:
            self.__database = PDAQManager.get_database(self, allow_create=True)
            if self.__database is None:
                raise Exception("Cannot get database for %s" % (self.name, ))
        return self.__database

    def get_cached_entry(self, revision):
        "Return the entry for this revision, or None if none exists"
        return self.database.get_cached_entry(revision)

    def get_path_prefix(self, svn_url):
        "Return the base filesystem path used by this Subversion URL"

        # cache the base URL
        base_url = self.base_url

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

    @property
    def is_loaded(self):
        return self.__database is not None and \
          self.__database.is_loaded

    def load_from_db(self, shallow=False):
        """
        If 'shallow' is False, do NOT extract list of files for each entry
        """
        if not self.database.is_loaded:
            self.database.load_from_db(shallow=shallow)

    def load_from_log(self, debug=False, verbose=False):
        self.database.load_from_log(debug=debug, verbose=verbose)

    @property
    def name(self):
        return self.project_name

    @property
    def total_entries(self):
        """
        Return the number of entries in the list of all Subversion log entries
        """
        if not self.database.is_loaded:
            return 0

        return self.database.num_entries()


def get_pdaq_project_data(name_or_url):
    """
    Translate a pDAQ project name or Subversion URL
    into a tuple containing (url, project_name)
    """
    pdaq_svn_url_prefix = "http://code.icecube.wisc.edu"

    if name_or_url is None or name_or_url == "pdaq":
        url = pdaq_svn_url_prefix + "/daq/meta-projects/pdaq"
        svn_project = "pdaq"
    elif name_or_url.find("/") < 0:
        if name_or_url in ("fabric-common", "fabric_common"):
            prefix = "svn"
            repo_name = "fabric-common"
        else:
            prefix = "daq"
            repo_name = name_or_url
        url = os.path.join(pdaq_svn_url_prefix, prefix, "projects", repo_name)
        svn_project = repo_name
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

    return (url, svn_project)


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

    # project branch/tag names to ignore:
    #   pDAQ release candidates are named _rc#
    #   Non-release debugging candidates are named _debug#
    __IGNORED = ("_rc", "_debug", "alberto")

    __DATABASES = {}
    __PROJECTS = {}

    __HOME_DIRECTORY = None

    def __init__(self):
        pass

    @classmethod
    def __ignore_project(cls, tag_name):
        """
        If this release/branch name should be ignored, return True
        """
        if tag_name == "":
            if "" in cls.__IGNORED:
                return True
        else:
            for substr in cls.__IGNORED:
                if tag_name.find(substr) >= 0:
                    return True

        return False

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
        url, svn_project = get_pdaq_project_data(name_or_url)

        if svn_project not in cls.__PROJECTS:
            cls.__PROJECTS[svn_project] = SVNProject(url, debug=debug,
                                                     verbose=verbose)

        return cls.__PROJECTS[svn_project]

    @classmethod
    def get_database(cls, metadata, allow_create=False):
        """
        Return the repository database which has been loaded from the metadata
        """
        if metadata.project_name not in cls.__DATABASES:
            database = SVNRepositoryDB(metadata, allow_create=allow_create,
                                       directory=cls.__HOME_DIRECTORY,
                                       ignore_func=cls.__ignore_project)
            #cls.__exit_if_unknown_authors(database)
            cls.__DATABASES[metadata.project_name] = database

        return cls.__DATABASES[metadata.project_name]

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
