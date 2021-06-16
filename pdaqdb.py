#!/usr/bin/env python

from __future__ import print_function

import os
import sys

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

from cmdrunner import CommandException
from svn import SVNMetadata
from project_db import ProjectDatabase


class SVNProject(SVNMetadata):
    def __init__(self, url, debug=False, verbose=False):
        super(SVNProject, self).__init__(url)

        self.__database = None

    def __str__(self):
        return "SVNProject[%s,%s]" % (super(SVNProject, self), self.__database)

    def close_db(self):
        if self.__database is not None:
            PDAQManager.forget_database(self.name)
            self.__database.close()
            self.__database = None

    @property
    def database(self):
        "Return the project database for this project"
        if self.__database is None:
            self.__database = PDAQManager.get_database(self, allow_create=True)
            if self.__database is None:
                raise Exception("Cannot get database for %s" % (self.name, ))
        return self.__database

    def get_cached_entry(self, revision):
        "Return the entry for this revision, or None if none exists"
        if self.__database is None:
            raise Exception("Cannot get database for %s" % (self.name, ))
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
            self.database.load_database_entries(shallow=shallow)

    def load_from_log(self, save_to_db=False, debug=False, verbose=False):
        self.database.load_log_entries(self.project_url, save_to_db=save_to_db,
                                       debug=debug, verbose=verbose)

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


class PDAQManager(object):
    "Manage all pDAQ SVN data"


    PROJECT_NAMES = ("PyDOM", "cluster-config", "config", "config-scripts",
                     "daq-common", "daq-integration-test", "daq-io", "daq-log",
                     "daq-moni-tool", "daq-pom-config", "daq-request-filler",
                     "daq-testframe", "daq-test-util", "dash",
                     "eventBuilder-prod", "fabric-common", "icebucket",
                     "jhdf5", "juggler", "jzmq", "new-dispatch", "obsolete",
                     "payload", "payload-generator", "pdaq-user",
                     "pytest-plugin", "secondaryBuilders", "splicer",
                     "StringHub", "oldtrigger", "trigger", "trigger-common",
                     "trigger-testbed", "xmlrpc-current")

    PROJECT_DOMHUB = ("configboot", "domapp-tools", "domapp-tools-python",
                      "domhub", "domhub-tools", "domhub-tools-python", "moat",
                      "testdaq-user")

    PROJECT_IGNORED = ("fits", "powerManagement", "trackengine",
                       "trigger-config")

    # project branch/tag names to ignore:
    #   pDAQ release candidates are named _rc# or -RC#
    #   Non-release debugging candidates are named _debug#
    __IGNORED = ("_rc", "-RC", "_debug", "alberto", "Furtherless")

    __DATABASES = {}
    __PROJECTS = {}

    __DBFILE_DIRECTORY = None

    def __init__(self):
        pass

    @classmethod
    def __get_pdaq_project_data(cls, name_or_url):
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
            url = os.path.join(pdaq_svn_url_prefix, prefix, "projects",
                               repo_name)
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
        url, svn_project = cls.__get_pdaq_project_data(name_or_url)

        if svn_project not in cls.__PROJECTS:
            cls.__PROJECTS[svn_project] = SVNProject(url, debug=debug,
                                                     verbose=verbose)

        return cls.__PROJECTS[svn_project]

    @classmethod
    def get_database(cls, metadata, allow_create=False):
        """
        Return the repository database which has been loaded from the metadata
        """
        # if directory containing all databases hasn't been set,
        #  assume they're stored in the user's home directory
        if cls.__DBFILE_DIRECTORY is None:
            cls.set_home_directory(os.environ["HOME"])

        if metadata.project_name not in cls.__DATABASES:
            database = ProjectDatabase(metadata.project_name,
                                       allow_create=allow_create,
                                       directory=cls.__DBFILE_DIRECTORY,
                                       ignore_func=cls.__ignore_project)
            #cls.__exit_if_unknown_authors(database)
            cls.__DATABASES[metadata.project_name] = database

        return cls.__DATABASES[metadata.project_name]

    @classmethod
    def set_home_directory(cls, directory="."):
        """
        Set the directory which should contain all the database files
        """
        # get the absolute path for the directory
        newpath = os.path.abspath(directory)

        # resetting the home directory could be confusing, don't allow it
        if cls.__DBFILE_DIRECTORY is not None and \
          cls.__DBFILE_DIRECTORY != newpath:
            raise Exception("Cannot override DB directory \"%s\""
                            " with \"%s\"" % (cls.__DBFILE_DIRECTORY, newpath),
                            file=sys.stderr)

        cls.__DBFILE_DIRECTORY = newpath


def main():
    "Main method"
    for proj in PDAQManager.PROJECT_NAMES:
        _ = PDAQManager.get(proj)
        # print("%s :: %s" % (proj, svnprj))


if __name__ == "__main__":
    main()
