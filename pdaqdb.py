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
    def __init__(self, name, url, debug=False, verbose=False):
        self.__name = name
        self.__database = None

        super(SVNProject, self).__init__(url)

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
            self.__database = PDAQManager.get_database(self.name,
                                                       allow_create=True)
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
        return self.__name

    @property
    def svn_name(self):
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
    def get(cls, name, renamed=None, debug=False, verbose=False):
        """
        Return the object which captures all information about the requested
        Subversion project
        """
        if renamed is not None and name not in renamed:
            svn_name = name
        else:
            svn_name = renamed[name]

        url, svn_project = cls.__get_pdaq_project_data(svn_name)

        if name not in cls.__PROJECTS:
            cls.__PROJECTS[name] = SVNProject(name, url, debug=debug,
                                              verbose=verbose)

        return cls.__PROJECTS[name]

    @classmethod
    def get_database(cls, project_name, allow_create=False):
        """
        Return the repository database for this project
        """
        # if directory containing all databases hasn't been set,
        #  assume they're stored in the user's home directory
        if cls.__DBFILE_DIRECTORY is None:
            cls.set_home_directory(os.environ["HOME"])

        if project_name not in cls.__DATABASES:
            database = ProjectDatabase(project_name,
                                       allow_create=allow_create,
                                       directory=cls.__DBFILE_DIRECTORY,
                                       ignore_func=cls.__ignore_project)
            #cls.__exit_if_unknown_authors(database)
            cls.__DATABASES[project_name] = database

        return cls.__DATABASES[project_name]

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

    (PROJ_PDAQ, PROJ_DOMHUB, PROJ_REL4XX, PROJ_WEIRD, PROJ_IGNORE) = \
      range(5)

    PROJECTS = {
        # pDAQ projects
        "PyDOM": PROJ_PDAQ,
        "cluster-config": PROJ_PDAQ,
        "config": PROJ_PDAQ,
        "config-scripts": PROJ_PDAQ,
        "daq-common": PROJ_PDAQ,
        "daq-integration-test": PROJ_PDAQ,
        "daq-io": PROJ_PDAQ,
        "daq-log": PROJ_PDAQ,
        "daq-moni-tool": PROJ_PDAQ,
        "daq-pom-config": PROJ_PDAQ,
        "daq-request-filler": PROJ_PDAQ,
        "daq-testframe": PROJ_PDAQ,
        "daq-test-util": PROJ_PDAQ,
        "dash": PROJ_PDAQ,
        "eventBuilder": PROJ_PDAQ,
        "fabric-common": PROJ_PDAQ,
        "icebucket": PROJ_PDAQ,
        "jhdf5": PROJ_PDAQ,
        "juggler": PROJ_PDAQ,
        "jzmq": PROJ_PDAQ,
        "new-dispatch": PROJ_PDAQ,
        "obsolete": PROJ_PDAQ,
        "payload": PROJ_PDAQ,
        "payload-generator": PROJ_PDAQ,
        "pdaq-user": PROJ_PDAQ,
        "pytest-plugin": PROJ_PDAQ,
        "secondaryBuilders": PROJ_PDAQ,
        "splicer": PROJ_PDAQ,
        "StringHub": PROJ_PDAQ,
        "oldtrigger": PROJ_PDAQ,
        "trigger": PROJ_PDAQ,
        "trigger-common": PROJ_PDAQ,
        "trigger-testbed": PROJ_PDAQ,
        "xmlrpc-current": PROJ_PDAQ,
        "pdaq": PROJ_PDAQ,
        # domhub projects
        "configboot": PROJ_DOMHUB,
        "domapp-tools": PROJ_DOMHUB,
        "domapp-tools-python": PROJ_DOMHUB,
        "domhub": PROJ_DOMHUB,
        "domhub-tools": PROJ_DOMHUB,
        "domhub-tools-python": PROJ_DOMHUB,
        "moat": PROJ_DOMHUB,
        "testdaq-user": PROJ_DOMHUB,
        # rel4xx projects
        "dom-loader": PROJ_REL4XX,
        "dom-ws": PROJ_REL4XX,
        "domapp": PROJ_REL4XX,
        "fb-cpld": PROJ_REL4XX,
        "hal": PROJ_REL4XX,
        "iceboot": PROJ_REL4XX,
        "stf-gen1": PROJ_REL4XX,
        "testdomapp": PROJ_REL4XX,
        # weird projects
        "dom-cal": PROJ_WEIRD,
        "dom-cpld": PROJ_WEIRD,
        "dom-fpga": PROJ_WEIRD,
        # ignored projects
        "fits": PROJ_IGNORE,
        "powerManagement": PROJ_IGNORE,
        "trackengine": PROJ_IGNORE,
        "trigger-config": PROJ_IGNORE,
    }

    RENAMED = {
        "eventBuilder": "eventBuilder-prod",
        "stf-gen1": "stf",
    }

    for name, ptype in PROJECTS.items():
        proj = PDAQManager.get(name, renamed=RENAMED)
        if proj is None:
            print("Cannot get \"%s\"" % name)

        # print("%s :: %s" % (proj, svnprj))


if __name__ == "__main__":
    main()
