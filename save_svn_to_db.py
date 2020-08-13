#!/usr/bin/env python3

from __future__ import print_function

import argparse
import sys

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

from cmdrunner import CommandException
from svn import SVNMetadata, svn_get_externals, svn_log
from svndb import SVNEntry, SVNRepositoryDB


class SVNProject(object):
    def __init__(self, url, debug=False, verbose=False):
        self.__loaded_trunk = {}
        self.__revision_log = {}

        self.__metadata = SVNMetadata(url)

    def __load_log_entries(self, rel_url, rel_name, revision=None,
                           debug=False):
        "Add all SVN log entries to the internal revision log dictionary"

        # get information about this SVN repository
        try:
            metadata = SVNMetadata(rel_url)
        except CommandException as cex:
            if str(cex).find("W170000") >= 0:
                print("WARNING: Ignoring nonexistent SVN repository %s" %
                      (rel_url, ), file=sys.stderr)
                return
            raise

        prev = None
        for log_entry in svn_log(rel_url, revision=revision, end_revision=1,
                                 debug=debug):
            existing = self.get_entry(log_entry.revision)
            if existing is not None:
                existing.check_duplicate(log_entry)
                entry = existing
            else:
                entry = SVNEntry(metadata, rel_name, metadata.branch_name,
                                 log_entry.revision, log_entry.author,
                                 log_entry.date_string, log_entry.num_lines,
                                 log_entry.filedata, log_entry.loglines)
                self.add_entry(entry)

            if prev is not None:
                prev.set_previous(entry)
            prev = entry

            if existing is not None:
                # if we're on a branch and we've reached the trunk, we're done
                break

    @classmethod
    def __build_base_suffix(cls, svn_url, base_url):
        "Build the base URL prefix which is stripped from each file URL"

        if not svn_url.startswith(base_url):
            raise CommandException("URL \"%s\" does not start with"
                                   " base URL \"%s\"" % (svn_url, base_url))

        prefix = svn_url[len(base_url):]
        if not prefix.startswith("/"):
            raise CommandException("Cannot strip base URL \"%s\" from \"%s\"" %
                                   (base_url, svn_url))

        prefix = prefix[1:]
        if not prefix.endswith("/"):
            prefix += "/"

        return prefix

    def load_from_url(self, rel_url, rel_name, debug=False, verbose=False):
        if verbose:
            print("Loading log entries from %s" % rel_name)

        self.__load_log_entries(rel_url, rel_name, debug=debug)

        # fetch external project URLs
        externals = {}
        for revision, url, subdir in svn_get_externals(rel_url):
            # load all log entries for this repository
            try:
                self.__load_log_entries(url, rel_name, revision=revision,
                                        debug=debug)
            except CommandException as cex:
                # if the exception did not involve a dead link, reraise it
                if str(cex).find("E160013") < 0:
                    raise

                # complain about dead links and continue
                print("WARNING: Repository %s does not exist" %
                      (url, ), file=sys.stderr)

        if verbose:
            print("After %s, revision log contains %d entries" %
                  (rel_name, len(self.__revision_log)))

    def add_entry(self, entry):
        key = self.make_key(entry.revision)
        if key in self.__revision_log:
            raise Exception("Cannot overwrite <%s>%s with <%s>%s" %
                            (type(self.__revision_log[key]),
                             self.__revision_log[key], type(entry), entry))
        self.__revision_log[key] = entry

    @property
    def entries(self):
        for entry in sorted(self.__revision_log.values(),
                            key=lambda x: x.date_string):
            yield entry

    @property
    def entry_pairs(self):
        for pair in sorted(self.__revision_log.items(),
                           key=lambda x: x[1].date_string):
            yield pair

    def get_entry(self, revision):
        key = self.make_key(revision)
        if key not in self.__revision_log:
            return None
        return self.__revision_log[key]

    def make_key(self, revision):
        return "%s#%d" % (self.__metadata.root_url, revision)

    @property
    def metadata(self):
        return self.__metadata

    @property
    def name(self):
        return self.__metadata.project_name

    @property
    def num_entries(self):
        return len(self.__revision_log)


def add_arguments(parser):
    "Add command-line arguments"

    parser.add_argument("-v", "--verbose", dest="verbose",
                        action="store_true", default=False,
                        help="Print details")
    parser.add_argument("-x", "--debug", dest="debug",
                        action="store_true", default=False,
                        help="Print debugging messages")
    parser.add_argument(dest="svn_project", default=None,
                        help=("Subversion/Mantis project name"))


def ignore_tag(tag_name):
    """
    pDAQ release candidates are named _rc#
    Non-release debugging candidates are named _debug#
    """
    return tag_name.find("_rc") >= 0 or tag_name.find("_debug") >= 0


def main():
    parser = argparse.ArgumentParser()
    add_arguments(parser)
    args = parser.parse_args()

    daq_svn = "http://code.icecube.wisc.edu/daq"

    mantis_projects = None
    if args.svn_project is None or args.svn_project == "pdaq":
        url = daq_svn + "/meta-projects/pdaq/trunk"
    elif args.svn_project.find("/") < 0:
        url = daq_svn + "/projects/" + args.svn_project
    else:
        upieces = urlparse.urlparse(args.svn_project)
        upath = upieces.path.split(os.sep)

        if upath[-1] == "trunk":
            del upath[-1]

        url = urlparse.urlunparse((upieces.scheme, upieces.netloc,
                                   os.sep.join(upath), upieces.params,
                                   upieces.query, upieces.fragment))

    # pDAQ used 'releases' instead of 'tags'
    SVNMetadata.set_layout(SVNMetadata.DIRTYPE_TAGS, "releases")

    # load log entries from all URLs
    project = SVNProject(url, debug=args.debug, verbose=args.verbose)
    for dirtype, url in project.metadata.all_urls(ignore=ignore_tag):
        project.load_from_url(url, dirtype, debug=args.debug,
                              verbose=args.verbose)

    print("Loaded %d entries from %s" % (project.num_entries, project.name))

    print("Opening %s repository database" % project.name)
    svndb = SVNRepositoryDB(project.metadata)
    old_entries = {project.make_key(entry.revision): entry
                   for entry in svndb.all_entries}

    print("Saving %d entries to DB" % project.num_entries)
    total = 0
    added = 0
    for key, entry in project.entry_pairs:
        total += 1
        if key not in old_entries:
            svndb.save_entry(entry)
            added += 1
    if added == 0:
        print("No entries added, total is %d" % (total, ))
    elif added == total:
        print("Added %d entries" % (added, ))
    else:
        print("Added %d new entries, total now %d" % (added, total))


if __name__ == "__main__":
    main()
