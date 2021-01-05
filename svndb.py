#!/usr/bin/env python3
"""
Manage the SVN log database
"""

from __future__ import print_function

import os
import sqlite3
import sys

from cmdrunner import CommandException
from dictobject import DictObject
from i3helper import Comparable
from svn import SVNConnectException, SVNDate, SVNException, SVNMetadata, \
     svn_log


class MetadataManager(object):
    KNOWN_REPOS = {}
    KNOWN_IDS = {}

    @classmethod
    def __make_key(cls, base_subdir, project_name):
        return "%s/%s" % (base_subdir, project_name)

    @classmethod
    def get(cls, metadata):
        key = cls.__make_key(metadata.base_subdir, metadata.project_name)
        if key not in cls.KNOWN_REPOS:
            cls.KNOWN_REPOS[key] = metadata

        return cls.KNOWN_REPOS[key]

    @classmethod
    def get_by_id(cls, project_id, project_name=None, original_url=None,
                  root_url=None, base_subdir=None, trunk_subdir=None,
                  branches_subdir=None, tags_subdir=None):
        if project_id in MetadataManager.KNOWN_IDS:
            return cls.KNOWN_IDS[project_id]
        if project_name is None or base_subdir is None:
            raise SVNException("Unknown project #%d" % (project_id, ))

        key = cls.__make_key(base_subdir, project_name)
        if key not in cls.KNOWN_REPOS:
            cls.KNOWN_REPOS[key] = \
              SVNMetadata(original_url, repository_root=root_url,
                          project_base=base_subdir, project_name=project_name,
                          trunk_subdir=trunk_subdir,
                          branches_subdir=branches_subdir,
                          tags_subdir=tags_subdir)

        return cls.KNOWN_REPOS[key]

    @classmethod
    def has_id(cls, project_id):
        return project_id in MetadataManager.KNOWN_IDS


class SVNEntry(Comparable, DictObject):
    "Object containing information from a single Subversion log entry"

    def __init__(self, metadata, tag_name, branch_name, revision, author,
                 svn_date, num_lines, files, loglines, git_branch=None,
                 git_hash=None):
        super(SVNEntry, self).__init__()

        self.metadata = metadata
        self.tag_name = tag_name
        self.branch_name = branch_name
        self.revision = revision
        self.author = author
        self.__date = SVNDate(svn_date)
        self.num_lines = num_lines
        self.filelist = None if files is None else files[:]
        self.loglines = loglines[:]
        self.git_branch = git_branch
        self.git_hash = git_hash

        self.__previous = None

    def __str__(self):
        if self.filelist is None:
            fstr = "[not loaded]"
        else:
            fstr = str(len(self.filelist))

        if self.__previous is None:
            pstr = ""
        else:
            pstr = ">>%s#%d" % (self.__previous.tag_name,
                                self.__previous.revision)

        return "%s#%d@%s*%s%s" % (self.tag_name, self.revision, self.date,
                                  fstr, pstr)

    def check_duplicate(self, entry, verbose=True):
        """
        Compare this object against another log entry object, returning True
        if it contains the same information.  If 'verbose' is True, also
        print a message with details of the first difference
        """

        if self.revision != entry.revision:
            if verbose:
                print("Rev#%d != #%d" % (self.revision, entry.revision),
                      file=sys.stderr)
            return False

        if self.author != entry.author:
            if verbose:
                print("Rev#%d duplicate mismatch: Author \"%s\" != \"%s\"" %
                      (self.revision, entry.author, self.author),
                      file=sys.stderr)
            return False

        if self.date != entry.date:
            if verbose:
                print("Rev#%d duplicate mismatch: Date \"%s\" != \"%s\"" %
                      (self.revision, entry.date, self.date), file=sys.stderr)
            return False

        if self.num_lines != entry.num_lines:
            if verbose:
                print("Rev#%d duplicate mismatch: Number of lines %d != %d" %
                      (self.revision, entry.num_lines, self.num_lines),
                      file=sys.stderr)
            return False

        try:
            if self.__previous != entry.previous:
                if verbose:
                    print("Rev#%d duplicate mismatch: Previous rev %s != %s" %
                          (self.revision, entry.previous, self.previous),
                          file=sys.stderr)
            return False
        except AttributeError:
            pass

        return True

    @property
    def compare_key(self):
        if self.__previous is None:
            prev_rev = None
        else:
            prev_rev = self.__previous.revision

        return (self.tag_name, self.branch_name, self.revision, prev_rev)

    @property
    def date_string(self):
        return self.__date.string

    @property
    def date(self):
        return self.__date.datetime

    @property
    def log_message(self):
        # build the commit message
        message = None
        for line in self.loglines:
            if message is None:
                message = line
            else:
                message += "\n" + line
        return message

    @property
    def previous(self):
        "Return the previous log entry"
        return self.__previous

    def set_previous(self, entry):
        "Set the previous log entry"
        if self.__previous is not None:
            if self.__previous.revision == entry.revision:
                # complain about duplicate entries
                if not self.__previous.check_duplicate(entry):
                    print("WARNING: Overwriting rev#%d previous entry %s"
                          " with %s" % (self.revision, self.__previous, entry),
                          file=sys.stderr)
            elif self.__previous.revision > entry.revision:
                curr = self.__previous
                prev = None
                while curr.revision > entry.revision and \
                  curr.previous is not None:
                    prev = curr
                    curr = curr.previous
                print("XXX: Cannot overwrite #%d prev #%d with #%d" %
                      (self.revision, prev.revision, curr.revision),
                      file=sys.stderr)
                return

        if self.__previous is not None and \
          self.__previous.revision != entry.revision:
            # complain about duplicate entries
            print("XXX: Overwriting rev#%d previous entry %s"
                  " with %s" % (self.revision, self.__previous, entry),
                  file=sys.stderr)

        self.__previous = entry


class SVNRepositoryDB(SVNMetadata):
    """
    Manage the SVN log database
    """

    # project branch/tag names to ignore:
    #   pDAQ release candidates are named _rc#
    #   Non-release debugging candidates are named _debug#
    IGNORED = ("_rc", "_debug")

    def __init__(self, metadata_or_svn_url, allow_create=True, directory=None):
        """
        Open (and possibly create) the SVN database for this project
        metadata_or_svn_url - either an SVNMetadata object or a Subversion URL
        """


        if not isinstance(metadata_or_svn_url, SVNMetadata):
            orig_url = metadata_or_svn_url
        else:
            orig_url = metadata_or_svn_url.original_url
        super(SVNRepositoryDB, self).__init__(orig_url)

        self.__name = self.project_name

        # build the absolute path to the database file
        if directory is None:
            directory = "."
        relpath = os.path.join(directory, "%s-svn.db" % (self.__name, ))
        self.__path = os.path.abspath(relpath)

        # open the database file
        self.__conn = sqlite3.connect(self.__path)
        self.__conn.row_factory = sqlite3.Row

        # if necessary, create all the tables
        if allow_create:
            self.__create_tables()

        try:
            proj_id = self.__add_project_to_db()
        except sqlite3.OperationalError:
            proj_id = None
        self.__project_id = proj_id

        self.__ignore_func = self.__ignore_project

        self.__cached_entries = None
        self.__urls_by_date = None

    def __str__(self):
        metastr = str(super(SVNRepositoryDB, self))
        return "SVNRepositoryDB(%s#%s: %s)" % \
          (self.__name, self.__project_id, metastr)

    def __add_entry_to_cache(self, entry, prev_revision=None, save_to_db=False):
        if self.__cached_entries is not None and \
          entry.revision in self.__cached_entries:
            raise Exception("Cannot add duplicate entry for %s rev %s" %
                            (self.__name, entry.revision))

        # make sure we can continue
        if self.__cached_entries is None:
            if prev_revision is not None:
                raise Exception("Cannot set previous entry for rev %s;"
                                " unknown previous rev %s" %
                                (entry.prevision, prev_revision))
        elif entry.revision in self.__cached_entries:
            if entry != self.__cached_entries[entry.revision]:
                raise Exception("Cannot replace existing entry for %s rev %s" %
                                (self.__name, entry.revision))
            return

        # if known, set previous revision
        if prev_revision is None and entry.previous is not None:
            if entry.previous.revision != prev_revision:
                raise Exception("Cannot change previous revision"
                                " for %s rev %s from rev %s to %s" %
                                (self.__name, entry.revision,
                                 entry.previous.revision, prev_revision))
            prev_revision = entry.previous.revision
        if prev_revision is not None:
            if prev_revision not in self.__cached_entries:
                raise Exception("Cannot find %s revision %s" %
                                (self.__name, prev_revision))
            entry.set_previous(self.__cached_entries[prev_revision])

        if save_to_db:
            self.__save_entry_to_database(entry)

        if self.__cached_entries is None:
            self.__cached_entries = {}

        self.__cached_entries[entry.revision] = entry

    def __add_project_to_db(self):
        "Add this project to the svn_project table"

        with self.__conn:
            cursor = self.__conn.cursor()

            for first_attempt in (True, False):
                cursor.execute("select * from svn_project where name=?",
                               (self.project_name, ))

                row = cursor.fetchone()
                if row is not None:
                    return row["project_id"]

                if first_attempt:
                    cursor.execute("insert into svn_project(name,"
                                   " original_url, root_url,"
                                   " base_subdir, trunk_subdir,"
                                   " branches_subdir, tags_subdir)"
                                   " values (?, ?, ?, ?, ?, ?, ?)",
                                   (self.project_name, self.original_url,
                                    self.root_url, self.base_subdir,
                                    self.trunk_subdir, self.branches_subdir,
                                    self.tags_subdir))

        raise SVNException("Cannot find or add \"%s\" in the database" %
                           (self.project_name, ))

    def __create_tables(self):
        with self.__conn:
            cursor = self.__conn.cursor()

            cursor.execute("create table if not exists svn_project("
                           " project_id INTEGER PRIMARY KEY,"
                           " name TEXT NOT NULL,"
                           " original_url TEXT NOT NULL,"
                           " root_url TEXT NOT NULL,"
                           " base_subdir TEXT NOT NULL,"
                           " trunk_subdir TEXT NOT NULL,"
                           " branches_subdir TEXT,"
                           " tags_subdir TEXT)")

            cursor.execute("create table if not exists svn_log("
                           " log_id INTEGER PRIMARY KEY,"
                           " project_id INTEGER,"
                           " tag TEXT NOT NULL,"
                           " branch TEXT NOT NULL, "
                           " revision INTEGER NOT NULL,"
                           " author TEXT NOT NULL,"
                           " date TIMESTAMP NOT NULL,"
                           " num_lines INTEGER,"
                           " message TEXT,"
                           " prev_revision INTEGER,"
                           " git_branch TEXT,"
                           " git_hash TEXT,"
                           " FOREIGN KEY(project_id) REFERENCES"
                           "  svn_project(project_id))")

            cursor.execute("create unique index if not exists"
                           " svn_log_unique on svn_log("
                           " project_id, revision)")

            cursor.execute("create table if not exists svn_log_file("
                           " logfile_id INTEGER PRIMARY KEY,"
                           " log_id INTEGER,"
                           " action TEXT NOT NULL,"
                           " file TEXT NOT NULL,"
                           " FOREIGN KEY(log_id) REFERENCES"
                           " svn_log(log_id))")

    def __get_files(self, log_id):
        """
        Return a list of all (action, filename) pairs from the log message
        """

        cursor = self.__conn.cursor()
        cursor.execute("select action, file from svn_log_file"
                       " where log_id=? order by logfile_id",
                       (log_id, ))

        files = []
        for row in cursor.fetchall():
            files.append((row["action"], row["file"]))

        return files

    @classmethod
    def __ignore_project(cls, tag_name):
        """
        If this project name contains one or more substrings listed in IGNORED,
        ignore it
        """
        for substr in cls.IGNORED:
            if tag_name.find(substr) >= 0:
                return True

        return False

    def __load_log_entries(self, metadata, rel_url, rel_name, revision="HEAD",
                           debug=False, verbose=False):
        """
        Add all Subversion log entries for a trunk, branch, or tag
        to this object's internal cache
        """
        # always attempt to save/update log entries in the database
        save_to_db = True

        prev = None
        for log_entry in svn_log(rel_url, revision=revision, end_revision=1,
                                 debug=debug, verbose=verbose):
            existing = self.get_cached_entry(log_entry.revision)
            if existing is not None:
                existing.check_duplicate(log_entry)
                entry = existing
            else:
                entry = SVNEntry(metadata, rel_name, metadata.branch_name,
                                 log_entry.revision, log_entry.author,
                                 log_entry.date_string, log_entry.num_lines,
                                 log_entry.filedata, log_entry.loglines)
                self.__add_entry_to_cache(entry, save_to_db=save_to_db)

            if prev is not None:
                prev.set_previous(entry)
                if save_to_db:
                    self.__update_previous_in_database(prev)
            prev = entry

            if existing is not None:
                # if we're on a branch and we've reached the trunk, we're done
                break

    def __save_entry_to_database(self, entry):
        "Save a single SVN log entry to the database"

        if entry.filelist is None:
            raise Exception("File list has not been loaded")

        # if it exists, get previouos revision number
        if entry.previous is None:
            prev_revision = None
        else:
            prev_revision = entry.prevision.revision

        with self.__conn:
            cursor = self.__conn.cursor()

            message = "\n".join(entry.loglines)

            try:
                cursor.execute("insert into svn_log(project_id, tag, branch,"
                               " revision, author, date, num_lines, message,"
                               " prev_revision, git_branch, git_hash)"
                               " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                               (self.__project_id, entry.tag_name,
                                entry.branch_name, entry.revision,
                                entry.author, entry.date, entry.num_lines,
                                message, prev_revision, entry.git_branch,
                                entry.git_hash))
            except sqlite3.IntegrityError:
                # entry exists, update it with the new data
                cursor.execute("update svn_log set tag=?, branch=?,"
                               " author=?, date=?, num_lines=?, message=?,"
                               " prev_revision=?, git_branch=?, git_hash=?"
                               " where project_id=? and revision=?",
                               (entry.tag_name, entry.branch_name,
                                entry.author, entry.date, entry.num_lines,
                                message, prev_revision, entry.git_branch,
                                entry.git_hash, self.__project_id,
                                entry.revision))

            log_id = cursor.lastrowid

            for action, filename in entry.filelist:
                cursor.execute("insert into svn_log_file(log_id, action, file)"
                               " values (?, ?, ?)", (log_id, action, filename))

    def __update_previous_in_database(self, entry):
        "Update the previous revision in the database"

        # if it exists, get previouos revision number
        if entry.previous is None:
            raise Exception("%s rev %s previous revision has not been set" %
                            (self.__name, entry.revision))

        with self.__conn:
            cursor = self.__conn.cursor()

            cursor.execute("update svn_log set prev_revision=?"
                           " where project_id=? and revision=?",
                           (entry.previous.revision, self.__project_id,
                            entry.revision))

    @property
    def all_entries(self):
        "Iterate through all SVN log entries in the database, ordered by key"
        if self.__cached_entries is None:
            try:
                self.load_from_db()
            except sqlite3.OperationalError:
                return

        for _, entry in sorted(self.__cached_entries.items(),
                               key=lambda x: x[0]):
            yield entry

    @property
    def all_entries_by_date(self):
        "Iterate through all SVN log entries in the database, ordered by date"
        if self.__cached_entries is None:
            try:
                self.load_from_db()
            except sqlite3.OperationalError:
                return

        for entry in sorted(self.__cached_entries.values(),
                            key=lambda x: x.date):
            yield entry

    @property
    def all_urls_by_date(self):
        """
        Sort all Subversion URLs for trunk and branches/tags subdirectories
        by the date of the first entry.
        Return (svn_url, first_revision, first_date)
        """
        if self.__urls_by_date is None:
            datedict = {}
            for entry in sorted(self.__cached_entries.values(),
                                key=lambda x: x.date):
                if entry.branch_name not in datedict:
                    datedict[entry.branch_name] = entry

            finaldict = {}
            for dirtype, dirname, dirurl in self.all_urls():
                typename = self.typename(dirtype)
                if typename == self.TRUNK_NAME and typename == dirname:
                    dirkey = dirname
                else:
                    dirkey = "%s/%s" % (typename, dirname)
                if dirkey not in datedict:
                    if not self.__ignore_project(dirname):
                        print("[Ignoring %s branch/tag %s]" %
                              (self.name, dirkey))
                    continue

                finaldict[dirurl] = datedict[dirkey]

            self.__urls_by_date = finaldict

        for svn_url, entry in sorted(self.__urls_by_date.items(),
                                     key=lambda x : x[1].date):
            yield svn_url, entry.revision, entry.date

    def close(self):
        if self.__conn is not None:
            self.__conn.close()
            self.__conn = None
        if self.__cached_entries is not None:
            self.__cached_entries = None
        if self.__urls_by_date is not None:
            self.__urls_by_date = None

    def entries(self, branch_name):
        "Iterate through SVN log entries in the database"

        if branch_name is None or branch_name == "":
            branch_name = SVNMetadata.TRUNK_NAME

        for entry in self.all_entries:
            if entry.branch_name == branch_name:
                yield entry

    def find_git_hash(self, branch_name, revision):
        if revision is None:
            raise Exception("Cannot fetch unknown %s revision" %
                            (self.__name, ))

        # find the git branch/hash associated with this revision
        result = self.find_revision(branch_name, revision, with_git_hash=True)
        if result is None:
            result = self.find_revision(SVNMetadata.TRUNK_NAME, revision,
                                        with_git_hash=True)
        if result is None:
            git_branch, git_hash = (None, None)
        else:
            _, git_branch, git_hash = result

        # find the revision associated with this Git hash
        _, svn_rev = self.find_revision_from_hash(git_hash)

        return git_branch, git_hash, svn_rev

    @classmethod
    def find_id(cls, conn, project_id):
        if MetadataManager.has_id(project_id):
            return MetadataManager.get_by_id(project_id)

        with conn:
            cursor = conn.cursor()

            cursor.execute("select * from svn_project where project_id=?",
                           (project_id, ))

            row = cursor.fetchone()
            if row is None:
                raise SVNException("Cannot find project #%d in the database" %
                                   (project_id, ))
            return MetadataManager.get_by_id(project_id, row["name"],
                                             row["original_url"],
                                             row["root_url"],
                                             row["base_subdir"],
                                             row["trunk_subdir"],
                                             row["branches_subdir"],
                                             row["tags_subdir"])

    def find_previous_revision(self, branch_name, entry):
        saved_entry = entry

        while True:
            result = self.find_revision(branch_name, entry.revision,
                                           with_git_hash=True)
            if result is None or result[1] is None and result[2] is None and \
              branch_name != SVNMetadata.TRUNK_NAME:
                result = self.find_revision(SVNMetadata.TRUNK_NAME,
                                               entry.revision,
                                               with_git_hash=True)

            if result is not None and result[1] is not None and \
              result[2] is not None:
                prev_rev = entry.revision
                _, prev_branch, prev_hash = result
                return prev_rev, prev_branch, prev_hash

            if entry.previous is None:
                raise Exception("Cannot find committed ancestor for"
                                " %s SVN r%s (started from r%s)" %
                                (self.name, entry.revision,
                                 saved_entry.revision))
            entry = entry.previous

    def find_revision(self, svn_branch, revision, with_git_hash=False):
        """
        Look for the Git branch and hash associated with 'revision'.
        If 'revision' is None, find the latest revision.
        Return (revision, git_branch, git_hash)
        """

        if svn_branch is None:
            svn_branch = SVNMetadata.TRUNK_NAME

        with self.__conn:
            cursor = self.__conn.cursor()

            if with_git_hash:
                hash_query_str = " and git_hash!=''"
            else:
                hash_query_str = ""

            if revision is None:
                cursor.execute("select revision, git_branch, git_hash"
                               " from svn_log where branch=?" +
                               hash_query_str +
                               " order by revision desc limit 1",
                               (svn_branch, ))
            else:
                cursor.execute("select revision, git_branch, git_hash"
                               " from svn_log"
                               " where branch=? and revision<=?" +
                               hash_query_str +
                               " order by revision desc limit 1",
                               (svn_branch, revision, ))

            row = cursor.fetchone()
            if row is None:
                return None
            if len(row) != 3:
                raise SVNException("Expected 3 columns, not %d" % (len(row), ))
            if row[0] is None:
                raise SVNException("No revision found in %s" % (row, ))

            if len(row[2]) == 7:
                raise Exception("Found short hash %s for %s %s rev %s" %
                                (row[1], self.__name, svn_branch, revision))

            return int(row[0]), row[1], row[2]

    def find_revision_from_date(self, svn_branch, date_string,
                                with_git_hash=False):
        """
        Look for revision at or before 'date_string'.  If there is none,
        find the first revision.
        Return (branch, revision)
        """
        if svn_branch is None:
            svn_branch = SVNMetadata.TRUNK_NAME

        with self.__conn:
            cursor = self.__conn.cursor()

            if with_git_hash:
                hash_query_str = " and git_hash!=''"
            else:
                hash_query_str = ""

            cursor.execute("select revision from svn_log"
                           " where branch=? and date<=?" + hash_query_str +
                           " order by date desc limit 1",
                           (svn_branch, date_string, ))
            row = cursor.fetchone()
            if row is None:
                cursor.execute("select revision from svn_log"
                               " where branch=?" + hash_query_str +
                               " order by revision asc limit 1",
                               (svn_branch, ))

                row = cursor.fetchone()
                if row is None:
                    return None
            return int(row[0])

    def find_revision_from_hash(self, git_hash):
        """
        Return (branch, revision) associated with Git hash 'git_hash'.
        If 'git_hash' is not found, return (None, None)
        """
        with self.__conn:
            cursor = self.__conn.cursor()

            cursor.execute("select branch, revision from svn_log"
                           " where git_hash like '%s%%'"
                           " order by revision asc limit 1" % (git_hash, ))
            row = cursor.fetchone()
            if row is None:
                return (None, None)

            return row[0], int(row[1])

    def get_cached_entry(self, revision):
        "Return the entry for this revision, or None if none exists"
        if self.__cached_entries is None or \
          revision not in self.__cached_entries:
            return None
        return self.__cached_entries[revision]

    @property
    def is_loaded(self):
        return self.__cached_entries is not None

    def load_from_db(self, shallow=False):
        """
        If 'shallow' is False, do NOT extract list of files for each entry
        """
        if self.__cached_entries is not None:
            raise Exception("Entries for %s have already been loaded from"
                            " the database" % (self.__name, ))

        self.__cached_entries = {}
        with self.__conn:
            cursor = self.__conn.cursor()

            cursor.execute("select * from svn_log order by revision")

            for row in cursor.fetchall():
                if shallow:
                    files = None
                else:
                    files = self.__get_files(row["log_id"])

                metadata = self.find_id(self.__conn, row["project_id"])

                entry = SVNEntry(metadata, row["tag"], row["branch"],
                                 row["revision"], row["author"], row["date"],
                                 row["num_lines"], files,
                                 row["message"].split("\n"), row["git_branch"],
                                 row["git_hash"])

                if row["prev_revision"] is None or row["prev_revision"] == "":
                    prev_revision = None
                else:
                    prev_revision = int(row["prev_revision"])

                self.__add_entry_to_cache(entry, prev_revision)

    def load_from_log(self, debug=False, verbose=False):
        for _, dirname, dirurl in self.all_urls(ignore=self.__ignore_func):
            # get information about this SVN trunk/branch/tag
            try:
                metadata = SVNMetadata(dirurl)
            except CommandException as cex:
                if str(cex).find("W160013") >= 0 or \
                  str(cex).find("W170000") >= 0:
                    print("WARNING: Ignoring nonexistent SVN repository %s" %
                          (dirurl, ), file=sys.stderr)
                    continue
                raise

            if verbose:
                print("Loading log entries from %s(%s)" %
                      (metadata.project_name, dirurl))

            for _ in (0, 1, 2):
                try:
                    self.__load_log_entries(metadata, dirurl, dirname,
                                            debug=debug, verbose=verbose)
                    break
                except SVNConnectException:
                    continue

            if verbose:
                print("After %s, revision log contains %d entries" %
                      (dirname, self.total_entries))

    @property
    def name(self):
        return self.__name

    def num_entries(self, branch_name=None):
        "Return the number of cached SVN log entries"

        if branch_name is None:
            entry_gen = self.all_entries
        else:
            entry_gen = self.entries(branch_name)
        num = 0
        for _ in entry_gen:
            num += 1

        return num

    @property
    def path(self):
        return self.__path

    def save_revision(self, revision, git_branch, git_hash):
        if self.__cached_entries is None:
            self.load_from_db()

        if len(git_hash) <= 7:
            raise Exception("Cannot add short hash \"%s\" for %s rev %s" %
                            (git_hash, self.__name, revision))

        if revision not in self.__cached_entries:
            raise SVNException("Cannot find revision %d (for git %s/%s)" %
                               (revision, git_branch, git_hash))

        entry = self.__cached_entries[revision]

        need_update = False
        if entry.git_branch is None or entry.git_hash is None:
            need_update = True
        elif entry.git_branch != git_branch or entry.git_hash != git_hash:
            need_update = True

        if not need_update:
            return

        with self.__conn:
            cursor = self.__conn.cursor()

            cursor.execute("update svn_log set git_branch=?, git_hash=?"
                           " where project_id=? and revision=?",
                           (git_branch, git_hash, self.__project_id, revision))

        entry.git_branch = git_branch
        entry.git_hash = git_hash

    @property
    def total_entries(self):
        "Return the number of SVN log entries in the database"

        count = None
        with self.__conn:
            cursor = self.__conn.cursor()

            try:
                cursor.execute("select count(*) from svn_log")

                row = cursor.fetchone()
                if row is not None:
                    count = int(row[0])
            except sqlite3.OperationalError:
                count = 0

        return count

    def trim(self, revision):
        "Trim all entries earlier than 'revision'"
        with self.__conn:
            cursor = self.__conn.cursor()

            cursor.execute("select max(rowid) from svn_log"
                           " where revision<?", (revision, ))

            row = cursor.fetchone()
            if row is None:
                return

            log_id = int(row[0])

            cursor.execute("delete from svn_log where revision<?",
                           (revision, ))
            cursor.execute("delete from svn_log_file where log_id<?",
                           (log_id, ))
