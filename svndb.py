#!/usr/bin/env python3
"""
Manage the SVN log database
"""

from __future__ import print_function

import os
import sqlite3
import sys

from datetime import datetime

from dictobject import DictObject
from svn import SVNException, SVNMetadata


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


class SVNEntry(DictObject):
    "Object containing information from a single Subversion log entry"

    def __init__(self, metadata, tag_name, branch_name, revision, author,
                 date_string, num_lines, files, loglines, git_branch=None,
                 git_hash=None):
        super(SVNEntry, self).__init__()

        self.metadata = metadata
        self.tag_name = tag_name
        self.branch_name = branch_name
        self.revision = revision
        self.author = author
        self.date_string = date_string
        self.num_lines = num_lines
        self.filelist = files[:]
        self.loglines = loglines[:]
        self.git_branch = git_branch
        self.git_hash = git_hash

        self.__previous = None
        self.__datetime = None

    def __str__(self):
        if self.__previous is None:
            pstr = ""
        else:
            pstr = ">>%s#%d" % (self.__previous.tag_name,
                                self.__previous.revision)

        return "%s#%d@%s*%d%s" % (self.tag_name, self.revision,
                                  self.date_string, len(self.filelist), pstr)

    def check_duplicate(self, svn_log, verbose=True):
        """
        Compare this object against another log entry object, returning True
        if it contains the same information.  If 'verbose' is True, also
        print a message with details of the first difference
        """

        if self.revision != svn_log.revision:
            if verbose:
                print("Rev#%d != #%d" % (self.revision, svn_log.revision),
                      file=sys.stderr)
            return False

        if self.author != svn_log.author:
            if verbose:
                print("Rev#%d duplicate mismatch: Author \"%s\" != \"%s\"" %
                      (self.revision, svn_log.author, self.author),
                      file=sys.stderr)
            return False

        if self.date_string != svn_log.date_string:
            if verbose:
                print("Rev#%d duplicate mismatch: Date \"%s\" != \"%s\"" %
                      (self.revision, svn_log.date_string,
                       self.date_string), file=sys.stderr)
            return False

        if self.num_lines != svn_log.num_lines:
            if verbose:
                print("Rev#%d duplicate mismatch: Number of lines %d != %d" %
                      (self.revision, svn_log.num_lines, self.num_lines),
                      file=sys.stderr)
            return False

        return True

    @property
    def datetime(self):
        if self.__datetime is None and self.date_string is not None:
            idx = self.date_string.find(" (")
            if idx <= 0:
                dstr = self.date_string
            else:
                dstr = self.date_string[:idx]

            self.__datetime = datetime.strptime(dstr, "%Y-%m-%d %H:%M:%S %z")

        return self.__datetime

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


class SVNRepositoryDB(object):
    """
    Manage the SVN log database
    """

    def __init__(self, metadata_or_svn_url, allow_create=True, directory=None):
        """
        Open (and possibly create) the SVN database for this project
        metadata_or_svn_url - either an SVNMetadata object or a Subversion URL
        """

        if isinstance(metadata_or_svn_url, SVNMetadata):
            self.__metadata = metadata_or_svn_url
        else:
            self.__metadata = SVNMetadata(metadata_or_svn_url)

        self.__project = self.__metadata.project_name

        if directory is None:
            directory = "."
        relpath = os.path.join(directory, "%s-svn.db" % (self.__project, ))
        self.__path = os.path.abspath(relpath)

        self.__conn = self.__open_db(self.__path, allow_create=allow_create)
        self.__project_id = self.__add_project_to_db()

        self.__top_url = None
        self.__cached_entries = None

    def __str__(self):
        return "SVNRepositoryDB(%s#%s: %s)" % \
          (self.__project, self.__project_id, self.__metadata)

    def __add_project_to_db(self):
        "Add this project to the svn_project table"

        with self.__conn:
            cursor = self.__conn.cursor()

            for first_attempt in (True, False):
                cursor.execute("select * from svn_project where name=?",
                               (self.__metadata.project_name, ))

                row = cursor.fetchone()
                if row is not None:
                    return row["project_id"]

                if first_attempt:
                    mdt = self.__metadata
                    cursor.execute("insert into svn_project(name,"
                                   " original_url, root_url,"
                                   " base_subdir, trunk_subdir,"
                                   " branches_subdir, tags_subdir)"
                                   " values (?, ?, ?, ?, ?, ?, ?)",
                                   (mdt.project_name, mdt.original_url,
                                    mdt.root_url, mdt.base_subdir,
                                    mdt.trunk_subdir, mdt.branches_subdir,
                                    mdt.tags_subdir))

        raise SVNException("Cannot find or add \"%s\" in the database" %
                           (self.__metadata.project_name, ))

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

    def __load_entries(self):
        with self.__conn:
            cursor = self.__conn.cursor()

            cursor.execute("select * from svn_log order by revision")

            entries = {}
            for row in cursor.fetchall():
                metadata = self.find_id(self.__conn, row["project_id"])

                files = self.__get_files(row["log_id"])

                entry = SVNEntry(metadata, row["tag"], row["branch"],
                                 row["revision"], row["author"], row["date"],
                                 row["num_lines"], files,
                                 row["message"].split("\n"),
                                 row["git_branch"], row["git_hash"])
                entries[entry.revision] = entry

                if row["prev_revision"] is not None:
                    if row["prev_revision"] not in entries:
                        print("WARNING: Cannot find previous %s revision #%d"
                              " for revision #%d" %
                              (self.__project, row["prev_revision"],
                               row["revision"]))
                    else:
                        entry.set_previous(entries[row["prev_revision"]])

        self.__cached_entries = entries

    @classmethod
    def __open_db(cls, path, allow_create=False):
        """
        If necessary, create the tables in the SVN database, then return
        a connection object
        """

        conn = sqlite3.connect(path)

        conn.row_factory = sqlite3.Row

        if allow_create:
            with conn:
                cursor = conn.cursor()

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
                               " date TEXT NOT NULL,"
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

        return conn

    def add_git_commit(self, revision, git_branch, git_hash):
        if self.__cached_entries is None:
            self.__load_entries()

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
    def all_entries(self):
        "Iterate through all SVN log entries in the database"
        if self.__cached_entries is None:
            self.__load_entries()

        for _, entry in sorted(self.__cached_entries.items(),
                               key=lambda x: x[0]):
            yield entry

    def entries(self, branch_name):
        "Iterate through SVN log entries in the database"

        if branch_name is None or branch_name == "":
            branch_name = SVNMetadata.TRUNK_NAME

        for entry in self.all_entries:
            if entry.branch_name == branch_name:
                yield entry

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

    def find_revision(self, revision):
        with self.__conn:
            cursor = self.__conn.cursor()

            if revision is None:
                cursor.execute("select revision, git_branch, git_hash"
                               " from svn_log"
                               " order by revision desc limit 1")
            else:
                cursor.execute("select revision, git_branch, git_hash"
                               " from svn_log"
                               " where revision<=? order by revision desc"
                               " limit 1", (revision, ))

            row = cursor.fetchone()
            if row is None:
                return (None, None, None)
            if len(row) != 3:
                raise SVNException("Expected 3 columns, not %d" % (len(row), ))
            if row[0] is None:
                raise SVNException("No revision found in %s" % (row, ))

            return int(row[0]), row[1], row[2]

    def num_entries(self, branch_name=None):
        "Return the number of SVN log entries in the database"

        with self.__conn:
            cursor = self.__conn.cursor()

            if branch_name is None:
                cursor.execute("select count(*) from svn_log")
            else:
                cursor.execute("select count(*) from svn_log where branch=?",
                               (branch_name, ))

            row = cursor.fetchone()
            if row is None:
                return None

            return int(row[0])

    @property
    def path(self):
        return self.__path

    @property
    def project(self):
        return self.__metadata.project_name

    @property
    def metadata(self):
        return self.__metadata

    def save_entry(self, entry):
        "Save a single SVN log entry to the database"

        with self.__conn:
            cursor = self.__conn.cursor()

            if entry.previous is None:
                prev_rev = None
            else:
                prev_rev = entry.previous.revision

            message = None
            for line in entry.loglines:
                if message is None:
                    message = line
                else:
                    message += "\n" + line

            cursor.execute("insert into svn_log(project_id, tag, branch,"
                           " revision, author, date, num_lines, message,"
                           " prev_revision, git_branch, git_hash)"
                           " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                           (self.__project_id, entry.tag_name,
                            entry.branch_name, entry.revision, entry.author,
                            entry.date_string, entry.num_lines, message,
                            prev_rev, entry.git_branch, entry.git_hash))

            log_id = cursor.lastrowid

            for action, filename in entry.filelist:
                cursor.execute("insert into svn_log_file(log_id, action, file)"
                               " values (?, ?, ?)", (log_id, action, filename))

    @property
    def top_url(self):
        return self.__metadata.project_url

    @property
    def total_entries(self):
        "Return the number of SVN log entries in the database"

        count = None
        with self.__conn:
            cursor = self.__conn.cursor()

            cursor.execute("select count(*) from svn_log")

            row = cursor.fetchone()
            if row is not None:
                count = int(row[0])

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


    @property
    def trunk_url(self):
        return self.__metadata.trunk_url
