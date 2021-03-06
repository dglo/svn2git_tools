#!/usr/bin/env python

from __future__ import print_function

import os
import re
import sqlite3
import sys

from decorators import classproperty
from dictobject import DictObject
from i3helper import Comparable
from svn import SVNDate, SVNMetadata, svn_list, svn_log

# Python3 redefined 'unicode' to be 'str'
if sys.version_info[0] >= 3:
    unicode = str


class DBException(Exception):
    "General database exception"


class AuthorDB(object):
    # mapping from SVN users to Git authors
    __AUTHORS_FILENAME = None
    __AUTHORS = {}

    @classproperty
    def filename(cls):
        return cls.__AUTHORS_FILENAME

    @classmethod
    def get_author(cls, username):
        """
        Return the Git author associated with this Subversion username
        """
        if cls.__AUTHORS_FILENAME is None:
            raise DBException("Authors have not been loaded")

        if username not in cls.__AUTHORS:
            raise DBException("No author found for Subversion username"
                              " \"%s\"" % (username, ))

        return cls.__AUTHORS[username]

    @classmethod
    def has_author(cls, username):
        """
        Return True if a Git author is associated with this Subversion username
        """
        if cls.__AUTHORS_FILENAME is None:
            raise DBException("Authors have not been loaded")

        return username in cls.__AUTHORS

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


class SVNEntry(Comparable, DictObject):
    "Object containing information from a single Subversion log entry"

    def __init__(self, tag_name, branch_name, revision, author, svn_date,
                 num_lines, files, loglines, git_branch=None, git_hash=None):
        super(SVNEntry, self).__init__()

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
        self.__saved = False

        self.__previous = None

    def __str__(self):
        if self.filelist is None:
            fstr = "[not loaded]"
        else:
            fstr = unicode(len(self.filelist))

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

    def clear_saved(self):
        self.__saved = False

        # clear data from previous runs
        self.__previous = None
        self.git_hash = None
        self.git_branch = None

    @property
    def compare_key(self):
        """
        Return a tuple which Comparable can use to compare entries
        """
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
        if self.loglines is not None:
            for line in self.loglines:
                if message is None:
                    message = str(line)
                else:
                    message += "\n" + str(line)
        if message is None:
            message = ""
        return message

    @property
    def previous(self):
        "Return the previous log entry"
        return self.__previous

    @property
    def is_saved(self):
        return self.__saved

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

    def set_saved(self, val=True):
        self.__saved = val


class ProjectDatabase(object):
    def __init__(self, project_name, allow_create=False, directory=".",
                 ignore_func=None):
        # remember this project's name
        self.__name = project_name
        self.__ignore_func = ignore_func

        # build the database file name
        basename = "%s-svn.db" % (project_name, )

        # find the absolute path to the database file
        path = os.path.abspath(os.path.join(directory, basename))

        # if not trying to create the database, die if it doesn't exist
        if not allow_create and not os.path.exists(path):
            raise DBException("%s database does not exist\n\t(%s)" %
                              (project_name, path))

        # open the database file
        self.__conn = sqlite3.connect(path)
        self.__conn.row_factory = sqlite3.Row

        # if necessary, create all the tables
        if allow_create:
            self.__create_tables()

        # dictionary mapping SVN revision numbers to SVN log entries
        self.__cached_entries = None

        # dictionary mapping project URLs to their date returned by svn_list()
        self.__cached_urls = None

    def __create_tables(self):
        with self.__conn:
            cursor = self.__conn.cursor()

            cursor.execute("create table if not exists svn_log("
                           " revision INTEGER NOT NULL PRIMARY KEY,"
                           " tag TEXT NOT NULL,"
                           " branch TEXT NOT NULL, "
                           " author TEXT NOT NULL,"
                           " date TIMESTAMP NOT NULL,"
                           " num_lines INTEGER,"
                           " message TEXT,"
                           " prev_revision INTEGER,"
                           " git_branch TEXT,"
                           " git_hash TEXT)")

            cursor.execute("create table if not exists svn_log_file("
                           " logfile_id INTEGER PRIMARY KEY,"
                           " revision INTEGER,"
                           " action TEXT NOT NULL,"
                           " file TEXT NOT NULL,"
                           " FOREIGN KEY(revision) REFERENCES"
                           " svn_log(revision))")

    def __get_files(self, revision):
        """
        Return a list of all (action, filename) pairs from the log message
        """

        cursor = self.__conn.cursor()
        cursor.execute("select action, file from svn_log_file"
                       " where revision=? order by logfile_id",
                       (revision, ))

        files = []
        for row in cursor.fetchall():
            files.append((row["action"], row["file"]))

        return files

    def __list_svn_url(self, url, debug=False):
        ignored = []
        for _, _, svn_date, name in svn_list(url, list_verbose=True):
            if name.endswith("/"):
                name = name[:-1]

                # ignore entry for current directory
                yield name, svn_date
                continue

            ignored.append(name)

        if debug:
            num_ignored = len(ignored)
            if num_ignored > 0:
                if num_ignored == 1:
                    noun = "entry"
                else:
                    noun = "entries"
                print("WARNING: Ignoring %s file %s %s" %
                      (self.__name, noun, ", ".join(ignored)), file=sys.stderr)

    def __save_entry_to_database(self, entry):
        "Save a single SVN log entry to the database"

        if entry.filelist is None:
            raise DBException("File list has not been loaded")

        # if it exists, get previouos revision number
        if entry.previous is None:
            prev_revision = None
        else:
            prev_revision = entry.previous.revision

        with self.__conn:
            cursor = self.__conn.cursor()

            try:
                cursor.execute("insert into svn_log(revision, tag, branch,"
                               " author, date, num_lines, message,"
                               " prev_revision, git_branch, git_hash)"
                               " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                               (entry.revision, entry.tag_name,
                                entry.branch_name, entry.author, entry.date,
                                entry.num_lines, entry.log_message,
                                prev_revision, entry.git_branch,
                                entry.git_hash))
            except sqlite3.IntegrityError:
                # entry exists, update it with the new data
                cursor.execute("update svn_log set tag=?, branch=?, author=?,"
                               " date=?, num_lines=?, message=?,"
                               " prev_revision=?, git_branch=?, git_hash=?"
                               " where revision=?",
                               (entry.tag_name, entry.branch_name,
                                entry.author, entry.date, entry.num_lines,
                                entry.log_message, prev_revision,
                                entry.git_branch, entry.git_hash,
                                entry.revision))

            for action, filename in entry.filelist:
                cursor.execute("insert into svn_log_file(revision, action,"
                               " file) values (?, ?, ?)",
                               (entry.revision, action, filename))

        entry.set_saved(True)

    def __save_log_entries(self, url, branch, save_to_db=False, verbose=False):
        next_entry = None

        # if the branch name contains a slash separator,
        #  assume the final element is the release/branch name
        idx = branch.find("/")
        if idx < 0:
            tag_name = branch
        else:
            tag_name = branch[idx+1:]

        if verbose:
            print("%s %s:%s" % ("Saving" if save_to_db else "Loading",
                                self.__name, branch))

        # build the log entry generator as a standalone object
        #  so we can close it if we exit the loop early
        log_gen = svn_log(url, revision="HEAD", end_revision=1)

        # loop through all the log entries
        for logentry in log_gen:
            # if we've already seen this entry, then we've definitely seen
            #  all the earlier entries
            if self.__cached_entries is not None and \
              logentry.revision in self.__cached_entries:
                # since there are no more interesting entries, exit the loop
                break

            entry = SVNEntry(tag_name, branch, logentry.revision,
                             logentry.author, logentry.date_string,
                             logentry.num_lines, logentry.filedata,
                             logentry.loglines)

            # if necessary, initialize the cache dictionary
            if self.__cached_entries is None:
                self.__cached_entries = {}

            # save this entry
            self.__cached_entries[entry.revision] = entry
            if save_to_db:
                self.__save_entry_to_database(entry)

            # remember this entry for the next trip through the loop
            next_entry = entry

        # close the generator so it cleans up the `svn log` process
        log_gen.close()

    @property
    def all_entries(self):
        "Iterate through all cached SVN log entries, ordered by key"
        for entry in self.entries():
            yield entry

    def get_cached_entry(self, revision):
        if self.__cached_entries is None or \
          revision not in self.__cached_entries:
            return None

        return self.__cached_entries[revision]

    def close(self):
        if self.__conn is not None:
            self.__conn.close()
            self.__conn = None
        if self.__cached_entries is not None:
            self.__cached_entries = None
        #if self.__urls_by_date is not None:
        #    self.__urls_by_date = None

    def entries(self, branch_name=None):
        "Iterate through cached SVN log entries matching 'branch_name'"

        if branch_name is None or branch_name == "":
            branch_name = SVNMetadata.TRUNK_NAME

        for _, entry in sorted(self.__cached_entries.items(),
                               key=lambda x: x[0]):
            if entry.branch_name == branch_name:
                if self.__ignore_func is not None and \
                  not self.__ignore_func(entry.branch_name):
                    yield entry

    def find_first_revision(self, branch_name):
        if self.__cached_entries is None:
            raise DBException("%s project database has not been loaded" %
                              (self.__name, ))

        for entry in sorted(self.__cached_entries.values(),
                            key=lambda x: x.date):
            if entry.branch_name == branch_name:
                return entry.revision

        return None

    def find_hash_from_revision(self, svn_branch, revision,
                                with_git_hash=False):
        """
        Look for the Git branch and hash associated with 'revision'.
        If 'revision' is None, find the latest revision.
        Return (git_branch, git_hash, svn_branch, svn_revision)
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
                rev_query_str = ""
            else:
                rev_query_str = " and revision<=%s" % (revision, )

            cursor.execute("select git_branch, git_hash, branch, revision"
                           " from svn_log where branch=?" +
                           rev_query_str + hash_query_str +
                           " order by revision desc limit 1",
                           (svn_branch, ))

            row = cursor.fetchone()
            if row is None:
                return None
            if len(row) != 4:
                raise DBException("Expected 4 columns, not %d" % (len(row), ))
            if row[2] is None:
                raise DBException("No revision found in %s" % (row, ))

            if len(row[1]) == 7:
                raise DBException("Found short hash %s for %s %s rev %s" %
                                  (row[1], self.__name, svn_branch, revision))

            return row[0], row[1], row[2], int(row[3])

    def find_previous_revision(self, branch_name, entry):
        """
        Look for the first Git branch and hash preceeding 'revision'.
        If 'revision' is None, find the latest revision.
        Return (svn_branch, svn_revision, git_branch, git_hash) or
        throw an exception if not found.
        """
        saved_entry = entry

        while True:
            result = self.find_hash_from_revision(branch_name, entry.revision,
                                                  with_git_hash=True)
            if result is None or result[1] is None and result[2] is None and \
              branch_name != SVNMetadata.TRUNK_NAME:
                result = self.find_hash_from_revision(SVNMetadata.TRUNK_NAME,
                                                      entry.revision,
                                                      with_git_hash=True)

            if result is not None and result[0] is not None and \
              result[1] is not None:
                git_branch, git_hash, _, _ = result
                return entry.branch_name, entry.revision, git_branch, git_hash

            if entry.previous is None:
                raise DBException("Cannot find committed ancestor for"
                                  " %s SVN r%s (started from r%s)" %
                                  (self.name, entry.revision,
                                   saved_entry.revision))
            entry = entry.previous

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

            # check for first entry before 'date' on a branch
            cursor.execute("select revision from svn_log"
                           " where branch=? and date<=?" + hash_query_str +
                           " order by date desc limit 1",
                           (svn_branch, date_string, ))
            row = cursor.fetchone()

            if row is None and svn_branch != SVNMetadata.TRUNK_NAME:
                # check for first entry before 'date' on trunk
                cursor.execute("select revision from svn_log"
                               " where branch=? and date<=?" +
                               hash_query_str +
                               " order by date desc limit 1",
                               (SVNMetadata.TRUNK_NAME, date_string, ))
                row = cursor.fetchone()

            if row is None:
                # if we didnt find anything, return the first revision
                cursor.execute("select revision from svn_log"
                               " where branch=?" + hash_query_str +
                               " order by revision asc limit 1",
                               (svn_branch, ))

                row = cursor.fetchone()

            if row is None:
                return None

            return int(row[0])

    def has_cached_entry(self, revision):
        if self.__cached_entries is None:
            return False

        return revision in self.__cached_entries

    @property
    def has_unknown_authors(self):
        unknown = False
        for entry in self.all_entries:
            if not AuthorDB.has_author(entry.author):
                print("SVN committer \"%s\" missing from \"%s\"" %
                      (entry.author, AuthorDB.filename), file=sys.stderr)
                unknown = True

        return unknown

    @property
    def is_loaded(self):
        return self.__cached_entries is not None

    def load_database_entries(self, shallow=False):
        """
        If 'shallow' is False, do NOT extract list of files for each entry
        """
        if self.__cached_entries is not None:
            raise DBException("Entries for %s have already been loaded" %
                              (self.__name, ))

        self.__cached_entries = {}
        with self.__conn:
            cursor = self.__conn.cursor()

            cursor.execute("select * from svn_log order by revision")

            for row in cursor.fetchall():
                if shallow:
                    files = None
                else:
                    files = self.__get_files(int(row["revision"]))

                entry = SVNEntry(row["tag"], row["branch"], row["revision"],
                                 row["author"], row["date"], row["num_lines"],
                                 files, row["message"].split("\n"),
                                 row["git_branch"], row["git_hash"])

                if row["prev_revision"] is not None and \
                  row["prev_revision"] != "":
                    prev_num = int(row["prev_revision"])
                    if prev_num not in self.__cached_entries:
                        raise DBException("Missing previous revision %d for"
                                          " %s revision %d" %
                                          (prev_num, self.__name,
                                           entry.revision))

                    entry.set_previous(self.__cached_entries[prev_num])

                # if necessary, initialize the cache dictionary
                if self.__cached_entries is None:
                    self.__cached_entries = {}

                # save this entry
                self.__cached_entries[entry.revision] = entry

    def load_log_entries(self, url, save_to_db=False, verbose=False):
        if save_to_db:
            self.trim()

        for dirname, sub_url, _ in self.project_urls(url):
            self.__save_log_entries(sub_url, dirname, save_to_db=save_to_db,
                                    verbose=verbose)

    @property
    def name(self):
        return self.__name

    def num_entries(self, branch_name=None):
        "Return the number of cached SVN log entries"

        # if we don't have any entries, return a bogus count
        if self.__cached_entries is None:
            return -1

        # if no branch was specified, return the count of all entries
        if branch_name is None:
            return len(self.__cached_entries)

        # count the number of entries matching 'branch_name'
        total = 0
        for entry in self.__cached_entries.values():
            if entry.branch_name == branch_name:
                total += 1

        return total

    def project_urls(self, url, verbose=False):
        # load file entries found at 'url'
        entries = {}
        for filename, svn_date in self.__list_svn_url(url):
            entries[filename] = svn_date

        found = False
        for dirname in (SVNMetadata.TRUNK_NAME, SVNMetadata.TAG_NAME,
                        SVNMetadata.BRANCH_NAME):
            # if this subdirectory doesn't exist, skip it
            if dirname not in entries:
                continue

            sub_url = "/".join((url, dirname))
            if dirname == SVNMetadata.TRUNK_NAME:
                found = True
                yield dirname, sub_url, entries[dirname]
            else:
                for filename, svn_date in sorted(self.__list_svn_url(sub_url),
                                                 key=lambda x: x[1]):
                    if filename == ".":
                        continue

                    branch = "/".join((dirname, filename))
                    found = True
                    yield branch, "/".join((url, branch)), svn_date

        if found == 0:
            yield SVNMetadata.TRUNK_NAME, url, entries["."]

    def save_revision(self, revision, git_branch, git_hash):
        if self.__cached_entries is None:
            raise DBException("Project %s database has not been loaded" %
                              (self.__name, ))

        if len(git_hash) <= 7:
            raise DBException("Cannot add short hash \"%s\" for %s rev %s" %
                              (git_hash, self.__name, revision))

        if revision not in self.__cached_entries:
            raise DBException("Cannot find revision %d (for git %s/%s)" %
                              (revision, git_branch, git_hash))

        entry = self.__cached_entries[revision]
        if not entry.is_saved:
            self.__save_entry_to_database(entry)

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
                           " where revision=?",
                           (git_branch, git_hash, revision))

        entry.git_branch = git_branch
        entry.git_hash = git_hash

    def trim(self, revision=None):
        "Trim all entries earlier than 'revision'"
        with self.__conn:
            cursor = self.__conn.cursor()

            cursor.execute("delete from svn_log")
            cursor.execute("delete from svn_log_file")

        if self.__cached_entries is not None:
            for entry in self.__cached_entries.values():
                entry.clear_saved()

    def update_previous_in_database(self, entry):
        "Update the previous revision in the database"

        # die if there's no previouos entry
        if entry.previous is None:
            raise DBException("%s rev %s previous revision has not been set" %
                              (self.__name, entry.revision))

        with self.__conn:
            cursor = self.__conn.cursor()

            cursor.execute("update svn_log set prev_revision=?"
                           " where revision=?",
                           (entry.previous.revision, entry.revision))

            if cursor.rowcount == 0:
                raise Exception("Failed to update %s rev %s" %
                                (self.__name, revision))


def main():
    # the pDAQ projects store tags under the 'releases' subdirectory
    SVNMetadata.set_layout(SVNMetadata.DIRTYPE_TAGS, "releases")

    for prj in sys.argv[1:]:
        if prj == "pdaq":
            top_url = "http://code.icecube.wisc.edu/daq/meta-projects/%s" % \
              (prj, )
        elif prj == "fabric-common":
            top_url = "http://code.icecube.wisc.edu/svn/projects/%s" % (prj, )
        else:
            top_url = "http://code.icecube.wisc.edu/daq/projects/%s" % (prj, )

        prj_db = ProjectDatabase(prj, allow_create=True)
        print("=== %s" % (prj, ))
        for name, url, svn_date in prj_db.project_urls(top_url):
            print("%s[%s]\n  %s" % (name, svn_date, url))


if __name__ == "__main__":
    main()
