#!/usr/bin/env python

from __future__ import print_function

import datetime
import os
import sys

from mysqldump import DataRow, DataTable, MySQLDump, MySQLException

# Python3 redefined 'unicode' to be 'str'
if sys.version_info[0] >= 3:
    unicode = str


class MantisSchema(object):
    MAIN = "mantis_bug_table"
    CATEGORY = "mantis_category_table"
    PROJECT = "mantis_project_table"
    USER = "mantis_user_table"
    TEXT = "mantis_bug_text_table"
    BUGNOTE = "mantis_bugnote_table"
    BUGNOTE_TEXT = "mantis_bugnote_text_table"

    IGNORED = ("mantis_bug_file_table", "mantis_bug_history_table",
               "mantis_bug_monitor_table", "mantis_bug_relationship_table",
               "mantis_bug_revision_table", "mantis_bug_tag_table",
               "mantis_config_table", "mantis_custom_field_project_table",
               "mantis_custom_field_string_table", "mantis_custom_field_table",
               "mantis_filters_table", "mantis_news_table",
               "mantis_plugin_table", "mantis_project_file_table",
               "mantis_project_hierarchy_table",
               "mantis_project_user_list_table",
               "mantis_project_version_table", "mantis_tag_table",
               "mantis_tokens_table", "mantis_user_pref_table",
               "mantis_user_profile_table")
    DEPENDENCIES = (CATEGORY, PROJECT, USER, TEXT, BUGNOTE, BUGNOTE_TEXT)
    ALL_TABLES = (MAIN, ) + DEPENDENCIES


class MantisNote(object):
    def __init__(self, reporter, submitted_tstamp, modified_tstamp):
        self.__reporter = reporter
        self.__submitted_tstamp = submitted_tstamp
        self.__modified_tstamp = modified_tstamp

        self.__date_submitted = None
        self.__last_modified = None

        self.__text = None

    @property
    def date_submitted(self):
        if self.__date_submitted is None:
            self.__date_submitted = \
              datetime.datetime.fromtimestamp(self.__submitted_tstamp)
        return self.__date_submitted

    @property
    def last_modified(self):
        if self.__last_modified is None:
            self.__last_modified = \
              datetime.datetime.fromtimestamp(self.__modified_tstamp)
        return self.__last_modified

    @property
    def reporter(self):
        return self.__reporter

    @property
    def text(self):
        return self.__text

    @text.setter
    def text(self, text):
        if text is None:
            raise MySQLException("Cannot set note text to None")

        if self.__text is not None:
            raise MySQLException("Cannot overwrite note text")
        self.__text = text


class MantisIssue(DataRow):
    STATUS_NEW = 10
    STATUS_FEEDBACK = 20
    STATUS_ACKNOWLEDGED = 30
    STATUS_CONFIRMED = 40
    STATUS_ASSIGNED = 50
    STATUS_RESOLVED = 80
    STATUS_CLOSED = 90

    def __init__(self, table):
        super(MantisIssue, self).__init__(table)

        self.__project_name = None
        self.__category_name = None
        self.__reporter_name = None

        self.__date_submitted = None
        self.__last_updated = None

        self.__loaded_text = False
        self.__description = None
        self.__steps_to_reproduce = None
        self.__additional_information = None

        self.__loaded_notes = False
        self.__notes = None

        self.__priority = None
        self.__status = None

    def __str__(self):
        return "Issue#%d" % self.id

    @classmethod
    def __find_name(cls, table, row_id, id_name, fldname="name"):
        name = None
        if table is not None:
            for row in table.find(row_id):
                if name is None:
                    name = row[fldname]
                else:
                    raise MySQLException("Found multiple entries for %s #%d" %
                                         (id_name, row_id, ))
        return name

    @classmethod
    def __fix_sql_text(cls, text):
        if text is None:
            return None
        if isinstance(text, bytes):
            text = text.decode("utf-8", "ignore")
        lines = text.split(r"\r\n")
        fixed = "\r\n".join(lines).replace("\\\\", "\\").rstrip()
        return fixed

    def __load_from_text(self):
        self.__loaded_text = True

        if self.table.text is None:
            return

        found_one = False
        for row in self.table.text.find(self.id):
            if found_one:
                raise MySQLException("Found multiple entries for text #%d" %
                                     (self.id, ))
            found_one = True

            self.__description = self.__fix_sql_text(row.description)
            self.__steps_to_reproduce = \
              self.__fix_sql_text(row.steps_to_reproduce)
            self.__additional_information = \
              self.__fix_sql_text(row.additional_information)

    def __load_notes(self):
        self.__loaded_notes = True

        if self.table.bugnote is None or self.table.bugnote_text is None:
            print("!!! NO NOTES")  # XXX
            return

        notes = None
        for row in self.table.bugnote.find(self.id, "bug_id"):
            reporter = self.__find_name(self.table.users, row.reporter_id,
                                        "reporter_id", "username")

            note = MantisNote(reporter, row.date_submitted, row.last_modified)

            for trow in self.table.bugnote_text.find(row.bugnote_text_id):
                note.text = self.__fix_sql_text(trow.note)
                break

            if notes is None:
                notes = []
            notes.append(note)

        self.__notes = notes

    @property
    def additional_information(self):
        if not self.__loaded_text:
            self.__load_from_text()
        return self.__additional_information

    @property
    def category(self):
        if self.__category_name is None:
            self.__category_name = self.__find_name(self.table.categories,
                                                    self.category_id,
                                                    "category_id")
        return self.__category_name

    @property
    def date_submitted(self):
        if self.__date_submitted is None:
            self.__date_submitted = \
              datetime.datetime.fromtimestamp(self["date_submitted"])

        return self.__date_submitted

    @property
    def description(self):
        if not self.__loaded_text:
            self.__load_from_text()
        return self.__description

    def dump(self):
        print("%s:%s #%d" % (self.project_name, self.category_name, self.id))

    @property
    def has_notes(self):
        if not self.__loaded_notes:
            self.__load_notes()

        return self.__notes is not None

    @property
    def is_closed(self):
        return self["status"] == self.STATUS_CLOSED

    @property
    def is_resolved(self):
        return self["status"] == self.STATUS_RESOLVED

    @property
    def last_updated(self):
        if self.__last_updated is None:
            self.__last_updated = \
              datetime.datetime.fromtimestamp(self["last_updated"])

        return self.__last_updated

    @property
    def notes(self):
        if not self.__loaded_notes:
            self.__load_notes()

        if self.__notes is not None:
            for note in self.__notes:
                yield note

    @property
    def priority(self):
        if self.__priority is None:
            if self["priority"] == 10:
                self.__priority = "none"
            elif self["priority"] == 20:
                self.__priority = "low"
            elif self["priority"] == 30:
                self.__priority = "normal"
            elif self["priority"] == 40:
                self.__priority = "high"
            elif self["priority"] == 50:
                self.__priority = "urgent"
            elif self["priority"] == 80:
                self.__priority = "immediate"
            else:
                self.__priority = "??%s??" % (self["priority"], )
        return self.__priority

    @property
    def project(self):
        if self.__project_name is None:
            self.__project_name = self.__find_name(self.table.projects,
                                                   self.project_id,
                                                   "project_id")
        return self.__project_name

    @property
    def reporter(self):
        if self.__reporter_name is None:
            self.__reporter_name = self.__find_name(self.table.users,
                                                    self.reporter_id,
                                                    "reporter_id", "username")
            # handle missing users
            if self.__reporter_name is None:
                self.__reporter_name = "user%d" % self.reporter_id

        return self.__reporter_name

    @property
    def status(self):
        if self.__status is None:
            if self["status"] == self.STATUS_NEW:
                self.__status = "new"
            elif self["status"] == self.STATUS_FEEDBACK:
                self.__status = "feedback"
            elif self["status"] == self.STATUS_ACKNOWLEDGED:
                self.__status = "acknowledged"
            elif self["status"] == self.STATUS_CONFIRMED:
                self.__status = "confirmed"
            elif self["status"] == self.STATUS_ASSIGNED:
                self.__status = "assigned"
            elif self["status"] == self.STATUS_RESOLVED:
                self.__status = "resolved"
            elif self["status"] == self.STATUS_CLOSED:
                self.__status = "closed"
            else:
                self.__status = "??%s??" % (self["status"], )
        return self.__status

    @property
    def steps_to_reproduce(self):
        if not self.__loaded_text:
            self.__load_from_text()
        return self.__steps_to_reproduce


class MantisBugTable(DataTable):
    def __init__(self, name):
        self.__dependencies = {}

        super(MantisBugTable, self).__init__(name)

    def add_table(self, tblname, data_table):
        if tblname in MantisSchema.DEPENDENCIES:
            self.__dependencies[tblname] = data_table
        else:
            raise MySQLException("Cannot add unknown Mantis table \"%s\"" %
                                 (tblname, ))

    @property
    def bugnote(self):
        return self.__dependencies[MantisSchema.BUGNOTE]

    @property
    def bugnote_text(self):
        return self.__dependencies[MantisSchema.BUGNOTE_TEXT]

    @property
    def categories(self):
        return self.__dependencies[MantisSchema.CATEGORY]

    def create_row(self):
        return MantisIssue(self)

    @property
    def projects(self):
        return self.__dependencies[MantisSchema.PROJECT]

    @property
    def text(self):
        return self.__dependencies[MantisSchema.TEXT]

    @property
    def users(self):
        return self.__dependencies[MantisSchema.USER]


class MantisDump(MySQLDump):
    @classmethod
    def create_data_table(cls, name):
        if name == MantisSchema.MAIN:
            return MantisBugTable(name)

        if name in MantisSchema.DEPENDENCIES:
            return super(MantisDump, cls).create_data_table(name)

        if name not in MantisSchema.IGNORED:
            print("WARNING: Ignoring unknown Mantis table \"%s\"" % name)
        return None


    @classmethod
    def convert_to_sqlite3(cls, dump_path, debug=False, verbose=False):
        all_tbls = MantisSchema.ALL_TABLES
        for table in MantisDump.read_mysqldump(dump_path,
                                               include_list=all_tbls,
                                               debug=debug, verbose=verbose):
            if verbose:
                print("-- found %s" % (table.name, ))

            #cre_cmd = "create table if not exists %s(" % (table.name, )



def main():
    debug = False
    verbose = False
    filenames = []
    for arg in sys.argv[1:]:
        if arg == "-v":
            verbose = True
            continue

        if arg == "-x":
            debug = True
            continue

        if not os.path.exists(arg):
            print("ERROR: Cannot find \"%s\"" % (arg, ), file=sys.stderr)
            continue

        filenames.append(arg)

    for filename in filenames:
        MantisDump.convert_to_sqlite3(filename, debug=debug, verbose=verbose)

if __name__ == "__main__":
    main()
