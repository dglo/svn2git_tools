#!/usr/bin/env python

from __future__ import print_function

import datetime
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

    DEPENDENCIES = (CATEGORY, PROJECT, USER, TEXT, BUGNOTE, BUGNOTE_TEXT)
    ALL_TABLES = (MAIN, ) + DEPENDENCIES


class MantisNote(object):
    def __init__(self, reporter, date_submitted, last_modified):
        self.__reporter = reporter
        self.__date_submitted = date_submitted
        self.__last_modified = last_modified

        self.__text = None

    @property
    def date_submitted(self):
        return self.__date_submitted

    @property
    def last_modified(self):
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
        self.__text = text.decode("utf-8")


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
        lines = unicode(text).split(r"\r\n")
        return "\r\n".join(lines).replace("\\\\", "\\").rstrip().encode("utf-8")

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
        for row in self.table.bugnote.rows:
            if row.bug_id != self.id:
                continue

            reporter = self.__find_name(self.table.users, row.reporter_id,
                                        "reporter_id", "username")
            cre_date = datetime.datetime.fromtimestamp(row.date_submitted)
            mod_date = datetime.datetime.fromtimestamp(row.last_modified)

            note = MantisNote(reporter, cre_date, mod_date)

            for trow in self.table.bugnote_text.rows:
                if row.bugnote_text_id == trow.id:
                    note.text = self.__fix_sql_text(trow.note)

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
    def __init__(self):
        self.__dependencies = {}

        super(MantisBugTable, self).__init__()

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
            return MantisBugTable()

        if name in MantisSchema.DEPENDENCIES:
            return super(MantisDump, cls).create_data_table(name)

        raise MySQLException("Unknown table \"%s\"" % name)
