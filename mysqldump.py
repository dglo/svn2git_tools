#!/usr/bin/env python

from __future__ import print_function

import gzip
import re
import sys


class MySQLException(Exception):
    "Generic MySQL exception"


class SQLColumnDef(object):
    NO_DEFAULT = "XXX_NO_DEFAULT_XXX"

    def __str__(self):
        if not self.__unsigned:
            ustr = ""
        else:
            ustr = " unsigned"

        if not self.__not_null:
            nstr = ""
        else:
            nstr = " NOT NULL"

        if self.__dflt_value == self.NO_DEFAULT:
            dstr = ""
        else:
            dstr = " default <%s>%s" % (type(self.__dflt_value),
                                        self.__dflt_value)
        return "%s[%s]*%s%s%s%s" % (self.__name, self.__type, self.__length,
                                    ustr, nstr, dstr)

    def __init__(self, fldname, fldtype, fldlen, fldmods):
        self.__name = fldname
        self.__type = fldtype
        self.__length = fldlen

        self.__unsigned = False
        self.__not_null = False
        self.__dflt_value = self.NO_DEFAULT

        state_not = 1
        state_dflt = 2

        state = None
        for keywd in fldmods.split():
            if keywd == "unsigned":
                self.__unsigned = True
            elif keywd == "NOT":
                state = state_not
            elif keywd == "NULL":
                if state == state_not:
                    self.__not_null = True
                elif state == state_dflt:
                    self.__dflt_value = None
                else:
                    raise MySQLException("State is %s for keyword \"%s\" in"
                                         " \"%s\"" % (state, keywd, fldmods))
                state = None
            elif state == state_not:
                raise MySQLException("Not handling \"NOT %s\"" % keywd)
            elif keywd == "DEFAULT":
                state = state_dflt
            elif state == state_dflt:
                self.__dflt_value = keywd
                state = None
            elif state is not None:
                raise MySQLException("State is %s for keyword \"%s\" in"
                                     " \"%s\"" % (state, keywd, fldmods))

    @property
    def field_type(self):
        return self.__type

    @property
    def name(self):
        return self.__name


class SQLTableDef(object):
    def __init__(self, name):
        self.__name = name
        self.__columns = []

        self.__data_table = None

    def __str__(self):
        if self.__data_table is None:
            dstr = ""
        else:
            dstr = "*%d" % len(self.__data_table)
        return self.__name + dstr

    def add_column(self, fldname, fldtype, fldlen, fldmods):
        self.__columns.append(SQLColumnDef(fldname, fldtype, fldlen, fldmods))

    def column(self, idx):
        return self.__columns[idx]

    @property
    def columns(self):
        for col in self.__columns:
            yield col

    @property
    def data_table(self):
        return self.__data_table

    @property
    def name(self):
        return self.__name

    def set_data_table(self, data_table):
        if self.__data_table is not None:
            raise MySQLException("Cannot change data table from <%s>%s"
                                 " to <%s>%s" %
                                 (type(self.__data_table), self.__data_table,
                                  type(data_table), data_table))

        self.__data_table = data_table


class DataRow(dict):
    def __init__(self, table):
        self.__table = table

        super(DataRow, self).__init__()

    def __getattr__(self, name):
        if name not in self:
            raise AttributeError("Unknown attribute \"%s\"" % (name, ))
        return self[name]

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        if name not in self:
            raise AttributeError("Unknown attribute \"%s\"" % (name, ))
        del self[name]

    def set_value(self, column, value):
        self[column.name] = value

    @property
    def table(self):
        return self.__table


class DataTable(object):
    def __init__(self):
        self.__rows = []

    def __len__(self):
        return len(self.__rows)

    def add_row(self, row_obj):
        self.__rows.append(row_obj)

    def create_row(self):
        return DataRow(self)

    def find(self, xid):
        for row in self.rows:
            if row.id == xid:
                yield row

    @property
    def rows(self):
        for row in self.__rows:
            yield row


class MySQLDump(object):
    CRE_TBL_PAT = re.compile(r"^CREATE\s+TABLE\s+`(\S+)`\s+\(\s*$")
    CRE_FLD_PAT = re.compile(r"^\s+`(\S+)` ([^\s\(]+)(?:\((\d+)\))?"
                             r"(\s+.*),\s*$")
    INS_TBL_PAT = re.compile(r"^INSERT\s+INTO\s+`(\S+)`\s+VALUES\s+"
                             r"\((.*)\);\s*$")

    ROW_PAT = re.compile(r"(?:[^\s,']|'(?:\\.|[^'])*')+")

    TABLES_TO_SAVE = []

    @classmethod
    def create_data_table(cls, name):
        return DataTable()

    @classmethod
    def parse_row(cls, rowstr, table):
        row_obj = table.data_table.create_row()

        for idx, vstr in enumerate(cls.ROW_PAT.findall(rowstr)):
            col = table.column(idx)
            if col.field_type.endswith("int"):
                value = int(vstr)
            elif col.field_type == "varchar" or col.field_type == "text":
                if vstr[0] == "'" and vstr[-1] == "'":
                    value = vstr[1:-1]
                else:
                    value = vstr
                value = value.replace("\\'", "'")
                value = value.replace('\\"', '"')
            else:
                raise MySQLException("Unknown field type \"%s\" for %s.%s" %
                                     (col.field_type, table.name, col.name))

            row_obj.set_value(col, value)

        table.data_table.add_row(row_obj)
        return row_obj

    @classmethod
    def read_mysqldump(cls, filename, tables_to_save, verbose=False):
        tables = {}

        with gzip.open(filename, "rb") as fin:
            cre_tbl = None
            for line in fin:
                line = line.decode("latin-1").rstrip()

                if cre_tbl is not None:
                    if line.startswith(")"):
                        cre_tbl = None
                    elif line.find(" KEY ") >= 0:
                        pass
                    else:
                        mtch = cls.CRE_FLD_PAT.match(line)
                        if mtch is None:
                            print("!!! Bad table field: %s" % (line, ),
                                  file=sys.stderr)
                            continue

                        fldname = mtch.group(1)
                        fldtype = mtch.group(2)
                        if mtch.group(3) is None:
                            fldlen = None
                        else:
                            fldlen = int(mtch.group(3))
                        fldmods = mtch.group(4)

                        cre_tbl.add_column(fldname, fldtype, fldlen, fldmods)

                    continue

                if line.find("CREATE TABLE ") == 0:
                    mtch = cls.CRE_TBL_PAT.match(line)
                    if mtch is None:
                        print("??? %s" % (line, ), file=sys.stderr)
                    else:
                        cre_tbl = SQLTableDef(mtch.group(1))
                        tables[cre_tbl.name] = cre_tbl
                    continue

                if line.find("INSERT INTO ") == 0:
                    mtch = cls.INS_TBL_PAT.match(line)
                    if mtch is None:
                        print("Bad match for line starting \"%s\"" % line[:50],
                              file=sys.stderr)
                        continue

                    tblname = mtch.group(1)
                    tblrows = mtch.group(2)

                    if tblname not in tables:
                        print("Cannot parse values for unknown table \"%s\"" %
                              (tblname, ), file=sys.stderr)
                        continue

                    ins_tbl = tables[tblname]
                    if ins_tbl.name not in tables_to_save:
                        continue

                    if ins_tbl.data_table is None:
                        dtbl = cls.create_data_table(ins_tbl.name)
                        ins_tbl.set_data_table(dtbl)

                    if verbose:
                        print("-- reading %s" % tblname)
                    for rowstr in tblrows.split("),("):
                        row = cls.parse_row(rowstr, ins_tbl)
                        if verbose:
                            print(unicode(row))

        return tables


if __name__ == "__main__":
    pass
