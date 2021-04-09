#!/usr/bin/env python

from __future__ import print_function

import gzip
import re
import sys

# Python3 redefined 'unicode' to be 'str'
if sys.version_info[0] >= 3:
    unicode = str


class MySQLException(Exception):
    "Generic MySQL exception"


class SQLColumnDef(object):
    NO_DEFAULT = "XXX_NO_DEFAULT_XXX"

    def __init__(self, fldname, fldtype, fldlen, is_unsigned, not_null,
                 default):
        self.__name = fldname
        self.__type = fldtype
        self.__length = fldlen

        self.__unsigned = is_unsigned
        self.__not_null = not_null
        self.__dflt_value = default

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

    def add_column(self, colname, coltype, collen, is_unsigned, not_null,
                   default):
        self.__columns.append(SQLColumnDef(colname, coltype, collen,
                                           is_unsigned, not_null, default))

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

    def find(self, xid, fldname="id"):
        for row in self.__rows:
            if row[fldname] == xid:
                yield row

    @property
    def rows(self):
        for row in self.__rows:
            yield row


class MySQLDump(object):
    CRE_TBL_PAT = re.compile(r"^CREATE\s+TABLE\s+`(\S+)`\s+\(\s*$")
    CRE_FLD_PAT = re.compile(r"^\s+`(\S+)`\s+([^\s\(,]+)(?:\((\d+)\))?"
                             r"(\s+unsigned)?(\s+NOT\s+NULL)?"
                             r"(\s+AUTO_INCREMENT)?"
                             r"(?:\s+DEFAULT\s+(?:'([^']*)'|(NULL)))?"
                             r"(.*)$")
    INS_TBL_PAT = re.compile(r"^INSERT\s+INTO\s+`(\S+)`\s+(\(.*\)\s+)?"
                             r"VALUES\s+\((.*)\);\s*$")

    ROW_PAT = re.compile(r"(?:[^\s,']|'(?:\\.|[^'])*')+")

    TABLES_TO_SAVE = []

    @classmethod
    def __use_table(cls, tblname, include_list, omit_list):
        """
        If we have a list of tables to include,
          reject any tables which ARE NOT on the list
        If we have a list of tables to omit,
          reject any tables which ARE on the list
        """

        if include_list is not None and tblname not in include_list:
            return False

        if omit_list is not None and tblname in omit_list:
            return False

        return True

    @classmethod
    def create_data_table(cls, name):
        return DataTable()

    @classmethod
    def parse_row(cls, rowstr, table):
        row_obj = table.data_table.create_row()

        for idx, vstr in enumerate(cls.ROW_PAT.findall(rowstr)):
            try:
                col = table.column(idx)
            except IndexError:
                print("Found extra data in %s insert #%d" % (table.name, idx))
                continue

            if col.field_type.endswith("int") or \
              col.field_type == "float" or col.field_type == "double":
                if vstr == "NULL":
                    value = None
                else:
                    try:
                        value = int(vstr)
                    except ValueError:
                        try:
                            errmsg = "ERROR: Bad %s field \"%s\" for %s.%s" % \
                              (col.field_type, vstr, table.name, col.name)
                        except:
                            errmsg = "ERROR: Bad %s field \"%s\" for %s.%s" % \
                              (col.field_type, table.name, col.name)

                        print(errmsg, file=sys.stderr)
                        continue

            elif col.field_type == "varchar" or \
              col.field_type.endswith("text") or \
              col.field_type.endswith("blob"):
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
    def read_mysqldump(cls, filename, include_list=None, omit_list=None,
                       verbose=False):
        tables = {}

        with gzip.open(filename, "rb") as fin:
            cre_tbl = None
            prev_insert = None
            for line in fin:
                line = line.decode("latin-1").rstrip()

                if cre_tbl is not None:
                    # found the end of the CREATE TABLE stmt, clear 'cre_tbl'
                    if line.startswith(")"):
                        cre_tbl = None
                        continue

                    # ignore indexes for now
                    if line.find(" KEY ") >= 0:
                        continue

                    mtch = cls.CRE_FLD_PAT.match(line)
                    if mtch is not None:
                        fldname = mtch.group(1)
                        fldtype = mtch.group(2)
                        if mtch.group(3) is None:
                            fldlen = None
                        else:
                            fldlen = int(mtch.group(3))
                        is_unsigned = mtch.group(4) is not None
                        not_null = mtch.group(5) is not None
                        autoinc = mtch.group(6) is not None
                        def_val = mtch.group(7)
                        def_null = mtch.group(8)
                        if def_val is not None:
                            if def_null is not None:
                                raise Exception("DEFAULT pattern for %s.%s"
                                                " matched %s and %s" %
                                                (cre_tbl.name, fldname,
                                                 def_val, def_null))
                            default = def_val
                        elif def_null is not None:
                            default = def_null
                        else:
                            default = None
                        extra = mtch.group(9)
                        if extra != "" and extra != ",":
                            print("WARNING: Found extra stuff \"%s\""
                                  " for %s.%s column definition" %
                                  (extra, cre_tbl.name, fldname),
                                  file=sys.stderr)
                            print("Groups: %s" % (mtch.groups(), ))

                        cre_tbl.add_column(fldname, fldtype, fldlen,
                                           is_unsigned, not_null, default)
                        continue

                    print("!!! Bad table field: %s" % (line, ), file=sys.stderr)
                    continue

                if line.find("CREATE TABLE ") == 0:
                    mtch = cls.CRE_TBL_PAT.match(line)
                    if mtch is None:
                        print("??? %s" % (line, ), file=sys.stderr)
                    else:
                        tblname = mtch.group(1)

                        if cls.__use_table(tblname, include_list, omit_list):
                            cre_tbl = SQLTableDef(tblname)
                            tables[cre_tbl.name] = cre_tbl
                    continue

                if line.find("INSERT INTO ") == 0:
                    mtch = cls.INS_TBL_PAT.match(line)
                    if mtch is None:
                        print("Bad match for line starting \"%s\"" % line[:50],
                              file=sys.stderr)
                        continue

                    tblname = mtch.group(1)
                    tblrows = mtch.group(3)

                    if not cls.__use_table(tblname, include_list, omit_list):
                        continue

                    if tblname not in tables:
                        print("Cannot parse values for unknown table \"%s\"" %
                              (tblname, ), file=sys.stderr)
                        continue

                    # if we've got all the data for a table,
                    #  return it now and remove it from the cache
                    if prev_insert is not None and prev_insert != tblname:
                        prev_tbl = tables[prev_insert]
                        del tables[prev_insert]
                        yield prev_tbl
                    prev_insert = tblname

                    # find the table being referenced by INSERT INTO
                    ins_tbl = tables[tblname]
                    if ins_tbl.data_table is None:
                        dtbl = cls.create_data_table(ins_tbl.name)
                        ins_tbl.set_data_table(dtbl)

                    # parse INSERT INTO and add data to the table
                    if verbose:
                        print("-- reading %s" % tblname)
                    for rowstr in tblrows.split("),("):
                        row = cls.parse_row(rowstr, ins_tbl)
                        if verbose:
                            print("%s: %s" % (tblname, unicode(row)))

        # return remaining tables without any data
        for tbl in tables.values():
            yield tbl


if __name__ == "__main__":
    pass
