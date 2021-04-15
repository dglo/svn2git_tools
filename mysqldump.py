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


class SQLBaseDef(object):
    def __init__(self, colname, coltype, default_value=None, is_enum=False):
        self.__name = colname
        self.__type = coltype
        self.__dflt_value = default_value
        self.__is_enum = is_enum

    @classmethod
    def __is_quoted(cls, rawstr):
        return len(rawstr) >= 2 and \
          (rawstr[0] == "'" or rawstr[0] == '"') and \
          (rawstr[-1] == rawstr[0])

    @classmethod
    def __quote_string(cls, rawstr):
        quote = None
        for qchar in '"', "'":
            if qchar not in rawstr:
                quote = qchar
                break

        if quote is None:
            return None

        return "%s%s%s" % (quote, rawstr, quote)

    @property
    def data_type(self):
        return self.__type

    @property
    def default_string(self):
        if self.__is_quoted(self.__dflt_value):
            vstr = self.__dflt_value
        else:
            vstr = self.__quote_string(self.__dflt_value)
        return " DEFAULT %s" % (vstr, )

    @property
    def default_value(self):
        return self.__dflt_value

    @property
    def is_enum(self):
        return self.__is_enum

    @property
    def name(self):
        return self.__name


class SQLColumnDef(SQLBaseDef):
    def __init__(self, colname, coltype, collen, is_unsigned, not_null,
                 default_value):
        self.__length = collen

        self.__unsigned = is_unsigned
        self.__not_null = not_null

        super(SQLColumnDef, self).__init__(colname, coltype, default_value)

    def __str__(self):
        if not self.__unsigned:
            ustr = ""
        else:
            ustr = " unsigned"

        if not self.__not_null:
            nstr = ""
        else:
            nstr = " NOT NULL"

        if self.default_value is None:
            dstr = ""
        else:
            dstr = " default <%s>%s" % (type(self.default_value),
                                        self.default_value)

        return "%s[%s]*%s%s%s%s" % (self.name, self.data_type, self.__length,
                                    ustr, nstr, dstr)

    @property
    def is_not_null(self):
        return self.__not_null


class SQLEnumDef(SQLBaseDef):
    def __init__(self, colname, coltype, values, not_null, default_value):
        self.__values = values
        self.__not_null = not_null

        super(SQLEnumDef, self).__init__(colname, coltype, default_value,
                                         is_enum=True)

    def __str__(self):
        if not self.__not_null:
            nstr = ""
        else:
            nstr = " NOT NULL"

        if self.default_value is None:
            dstr = ""
        else:
            dstr = " default <%s>%s" % (type(self.default_value),
                                        self.default_value)

        return "%s[%s](%s)%s%s" % (self.name, self.data_type,
                                     ",".join(self.__values), nstr, dstr)


class SQLKeyDef(object):
    def __init__(self, keyname, keytype, keycols):
        self.__name = keyname
        self.__type = keytype
        self.__columns = keycols

    @property
    def key_type(self):
        return self.__type

    @property
    def name(self):
        return self.__name


class SQLTableDef(object):
    def __init__(self, name):
        self.__name = name
        self.__columns = []
        self.__keys = []

        self.__data_table = None

    def __str__(self):
        if self.__data_table is None:
            dstr = "<NoData>"
        else:
            dstr = "*%d" % len(self.__data_table)
        return self.__name + dstr

    def add_column(self, colname, coltype, collen, is_unsigned, not_null,
                   default):
        self.__columns.append(SQLColumnDef(colname, coltype, collen,
                                           is_unsigned, not_null, default))

    def add_enum(self, colname, coltype, values, not_null, default):
        self.__columns.append(SQLEnumDef(colname, coltype, values, not_null,
                                         default))

    def add_key(self, keyname, keytype, keycols):
        self.__keys.append(SQLKeyDef(keyname, keytype, keycols))

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
    def keys(self):
        for key in self.__keys:
            yield key

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

    def find(self, xid, colname="id"):
        for row in self.__rows:
            if row[colname] == xid:
                yield row

    @property
    def rows(self):
        for row in self.__rows:
            yield row


class MySQLDump(object):
    CRE_TBL_PAT = re.compile(r"^CREATE\s+TABLE\s+`(\S+)`\s+\(\s*$")
    CRE_COL_PAT = re.compile(r"^\s+`(\S+)`\s+([^\s\(,]+)(?:\((\d+)\))?"
                             r"(\s+unsigned)?(\s+NOT\s+NULL)?"
                             r"(\s+AUTO_INCREMENT)?"
                             r"(?:\s+DEFAULT\s+(?:'([^']*)'|(NULL)))?"
                             r"(.*)$")
    CRE_ENUM_PAT = re.compile(r"^\s+`(\S+)`\s+(enum|set)\(([^\)]+)\)"
                              r"(\s+NOT\s+NULL)?(?:\s+DEFAULT\s+(\S+))?,\s*$")
    CRE_KEY_PAT = re.compile(r"\s+(?:\s+(PRIMARY|UNIQUE|FOREIGN))?\s+KEY"
                             r"(?:\s+`(\S+)`)?\s+\((.*)\),?\s*")
    SUB_KEY_PAT = re.compile(r"(?:`([^`]+)`,?)")
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

            if col.is_enum:
                value = vstr
            elif col.data_type.endswith("int") or \
              col.data_type == "float" or col.data_type == "double":
                if vstr == "NULL":
                    value = None
                else:
                    try:
                        if col.data_type.endswith("int"):
                            value = int(vstr)
                        else:
                            value = float(vstr)
                    except ValueError:
                        errmsg = "ERROR: Bad %s data \"%s\" for %s.%s" % \
                          (col.data_type, vstr[:10].encode("ascii", "ignore"), table.name, col.name)

                        print(errmsg, file=sys.stderr)
                        continue

            elif col.data_type.endswith("char") or \
              col.data_type.endswith("text") or \
              col.data_type.endswith("blob"):
                if vstr[0] == "'" and vstr[-1] == "'":
                    value = vstr[1:-1]
                else:
                    value = vstr
                value = value.replace("\\'", "'")
                value = value.replace('\\"', '"')
            elif col.data_type == "date" or col.data_type == "datetime" or \
              col.data_type == "time":
                if vstr[0] == "'" and vstr[-1] == "'":
                    value = vstr[1:-1]
                else:
                    value = vstr
            else:
                raise MySQLException("Unknown field type \"%s\" for %s.%s" %
                                     (col.data_type, table.name, col.name))

            row_obj.set_value(col, value)

        table.data_table.add_row(row_obj)
        return row_obj

    @classmethod
    def read_mysqldump(cls, filename, include_list=None, omit_list=None,
                       debug=False, verbose=False):
        with gzip.open(filename, "rb") as fin:
            table = None
            creating = False
            prev_insert = None
            for line in fin:
                line = line.decode("latin-1").rstrip()

                if creating:
                    # found the end of the CREATE TABLE stmt, clear table var
                    if line.startswith(")"):
                        creating = False
                        continue

                    # ignore indexes for now
                    if line.find(" KEY ") >= 0:
                        mtch = cls.CRE_KEY_PAT.match(line)
                        if mtch is not None:
                            keytype = mtch.group(1)
                            keyname = mtch.group(2)
                            tmpcols = mtch.group(3)

                            keycols = cls.SUB_KEY_PAT.findall(tmpcols)

                            table.add_key(keyname, keytype, keycols)
                        continue

                    # parse table.enum declaration
                    if line.find(" enum(") > 0 or line.find(" set(") > 0:
                        mtch = cls.CRE_ENUM_PAT.match(line)
                        if mtch is not None:
                            colname = mtch.group(1)
                            coltype = mtch.group(2)
                            values = mtch.group(2).split(",")
                            not_null = mtch.group(3) is not None
                            default = mtch.group(4)

                            table.add_enum(colname, coltype, values, not_null,
                                           default)

                            continue

                    # parse table.column declaration
                    mtch = cls.CRE_COL_PAT.match(line)
                    if mtch is not None:
                        colname = mtch.group(1)
                        coltype = mtch.group(2)
                        if mtch.group(3) is None:
                            collen = None
                        else:
                            collen = int(mtch.group(3))
                        is_unsigned = mtch.group(4) is not None
                        not_null = mtch.group(5) is not None
                        autoinc = mtch.group(6) is not None
                        def_val = mtch.group(7)
                        def_null = mtch.group(8)
                        if def_val is not None:
                            if def_null is not None:
                                raise Exception("DEFAULT pattern for %s.%s"
                                                " matched %s and %s" %
                                                (table.name, colname,
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
                                  (extra, table.name, colname),
                                  file=sys.stderr)
                            print("Groups: %s" % (mtch.groups(), ))

                        table.add_column(colname, coltype, collen,
                                         is_unsigned, not_null, default)
                        continue

                    print("!!! Bad table field: %s" % (line, ), file=sys.stderr)
                    continue

                if line.find("CREATE TABLE ") == 0:
                    if table is not None:
                        yield table
                        table = None

                    mtch = cls.CRE_TBL_PAT.match(line)
                    if mtch is None:
                        print("??? %s" % (line, ), file=sys.stderr)
                    else:
                        tblname = mtch.group(1)

                        if cls.__use_table(tblname, include_list, omit_list):
                            table = SQLTableDef(tblname)
                            creating = True
                    continue

                if line.find("INSERT INTO ") == 0:
                    mtch = cls.INS_TBL_PAT.match(line)
                    if mtch is None:
                        print("Bad match for line starting \"%s\"" % line[:50],
                              file=sys.stderr)
                        continue

                    # if we're not saving this table, ignore this INSERT
                    if table is None:
                        continue

                    tblname = mtch.group(1)
                    tblrows = mtch.group(3)

                    if tblname != table.name:
                        print("Ignoring values for \"%s\""
                              " (current table is \"%s\")" %
                              (tblname, table.name), file=sys.stderr)
                        continue

                    if not cls.__use_table(tblname, include_list, omit_list):
                        continue

                    # find the table being referenced by INSERT INTO
                    if table.data_table is None:
                        dtbl = cls.create_data_table(table.name)
                        table.set_data_table(dtbl)

                    # parse INSERT INTO and add data to the table
                    if verbose:
                        print("-- reading %s" % tblname)
                    for rowstr in tblrows.split("),("):
                        row = cls.parse_row(rowstr, table)
                        if verbose:
                            print("%s: %s" % (tblname, unicode(row)))

        # return final table
        if table is not None:
            yield table


if __name__ == "__main__":
    pass
