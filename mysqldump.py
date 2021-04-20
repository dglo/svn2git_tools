#!/usr/bin/env python

from __future__ import print_function

import gzip
import os
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


class DumpParserData(object):
    (PT_UNKNOWN, PT_CREATE, PT_INSERT) = (0, 1, 2);

    def __init__(self):
        self.blkbuf = None
        self.state = self.PT_UNKNOWN
        self.table = None
        self.ins_count = None

    @property
    def state_string(self):
        if self.state == self.PT_UNKNOWN:
            return "UNK"
        if self.state == self.PT_CREATE:
            return "CRE"
        if self.state == self.PT_INSERT:
            return "INS"
        return "??%s??" % (self.state, )


class MySQLDump(object):
    CRE_TBL_PAT = re.compile(r"^CREATE\s+TABLE\s+`(\S+)`\s+\(\s*")
    CRE_COL_PAT = re.compile(r"^\s*`(\S+)`\s+([^\s\(,]+)(?:\((\d+)\))?"
                             r"(\s+unsigned)?(\s+NOT\s+NULL)?"
                             r"(\s+AUTO_INCREMENT)?"
                             r"(?:\s+DEFAULT\s+(?:'([^']*)'|(NULL)))?"
                             r"(.*),?$")
    CRE_ENUM_PAT = re.compile(r"^\s*`(\S+)`\s+(enum|set)\(([^\)]+)\)"
                              r"(\s+NOT\s+NULL)?(?:\s+DEFAULT\s+(\S+))?,\s*$")
    CRE_KEY_PAT = re.compile(r"\s*(?:\s+(PRIMARY|UNIQUE|FOREIGN))?\s+KEY"
                             r"(?:\s+`(\S+)`)?\s+\((.*)\),?\s*")
    SUB_KEY_PAT = re.compile(r"(?:`([^`]+)`,?)")

    INS_INTO_PAT = re.compile(r"^INSERT\s+INTO\s+`(\S+)`(?:\s+\([^\)]+\))?"
                              r"\s+VALUES\s+\(")

    DATA_PAT = re.compile(r"([^\s,']*|'(?:\\.|[^'])*'),")

    # takens used by tokenize_mysqldump()
    (T_TBLNEW, T_COLUMN, T_KEY, T_ENUM, T_INSERT, T_INDATA) = \
      ("TN", "CL", "KY", "EN", "IN", "DI")

    @classmethod
    def __parse_create_line(cls, pdata, line):
        # parse index declaration
        if line.find(" KEY ") >= 0:
            mtch = cls.CRE_KEY_PAT.match(line)
            if mtch is None:
                raise Exception("Could not parse KEY line \"%s\"" % (line, ))

            keytype = mtch.group(1)
            keyname = mtch.group(2)
            tmpcols = mtch.group(3)

            keycols = cls.SUB_KEY_PAT.findall(tmpcols)

            return (cls.T_KEY, keyname, keytype, keycols)

        # parse table.enum declaration
        if line.find(" enum(") > 0 or line.find(" set(") > 0:
            mtch = cls.CRE_ENUM_PAT.match(line)
            if mtch is None:
                raise Exception("Could not parse enum/set line \"%s\"" %
                                (line, ))

            colname = mtch.group(1)
            coltype = mtch.group(2)
            values = mtch.group(3).split(",")
            not_null = mtch.group(4) is not None
            default = mtch.group(5)

            return (cls.T_ENUM, colname, coltype, values, not_null, default)

        # parse table.column declaration
        mtch = cls.CRE_COL_PAT.match(line)
        if mtch is None:
            raise Exception("Could not parse column line \"%s\"" %
                            (line, ))

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

        if pdata.table is None:
            pname = "unknown"
        else:
            pname = pdata.table.name

        if def_val is not None:
            if def_null is not None:
                raise Exception("DEFAULT pattern for %s.%s matched %s and %s" %
                                (pname, colname, def_val, def_null))

            default = def_val
        elif def_null is not None:
            default = def_null
        else:
            default = None
        extra = mtch.group(9)
        if extra != "" and extra != ",":
            print("WARNING: Found extra stuff \"%s\" for %s.%s column"
                  " definition" % (extra, pname, colname), file=sys.stderr)

        return (cls.T_COLUMN, colname, coltype, collen, is_unsigned, not_null,
                default)

    @classmethod
    def __parse_data_row(cls, rowstr, table):
        row_obj = table.data_table.create_row()

        for idx, vstr in enumerate(cls.DATA_PAT.findall(rowstr)):
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
                          (col.data_type, vstr[:10].encode("ascii", "ignore"),
                           table.name, col.name)

                        print(errmsg, file=sys.stderr)
                        continue

            elif col.data_type.endswith("char") or \
              col.data_type.endswith("text") or \
              col.data_type.endswith("blob"):
                if len(vstr) > 2 and vstr[0] == "'" and vstr[-1] == "'":
                    value = vstr[1:-1]
                else:
                    value = vstr
                value = value.replace("\\'", "'")
                value = value.replace('\\"', '"')
            elif col.data_type == "date" or col.data_type == "datetime" or \
              col.data_type == "time":
                if len(vstr) > 2 and vstr[0] == "'" and vstr[-1] == "'":
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
    def __parse_unknown(cls, pdata, include_list, omit_list):
        if pdata.blkbuf.startswith("CREATE TABLE "):
            mtch = cls.CRE_TBL_PAT.match(pdata.blkbuf)
            if mtch is None:
                return None

            tblname = mtch.group(1)
            pdata.blkbuf = pdata.blkbuf[mtch.end():]
            pdata.state = DumpParserData.PT_CREATE

            return (cls.T_TBLNEW, tblname)

        if pdata.blkbuf.startswith("INSERT INTO "):
            mtch = cls.INS_INTO_PAT.match(pdata.blkbuf)
            if mtch is not None:
                tblname = mtch.group(1)
                value_text = pdata.blkbuf[mtch.end():]

                # initialize things for parsing INSERT stmts
                pdata.state = DumpParserData.PT_INSERT
                pdata.blkbuf = value_text
                pdata.ins_count = 0

                return (cls.T_INSERT, tblname)

        return None

    @classmethod
    def __partial_string(cls, string, maxlen=20, from_end=False):
        if string is None:
            return "NULL"

        if len(string) < maxlen:
            return "\"%s\"" % (string, )

        fmt = "...\"%s\"" if from_end else "\"%s\"..."
        return fmt % (string[:maxlen], )

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
    def read_mysqldump(cls, filename, include_list=None, omit_list=None,
                       debug=False, verbose=False):
        table = None
        for tokens in cls.tokenize_mysqldump(filename,
                                             include_list=include_list,
                                             omit_list=omit_list, debug=debug,
                                             verbose=verbose):
            if tokens[0] == cls.T_TBLNEW:
                if table is not None:
                    yield table

                (tblname, ) = tokens[1:]
                table = SQLTableDef(tblname)
            elif tokens[0] == cls.T_COLUMN:
                (colname, coltype, collen, is_unsigned, not_null, default) = \
                  tokens[1:]
                table.add_column(colname, coltype, collen, is_unsigned,
                                 not_null, default)
            elif tokens[0] == cls.T_KEY:
                (keyname, keytype, keycols) = tokens[1:]
                table.add_key(keyname, keytype, keycols)
            elif tokens[0] == cls.T_ENUM:
                (colname, coltype, values, not_null, dflt_value) = tokens[1:]
                table.add_enum(colname, coltype, values, not_null, dflt_value)
            elif tokens[0] == cls.T_INDATA:
                if table.data_table is None:
                    dtbl = cls.create_data_table(table.name)
                    table.set_data_table(dtbl)

                (rowstr, ) = tokens[1:]
                cls.__parse_data_row(rowstr, table)

        # return final table
        if table is not None:
            yield table

    @classmethod
    def tokenize_mysqldump(cls, filename, include_list=None, omit_list=None,
                           debug=False, verbose=False):
        pdata = DumpParserData()

        saw_final_block = False
        with gzip.open(filename, "rb") as fin:
            while True:
                block = fin.read(16384)  #fin.read(1024)
                if debug:
                    if block is None:
                        blkstr = "NONE"
                    elif len(block) < 20:
                        blkstr = "\"%s\"" % block
                    else:
                        blkstr = "\"%s\"..." % block[:20]
                    print("---{%s} %s" % (pdata.state_string, blkstr),
                          file=sys.stderr)

                if block is None or block == "":
                    if saw_final_block:
                        raise Exception("Cannot parse final block \"%s\"" %
                                        (pdata.blkbuf, ))
                    saw_final_block = True
                    break

                if pdata.blkbuf is None:
                    pdata.blkbuf = block
                else:
                    pdata.blkbuf += block

                while True:
                    if debug:
                        print("BUF{%s} %s%s%s %s" %
                              (pdata.state_string,
                               " <NoTbl>" if pdata.table is None
                               else " table %s" % pdata.table.name,
                               "" if pdata.state != pdata.PT_INSERT
                               else " ins#%d" % pdata.ins_count,
                               "<NoStr>" if pdata.blkbuf is None else
                               "\"%s\"..." % (pdata.blkbuf[:20], )),
                              file=sys.stderr)
                    if pdata.blkbuf == "":
                        break

                    # parse CREATE TABLE line
                    if pdata.state == DumpParserData.PT_CREATE:
                        sepidx = pdata.blkbuf.find("\n")
                        if sepidx < 0:
                            break

                        line = pdata.blkbuf[:sepidx]
                        pdata.blkbuf = pdata.blkbuf[sepidx+1:]

                        if line.startswith(")") and line.endswith(";"):
                            pdata.state = DumpParserData.PT_UNKNOWN
                        else:
                            yield cls.__parse_create_line(pdata, line)
                        continue

                    # parse INSERT INTO line
                    if pdata.state == DumpParserData.PT_INSERT:
                        # look for the end of the INSERT command
                        nlidx = pdata.blkbuf.find(");\n")

                        # find the end of the (incomplete?) INSERT command
                        segend = nlidx if nlidx >= 0 else len(pdata.blkbuf)

                        # break line into row segments
                        final_seg = None
                        total_len = 0
                        for segment in pdata.blkbuf[:segend].split("),("):
                            # if we've seen a previous segment...
                            if final_seg is not None:
                                # return this segment
                                pdata.ins_count += 1
                                yield (cls.T_INDATA, final_seg)

                                # update the number of characters consumed
                                total_len += len(final_seg) + 3

                            # we have a new final segment!
                            final_seg = segment

                        # if we haven't seen the end of the INSERT command...
                        if nlidx < 0:
                            # if we  found at least one segment...
                            if total_len > 0:
                                # look for the final segment
                                segpos = pdata.blkbuf.find(final_seg,
                                                           total_len)
                                if segpos < 0:
                                    raise Exception("Cannot find subsegment in"
                                                    " buffer!")

                                # remove everything before the final segment
                                pdata.blkbuf = pdata.blkbuf[segpos:]

                            # we haven't found the end of the INSERT command,
                            #  get more data
                            break

                        # return final segment, if it is complete
                        if nlidx >= 0:
                            if final_seg is None:
                                raise Exception("New-line index is %d but"
                                                " final segment is None!" %
                                                (nlidx, ))

                            # send the final segment
                            pdata.ins_count += 1
                            yield (cls.T_INDATA, final_seg)

                            segpos = nlidx + 3

                            # prune the buffer after the INSERT command
                            pdata.blkbuf = pdata.blkbuf[segpos:]
                            final_seg = None

                        # done with INSERT, look for the next statement
                        pdata.state = DumpParserData.PT_UNKNOWN
                        continue

                    # parse unknown line
                    if pdata.state == DumpParserData.PT_UNKNOWN:
                        tokens = cls.__parse_unknown(pdata, include_list,
                                                     omit_list)
                        if tokens is not None:
                            yield tokens
                            continue

                    # if we can't find a newline, add more data to the buffer
                    sepidx = pdata.blkbuf.find("\n")
                    if sepidx < 0:
                        break

                    # get first line of text (and remove it from the buffer)
                    segment = pdata.blkbuf[:sepidx]
                    pdata.blkbuf = pdata.blkbuf[sepidx+1:]

                    # complain if we don't recognize this line
                    if not segment.startswith("--") and \
                      not segment.startswith("/*!") and \
                      not segment.startswith("UNLOCK TABLES;") and \
                      not segment.startswith("LOCK TABLES ") and \
                      not segment.startswith("DROP TABLE IF EXISTS ") and \
                      segment.rstrip() != "":
                        print("WARNING: Ignoring %s (state=%s)" %
                              (cls.__partial_string(segment),
                               pdata.state_string), file=sys.stderr)


            if pdata.table is not None:
                if debug:
                    print("EndYield <%s>%s" % (type(pdata.table), pdata.table),
                          file=sys.stderr)
                yield pdata.table


#from profile_code import profile
#@profile(output_file="/tmp/mysqldump.prof", strip_dirs=True, save_stats=True)
def main():
    for filename in sys.argv[1:]:
        if not os.path.exists(filename):
            print("ERROR: Cannot find \"%s\"" % (filename, ), file=sys.stderr)
            continue

        print("== %s" % (filename, ))
        for tokens in MySQLDump.tokenize_mysqldump(filename):
            print("%s -> %s" % (tokens[0], tokens[1:]))


if __name__ == "__main__":
    main()
