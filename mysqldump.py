#!/usr/bin/env python

from __future__ import print_function

import gzip
import os
import re
import sys

# Python3 redefined 'unicode' to be 'str'
if sys.version_info[0] >= 3:
    unicode = str


LOUD = False


class MySQLException(Exception):
    "Generic MySQL exception"


class DumpParser(object):
    (PT_UNKNOWN, PT_CREATE, PT_INSERT) = (0, 1, 2);

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

    # takens returned by tokenize_mysqldump()
    (T_TBLNEW, T_COLUMN, T_KEY, T_ENUM, T_INSERT, T_INDATA, T_INEND,
     T_COMMENT) = ("TN", "CL", "KY", "EN", "IN", "DI", "I$", "##")

    def __init__(self, filename):
        self.__name = filename
        self.__gzipped = self.__is_gzipped(filename)

        self.__blkbuf = None
        self.__state = self.PT_UNKNOWN

    @classmethod
    def __is_gzipped(cls, filename):
        with open(filename, "rb") as tmpin:
            return tmpin.read(2) == b"\x1f\x8b"

    @classmethod
    def __parse_create_line(cls, table_name, line):
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

        if def_val is not None:
            if def_null is not None:
                raise Exception("DEFAULT pattern for %s.%s matched %s and %s" %
                                (table_name, colname, def_val, def_null))

            default = def_val
        elif def_null is not None:
            default = def_null
        else:
            default = None
        extra = mtch.group(9)
        if extra != "" and extra != ",":
            print("WARNING: Found extra stuff \"%s\" for %s.%s column"
                  " definition" % (extra, table_name, colname),
                  file=sys.stderr)

        return (cls.T_COLUMN, colname, coltype, collen, is_unsigned, not_null,
                default)

    def __parse_insert_into(self):
        # look for the end of the INSERT command
        nlidx = self.__blkbuf.find(");\n")
        found_end = nlidx > 0

        # find the end of the (incomplete?) INSERT command
        segend = nlidx if found_end else len(self.__blkbuf)

        # break line into row segments
        final_seg = None
        total_len = 0
        for segment in self.__blkbuf[:segend].split("),("):
            # if we've seen a previous segment...
            if final_seg is not None:
                # return this segment
                yield (self.T_INDATA, final_seg)

                # update the number of characters consumed
                total_len += len(final_seg) + 3

            # we have a new final segment!
            final_seg = segment

        # if we haven't seen the end of the INSERT command...
        if not found_end:
            # if we found at least one segment...
            if total_len > 0:
                # look for the final segment
                segpos = self.__blkbuf.find(final_seg, total_len)
                if segpos < 0:
                    raise Exception("Cannot find subsegment in buffer!")

                # remove everything before the final segment
                self.__blkbuf = self.__blkbuf[segpos:]

            # we haven't found the end of the INSERT command,
            #  get more data
            yield (self.T_INEND, False)
        else:
            if final_seg is None:
                raise Exception("New-line index is %d but final segment"
                                " is None!" % (nlidx, ))

            # send the final segment
            yield (self.T_INDATA, final_seg)

            # prune the buffer after the INSERT command
            self.__blkbuf = self.__blkbuf[nlidx+3:]
            final_seg = None

            # done with INSERT, look for the next statement
            self.__state = self.PT_UNKNOWN
            yield (self.T_INEND, True)

    def __parse_unknown(self, debug=False):
        if self.__blkbuf.startswith("-- "):
            sepidx = self.__blkbuf.find("\n")
            if sepidx < 0:
                return None

            comment = self.__blkbuf[:sepidx]
            self.__blkbuf = self.__blkbuf[sepidx+1:]

            return (self.T_COMMENT, comment)

        if self.__blkbuf.startswith("CREATE TABLE "):
            mtch = self.CRE_TBL_PAT.match(self.__blkbuf)
            if mtch is None:
                return None

            tblname = mtch.group(1)
            self.__blkbuf = self.__blkbuf[mtch.end():]
            self.__state = self.PT_CREATE

            return (self.T_TBLNEW, tblname)

        if self.__blkbuf.startswith("INSERT INTO "):
            sepidx = self.__blkbuf.find("VALUES (")
            if sepidx < 0:
                return None

            line = self.__blkbuf[:sepidx+8]
            self.__blkbuf = self.__blkbuf[sepidx+8:]

            mtch = self.INS_INTO_PAT.match(line)
            if mtch is None:
                raise Exception("Cannot find r\"%s\" in \"%s\"" %
                                (self.INS_INTO_PAT.pattern, line))

            tblname = mtch.group(1)
            value_text = line[mtch.end():]

            # initialize things for parsing INSERT stmts
            self.__state = self.PT_INSERT

            return (self.T_INSERT, tblname)

        return None

    @classmethod
    def __partial_string(cls, string, maxlen=20, from_end=False):
        if string is None:
            return "NULL"

        if len(string) < maxlen:
            return "\"%s\"" % (string, )

        fmt = "...\"%s\"" if from_end else "\"%s\"..."
        return fmt % (string[:maxlen], )

    @property
    def state_string(self):
        if self.__state == self.PT_UNKNOWN:
            return "UNK"
        if self.__state == self.PT_CREATE:
            return "CRE"
        if self.__state == self.PT_INSERT:
            return "INS"
        return "??%s??" % (self.__state, )

    def tokenize(self, debug=False, verbose=False):
        saw_final_block = False
        if self.__gzipped:
            fin = gzip.open(self.__name, "rb")
        else:
            fin = open(self.__name, "rb")
        try:
            while True:
                block = fin.read(16384)  #fin.read(1024)
                if debug and LOUD:
                    if block is None:
                        blkstr = "NONE"
                    elif len(block) < 20:
                        blkstr = "\"%s\"" % block
                    else:
                        blkstr = "\"%s\"..." % block[:20]
                    print("---{%s} %s" % (self.state_string, blkstr),
                          file=sys.stderr)

                if block is None or len(block) == 0:
                    if saw_final_block:
                        raise Exception("Cannot parse final block \"%s\"" %
                                        (self.__blkbuf, ))
                    saw_final_block = True
                    break

                if isinstance(block, bytes):
                    block = block.decode("utf-8", "ignore")

                if self.__blkbuf is None:
                    self.__blkbuf = block
                else:
                    self.__blkbuf += block

                while True:
                    if debug and LOUD:
                        if self.__blkbuf is None:
                            blklen = 0
                            blkstr = "<NoStr>"
                        else:
                            blklen = len(self.__blkbuf)
                            blkstr = "\"%s\"..." % (self.__blkbuf[:20], )

                        print("BUF{%s}*%d %s" % (self.state_string, blklen,
                                                 blkstr), file=sys.stderr)
                    if self.__blkbuf == "":
                        break

                    # parse CREATE TABLE line
                    if self.__state == self.PT_CREATE:
                        sepidx = self.__blkbuf.find("\n")
                        if sepidx < 0:
                            break

                        line = self.__blkbuf[:sepidx]
                        self.__blkbuf = self.__blkbuf[sepidx+1:]

                        if line.startswith(")") and line.endswith(";"):
                            self.__state = self.PT_UNKNOWN
                        else:
                            token = self.__parse_create_line("unknown", line)
                            if debug:
                                print("TOKEN1[%s] <- %s" %
                                      (token[0], token[1:]), file=sys.stderr)
                            yield token
                        continue

                    # parse INSERT INTO line
                    if self.__state == self.PT_INSERT:
                        finished = None
                        for token in self.__parse_insert_into():
                            if finished is not None:
                                raise Exception("Found unexpected tokens")

                            if token[0] != self.T_INEND:
                                # yield normal token
                                if debug:
                                    if token[0] == self.T_INDATA:
                                        if len(token[1]) <= 20:
                                            text = '"' + token[1] + '"'
                                        else:
                                            text = '"' + token[1][:20] + '"...'
                                    else:
                                        text = str(token[1:])

                                    print("TOKEN2[%s] <- %s" %
                                          (token[0], text), file=sys.stderr)
                                yield token
                            else:
                                # if we're done parsing, stash the result
                                finished = token[1]
                                if finished and debug:
                                    print("XXX Found end of insert",
                                          file=sys.stderr)

                        # if we're still parsing, get more data
                        if not finished:
                            break

                        # done with INSERT command, continue parsing buffer
                        continue

                    # parse unknown line
                    if self.__state == self.PT_UNKNOWN:
                        token = self.__parse_unknown(debug=debug)
                        if token is not None:
                            if debug:
                                print("TOKEN3[%s] <- %s" %
                                      (token[0], token[1:]), file=sys.stderr)
                            yield token
                            continue

                    # if we can't find a newline, add more data to the buffer
                    sepidx = self.__blkbuf.find("\n")
                    if sepidx < 0:
                        break

                    # get first line of text (and remove it from the buffer)
                    segment = self.__blkbuf[:sepidx]
                    self.__blkbuf = self.__blkbuf[sepidx+1:]

                    # complain if we don't recognize this line
                    if not segment.startswith("--") and \
                      not segment.startswith("/*!") and \
                      not segment.startswith("UNLOCK TABLES;") and \
                      not segment.startswith("LOCK TABLES ") and \
                      not segment.startswith("DROP TABLE IF EXISTS ") and \
                      segment.rstrip() != "":
                        print("WARNING: Ignoring %s (state=%s)" %
                              (self.__partial_string(segment),
                               self.state_string), file=sys.stderr)
        finally:
            fin.close()


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
    def __init__(self, name):
        self.__name = name
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
    def name(self):
        return self.__name

    @property
    def rows(self):
        for row in self.__rows:
            yield row


class MySQLDump(object):
    DATA_PAT = re.compile(r"([^\s,']*|'(?:\\.|[^'])*'),")

    @classmethod
    def __parse_data_row(cls, rowstr, table, debug=False):
        row_obj = table.data_table.create_row()

        cmpstr = None
        #for idx, vstr in enumerate(cls.DATA_PAT.findall(rowstr)):
        for idx, vstr in enumerate(cls.__split_row(rowstr, debug=debug)):
            if cmpstr is None:
                cmpstr = vstr
            else:
                cmpstr += "," + vstr

            try:
                col = table.column(idx)
            except IndexError:
                if len(vstr) < 20:
                    tmpstr = '"' + vstr + '"'
                else:
                    tmpstr = '"%s"...' % vstr[:20]
                print("WARNING: Found extra data %s in %s insert #%d"
                      " (after %d rows)" %
                      (tmpstr, table.name, idx, len(table.data_table)),
                      file=sys.stderr)
                continue

            if debug:
                print("         %s.%s <- \"%s\"" % (table.name, col.name, vstr),
                      file=sys.stderr)

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
                          (col.data_type, vstr[:10],
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

            if debug:
                print("%s.%s <- %s" % (table.name, col.name, value))
            row_obj.set_value(col, value)

        if False and cmpstr != rowstr:
            raise Exception("!!MISMATCH!!\n%s\nRaw: %s\n%s\nNew: %s" %
                            ("-"*79, rowstr, "-"*79, cmpstr))

        if debug:
            print("AddRow[%s] <- %s" % (table.name, row_obj), file=sys.stderr)
        table.data_table.add_row(row_obj)

    @classmethod
    def __split_row(cls, rowstr, debug=False):
        segment = None
        while True:
            if debug and loud:
                if segment is None:
                    print("LoopTop: %s" % rowstr)
                else:
                    print("LoopTop: %s|<=>|%s" % (segment, rowstr))
            idx = rowstr.find(',')
            if idx < 0:
                if segment is None:
                    substr = rowstr
                else:
                    substr = segment + rowstr
                    segment = None
                if debug and loud:
                    if len(substr) < 20:
                        tmpstr = '"' + substr + '"'
                    else:
                        tmpstr = '"%s"...' % substr[:20]
                    print("      *** Yield#1 %s" % tmpstr, file=sys.stderr)
                yield substr
                break

            # if last character is a quote, look for slashes before it
            preidx = idx - 1
            num_slash = 0
            if debug and loud:
                print("      PreSlash idx#%d (num#%d) <- '%s'"
                      "\n        Row \"%s\"" %
                      (preidx, num_slash, rowstr[preidx - (num_slash + 1)],
                       rowstr))
            while preidx - num_slash > 0 and \
              rowstr[preidx - (num_slash + 1)] == '\\':
                if debug and loud:
                    print("      _InSlash idx#%d (num#%d) <- '%s'" %
                          (preidx, num_slash,
                           rowstr[preidx - (num_slash + 1)]))
                num_slash += 1
            if debug and loud:
                print("      Slash*%d" % num_slash)
            if num_slash & 0x1 == 0x1:
                if segment is None:
                    segment = rowstr[:idx]
                else:
                    segment += rowstr[:idx]
                if segment[-1] != ',':
                    if debug and loud:
                        print("      AddComma#1 \"%s\"" % segment)
                    segment += ','
                rowstr = rowstr[idx+1:]
                if debug and loud:
                    print("      AddSeg \"%s\" Remain \"%s\"" %
                          (segment, rowstr))
                continue

            # compare first char of segment or rowdata with
            #  first character before comma in rowdata
            if debug and loud:
                print("      Seg \"%s\" Remain \"%s\"" % (segment, rowstr))
            firstchar = rowstr[0] if segment is None else segment[0]
            lastchar = rowstr[idx-1]
            if debug and loud:
                print("         FIRSTLAST \"%s\"(is_quote=%s)...\"%s\"(is_quote=%s)" %
                      (firstchar, firstchar == "'", lastchar,
                       lastchar == "'"), file=sys.stderr)
            if firstchar == "'":
                if lastchar == "'":
                    if segment is None:
                        substr = rowstr[:idx]
                    else:
                        substr = segment + rowstr[:idx]
                    rowstr = rowstr[idx+1:]
                    segment = None
                    if substr.endswith(","):
                        substr = substr[:-1]
                    if debug and loud:
                        if len(substr) < 20:
                            tmpstr = '"' + substr + '"'
                        else:
                            tmpstr = '"%s"...' % substr[:20]
                        print("      *** Yield#3 %s" % tmpstr, file=sys.stderr)
                    yield substr
                    continue

                if segment is None:
                    segment = rowstr[:idx]
                else:
                    segment += rowstr[:idx]
                if segment[-1] != ',':
                    if debug and loud:
                        print("      AddComma#2 \"%s\"" % segment)
                    segment += ','
                rowstr = rowstr[idx+1:]
                if debug and loud:
                    if len(rowstr) < 20:
                        tmpstr = '"' + rowstr + '"'
                    else:
                        tmpstr = '"%s"...' % rowstr[:20]
                    print("         SEG \"%s\"\n         ARRAY \"%s\"" %
                          (segment, tmpstr), file=sys.stderr)
                continue

            if segment is None:
                substr = rowstr[:idx]
                if debug and loud:
                    print("      RtnStr#1 \"%s\"" % substr)
            else:
                if debug and loud:
                    print("      RtnStr#2 \"%s\"+\"%s\"" %
                          (segment, rowstr[:idx]))
                substr = segment + rowstr[:idx]
                segment = None
            rowstr = rowstr[idx+1:]

            if debug and loud:
                if len(substr) < 20:
                    tmpstr = '"' + substr + '"'
                else:
                    tmpstr = '"%s"...' % substr[:20]
                print("      *** Yield#2 %s" % tmpstr, file=sys.stderr)
            yield substr

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
        return DataTable(name)

    @classmethod
    def read_mysqldump(cls, filename, include_list=None, omit_list=None,
                       debug=False, verbose=False):
        parser = DumpParser(filename)

        table = None
        for tokens in parser.tokenize(debug=debug, verbose=verbose):
            if tokens[0] == DumpParser.T_TBLNEW:
                if table is not None:
                    if cls.__use_table(table.name, include_list=include_list,
                                       omit_list=omit_list):
                        yield table

                (tblname, ) = tokens[1:]
                table = SQLTableDef(tblname)
            elif tokens[0] == DumpParser.T_COLUMN:
                (colname, coltype, collen, is_unsigned, not_null, default) = \
                  tokens[1:]
                table.add_column(colname, coltype, collen, is_unsigned,
                                 not_null, default)
            elif tokens[0] == DumpParser.T_KEY:
                (keyname, keytype, keycols) = tokens[1:]
                table.add_key(keyname, keytype, keycols)
            elif tokens[0] == DumpParser.T_ENUM:
                (colname, coltype, values, not_null, dflt_value) = tokens[1:]
                table.add_enum(colname, coltype, values, not_null, dflt_value)
            elif tokens[0] == DumpParser.T_INDATA:
                if table.data_table is None:
                    dtbl = cls.create_data_table(table.name)
                    if dtbl is not None:
                        table.set_data_table(dtbl)

                if table.data_table is not None:
                    (rowstr, ) = tokens[1:]
                    cls.__parse_data_row(rowstr, table, debug=debug)
            elif tokens[0] == DumpParser.T_COMMENT:
                pass

        # return final table
        if table is not None and table.data_table is not None:
            if cls.__use_table(table.name, include_list=include_list,
                               omit_list=omit_list):
                yield table


#from profile_code import profile
#@profile(output_file="/tmp/mysqldump.prof", strip_dirs=True, save_stats=True)
def main():
    for filename in sys.argv[1:]:
        if not os.path.exists(filename):
            print("ERROR: Cannot find \"%s\"" % (filename, ), file=sys.stderr)
            continue

        parser = DumpParser(filename)

        print("== %s" % (filename, ))
        for tokens in parser.tokenize():
            print("%s -> %s" % (tokens[0], tokens[1:]))


if __name__ == "__main__":
    main()
