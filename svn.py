#!/usr/bin/env python

from __future__ import print_function

import os
import re
import select
import subprocess
import sys
import tempfile

from datetime import datetime

from cmdrunner import CommandException, default_returncode_handler, \
     run_command, run_generator
from dictobject import DictObject


LOG_PROP_PAT = re.compile(r"^r(\d+)\s+"
                          r"\|\s+([^\|]+)\s+"
                          r"\|\s+([^\|]+)\s+"
                          r"\|\s+(\d+)\s+lines?\s*$")
LOG_FILE_PAT = re.compile(r"^\s+(\S+)\s+(.*\S)\s*$")


class SVNException(CommandException):
    "General Subversion exception"


class SVNConnectException(SVNException):
    "'svn' could not connect to the remote repository"


class SVNNonexistentException(SVNException):
    "Subversion URL is not valid"

    def __init__(self, url):
        self.url = url
        msg = "Bad Subversion URL \"%s\"" % (url, )
        super(SVNNonexistentException, self).__init__(msg)


class LogEntry(DictObject):
    """
    All information for a single Subversion log entry
    """
    def __init__(self, revision, author, date_string, num_lines):
        super(LogEntry, self).__init__()

        self.revision = revision
        self.author = author
        self.date_string = date_string
        self.num_lines = num_lines

        self.filedata = []
        self.loglines = []

        self.__date = None

    def __str__(self):
        "Return a brief description of this entry"
        return "r%d l%s [%s] %s" % (self.revision, self.num_lines,
                                    self.author, self.date_string)

    def add_filedata(self, modtype, filename):
        "Add a tuple containing the modification type and the file name"
        self.filedata.append((modtype, filename))

    def add_log(self, logline):
        "Add a line of text to the log message"
        self.loglines.append(logline.rstrip())

    def clean_data(self):
        "Remove trailing blank lines from the log message"
        while len(self.loglines) > 0 and self.loglines[-1] == "":
            del self.loglines[-1]

    @property
    def date(self):
        if self.date_string is None:
            return None

        if self.__date is None:
            idx = self.date_string.find("(")
            if idx < 0:
                dstr = self.date_string
            else:
                dstr = self.date_string[:idx-1]
            self.__date = datetime.strptime(dstr, "%Y-%m-%d %H:%M:%S %z")
        return self.__date


def handle_connect_stderr(cmdname, line, verbose=False):
    "Throw a special exception for SVN connection errors"

    # E170013: Unable to connect to a repository
    conn_err = line.find("E170013: ")
    if conn_err >= 0:
        raise SVNConnectException(line[conn_err+9:])

    # E175012: Connection timed out
    conn_err = line.find("E175012: ")
    if conn_err >= 0:
        raise SVNConnectException(line[conn_err+9:])

    # E000110: Error running context: Connection timed out
    conn_err = line.find("E000110: ")
    if conn_err >= 0 and line.find("Connection timed out") > 0:
        raise SVNConnectException(line[conn_err+9:])

    raise CommandException("%s failed: %s" % (cmdname, line))


def svnadmin_create(project_name, debug=False, dry_run=False, verbose=False):
    cmd_args = ("svnadmin", "create", project_name)

    run_command(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(), debug=debug,
                dry_run=dry_run, verbose=verbose)


def svn_add(filelist, sandbox_dir=None, debug=False, dry_run=False,
            verbose=False):
    "Add the specified files/directories to the SVN commit"

    if isinstance(filelist, (tuple, list)):
        if len(filelist) == 0:
            raise CommandException("Empty list of files to add")
        cmd_args = ["svn", "add"] + filelist
    else:
        if filelist == "":
            raise CommandException("No files to add")
        cmd_args = ("svn", "add", str(filelist))

    run_command(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
                working_directory=sandbox_dir, debug=debug, dry_run=dry_run,
                verbose=verbose)


def svn_checkout(svn_url, revision=None, target_dir=None,
                 ignore_externals=False, debug=False, dry_run=False,
                 verbose=False):
    "Check out a project in the current directory"
    cmd_args = ["svn", "checkout"]

    if revision is not None:
        cmd_args.append("-r%d" % revision)
    if ignore_externals:
        cmd_args.append("--ignore-externals")

    cmd_args.append(svn_url)

    if target_dir is not None:
        cmd_args.append(target_dir)

    run_command(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
                debug=debug, dry_run=dry_run, verbose=verbose)


def svn_commit(sandbox_dir, commit_message, debug=False, dry_run=False,
               verbose=False):
    "Commit all changes in the sandbox directory"
    if dry_run:
        print("SVN COMMIT %s" % (sandbox_dir, ))
        return

    logfile = tempfile.NamedTemporaryFile(mode="w", delete=False)
    try:
        # write log message to a temporary file
        print(commit_message, file=logfile, end="")
        logfile.close()

        cmd_args = ("svn", "commit", "-F", logfile.name)

        run_command(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
                    working_directory=sandbox_dir, debug=debug,
                    dry_run=dry_run, verbose=verbose)
    finally:
        os.unlink(logfile.name)


def svn_copy(source, destination, log_message=None, revision=None,
             pin_externals=False, sandbox_dir=None, debug=False,
             dry_run=False, verbose=False):
    "Copy source file/directory to destination"

    logfile = tempfile.NamedTemporaryFile(mode="w", delete=False)
    try:
        if log_message is not None:
            # write log message to a temporary file
            print(log_message, file=logfile, end="")
            logfile.close()

        cmd_args = ["svn", "copy"]

        if log_message is not None:
            cmd_args.append("-F%s" % logfile.name)

        if revision is not None:
            cmd_args.append("-r%s" % revision)
        if pin_externals:
            cmd_args.append("--pin-externals")

        cmd_args.append(source)
        cmd_args.append(destination)

        run_command(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
                    working_directory=sandbox_dir, debug=debug,
                    dry_run=dry_run, verbose=verbose)
    finally:
        os.unlink(logfile.name)


def svn_get_externals(svn_url=None, debug=False, dry_run=False, verbose=False):
    """
    Generate a list of tuples containing
    (revision, external_url, subdirectory)
    """
    try:
        for line in svn_propget(svn_url, "svn:externals", debug=debug,
                                dry_run=dry_run, verbose=False):
            line = line.rstrip().decode("utf-8")
            if line == "":
                continue

            flds = line.split()
            if len(flds) == 2:
                rev = None
                fld0 = flds[0]
                fld1 = flds[1]
            else:
                fld0 = fld1 = None
                for fld in flds:
                    if fld.startswith("-r"):
                        rev = int(fld[2:])
                    elif fld0 is None:
                        fld0 = fld
                    elif fld1 is None:
                        fld1 = fld
                    else:
                        raise CommandException("Bad external definition \"%s\""
                                               " for %s" % (line, svn_url))

            if fld0.startswith("http"):
                yield (rev, fld0, fld1)
                continue

            if fld1.startswith("http"):
                yield (rev, fld1, fld0)
                continue

            raise CommandException("Unrecognized externals line \"%s\"" %
                                   "  for %s" % (svn_url, line))

    except CommandException as cex:
        if str(cex).find("W200017") >= 0:
            # return None for projects with no externals
            return
        raise


def svn_get_properties(sandbox_dir, revision, debug=False, verbose=False):
    "Get the SVN properties for the specified revision"
    cmd_args = ("svn", "proplist", "--revprop", "-r", str(revision), "-v", ".")

    if debug:
        print("CMD: %s (in workspace %s)" % (" ".join(cmd_args), sandbox_dir))
    proc = subprocess.Popen(cmd_args,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, close_fds=True,
                            cwd=sandbox_dir)

    (state_reading, state_save_author, state_save_date, state_save_log) = \
      (1, 2, 3, 4)

    author = None
    date = None
    log = None

    state = state_reading

    cache = []
    for line in proc.stdout:
        line = line.rstrip().decode("utf-8")

        cache.append(line)

        if state == state_reading:
            svn_idx = line.find("svn:")
            if svn_idx >= 0:
                propname = line[svn_idx+4:]
                if propname == "author":
                    state = state_save_author
                elif propname == "date":
                    state = state_save_date
                elif propname == "log":
                    state = state_save_log
                else:
                    print("Unknown SVN property \"%s\"" %
                          (propname, ), file=sys.stderr)
            continue

        if state == state_save_author:
            author = line.strip()
            state = state_reading
        elif state == state_save_date:
            date = line.strip()
            state = state_reading
        elif state == state_save_log:
            if log is None:
                log = line.strip()
            else:
                log += "\n" + line
        else:
            print("Unknown get_properties state '%s'\n" %
                  (state, ), file=sys.stderr)

    # wait for subprocess to finish
    proc.wait()

    if proc.returncode != 0:
        if not verbose:
            print("Output from '%s'" % " ".join(cmd_args[:2]), file=sys.stderr)
            for line in cache:
                print(">> %s" % line, file=sys.stderr)
        raise SVNException("PropList failed with returncode %d" %
                           proc.returncode)

    if author is None:
        raise SVNException("No svn:author property for rev %s" % revision)
    if date is None:
        raise SVNException("No svn:date property for rev %s" % revision)
    if log is None:
        raise SVNException("No svn:log property for rev %s" % revision)

    return (author, date, log)


def svn_info(svn_url=None, debug=False, dry_run=False, verbose=False):
    """
    Return information about the SVN repository at 'svn_url', which is
    either a Subversion URL or a path to a Subversion sandbox directory.
    If no 'svn_url' is supplied, use the current directory.
    """
    if svn_url is None:
        svn_url = "."

    if dry_run:
        print("SVN INFO %s" % str(svn_url))
        return None

    cmd_args = ("svn", "info", svn_url)

    if debug:
        print("CMD: %s" % " ".join(cmd_args))
    proc = subprocess.Popen(cmd_args, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, close_fds=True)

    for line in proc.stderr:
        if line.find("W170000") >= 0:
            raise SVNNonexistentException(svn_url)
        raise SVNException("Cannot get info from %s: %s" %
                           (svn_url, line.rstrip().decode("utf-8")))

    info = DictObject()

    cache = []
    for line in proc.stdout:
        line = line.rstrip().decode("utf-8")

        if verbose:
            print("INFO>> %s" % (line, ))
        else:
            cache.append(line)

        if line == "":
            continue

        try:
            name, value = line.rstrip().split(": ", 1)
        except ValueError:
            print("Cannot split \"%s\" at colon" % line.rstrip())
            raise
        info.set_value(name.lower().replace(" ", "_"), value)

    # wait for subprocess to finish
    proc.wait()

    if proc.returncode != 0:
        if not verbose:
            print("Output from '%s'" % " ".join(cmd_args[:2]), file=sys.stderr)
            for line in cache:
                print(">> %s" % line, file=sys.stderr)
        if len(info) == 0:
            raise SVNException("Cannot get info from %s: process returned %d" %
                               (svn_url, proc.returncode))

    return info


class ListHandler(object):
    "Retry 'svn ls' command if it times out"

    VERBOSE_PAT = None

    def __init__(self, svn_url, revision=None, list_verbose=False,
                 debug=False, dry_run=False, verbose=False):
        if svn_url is None:
            svn_url = "."

        self.__cmd_args = ["svn", "ls", ]

        if revision is not None:
            self.__cmd_args.append("-r%s" % (revision, ))
        if list_verbose:
            self.__cmd_args.append("-v")

        self.__cmd_args.append(svn_url)

        self.__list_verbose = list_verbose

        self.__debug = debug
        self.__dry_run = dry_run
        self.__verbose = verbose

        self.__saw_error = False

    def handle_rtncode(self, cmdname, rtncode, lines, verbose=False):
        if not self.__saw_error:
            default_returncode_handler(cmdname, rtncode, lines,
                                       verbose=verbose)

    def handle_stderr(self, cmdname, line, verbose=False):
        self.__saw_error = True
        print("*** SVN LS error: %s" % (line, ), file=sys.stderr)

    def run(self):
        cmdname = " ".join(self.__cmd_args[:2]).upper()

        now_year = None
        while True:
            for line in run_generator(self.__cmd_args, cmdname,
                                      returncode_handler=self.handle_rtncode,
                                      stderr_handler=self.handle_stderr,
                                      debug=self.__debug,
                                      dry_run=self.__dry_run,
                                      verbose=self.__verbose):
                if not self.__list_verbose:
                    yield line
                    continue

                mtch = self.verbose_pattern().match(line)
                if mtch is not None:
                    size = int(mtch.group(1))
                    user = mtch.group(2)
                    month = mtch.group(3)
                    day = int(mtch.group(4))
                    year_or_time = mtch.group(5)
                    filename = mtch.group(6)

                    if year_or_time.find(":") < 0:
                        datestr = "%s %d %d" % (month, day, int(year_or_time))
                        date = datetime.strptime(datestr, "%b %d %Y")
                    else:
                        if now_year is None:
                            now = datetime.now()
                            now_year = now.year

                        datestr = "%s %d %d %s" % \
                          (month, day, now_year, year_or_time)
                        date = datetime.strptime(datestr, "%b %d %Y %H:%M")

                    yield (size, user, date, filename)
                    continue

                print("ERROR: Bad verbose listing line: %s" % (line, ),
                      file=sys.stderr)

            # no errors seen, we're done
            if not self.__saw_error:
                break

            # reset flag and try again
            self.__saw_error = False

    @classmethod
    def verbose_pattern(cls):
        if cls.VERBOSE_PAT is None:
            cls.VERBOSE_PAT = re.compile(r"^\s*(\d+)\s(.*\S)\s+(\S\S\S)"
                                         r"\s(\d\d)\s+(\d\d\d\d|\d\d:\d\d)"
                                         r"\s(.*)\s*$")
        return cls.VERBOSE_PAT


def svn_list(svn_url=None, revision=None, list_verbose=False, debug=False,
             dry_run=False, verbose=False):
    "List all entries of the Subversion directory found at 'url'"

    handler = ListHandler(svn_url, revision, list_verbose=list_verbose,
                          debug=debug, dry_run=dry_run, verbose=verbose)
    for list_data in handler.run():
        yield list_data


def svn_log(svn_url=None, revision=None, end_revision=None, num_entries=None,
            stop_on_copy=False, debug=False, dry_run=False):
    """
    Return a list of all log messages or, if an SVN revision is specified,
    a list with the single log message.
    """
    if svn_url is None:
        svn_url = "."

    if dry_run:
        if revision is None:
            rstr = ""
        elif end_revision is None:
            rstr = " -r%s" % revision
        else:
            rstr = " -r%s:%s" % (revision, end_revision)

        if num_entries is None:
            nstr = ""
        else:
            nstr = " -l%d" % int(num_entries)

        print("SVN LOG%s%s %s" % (rstr, nstr, svn_url))
        return

    # build the command
    cmd_args = ["svn", "log", "-v"]
    if revision is None:
        if end_revision is not None:
            raise CommandException("Found end revision %s without start"
                                   " revision" % (end_revision, ))
    else:
        if end_revision is None:
            cmd_args.append("-r%s" % revision)
        else:
            cmd_args.append("-r%s:%s" % (revision, end_revision))
    if num_entries is not None:
        cmd_args.append("-l%d" % int(num_entries))
    if stop_on_copy:
        cmd_args.append("--stop-on-copy")
    cmd_args.append(svn_url)

    if debug:
        print("CMD: %s" % " ".join(cmd_args))
    proc = subprocess.Popen(cmd_args, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, close_fds=True)

    # set up some constants before we start parsing
    (state_initial, state_saw_dashes, state_saw_props, state_file_list,
     state_logmsg) = (1, 2, 3, 4, 5)
    dashes = "-"*70

    # variable holding the current log entry
    logentry = None

    # parse everything
    state = state_initial
    saw_error = False
    eof_err = False
    while True:
        if eof_err:
            reads = [proc.stdout.fileno(), ]
        else:
            reads = [proc.stdout.fileno(), proc.stderr.fileno()]

        try:
            ret = select.select(reads, [], [])
        except select.error:
            # ignore a single interrupt
            if saw_error:
                break
            saw_error = True
            continue

        # deal with stderr and unknown input
        for fno in ret[0]:
            if fno == proc.stderr.fileno():
                line = proc.stderr.readline().rstrip().decode("utf-8")
                if line == "":
                    eof_err = True
                    continue

                if revision is None:
                    rstr = ""
                else:
                    rstr = " rev %d" % revision
                raise SVNException("Cannot list %s%s: %s" %
                                   (svn_url, rstr, line.strip()))

            if fno != proc.stdout.fileno():
                raise SVNException("Received line from unknown source"
                                   " for %s%s" % (svn_url, rstr))

        # parse the line from stdout
        line = proc.stdout.readline().rstrip().decode("utf-8")
        if state == state_initial:
            if line.startswith(dashes):
                state = state_saw_dashes
                if debug:
                    print(dashes)
                continue

            if revision is None:
                rstr = ""
            else:
                rstr = " rev %d" % revision
            raise SVNException("Bad initial line for %s%s: %s" %
                               (svn_url, rstr, line, ))

        if state == state_saw_dashes:
            if line == "":
                break

            mtch = LOG_PROP_PAT.match(line)
            if mtch is None:
                if revision is None:
                    rstr = ""
                else:
                    rstr = " rev %d" % revision
                raise SVNException("Bad post-dashes line for %s%s: %s" %
                                   (svn_url, rstr, line, ))

            state = state_saw_props

            trev = int(mtch.group(1))
            tauthor = mtch.group(2)
            tdatestr = mtch.group(3)
            tnum_lines = int(mtch.group(4))

            logentry = LogEntry(trev, tauthor, tdatestr, tnum_lines)
            if debug:
                print(str(logentry))

            continue

        if state == state_saw_props:
            if line.find("Changed paths") >= 0:
                state = state_file_list
                continue

            if revision is None:
                rstr = ""
            else:
                rstr = " rev %d" % revision
            raise SVNException("Bad post-properties line for %s%s: %s" %
                               (svn_url, rstr, line, ))

        if state == state_file_list:
            if line == "":
                state = state_logmsg
                continue

            mtch = LOG_FILE_PAT.match(line)
            if mtch is not None:
                modtype = mtch.group(1)
                filename = mtch.group(2)
                logentry.add_filedata(modtype, filename)
                continue

            if revision is None:
                rstr = ""
            else:
                rstr = " rev %d" % revision
            raise SVNException("Bad file line for %s%s: %s" %
                               (svn_url, rstr, line, ))

        if state == state_logmsg:
            if line.startswith(dashes):
                state = state_saw_dashes

                logentry.clean_data()
                yield logentry

                logentry = None

                continue

            logentry.add_log(line)
            continue

    if logentry is not None:
        logentry.clean_data()
        yield logentry

    # wait for subprocess to finish
    proc.wait()

    if proc.returncode != 0:
        if revision is None:
            rstr = ""
        else:
            rstr = " rev %d" % revision
        raise SVNException("Cannot list %s%s: process returned %d" %
                           (svn_url, rstr, proc.returncode))


def svn_mkdir(dirlist, sandbox_dir=None, create_parents=False, debug=False,
              dry_run=False, verbose=False):
    "Add the specified directories to the SVN workspace"

    if isinstance(dirlist, (tuple, list)):
        if len(dirlist) == 0:
            raise CommandException("Empty list of files to add")
    else:
        if dirlist == "":
            raise CommandException("No files to add")
        dirlist = (str(dirlist), )

    cmd_args = ["svn", "mkdir"]

    if create_parents:
        cmd_args.append("--parents")

    cmd_args += dirlist

    run_command(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
                working_directory=sandbox_dir, debug=debug, dry_run=dry_run,
                verbose=verbose)


def svn_propget(svn_url, propname, revision=None, sandbox_dir=None,
                debug=False, dry_run=False, verbose=False):
    "Return the value(s) associated with a Subversion property"
    if svn_url is None:
        svn_url = "."

    cmd_args = ["svn", "propget", propname]
    if revision is not None:
        cmd_args += ("--revprop", "-r", str(revision))
    cmd_args.append(svn_url)

    cmdname = " ".join(cmd_args[:2]).upper()
    for _ in (0, 1, 2):
        try:
            for line in run_generator(cmd_args, cmdname=cmdname,
                                      working_directory=sandbox_dir,
                                      stderr_handler=handle_connect_stderr,
                                      debug=debug, dry_run=dry_run,
                                      verbose=verbose):
                yield line
            break
        except SVNConnectException:
            continue


def svn_propset(svn_url, propname, value, sandbox_dir=None, revision=None,
                debug=False, dry_run=False, verbose=False):
    "Set a Subversion property value"
    if svn_url is None:
        svn_url = "."

    if value is None:
        raise SVNException("Cannot set property \"%s\" to None for %s" %
                           (propname, svn_url))

    propfile = tempfile.NamedTemporaryFile(mode="w", delete=False)
    try:
        print(value, end="", file=propfile)
        propfile.close()

        cmd_args = ["svn", "propset", propname]
        if revision is not None:
            cmd_args += ("--revprop", "-r", str(revision))
        cmd_args += ("-F", propfile.name, svn_url)

        for line in run_generator(cmd_args,
                                  cmdname=" ".join(cmd_args[:2]).upper(),
                                  working_directory=sandbox_dir,
                                  debug=debug, dry_run=dry_run,
                                  verbose=verbose):
            if line.find("set on repository revision %d" % (revision, )) < 0:
                raise CommandException("Bad 'propset' reply: %s" % str(line))
    finally:
        os.unlink(propfile.name)


def svn_remove(filelist, sandbox_dir=None, debug=False, dry_run=False,
               verbose=False):
    "Remove the specified files/directories from the SVN commit"

    if isinstance(filelist, (tuple, list)):
        if len(filelist) == 0:
            raise CommandException("Empty list of files to remove")
        cmd_args = ["svn", "remove"] + filelist
    else:
        if filelist == "":
            raise CommandException("No files to remove")
        cmd_args = ("svn", "remove", str(filelist))

    run_command(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
                working_directory=sandbox_dir, debug=debug, dry_run=dry_run,
                verbose=verbose)


def svn_revert(pathlist=None, recursive=False, sandbox_dir=None, debug=False,
               dry_run=False, verbose=False):
    "Revert all changes in the specified files/directories"

    if pathlist is None:
        pathlist = (".", )
    elif not isinstance(pathlist, (tuple, list)):
        pathlist = (str(pathlist), )

    cmd_args = ["svn", "revert"]

    if recursive:
        cmd_args.append("-R")

    cmd_args += pathlist

    run_command(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
                working_directory=sandbox_dir, debug=debug,
                dry_run=dry_run, verbose=verbose)


def svn_status(sandbox_dir=None, debug=False, dry_run=False, verbose=False):
    "Return the lines describing the status of the Subversion sandbox"

    cmd_args = ["svn", "status"]

    for line in run_generator(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
                              working_directory=sandbox_dir, debug=debug,
                              dry_run=dry_run, verbose=verbose):
        yield line


class SwitchHandler(object):
    def __init__(self, svn_url=None, revision=None, ignore_bad_externals=False,
                 ignore_externals=False, debug=False, dry_run=False,
                 verbose=False):

        cmd_args = ["svn", "switch"]
        if ignore_externals:
            cmd_args.append("--ignore-externals")
        if revision is None:
            cmd_args.append(str(svn_url))
        else:
            cmd_args.append("%s@%d" % (svn_url, revision))

        self.__cmd_args = cmd_args

        self.__ignore_bad_externals  = ignore_bad_externals
        self.__ignored_error = False

        self.__debug = debug
        self.__dry_run = dry_run
        self.__verbose = verbose

    def __handle_rtncode(self, cmdname, returncode, saved_output,
                         verbose=False):
        if not self.__ignored_error:
            default_returncode_handler(cmdname, rtncode, lines,
                                       verbose=verbose)

    def __handle_stderr(self, cmdname, line, verbose=False):
        if verbose:
            print("%s!! %s" % (cmdname, line))

        if line.startswith("svn: warning: "):
            print("SWITCH WARNING: %s" % (line, ), file=sys.stderr)
            return

        # E160013: File not found
        if self.__ignore_bad_externals and \
          line.startswith("svn: E160013: "):
            print("SWITCH WARNING: %s" % (line, ), file=sys.stderr)
            self.__ignored_error = True
            return

        raise CommandException("%s failed: %s" % (cmdname, line))

    def run(self):
        cmdname = " ".join(self.__cmd_args[:2]).upper()

        for line in run_generator(self.__cmd_args, cmdname,
                                  returncode_handler=self.__handle_rtncode,
                                  stderr_handler=self.__handle_stderr,
                                  debug=self.__debug, dry_run=self.__dry_run,
                                  verbose=self.__verbose):
            yield line



def svn_switch(svn_url=None, revision=None, ignore_bad_externals=False,
               ignore_externals=False, debug=False, dry_run=False,
               verbose=False):
    "Check out a project in the current directory"
    handler = SwitchHandler(svn_url=svn_url, revision=revision,
                            ignore_bad_externals=ignore_bad_externals,
                            ignore_externals=ignore_externals,
                            debug=debug, dry_run=dry_run, verbose=verbose)
    for line in handler.run():
        yield line


class UpdateHandler(object):
    def __init__(self, svn_url=None, sandbox_dir=None, revision=None,
                 ignore_bad_externals=False, ignore_externals=False,
                 debug=False, dry_run=False, verbose=False):
        if sandbox_dir is None:
            self.__sandbox_dir = "."
        else:
            self.__sandbox_dir = sandbox_dir

        self.__cmd_args = ["svn", "update"]

        if revision is not None:
            self.__cmd_args.append("-r%d" % revision)
        if ignore_externals:
            self.__cmd_args.append("--ignore-externals")

        if svn_url is not None:
            self.__cmd_args.append(str(svn_url))

        self.__ignore_bad_externals  = ignore_bad_externals
        self.__ignored_error = False

        self.__debug = debug
        self.__dry_run = dry_run
        self.__verbose = verbose

    def __handle_rtncode(self, cmdname, returncode, saved_output,
                         verbose=False):
        if not self.__ignored_error:
            default_returncode_handler(cmdname, rtncode, lines,
                                       verbose=verbose)

    def __handle_stderr(self, cmdname, line, verbose=False):
        if verbose:
            print("%s!! %s" % (cmdname, line))

        if line.startswith("svn: warning: "):
            print("UPDATE WARNING: %s" % (line, ), file=sys.stderr)
            return

        # E195005: 'xxx' is not the root of the repository
        # E205011: Failure occurred processing one or more externals
        if self.__ignore_bad_externals and \
          (line.startswith("svn: E195005: ") or \
           line.startswith("svn: E205011: ")):
            print("UPDATE WARNING: %s" % (line, ), file=sys.stderr)
            self.__ignored_error = True
            return

        handle_connect_stderr(cmdname, line, verbose=verbose)

    def run(self):
        cmdname = " ".join(self.__cmd_args[:2]).upper()

        for line in run_generator(self.__cmd_args, cmdname,
                                  working_directory=self.__sandbox_dir,
                                  returncode_handler=self.__handle_rtncode,
                                  stderr_handler=self.__handle_stderr,
                                  debug=self.__debug, dry_run=self.__dry_run,
                                  verbose=self.__verbose):
            yield line


def svn_update(svn_url=None, sandbox_dir=None, revision=None,
               ignore_bad_externals=False, ignore_externals=False, debug=False,
               dry_run=False, verbose=False):
    "Check out a project in the current directory"
    handler = UpdateHandler(svn_url=svn_url, sandbox_dir=sandbox_dir,
                            revision=revision,
                            ignore_externals=ignore_externals,
                            ignore_bad_externals=ignore_bad_externals,
                            debug=debug, dry_run=dry_run, verbose=verbose)
    for line in handler.run():
        yield line


class SVNMetadata(object):
    # top-level directory names
    DIRTYPE_TRUNK = "trunk"
    DIRTYPE_BRANCHES = "branches"
    DIRTYPE_TAGS = "tags"

    # lists of all possible top-level directory names
    TRUNK_NAME = DIRTYPE_TRUNK
    BRANCH_NAME = DIRTYPE_BRANCHES
    TAG_NAME = DIRTYPE_TAGS

    def __init__(self, url=None, directory=None, repository_root=None,
                 project_base=None, project_name=None, branch_name=None,
                 trunk_subdir=None, branches_subdir=None, tags_subdir=None):
        """
        Gather information about this Subversion repository.

        Parameters:
        url - if not None, Subversion URL to examine
        directory - If url is None, the path to a Subversion sandbox directory.
                    If directory is also None, examine the current directory.

        Optional parameters (these may have been fetched from a database)
        repository_root - the root URL for this Subversion repository
        project_base - the base path (relative to 'repository_root') for
                       repository projects (e.g. 'projects' or 'meta-projects')
        project_name - the name of this project
        trunk_subdir - either "" (for simple projects) or (probably) "trunk"
        branches_subdir - either None or the branch subdirectory name
        tags_subdir - either None or the tags subdirectory name

        Return an object with the following attributes:
        url - the original Subversion URL
        repository_root - the root URL for this Subversion repository
        project_base - the base path (relative to 'repository_root') for
                       repository projects (e.g. 'projects' or 'meta-projects')
        project_name - the name of this project
        top_url - the full URL for this project (may be different from 'url'
                  if the original URL pointed to a project subdirectory)
        """

        if url is None or repository_root is None or project_base is None or \
            project_name is None:
            (url, repository_root, project_base, project_name, branch_name) = \
              self.__load_metadata(url, directory)

        self.__url = url
        self.__root_url = repository_root
        self.__base_subdir = project_base
        self.__project_name = project_name
        self.__branch_name = branch_name

        self.__base_url = None
        self.__project_url = None
        self.__top_url = None

        # user may supply these
        self.__fetched_subdirs = trunk_subdir is not None
        self.__trunk_subdir = trunk_subdir
        self.__branches_subdir = branches_subdir
        self.__tags_subdir = tags_subdir

        self.__trunk_url = None
        self.__branches_url = None
        self.__tags_url = None

    def __str__(self):
        substr = self.__subdir_str(self.trunk_subdir, "T") + \
          self.__subdir_str(self.branches_subdir, "B") + \
          self.__subdir_str(self.tags_subdir, "G")

        return "Metadata(%s -> %s :: %s :: %s // %s)" % \
          (self.__url, self.__root_url, self.__base_subdir,
           self.__project_name, substr)

    @classmethod
    def __subdir_str(cls, subdir, subchar):
        if subdir is None:
            return "!" + subchar
        if subdir != "":
            return subchar + "+"
        return subchar

    @classmethod
    def __fetch_subdirs(cls, project_url):
        "Find the standard trunk/branches/tags subdirectories"

        (trunk_subdir, branches_subdir, tags_subdir) = (None, None, None)
        for entry in svn_list(project_url):
            if not entry.endswith("/"):
                continue

            entry = entry[:-1]
            if entry == SVNMetadata.TRUNK_NAME:
                trunk_subdir = entry
            elif entry == SVNMetadata.BRANCH_NAME:
                branches_subdir = entry
            elif entry == SVNMetadata.TAG_NAME:
                tags_subdir = entry

        if trunk_subdir is None:
            trunk_subdir = ""

        return (trunk_subdir, branches_subdir, tags_subdir)

    def __fill_subdirs(self):
        self.__fetched_subdirs = True
        self.__trunk_subdir, self.__branches_subdir, self.__tags_subdir = \
          self.__fetch_subdirs(self.project_url)

    def __load_metadata(self, url, directory):
        if url is not None:
            infodict = svn_info(url)
        else:
            if directory is None:
                directory = "."

            infodict = svn_info(directory)
            url = infodict.url

        # validate the relative URL and remove leading "^/"
        rel_url = infodict.relative_url
        if not rel_url.startswith("^/"):
            raise SVNException("Expected relative root \"%s\" to start"
                               " with \"^/\"" % str(rel_url))
        rel_url = rel_url[2:]

        # cut off the relative URL at the trunk/tags/branches directory
        branch_name = self.TRUNK_NAME
        for subdir in (self.TRUNK_NAME, self.BRANCH_NAME, self.TAG_NAME):
            idx = rel_url.find("/" + subdir)
            if idx >= 0:
                branch_name = rel_url[idx+1:]
                rel_url = rel_url[:idx]
                break

        # split the relative URL into base project URL and project name
        try:
            proj_base, name = rel_url.rsplit("/", 1)
        except ValueError:
            raise SVNException("Cannot extract project name from \"%s\"" %
                               (infodict.relative__url, ))

        return (url, infodict.repository_root, proj_base, name, branch_name)

    @property
    def base_subdir(self):
        return self.__base_subdir

    @property
    def base_url(self):
        if self.__base_url is None:
            self.__base_url = "/".join((self.__root_url,
                                        self.__base_subdir))
        return self.__base_url

    @property
    def branches_subdir(self):
        if not self.__fetched_subdirs:
            self.__fill_subdirs()
        return self.__branches_subdir

    @property
    def branches_url(self):
        "Return the URL for this project's 'branches' subdirectory"
        if self.__branches_url is None:
            if self.branches_subdir == "":
                self.__branches_url = self.project_url
            elif self.branches_subdir is not None:
                self.branches_url = "/".join((self.project_url,
                                              self.branches_subdir))
        return self.__branches_url

    def all_urls(self, ignore=None):
        """
        Generate Subversion URLs for trunk and all branch/tag subdirectories,
        returning tuples containing (dirtype, dirname, url)

        If specified, 'ignore' is a method which returns True if the
        branches/tags subdirectory should be ignored
        (I use this to screen out debugging/development branches)
        """

        # build the map of Subversion subdirectories
        dir_pairs = ((self.trunk_subdir, self.DIRTYPE_TRUNK),
                     (self.branches_subdir, self.DIRTYPE_BRANCHES),
                     (self.tags_subdir, self.DIRTYPE_TAGS))

        for dirname, dirtype in dir_pairs:
            if dirname is None:
                continue

            # build the Subversion URL
            if dirname == "":
                top_url = self.project_url
            else:
                top_url = "%s/%s" % (self.project_url, dirname)

            if dirtype == self.DIRTYPE_TRUNK:
                yield dirtype, dirname, top_url
                continue

            for entry in svn_list(top_url):
                if not entry.endswith("/"):
                    continue

                entry = entry[:-1]
                if ignore is not None and ignore(entry):
                    continue

                yield dirtype, dirname, "%s/%s" % (top_url, entry)

    @property
    def project_base(self):
        return self.__base_subdir

    @property
    def project_name(self):
        return self.__project_name

    @property
    def project_url(self):
        if self.__project_url is None:
            self.__project_url = "%s/%s" % \
              (self.base_url, self.__project_name)

        return self.__project_url

    @property
    def root_url(self):
        return self.__root_url

    @classmethod
    def set_layout(cls, dirtype, name):
        """
        Set this repository's "trunk", "branches", or "tags" subdirectory
        name to a non-standard value
        For example, a project might use "releases" instead of "tags"
        """
        if dirtype == cls.DIRTYPE_TRUNK:
            cls.TRUNK_NAME = name
        elif dirtype == cls.DIRTYPE_BRANCHES:
            cls.BRANCH_NAME = name
        elif dirtype == cls.DIRTYPE_TAGS:
            cls.TAG_NAME = name
        else:
            raise SVNException("Unknown directory type \"%s\"" % str(dirtype))

    @property
    def branch_name(self):
        return self.__branch_name

    @property
    def tags_subdir(self):
        if not self.__fetched_subdirs:
            self.__fetched_subdirs = True
            self.__fill_subdirs()
        return self.__tags_subdir

    @property
    def tags_url(self):
        "Return the URL for this project's 'tags' subdirectory"
        if self.__tags_url is None:
            if self.tags_subdir is not None:
                self.__tags_url = "/".join((self.project_url,
                                            self.tags_subdir))
        return self.__tags_url

    @property
    def top_url(self):
        if self.__top_url is None:
            self.__top_url = "/".join((self.__root_url, self.__base_subdir))

        return self.__top_url

    @property
    def trunk_subdir(self):
        if not self.__fetched_subdirs:
            self.__fetched_subdirs = True
            self.__fill_subdirs()
        return self.__trunk_subdir

    @property
    def trunk_url(self):
        "Return the URL for this project's 'trunk' directory"
        if self.__trunk_url is None:
            if self.trunk_subdir is not None:
                self.__trunk_url = "/".join((self.project_url,
                                             self.trunk_subdir))
        return self.__trunk_url

    @property
    def original_url(self):
        return self.__url


if __name__ == "__main__":
    SVNMetadata.set_layout(SVNMetadata.DIRTYPE_TAGS, "releases")
    for arg in sys.argv[1:]:
        metadata = SVNMetadata(arg)
