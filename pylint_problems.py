#!/usr/bin/env python3

from __future__ import print_function

import argparse
import fnmatch
import os
import re
import subprocess
import sys
import traceback


class PylintProblems(object):
    variable_message_problem = ["C0330", ]
    def __init__(self, file_blacklist=None, ignored=None):
        self.__file_blacklist = file_blacklist
        self.__ignored = ignored
        self.__file_map = {}
        self.__description = {}

    def add_file(self, filename):
        if self.__file_blacklist is not None:
            for entry in self.__file_blacklist:
                if filename.find(entry) >= 0:
                    return

        problem_pat = re.compile(r"^([^:]+):\s+(\d+):(-?\d+):\s+([A-Z]\d+):"
                                 r"\s+(.*)$")
        back_pat = re.compile(r".*\s+\(([^)]*)\)\.?\s*$")
        with open(filename, "r") as fin:
            for line in fin:
                mtch = problem_pat.match(line)
                if mtch is None:
                    print("%s: No problems in \"%s\"" %
                          (filename, line.rstrip()), file=sys.stderr)
                    continue

                name = mtch.group(1)
                linenum = int(mtch.group(2))
                charpos = int(mtch.group(3))
                problem_id = mtch.group(4)
                back_str = mtch.group(5)

                mtch2 = back_pat.match(back_str)
                if mtch2 is None:
                    description = back_str
                else:
                    description = mtch2.group(1)

                # should we ignore this problem?
                if self.__ignored is not None and \
                  problem_id in self.__ignored:
                    continue

                if problem_id not in self.__description:
                    self.__description[problem_id] = description
                elif description != self.__description[problem_id] and \
                    problem_id not in self.variable_message_problem:
                    print("ERROR: Problem %s is both \"%s\" and \"%s\"" %
                          (problem_id, self.__description[problem_id],
                           description), file=sys.stderr)

                if charpos == 0:
                    file_loc = "%s:%d" % (name, linenum)
                else:
                    file_loc = "%s:%d (char %d)" % (name, linenum, charpos)

                if problem_id not in self.__file_map:
                    self.__file_map[problem_id] = [file_loc, ]
                else:
                    self.__file_map[problem_id].append(file_loc)

    def report(self, verbose=False):
        counts = {}

        total = 0
        for prob_id, file_list in self.__file_map.items():
            flen = len(file_list)
            counts[prob_id] = flen
            total += flen

        for prob_id, count in sorted(counts.items(),
                                     key=lambda item: (item[1], item[0])):
            if prob_id in self.__description:
                descr = self.__description[prob_id]
            else:
                descr = "?? unknown ??"

            print("%3d   %s (%s)" % (count, prob_id, descr))

            if verbose:
                for file_loc in sorted(self.__file_map[prob_id]):
                    print("\t%s" % file_loc)

        print("\nTotal problems found: %d" % total)

    @classmethod
    def check_all(cls, topdir, file_blacklist=None, ignored=None,
                  package_whitelist=None, python2_only=False, verbose=False):
        for dirpath, _, files in os.walk(topdir):
            for pynm in files:
                if not pynm.endswith(".py"):
                    continue

                pypath = os.path.join(dirpath, pynm)
                cls.check_one(pypath, file_blacklist=file_blacklist,
                              ignored=ignored,
                              package_whitelist=package_whitelist,
                              python2_only=python2_only,
                              verbose=verbose)

    @classmethod
    def check_one(cls, path, file_blacklist=None, ignored=None,
                  package_whitelist=None, python2_only=False, verbose=False):
        # name of the file where pylint output is written
        lintpath = path + "lint"

        if not cls.is_newer_file(path, lintpath):
            tmppath = path + ".ltmp"
            try:
                cls.run_pylint(path, tmppath, file_blacklist=file_blacklist,
                               ignored=ignored,
                               package_whitelist=package_whitelist,
                               python2_only=python2_only, verbose=verbose)
                os.rename(tmppath, lintpath)
            except:  # pylint: disable=bare-except
                print("*** Error for %s\n%s" % (path, traceback.format_exc()))

                # remove temporary file
                if os.path.exists(tmppath):
                    os.unlink(tmppath)

    @classmethod
    def is_newer_file(cls, old_path, new_path):
        """
        Return True if 'new_path' has been modified more recently
        than 'old_path'
        """
        if not os.path.exists(new_path):
            return False

        if not os.path.exists(old_path):
            if old_path.find(".#") < 0:
                raise Exception("ERROR: %s doesn't exist!" % str(old_path))
            return False

        old_time = os.path.getmtime(old_path)
        new_time = os.path.getmtime(new_path)
        return new_time > old_time

    @classmethod
    def run_pylint(cls, path, outpath, file_blacklist=None, ignored=None,
                   package_whitelist=None, python2_only=False, verbose=False):
        # if this file is blacklisted, skip it
        basename = os.path.basename(path)
        if file_blacklist is not None and basename in file_blacklist:
            return

        cmd = ["pylint", ]

        # add any ignored checkers/messages
        if ignored is not None and len(ignored) > 0:
            cmd.append("--disable=%s" % ",".join(ignored))

        # add any whitelisted packages (e.g. lxml)
        if package_whitelist is not None and len(package_whitelist) > 0:
            cmd.append("--extension-pkg-whitelist=%s" %
                       ",".join(package_whitelist))

        if not python2_only:
            cmd.append("--py3k")

        # add the file to be checked
        cmd.append(path)

        if verbose:
            print("Updating pylint file for %s" % os.path.basename(path),
                  file=sys.stderr)

        try:
            proc = subprocess.run(cmd, capture_output=True, check=False,
                                  encoding="utf-8")
        except subprocess.CalledProcessError as cpe:
            if cpe.returncode >= 32:
                raise Exception("Cannot run 'pylint' on \"%s\"\n%s\n"
                                " Return code: %d" %
                                (path, cpe.output, cpe.returncode))
            #print("Using %d lines from exception:\n%s" %
            #      (len(cpe.output), traceback.format_exc()), file=sys.stderr)
            #outlines = cpe.output

        pylint_code_pat = re.compile(r".*: [A-Z]\d\d\d\d: .*$")
        fix_fmt_pat = re.compile(r"^(.*):(\d+:\d+: [A-Z]\d\d\d\d: .*)$")

        with open(outpath, "w") as fout:
            for line in proc.stdout.split("\n"):
                line = line.rstrip()

                mtch = pylint_code_pat.match(line)
                if mtch is None:
                    # line doesn't have a PyLint code (like C0123 or R5432)
                    continue

                mtch = fix_fmt_pat.match(line)
                if mtch is not None:
                    line = mtch.group(1) + ": " + mtch.group(2)

                print("%s" % line, file=fout)


def main():
    "Main program"

    parser = argparse.ArgumentParser()
    parser.add_argument("-2", "--python2-only", dest="python2_only",
                        action="store_true", default=False,
                        help=("Do not check for Python 3 porting problems"))
    parser.add_argument("-F", "--blacklist", dest="blacklist",
                        help=("Comma-separated list of files to ignore"))
    parser.add_argument("-i", "--ignore", dest="ignored",
                        help=("Comma-separated list of pylint problems"
                              " to ignore"))
    parser.add_argument("-R", "--print-full-report", dest="print_full",
                        action="store_true", default=False,
                        help=("Print all pylint problems and their locations"))
    parser.add_argument("-r", "--print-report", dest="print_report",
                        action="store_true", default=False,
                        help=("Print all pylint problems and their frequency"))
    parser.add_argument("-v", "--verbose", dest="verbose",
                        action="store_true", default=False,
                        help="Print a list of file positions for each problem")
    parser.add_argument("-w", "--whitelist", dest="whitelist",
                        help=("Comma-separated list of Python packages"
                              " to whitelist"))
    parser.add_argument(dest="files", nargs="*")
    args = parser.parse_args()

    # blacklisted files
    if args.blacklist is None:
        blacklist = None
    else:
        blacklist = args.blacklist.split(",")

    # whitelisted packages
    if args.whitelist is None:
        whitelist = None
    else:
        whitelist = args.whitelist.split(",")

    # list of ignored errors
    if args.ignored is None:
        ignored_list = None
    else:
        ignored_list = args.ignored.split(",")

    # run pylint against all modifed files
    if len(args.files) == 0:  # pylint: disable=len-as-condition
        PylintProblems.check_all(".", file_blacklist=blacklist,
                                 ignored=ignored_list,
                                 package_whitelist=whitelist,
                                 python2_only=args.python2_only,
                                 verbose=args.verbose)
    else:
        for prog in args.files:
            PylintProblems.check_one(prog, file_blacklist=blacklist,
                                     ignored=ignored_list,
                                     package_whitelist=whitelist,
                                     python2_only=args.python2_only,
                                     verbose=args.verbose)

    pprob = PylintProblems(file_blacklist=blacklist, ignored=ignored_list)
    for entry in fnmatch.filter(os.listdir("."), "*.pylint"):
        if entry.startswith(".#"):
            continue

        # ignore pylint output from blacklisted files
        if blacklist is not None:
            for fbad in blacklist:
                if entry.find(fbad) >= 0:
                    continue

        pprob.add_file(entry)

    if args.print_report or args.print_full:
        try:
            pprob.report(verbose=args.print_full)
        except IOError:
            pass


if __name__ == "__main__":
    main()
