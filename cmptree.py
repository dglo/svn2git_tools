#!/usr/bin/env python

from __future__ import print_function

import argparse
import filecmp
import os
import sys


class TreePath(object):
    LEFT = 1
    RIGHT = 2
    BOTH = 3

    def __init__(self, pathsrc, dirpath, filename):
        self.__pathsrc = pathsrc
        self.__dirpath = dirpath
        self.__filename = filename

    def __str__(self):
        return os.path.join(self.__dirpath, self.__filename)

    def __repr__(self):
        if self.__pathsrc == self.LEFT:
            clsname = "LeftPath"
        elif self.__pathsrc == self.RIGHT:
            clsname = "RightPath"
        elif self.__pathsrc == self.BOTH:
            clsname = "BothPath"

        return "%s(%s, %s)" % (clsname, self.__dirpath, self.__filename)

    @property
    def directory(self):
        return self.__dirpath

    @property
    def filename(self):
        return self.__filename

    @property
    def is_both(self):
        return self.__pathsrc == self.BOTH

    @property
    def is_left(self):
        return self.__pathsrc == self.LEFT

    @property
    def is_right(self):
        return self.__pathsrc == self.RIGHT


class BothPath(TreePath):
    def __init__(self, dirpath, filename):
        super(BothPath, self).__init__(TreePath.BOTH, dirpath, filename)


class LeftPath(TreePath):
    def __init__(self, dirpath, filename):
        super(LeftPath, self).__init__(TreePath.LEFT, dirpath, filename)


class RightPath(TreePath):
    def __init__(self, dirpath, filename):
        super(RightPath, self).__init__(TreePath.RIGHT, dirpath, filename)


class CompareTrees(object):
    IGNORE = [".git", ".gitignore", ".gitmodules", ".hg", ".hgignore",
              ".svn", "target"]
    IGNORE_EXT = [".pyc", ".class"]

    def __init__(self, left_dir, right_dir, ignore_empty_directories=False):
        self.__left_dir = left_dir
        self.__right_dir = right_dir
        self.__ignore_empty_dirs = ignore_empty_directories

        self.__compared = False

        self.__changed = None
        self.__added = None
        self.__deleted = None

        self.__modified_dict = None

    def __add_to_list(self, filelist, value):
        if filelist is None:
            filelist = []
        filelist.append(value)
        return filelist

    def __compare_trees(self, dcmp=None, depth=99, debug=False):
        self.__compared = True

        # clear previously cached values
        self.__modified_dict = None

        # if we haven't compared anything, build the comparison object
        if dcmp is None:
            dcmp = filecmp.dircmp(self.__left_dir, self.__right_dir,
                                  ignore=self.IGNORE)
        else:
            if debug:
                print("~~~ (%d: L%d/R%d/D%d/F%d) %s" %
                      (depth, len(dcmp.left_only), len(dcmp.right_only),
                       len(dcmp.diff_files), len(dcmp.funny_files), dcmp.left))
                sys.stdout.flush()

        left_subdir = self.__extract_subdir(dcmp.left, self.__left_dir)
        right_subdir = self.__extract_subdir(dcmp.right, self.__right_dir)
        if left_subdir != right_subdir:
            print("Expected left path \"%s\" to match right path \"%s\"" %
                  (left_subdir, right_subdir), file=sys.stderr)

        # build lists of added/changed/deleted files
        for name in dcmp.right_only:
            _, ext = os.path.splitext(name)
            if ext in self.IGNORE_EXT:
                continue

            if self.__ignore_empty_dirs and \
              self.__is_empty_dir(os.path.join(dcmp.right, name)):
                continue

            if debug:
                print("    ++ %s" % name)
            self.__added = self.__add_to_list(self.__added,
                                              RightPath(right_subdir, name))

        for name in dcmp.diff_files:
            _, ext = os.path.splitext(name)
            if ext in self.IGNORE_EXT:
                continue

            if debug:
                print("    ** %s" % name)
            self.__changed = \
              self.__add_to_list(self.__changed, BothPath(left_subdir, name))

        for name in dcmp.left_only:
            _, ext = os.path.splitext(name)
            if ext in self.IGNORE_EXT:
                continue

            if self.__ignore_empty_dirs and \
              self.__is_empty_dir(os.path.join(dcmp.left, name)):
                continue

            if debug:
                print("    -- %s" % name)
            self.__deleted = self.__add_to_list(self.__deleted,
                                                LeftPath(left_subdir, name))

        if depth > 0:
            # check subdirectories
            for subcmp in dcmp.subdirs.values():
                self.__compare_trees(subcmp, depth=depth-1, debug=debug)

        return dcmp

    def __extract_subdir(self, full_path, base_path):
        plen = len(base_path)
        if not full_path.startswith(base_path) or \
          (len(full_path) > plen and full_path[plen] != os.path.sep):
            raise Exception("Expected directory \"%s\" to start with \"%s\"" %
                            (full_path, base_path))

        return full_path[plen+1:]

    def __is_empty_dir(self, path):
        if os.path.isdir(path):
            for entry in os.listdir(path):
                return True
        return False

    def __len(self, filelist):
        return 0 if filelist is None else len(filelist)

    @property
    def added(self):
        if not self.__compared:
            self.__compare_trees()

        for value in self.__added:
            yield value

    @property
    def changed(self):
        if not self.__compared:
            self.__compare_trees()

        for value in self.__changed:
            yield value

    def compare(self, debug=False):
        if not self.__compared:
            self.__compare_trees(debug=debug)

    @property
    def deleted(self):
        if not self.__compared:
            self.__compare_trees()

        for value in self.__deleted:
            yield value

    @property
    def is_modified(self):
        if not self.__compared:
            dcmp = self.__compare_trees()
            #dcmp.report_full_closure()

        return self.__added is not None or \
          self.__changed is not None or \
          self.__deleted is not None

    @property
    def modified_trees(self):
        if self.__modified_dict is None:
            if not self.__compared:
                self.__compare_trees()

            topdict = {}
            for treelist in self.__added, self.__deleted, self.__changed:
                if treelist is None:
                    continue

                for tree_obj in treelist:
                    try:
                        topdir, _ = tree_obj.directory.split(os.path.sep, 1)
                    except:
                        topdir = tree_obj.directory
                    if topdir not in topdict:
                        topdict[topdir] = 1
                    else:
                        topdict[topdir] += 1
            self.__modified_dict = topdict

        return self.__modified_dict


def add_arguments(parser):
    "Add command-line arguments"

    parser.add_argument("-l", "--left-path", dest="left_path",
                        default=None,
                        help="Left path to compare")
    parser.add_argument("-r", "--right-path", dest="right_path",
                        default=None,
                        help="Right path to compare")

    parser.add_argument("-v", "--verbose", dest="verbose",
                        action="store_true", default=False,
                        help="Print details")
    parser.add_argument("-x", "--debug", dest="debug",
                        action="store_true", default=False,
                        help="Print debugging messages")
    parser.add_argument("-X", "--extra-verbose", dest="command_verbose",
                        action="store_true", default=False,
                        help="Print command output")


def main():
    parser = argparse.ArgumentParser()
    add_arguments(parser)
    args = parser.parse_args()

    if args.left_path is None:
        left_path = "/home/dglo/prj/pdaq-git/svn_tools/xxx/pdaq"
    else:
        left_path = args.left_path[:-1] \
          if args.left_path.endswith(os.path.sep) \
          else args.left_path

    if args.right_path is None:
        right_path = "/home/dglo/prj/pDAQ_Urban_Harvest9"
    else:
        right_path = args.right_path[:-1] \
          if args.right_path.endswith(os.path.sep) \
          else args.right_path

    treecmp = CompareTrees(left_path, right_path)
    treecmp.compare(debug=args.debug)
    if treecmp.is_modified:
        print("Found differences between \"%s\" and \"%s\"" %
              (left_path, right_path))

        for name, count in sorted(treecmp.modified_trees.items(),
                                  key=lambda x: x[1]):
            print("%s*%d" % (name, count))


if __name__ == "__main__":
    main()
