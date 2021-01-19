#!/usr/bin/env python

import argparse
import os
import sqlite3


def add_arguments(parser):
    "Add command-line arguments"

    parser.add_argument("-b", "--svn-branch",
                        dest="svn_branch", default=None,
                        help="The Subversion branch to check ")
    parser.add_argument("-D", "--date",
                        dest="find_date",
                        action="store_true", default=False,
                        help="Instead of revision, find date ")
    parser.add_argument("-H", "--git-hash",
                        dest="find_git_hash",
                        action="store_true", default=False,
                        help="Instead of revision, find Git hash ")

    parser.add_argument(dest="project", default=None,
                        help="Project name")
    parser.add_argument(dest="revision", default=None,
                        help="Subversion revision")


def find_revision(project, svn_branch, revision, find_date=False,
                  find_git_hash=False, directory="."):
    path = os.path.join(directory, "%s-svn.db" % (project, ))
    if not os.path.exists(path):
        raise Exception("Cannot find %s" % (path, ))

    conn = sqlite3.connect(path)

    conn.row_factory = sqlite3.Row

    with conn:
        cursor = conn.cursor()

        query_start = "select branch, revision, prev_revision," \
          " git_branch, git_hash, date, message" \
          " from svn_log "
        query_end = " order by revision desc limit 1"
        if find_date:
            fldname = "date"
            if svn_branch is None:
                cursor.execute(query_start + "where date<=?" + query_end,
                               (revision, ))
            else:
                cursor.execute(query_start + "where branch=? and date<=?" +
                               query_end, (svn_branch, revision))
        elif find_git_hash:
            fldname = "git_hash"
            if svn_branch is None:
                cursor.execute(query_start + "where git_hash like ?" +
                               query_end, ("%s%%" % revision, ))
            else:
                cursor.execute(query_start +
                               "where branch=? and git_hash like ?" +
                               query_end, (svn_branch, "%s%%" % revision))
        else:
            fldname = "revision"
            if svn_branch is None:
                cursor.execute(query_start + "where revision=?" + query_end,
                               (revision, ))
            else:
                cursor.execute(query_start + "where branch=? and revision=?" +
                               query_end, (svn_branch, revision, ))

        row = cursor.fetchone()
        if row is None:
            print("%s %s %s not found" % (project, fldname, revision))
        else:
            svn_branch = row[0]
            svn_revision = row[1]
            prev_revision = row[2]
            git_branch = row[3]
            git_revision = row[4]
            date = row[5]
            message = row[6]

            title = "%s %s %s" % (project, fldname, revision)
            print("%s" % title)
            print("-"*len(title))
            print("Date: %s" % (date, ))
            print("SVN: %s rev %s (prev %s)" %
                  (svn_branch, svn_revision, prev_revision))
            print("Git: %s/%s" % (git_branch, git_revision))
            print("-"*40)
            print(message)


def main():
    parser = argparse.ArgumentParser()
    add_arguments(parser)
    args = parser.parse_args()

    if args.find_date and args.find_git_hash:
        raise SystemExit("Cannot find both date (-D) and Git hash (-H)")

    find_revision(args.project, args.svn_branch, args.revision,
                  find_date=args.find_date, find_git_hash=args.find_git_hash)


if __name__ == "__main__":
    main()
