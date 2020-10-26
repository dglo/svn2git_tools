#!/usr/bin/env python

from __future__ import print_function

import os
import sqlite3
import sys

from svn import SVNDate, SVNException, svn_get_externals, svn_info, svn_status
from git import git_status, git_submodule_status


class DatabaseCollection(object):
    DATABASE_HOME = None
    CONNECTIONS = {}

    @classmethod
    def db_connection(cls, project):
        if project not in cls.CONNECTIONS:
            if cls.DATABASE_HOME is None:
                raise Exception("Database home has not been set for <%s>" %
                                (cls, ))

            dbfile = os.path.join(cls.DATABASE_HOME, "%s-svn.db" % (project, ))
            if not os.path.exists(dbfile):
                raise Exception("Cannot find \"%s\"" % (dbfile, ))

            conn = sqlite3.connect(dbfile)
            conn.row_factory = sqlite3.Row

            cls.CONNECTIONS[project] = conn

        return cls.CONNECTIONS[project]

    @classmethod
    def set_database_home(cls, database_home):
        "Set home directory for Subversion log databases"
        cls.DATABASE_HOME = database_home


class RepoStatus(DatabaseCollection):
    def __init__(self, gbranch, ghash, sbranch, srevision, gstatus=None,
                 sdate=None):
        self.git_branch = gbranch
        self.git_hash = ghash
        self.git_status = gstatus
        self.svn_branch = sbranch
        self.svn_revision = srevision
        self.svn_date = sdate

    def __str__(self):
        if self.git_branch is None:
            gbstr = ""
        else:
            gbstr = "%s/" % (self.git_branch, )
        if self.git_status is None or self.git_status == " ":
            gsstr = ""
        else:
            gsstr = "(%s)" % (self.git_status, )

        if self.svn_branch is None:
            sbstr = ""
        else:
            sbstr = "%s/" % (self.svn_branch, )
        if self.svn_date is None:
            sdstr = ""
        else:
            sdstr = "[%s]" % (self.svn_date, )

        return "git %s%s%s svn %s%s%s" % (gbstr, self.git_hash, gsstr, sbstr,
                                          self.svn_revision, sdstr)

    @classmethod
    def __extract_branch_from_rel_url(cls, name, rel_url):
        idx = rel_url.find(name)
        if idx < 0:
            raise Exception("Cannot find \"%s\" in relative URL \"%s\"" %
                            (name, rel_url))

        idx += len(name)
        if len(rel_url) == idx:
            return "trunk"

        if len(rel_url) < (idx + 2) or rel_url[idx] != "/":
            raise Exception("No branch information found for %s in"
                            " relative URL \"%s\"" % (name, rel_url))

        return rel_url[idx + 1:]

    @classmethod
    def __get_hash_from_revision(cls, project, revision):
        conn = cls.db_connection(project)

        with conn:
            cursor = conn.cursor()

            cursor.execute("select git_branch, git_hash from svn_log"
                           " where revision=?", (revision, ))
            row = cursor.fetchone()
            if row is None:
                return None, None

            return row[0], row[1]

    @classmethod
    def __get_revision_from_hash(cls, project, git_hash):
        conn = cls.db_connection(project)

        with conn:
            cursor = conn.cursor()

            cursor.execute("select branch, revision from svn_log"
                           " where git_hash like '%s%%'" % (git_hash, ))
            row = cursor.fetchone()
            if row is None:
                return None, None
            return row[0], int(row[1])

    @classmethod
    def compare(cls, metaproject, svn_repo, git_repo):
        # get list of projects which are in the SVN sandbox but not
        #  the Git sandbox
        missing = []
        for project in svn_repo:
            if project not in git_repo and project != metaproject:
                missing.append(project)

        extra = []
        for project, gstat in git_repo.items():
            # if this isn't in the SVN sandbox, it's an extra project
            if project not in svn_repo:
                if project != metaproject:
                    extra.append(project)
                continue

            sstat = svn_repo[project]
            if not sstat.git_hash.startswith(gstat.git_hash):

                print("%s:\n\t"
                      "Git %s rev %s hash is \"%s\",\n\t"
                      "SVN %s rev %s hash is \"%s\"" %
                      (project, gstat.svn_branch, gstat.svn_revision,
                       gstat.git_hash[12:], sstat.svn_branch,
                       sstat.svn_revision, sstat.git_hash[12:]))

        if len(missing) > 0:
            print("Found %d missing projects: %s" %
                  (len(missing), ", ".join(missing)))
        if len(extra) > 0:
            print("Found %d extra projects: %s" %
                  (len(extra), ", ".join(extra)))

    @classmethod
    def from_git(cls, project, sandbox_dir, gbranch=None, debug=False):
        repo_status = {}

        for line in git_status(sandbox_dir=sandbox_dir, debug=debug,
                               verbose=debug):
            detached = line.find(" detached at ")
            if detached > 0:
                dhash = line[detached + 13:]
                branch, revision = cls.__get_revision_from_hash(project, dhash)
                repo_status[project] = RepoStatus(gbranch, dhash, branch,
                                                  revision)
                continue

        for flds in git_submodule_status(sandbox_dir=sandbox_dir, debug=debug,
                                         verbose=debug):
            name, status, hashname, _ = flds
            xbranch, xrev = cls.__get_revision_from_hash(name, hashname)
            repo_status[name] = RepoStatus(None, hashname, xbranch, xrev,
                                           gstatus=status)

        return repo_status

    @classmethod
    def from_svn(cls, project, sandbox_dir, debug=False, verbose=False):
        repo_status = {}

        # build a list of external projects
        project_list = [project, ]
        for _, _, subdir in svn_get_externals(sandbox_dir, debug=debug,
                                              verbose=verbose):
            if subdir not in project_list:
                project_list.append(subdir)

        unknown = {}
        for line in svn_status(sandbox_dir=sandbox_dir, debug=debug,
                               verbose=debug):
            if line.startswith("X"):
                filename = line[1:].strip()
                if filename not in project_list:
                    print("WARNING: Found unknown external \"%s\"" %
                          (filename, ), file=sys.stderr)
                    project_list.append(filename)
                continue

            if line.startswith("?"):
                filename = line[1:].strip()
                if filename not in project_list:
                    unknown[filename] = 0
                continue

            if line.startswith("Performing status on external"):
                continue

            if line.rstrip() != "":
                print("WARNING: Unrecognized status line: %s" % (line, ))

        if len(unknown) > 0:
            print("WARNING: Found %s files unknown to SVN: %s" %
                  (len(unknown), ", ".join(unknown.keys())), file=sys.stderr)

        for extern in sorted(project_list):
            if extern == project:
                extpath = sandbox_dir
            else:
                extpath = os.path.join(sandbox_dir, extern)

            # ignore external projects which haven't been checked out
            if not os.path.exists(extpath):
                continue

            # ignore external projects which haven't been checked out
            if not os.path.exists(os.path.join(extpath, ".svn")):
                raise SVNException("Subdirectory %s is not an SVN project" %
                                   (extern, ))

            try:
                info = svn_info(extpath, debug=debug, verbose=verbose)
            except:
                import traceback; traceback.print_exc()
                from i3helper import read_input
                read_input("%s %% Failed to get info for %s: " %
                           (os.getcwd(), extpath))

            svn_branch = \
              cls.__extract_branch_from_rel_url(extern, info["relative_url"])
            svn_revision = info["revision"]
            svn_date = SVNDate(info["last_changed_date"])

            git_branch, git_hash = \
              cls.__get_hash_from_revision(extern, info["revision"])

            repo_status[extern] = RepoStatus(git_branch, git_hash, svn_branch,
                                             svn_revision, svn_date)

        return repo_status
