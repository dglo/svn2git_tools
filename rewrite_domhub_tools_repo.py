#!/usr/bin/env python3
"""
Copy the domapp-tools SVN repository to a new SVN repository which
organizes the releases in an understandable manner
"""

import argparse
import os
import shutil
import sys

from cmdrunner import run_generator
from i3helper import read_input
from svn import svn_add, svn_checkout, svn_commit, svn_copy, \
     svn_get_properties, svn_info, svn_log, svn_mkdir, svn_propset, \
     svn_remove, svn_status, svn_update, svnadmin_create


def add_arguments(parser):
    "Add command-line arguments"

    parser.add_argument("-N", "--nonstop", dest="nonstop",
                        action="store_true", default=False,
                        help="Do not pause for releases")
    parser.add_argument("-v", "--verbose", dest="verbose",
                        action="store_true", default=False,
                        help="Print details")
    parser.add_argument("-x", "--debug", dest="debug",
                        action="store_true", default=False,
                        help="Print debugging messages")


def __add_revisions(proj_url, orig_wrkspc, new_trunk, logfile_prefix,
                    startrev, endrev, orig_subrel=None, new_reldir=None,
                    pause_for_release=False, debug=False, verbose=False):
    subdirs = {}
    for entry in svn_log(proj_url, revision=startrev, end_revision=endrev):
        filedata = []
        for modtype, filename in entry.filedata:
            # ignore files outside the domhub-tools project
            if not filename.startswith(logfile_prefix):
                continue

            # trim initial path from filename
            filename = filename[len(logfile_prefix):]

            # ignore some paths
            if filename.find("Attic") >= 0 or \
              filename.startswith("trunk/moat/") or \
              filename.startswith("trunk/domapp/"):
                continue

            # split filename into individual pieces
            pathparts = filename.split(os.sep)

            # ignore stuff outside 'trunk'
            if pathparts[0] != "trunk":
                continue

            # only care about stuff in development directories
            if len(pathparts) >= 2 and \
              pathparts[1] not in ("trunk", "rel-100", "rel-200"):
                if not filename.startswith("tags"):
                    print("\t\t%s" % os.sep.join(pathparts[:2]))
                continue

            if len(pathparts) == 2:
                filedata.append((modtype, pathparts[0], pathparts[1]))
            else:
                subdir = os.sep.join(pathparts[:2])
                subdirs[subdir] = 1

                filedata.append((modtype, subdir, filename[len(subdir):]))

        removed = __remove_empty_moat_domapp(new_trunk, remove_svn=True,
                                             debug=debug, verbose=verbose)
        if removed:
            orig_trunk = os.path.join(orig_wrkspc, "trunk")
            __remove_empty_moat_domapp(orig_trunk, remove_svn=False,
                                       debug=debug, verbose=verbose)

        if not removed and len(filedata) == 0:
            print("Ignore r%d" % entry.revision)
            continue

        __update_for_revision(orig_wrkspc, list(subdirs.keys()), new_trunk,
                              entry.revision, "\n".join(entry.loglines),
                              pause_for_release=pause_for_release, debug=debug,
                              verbose=verbose)

        if orig_subrel is not None and new_reldir is not None:
            __update_for_revision(orig_wrkspc, (orig_subrel, ), new_reldir,
                                  entry.revision, "\n".join(entry.loglines),
                                  pause_for_release=pause_for_release,
                                  debug=debug, verbose=verbose)


def __add_revprop_hook(svn_repo):
    "Create a script which allows properties to be set"

    path = os.path.join(svn_repo, "hooks", "pre-revprop-change")
    with open(path, "w") as fout:
        print("#!/bin/sh", file=fout)
        print("", file=fout)
        print("echo REPOS $1 REV $2 USER $3 PROPNAME $4 ACTION $5", file=fout)
        print("exit 0", file=fout)

    # make the new script executable
    os.chmod(path, 0o775)


def __check_out_original_project(proj_url, orig_wrkspc, debug=False,
                                 verbose=False):
    "Check out original project into workspace named by 'orig_wrkspc'"

    if not os.path.exists(orig_wrkspc):
        svn_checkout(proj_url, target_dir=orig_wrkspc, debug=debug,
                     verbose=verbose)
    else:
        if not os.path.exists(os.path.join(orig_wrkspc, ".svn")):
            raise Exception("ERROR: Directory \"%s\" is not an SVN workspace" %
                            (orig_wrkspc, ))

        # make sure original repo is at HEAD
        for line in svn_update(orig_wrkspc, debug=debug, verbose=verbose):
            pass


def __create_project_repo(project, new_wrkspc, debug=False, verbose=False):
    "Create a new SVN repository for the project"

    curdir = os.getcwd()

    # build path for new repository and, if found, remove old version
    repo_dir = os.path.join(curdir, "rewrite_repo")
    if os.path.exists(repo_dir):
        shutil.rmtree(repo_dir)

    # create the new SVN repository and allow 'svn propset' operations
    svnadmin_create(repo_dir, debug=debug, verbose=verbose)
    __add_revprop_hook(repo_dir)

    # check out entire repository
    scratch_dir = os.path.join(curdir, "scratch")
    if os.path.exists(scratch_dir):
        raise SystemExit("Cannot create repository, %s exists" % scratch_dir)

    # check out entire repository
    try:
        svn_checkout("file:///" + repo_dir, target_dir=scratch_dir,
                     debug=debug, verbose=verbose)

        # create project in new repository
        proj_dir = os.path.join(scratch_dir, "projects", project)
        svn_mkdir(proj_dir, create_parents=True, debug=debug, verbose=verbose)
        for subdir in ("trunk", "branches", "tags"):
            svn_mkdir(os.path.join(proj_dir, subdir), debug=debug,
                      verbose=verbose)
        svn_commit(scratch_dir, "Initialize %s project" % project, debug=debug,
                   verbose=verbose)
    finally:
        if os.path.exists(scratch_dir):
            shutil.rmtree(scratch_dir)

    if os.path.exists(project):
        shutil.rmtree(project)

    proj_repo = "file:///" + proj_dir

    # check out new SVN project
    if os.path.exists(new_wrkspc):
        shutil.rmtree(new_wrkspc)
    svn_checkout("file:///" + os.path.join(repo_dir, "projects", project),
                 target_dir=new_wrkspc, debug=debug, verbose=verbose)


def __create_release(orig_wrkspc, new_wrkspc, rel_num, revision, debug=False,
                     verbose=False):

    # update original workspace to expected revision
    for _ in svn_update(orig_wrkspc, revision=revision, debug=debug,
                        verbose=verbose):
        pass

    # update new workspace to latest revision
    for _ in svn_update(new_wrkspc, debug=debug, verbose=verbose):
        pass

    # if no information was found, give up
    infodict = svn_info(new_wrkspc, debug=debug, verbose=verbose)
    if infodict is None:
        raise SystemExit("Giving up, cannot get info from new workspace")

    # cache current revision and relative URL
    release_rev = int(infodict.revision)
    rel_url = infodict.relative_url

    # create release
    logmsg = "domhub-tools release %d" % rel_num
    svn_copy(os.path.join(rel_url, "trunk"),
             os.path.join(rel_url, "tags", "rel-%d" % rel_num), logmsg,
             revision=release_rev, pin_externals=True, sandbox_dir=new_wrkspc,
             debug=debug, verbose=verbose)
    __fix_revision(orig_wrkspc, new_wrkspc, debug=debug, verbose=verbose)

    # update new workspace with new release info
    for _ in svn_update(new_wrkspc, debug=debug, verbose=verbose):
        pass


def __diff_dirs(orig_dir, new_dir, debug=False, dry_run=False, verbose=False):
    "Print the differences between two directories"
    cmd_args = ["diff", "-ru", orig_dir, new_dir]

    found_diffs = False
    for line in run_generator(cmd_args, cmdname=" ".join(cmd_args[:2]).upper(),
                              debug=debug, dry_run=dry_run, verbose=verbose):
        print("%s" % line)
        found_diffs = True

    if found_diffs:
        read_input("%%%%%% Found diffs for %s: " % new_dir)  # XXX


def __duplicate_workspace(orig_topdir, new_topdir):
    for entry in os.listdir(orig_topdir):
        if entry in ("domapp", "moat"):
            continue

        orig_path = os.path.join(orig_topdir, entry)
        new_path = os.path.join(new_topdir, entry)

        if os.path.isdir(orig_path):
            shutil.copytree(orig_path, new_path,
                            ignore=shutil.ignore_patterns('.svn'),
                            dirs_exist_ok=True)
        else:
            shutil.copy2(orig_path, new_path)


def __fix_makefile(rel_dir):
    mk_path = os.path.join(rel_dir, "Makefile")
    if not os.path.exists(mk_path):
        raise SystemExit("%s does not exist" % mk_path)

    fixed = False
    lines = []
    with open(mk_path, "r") as mkin:
        for line in mkin:
            line = line.rstrip()
            if line.startswith("SUBDIRS"):
                if line.find("moat") > 0 or line.find("domapp") > 0:
                    fixed = True
                    line = "SUBDIRS=\"domhub-tools domhub-testing\""
            lines.append(line)

    if fixed:
        with open(mk_path, "w") as mkout:
            for line in lines:
                print("%s" % line, file=mkout)

    return fixed


def __fix_revision(orig_wrkspc, new_wrkspc, debug=False, verbose=False):
    "Copy commit author and date from original revision to new revision"
    cur_rev = __get_workspace_revision(orig_wrkspc, debug=debug,
                                       verbose=verbose)
    (author, date, _) = svn_get_properties(orig_wrkspc, cur_rev, debug=debug,
                                           verbose=verbose)

    # commit initial set of files/directories
    commit_rev = __get_workspace_revision(new_wrkspc, update_to_latest=True,
                                          debug=debug, verbose=verbose)
    svn_propset(new_wrkspc, "svn:author", author, revision=commit_rev,
                debug=debug, verbose=verbose)
    svn_propset(new_wrkspc, "svn:date", date, revision=commit_rev, debug=debug,
                verbose=verbose)


def __get_workspace_revision(wrkspc, update_to_latest=False, debug=False,
                             verbose=False):
    # update workspace to latest revision
    if update_to_latest:
        for _ in svn_update(wrkspc, debug=debug, verbose=verbose):
            pass

    # if no information was found, return None
    infodict = svn_info(wrkspc, debug=debug, verbose=verbose)
    if infodict is None:
        return None

    # return latest revision
    return int(infodict.revision)


def __initial_commit(orig_wrkspc, new_wrkspc, initial_revision,
                     pause_for_release=False, debug=False, verbose=False):
    #read_input("%%%%%% Before new repo init: ")  # XXX

    # update the original repo to this revision
    for line in svn_update(orig_wrkspc, revision=initial_revision, debug=debug,
                           verbose=verbose):
        pass

    # build target path for initial files/directories
    new_trunk = os.path.join(new_wrkspc, "trunk")

    # copy initial release to new trunk
    rel100 = os.path.join(orig_wrkspc, "releases", "rel-100", "rel-100")
    __duplicate_workspace(rel100, new_trunk)

    #read_input("%%%%%% Before initial release: ")  # XXX

    svn_commit(new_wrkspc, "Initial commit for rel-100", debug=debug,
               verbose=verbose)
    __fix_revision(rel100, new_wrkspc, debug=debug, verbose=verbose)
    #read_input("%%%%%% Set properties: ")  # XXX

    # update new workspace with first commit info
    for line in svn_update(new_wrkspc, debug=debug, verbose=verbose):
        pass

    #read_input("%%%%%% Ready for initial release: ")  # XXX

    rel_num = 100

    # create rel-100 from devel code
    __create_release(orig_wrkspc, new_wrkspc, rel_num, 9488, debug=debug,
                     verbose=verbose)

    if pause_for_release:
        read_input("%%%%%% Created initial release: ")  # XXX

    orig_rel = os.path.join(orig_wrkspc, "releases", "rel-100", "rel-100")
    new_rel = os.path.join(new_wrkspc, "tags", "rel-%s" % rel_num)
    __duplicate_workspace(orig_rel, new_rel)
    __remove_extra(orig_rel, new_rel, debug=debug, verbose=debug)
    modified = __update_workspace_from_status(new_wrkspc, debug=debug,
                                              verbose=verbose)
    modified |= __fix_makefile(new_rel)
    if modified:
        if pause_for_release:
            read_input("%%%%%% Commit modified rel-%d: " % (rel_num, ))

        svn_commit(new_wrkspc, "Update for rel-%d" % rel_num, debug=debug,
                   verbose=verbose)
        __fix_revision(rel100, new_wrkspc, debug=debug, verbose=verbose)

    if pause_for_release:
        read_input("%%%%%% Done with rel-%d: " % (rel_num, ))

    __purge_nonsvn_files(new_wrkspc)

    # copy devel files to new trunk
    orig_devel = os.path.join(orig_wrkspc, "trunk", "devel")
    shutil.copytree(orig_devel, os.path.join(new_wrkspc, "trunk"),
                    ignore=shutil.ignore_patterns('.svn'), dirs_exist_ok=True)

    modified = __update_workspace_from_status(new_wrkspc, debug=debug,
                                              verbose=verbose)
    if not modified:
        raise SystemExit("Initial repo was not updated!!!")

    #read_input("%%%%%% Initial devel code: ")  # XXX

    # commit initial set of files/directories
    svn_commit(new_wrkspc, "Initial devel code", debug=debug, verbose=verbose)
    __fix_revision(orig_devel, new_wrkspc, debug=debug, verbose=verbose)


def __purge_nonsvn_files(topdir):
    subdirs = []
    for entry in os.listdir(topdir):
        # ignore SVN metadata
        if entry == ".svn":
            continue

        path = os.path.join(topdir, entry)

        # cache subdirectories for later examination
        if os.path.isdir(path):
            subdirs.append(path)
            continue

        # remove all other files
        os.remove(path)

    for subdir in subdirs:
        __purge_nonsvn_files(subdir)


def __remove_empty_moat_domapp(trunk, remove_svn=False, pause_for_purge=False,
                               debug=False, verbose=False):
    "If the 'domapp' and/or 'moat' subdirectory is empty, remove it"

    removed = False
    for entry in ("domapp", "moat"):
        path = os.path.join(trunk, entry)
        if not os.path.isdir(path) or len(os.listdir(path)) > 0:
            continue

        if pause_for_purge:
            read_input("%%%%%% Purging %s: " % path)  # XXX
        if remove_svn:
            svn_remove(path, debug=debug, verbose=verbose)
        else:
            os.rmdir(path)
        if pause_for_purge:
            read_input("%%%%%% Done purging %s: " % path)  # XXX
        removed = True

    return removed


def __remove_extra(orig_topdir, new_topdir, debug=False, verbose=False):
    "Remove extra files/directories from the new workspace"

    modified = False
    subdirs = []
    for entry in os.listdir(new_topdir):
        orig_path = os.path.join(orig_topdir, entry)
        new_path = os.path.join(new_topdir, entry)

        if not os.path.exists(orig_path):
            if os.path.isdir(new_path):
                shutil.rmtree(new_path)
            else:
                os.remove(new_path)
            svn_remove(new_path, debug=debug, verbose=verbose)
            modified = True
        elif os.path.isdir(orig_path):
            subdirs.append((orig_path, new_path))

    if len(subdirs) > 0:
        for orig_sub, new_sub in subdirs:
            modified |= __remove_extra(orig_sub, new_sub, debug=debug,
                                       verbose=verbose)

    return modified


def __update_for_revision(orig_wrkspc, subdir_list, new_wrkspc, revision,
                          logmsg, pause_for_release=False, debug=False,
                          verbose=False):
    # update the original repo to this revision
    for line in svn_update(orig_wrkspc, revision=revision, debug=debug,
                           verbose=verbose):
        pass

    if verbose:
        print("=== rev %d: %s" % (revision, logmsg))

    __purge_nonsvn_files(new_wrkspc)

    modified = False
    for subdir in subdir_list:
        orig_path = os.path.join(orig_wrkspc, subdir)

        shutil.copytree(orig_path, new_wrkspc,
                        ignore=shutil.ignore_patterns('.svn'),
                        dirs_exist_ok=True)

        __remove_empty_moat_domapp(new_wrkspc, remove_svn=False, debug=debug,
                                   verbose=verbose)
        modified |= __update_workspace_from_status(new_wrkspc, debug=debug,
                                                   verbose=verbose)
    if not modified:
        print("WARNING: No modifications found in %s for rev %d: %s" %
              (subdir_list, revision, logmsg), file=sys.stderr)
        return

    if verbose:
        print("--- SVN status")
        for line in svn_status(sandbox_dir=new_wrkspc):
            print("%s" % line)

        for subdir in subdir_list:
            orig_path = os.path.join(orig_wrkspc, subdir)
            print("=== OLD: %s\n~~~ NEW: %s" % (orig_path, new_wrkspc))
            print("--- Diff")
            __diff_dirs(orig_path, new_wrkspc, debug=debug, verbose=verbose)

    if pause_for_release:
        read_input("%%%%%% r%d: %s: " % (revision, logmsg))

    # commit this revision
    svn_commit(new_wrkspc, logmsg, debug=debug, verbose=verbose)

    # update commit with original author and date
    orig_path = os.path.join(orig_wrkspc, subdir_list[0])
    __fix_revision(orig_path, new_wrkspc, debug=debug, verbose=verbose)

    # update workspace with new release info
    for line in svn_update(new_wrkspc, debug=debug, verbose=verbose):
        pass


def __update_workspace_from_status(wrkspc, debug=False, verbose=False):
    """
    Use 'svn status' to determine which files need to be updated in
    the new SVN workspace
    """
    modified = False
    removed = []
    added = []
    for line in svn_status(sandbox_dir=wrkspc):
        code = line[0]
        white = line[1:7]
        filename = line[8:]

        if white.strip() != "":
            raise SystemExit("Found extra cruft in SVN STATUS line: %s" %
                             (line, ))

        if code == "?":
            added.append(filename)
        elif code in ("!", "D"):
            removed.append(filename)
        elif code != "M":
            print("SVN STATUS: Not handling \"%s\" for %s" % (code, filename))

        modified = True

    if len(added) > 0:
        svn_add(added, sandbox_dir=wrkspc, debug=debug, verbose=verbose)
    if len(removed) > 0:
        svn_remove(removed, sandbox_dir=wrkspc, debug=debug, verbose=verbose)

    return modified


def rewrite_project_repo(project, proj_url, orig_wrkspc, new_wrkspc,
                         pause_for_release=False, debug=False, verbose=False):
    """
    Copy the domapp-tools SVN repository to a new SVN repository which
    organizes the releases in an understandable manner
    """

    __create_project_repo(project, new_wrkspc, debug=debug, verbose=verbose)

    startrev = 9489
    endrev = 10633

    __initial_commit(orig_wrkspc, new_wrkspc, startrev,
                     pause_for_release=pause_for_release, debug=debug,
                     verbose=verbose)

    logfile_prefix = "/projects/domhub-tools/"

    # paths for original and new workspace trunk directories
    new_trunk = os.path.join(new_wrkspc, "trunk")

    # paths for original and new workspace rel-100 directories
    orig_rel100 = "trunk/rel-100"
    new_rel100 = os.path.join(new_wrkspc, "tags", "rel-100")

    __add_revisions(proj_url, orig_wrkspc, new_trunk, logfile_prefix,
                    startrev, endrev, orig_subrel=orig_rel100,
                    new_reldir=new_rel100, pause_for_release=pause_for_release,
                    debug=debug, verbose=verbose)

    rel_num = 101
    rel_rev = 10634
    #read_input("%%%%%% Ready for rel-%s: " % (rel_num, ))  # XXX

    __create_release(orig_wrkspc, new_wrkspc, rel_num, rel_rev, debug=debug,
                     verbose=verbose)

    if pause_for_release:
        read_input("%%%%%% Created rel-%s from r%d: " %
                   (rel_num, rel_rev))  # XXX

    orig_rel = os.path.join(orig_wrkspc, "releases", "rel-%s" % rel_num,
                            "devel")
    new_rel = os.path.join(new_wrkspc, "tags", "rel-%s" % rel_num)
    __duplicate_workspace(orig_rel, new_rel)

    #read_input("%%%%%% Copied %s to %s: " % (orig_rel, new_rel))  # XXX

    if verbose:
        print("~~~~~~ Status")
        for line in svn_status(sandbox_dir=new_wrkspc):
            print(">> %s" % str(line))

    modified = __update_workspace_from_status(new_wrkspc, debug=debug,
                                              verbose=verbose)
    modified |= __fix_makefile(new_rel)
    if modified:
        if pause_for_release:
            read_input("%%%%%% Commit modified rel-%d: " % (rel_num, ))  # XXX

        svn_commit(new_wrkspc, "Update for rel-%d" % rel_num, debug=debug,
                   verbose=verbose)
        __fix_revision(orig_rel, new_wrkspc, debug=debug, verbose=verbose)

    if pause_for_release:
        read_input("%%%%%% Done with rel-%d: " % (rel_num, ))  # XXX

    current_rev = rel_rev + 1

    for rel_num, rel_rev in ((200, 10799), (201, 10806), (202, 10978),
                             (203, 11122), (204, 11125), (205, 11276),
                             (206, 11289), (207, 11398), (208, 11475),
                             (209, 11566), (210, 11631), (211, 13200),
                             (212, 13891), (216, 15694)):

        # path for new workspace trunk directory
        new_trunk = os.path.join(new_wrkspc, "trunk")

        print("XXX REL-%d  revs %d-%d" % (rel_num, current_rev, rel_rev - 1))
        __add_revisions(proj_url, orig_wrkspc, new_trunk, logfile_prefix,
                        current_rev, rel_rev-1,
                        pause_for_release=pause_for_release, debug=debug,
                        verbose=verbose)

        #read_input("%%%%%% Ready for rel-%s: " % (rel_num, ))  # XXX

        __create_release(orig_wrkspc, new_wrkspc, rel_num, rel_rev,
                         debug=debug, verbose=verbose)

        if pause_for_release:
            read_input("%%%%%% Created rel-%s from r%d: " %
                       (rel_num, rel_rev))  # XXX

        if rel_num == 200:
            orig_rel = os.path.join(orig_wrkspc, "branches", "rel-2xx",
                                    "rel-%s" % rel_num)
        elif rel_num < 212:
            orig_rel = os.path.join(orig_wrkspc, "releases",
                                    "rel-%s" % rel_num, "rel-200")
        else:
            orig_rel = os.path.join(orig_wrkspc, "releases",
                                    "rel-%s" % rel_num)

        new_rel = os.path.join(new_wrkspc, "tags", "rel-%s" % rel_num)
        __duplicate_workspace(orig_rel, new_rel)
        __remove_extra(orig_rel, new_rel, debug=debug, verbose=debug)

        if pause_for_release:
            read_input("%%%%%% Copied %s to %s: " % (orig_rel, new_rel))  # XXX

        if verbose:
            print("~~~~~~ Status")
            for line in svn_status(sandbox_dir=new_wrkspc):
                print(">> %s" % str(line))

        modified = __update_workspace_from_status(new_wrkspc, debug=debug,
                                                  verbose=verbose)
        modified |= __fix_makefile(new_rel)
        if modified:
            if pause_for_release:
                read_input("%%%%%% Commit modified rel-%d: " %
                           (rel_num, ))  # XXX

            svn_commit(new_wrkspc, "Update for rel-%d" % rel_num, debug=debug,
                       verbose=verbose)
            __fix_revision(orig_rel, new_wrkspc, debug=debug, verbose=verbose)

        current_rev = rel_rev + 1

    read_input("%%%%%% Quit after rel-%d r%d: " % (rel_num, rel_rev))  # XXX


def main():
    "Main method"

    parser = argparse.ArgumentParser()
    add_arguments(parser)
    args = parser.parse_args()

    daq_projects_url = "http://code.icecube.wisc.edu/daq/projects"
    project = "domhub-tools"

    # names of original and new SVN workspaces
    orig_wrkspc = project + ".orig"
    new_wrkspc = project

    # full URL for this project
    proj_url = daq_projects_url + "/" + project

    __check_out_original_project(proj_url, orig_wrkspc, debug=args.debug,
                                 verbose=args.verbose)

    try:
        rewrite_project_repo(project, proj_url, orig_wrkspc, new_wrkspc,
                             pause_for_release=not args.nonstop,
                             debug=args.debug, verbose=args.verbose)
    finally:
        if os.path.exists(orig_wrkspc):
            shutil.rmtree(orig_wrkspc)


if __name__ == "__main__":
    main()
