#!/usr/bin/env python

from __future__ import print_function

import argparse
import getpass
import os
import shutil
import sys
import traceback

from cmptree import CompareTrees
from git import GitBadPathspecException, GitException, git_checkout, \
     git_clone, git_status, git_submodule_status, git_submodule_update
from i3helper import TemporaryDirectory, read_input
from svn import SVNConnectException, SVNMetadata, svn_checkout, \
     svn_get_externals, svn_info, svn_list, svn_switch


def add_arguments(parser):
    "Add command-line arguments"

    parser.add_argument("-G", "--github", dest="use_github",
                        action="store_true", default=False,
                        help="Create the repository on GitHub")
    parser.add_argument("-O", "--organization", dest="organization",
                        default=None,
                        help="GitHub organization to use when creating the"
                        " repository")
    parser.add_argument("-S", "--snapshot", dest="snapshot",
                        action="store_true", default=False,
                        help="Save Git status snapshots")
    parser.add_argument("-X", "--extra-verbose", dest="command_verbose",
                        action="store_true", default=False,
                        help="Print command output")
    parser.add_argument("-n", "--number-to-process", dest="num_to_process",
                        type=int, default=None,
                        help="Number of releases to process")
    parser.add_argument("-v", "--verbose", dest="verbose",
                        action="store_true", default=False,
                        help="Print details")
    parser.add_argument("-x", "--debug", dest="debug",
                        action="store_true", default=False,
                        help="Print debugging messages")

    parser.add_argument("--local-repo", dest="local_repo_path",
                        default=None,
                        help="The local directory which holds the Git repos")
    parser.add_argument("--no-pause", dest="pause",
                        action="store_false", default=True,
                        help="Do not pause when differences are found")

    parser.add_argument(dest="svn_project", default="pdaq",
                        help="Subversion/Mantis project name")


def __delete_untracked(git_sandbox, debug=False, verbose=False):
    untracked = False
    for line in git_status(sandbox_dir=git_sandbox, debug=debug,
                           verbose=verbose):
        if line.startswith("#"):
            if len(line) <= 1 or not line[1].isspace():
                line = line[1:]
            else:
                line = line[2:]

        if not untracked:
            if line.startswith("Untracked files:"):
                untracked = True
            continue

        filename = line.strip()
        if filename == "" or filename.find("use \"git add") >= 0:
            continue

        if filename.endswith("/"):
            shutil.rmtree(os.path.join(git_sandbox, filename[:-1]))
        else:
            os.remove(os.path.join(git_sandbox, filename))


def __fmt_rev(revision):
    return "HEAD" if revision is None else "rev %s" % revision


def __print_revisions(project_name, sandbox_dir, debug=False, verbose=False):
    infodict = svn_info(sandbox_dir)
    top_release = __prune_url(infodict.url, project_name)
    top_revision = infodict.last_changed_rev

    revdict = {}
    for flds in svn_get_externals(sandbox_dir=sandbox_dir, debug=debug,
                                  verbose=verbose):
        # unpack the fields
        sub_rev, sub_url, sub_dir = flds

        infodict = svn_info(os.path.join(sandbox_dir, sub_dir))
        revdict[sub_dir] = (__prune_url(sub_url, sub_dir),
                            None if sub_rev is None else int(sub_rev),
                            __prune_url(infodict.url, sub_dir),
                            None if infodict.last_changed_rev is None
                            else int(infodict.last_changed_rev))

    print("== %s: %s rev %s" % (project_name, top_release, top_revision))
    for name, flds in sorted(revdict.items(), key=lambda x: x[0]):
        if flds[0] == flds[2] and (flds[1] is None or flds[1] == flds[3]):
            ostr = ""
        else:
            ostr = " (orig %s %s)" % (flds[0], __fmt_rev(flds[1]))

        print("      %s: %s %s%s" % (name, flds[2], __fmt_rev(flds[3]), ostr))


def __prune_url(url, project_name):
    idx = url.find("projects/")
    if idx < 0:
        return url

    flds = url[idx+9:].split("/", 1)
    if len(flds) == 1 and flds[0] == project_name:
        return SVNMetadata.TRUNK_NAME

    if len(flds) != 2:
        print("WARNING: Bad split of \"%s\" into %s" % (url[idx+9:], flds))
        return url

    name, branch = flds
    if name != project_name:
        print("WARNING: Expected \"%s\", not \"%s\" from %s" %
              (project_name, name, url), file=sys.stderr)
        return url

    return branch


def __status_snapshot(git_wrkspc, release_name, master_hash=None,
                      suffix="snap", debug=False, verbose=False):
    filename = "/tmp/%s.%s" % (release_name, suffix)

    num = 1
    while os.path.exists(filename):
        filename = "/tmp/%s+%s.%s" % (release_name, num, suffix)
        num += 1

    with open(filename, "w") as out:
        if master_hash is not None:
            print("Master hash: %s" % (master_hash, ), file=out)

        print(file=out)
        for line in git_status(sandbox_dir=git_wrkspc, debug=debug,
                               verbose=verbose):
            print(line, file=out)

        print(file=out)
        for name, status, sha1, branch in \
          git_submodule_status(sandbox_dir=git_wrkspc, debug=debug,
                               verbose=verbose):
            print("%s%s %s (%s)" % (status, sha1, name, branch), file=out)


def compare_all(project_name, svn_base_url, git_url, ignored=None,
                num_to_process=None, pause_on_error=False, rel_subdir="tags",
                save_snapshot=False, command_verbose=False, debug=False,
                verbose=False):

    with TemporaryDirectory() as tmpdir:
        # check out Git version in 'pdaq-git' subdirectory
        if verbose:
            print("Check out from Git")
        git_wrkspc = os.path.join(tmpdir.name, project_name + "-git")
        try:
            git_clone(git_url, recurse_submodules=True,
                      sandbox_dir=tmpdir.name, target_dir=git_wrkspc,
                      debug=debug, verbose=command_verbose)
        except GitException:
            if debug:
                traceback.print_exc()
            raise SystemExit("Failed to clone %s" % git_url)

        # check out SVN version in 'pdaq-svn' subdirectory
        if verbose:
            print("Check out from Subversion")
        svn_wrkspc = os.path.join(tmpdir.name, project_name + "-svn")
        try:
            svn_checkout(svn_url="/".join((svn_base_url, "trunk")),
                         target_dir=svn_wrkspc, debug=debug,
                         verbose=command_verbose)
        except:
            # if there's no 'trunk' try just the base URL
            svn_checkout(svn_url=svn_base_url, target_dir=svn_wrkspc,
                         debug=debug, verbose=command_verbose)

        # compare trunk releases
        compare_loudly("trunk", svn_wrkspc, git_wrkspc, ignored=ignored,
                       pause_on_error=pause_on_error, debug=debug,
                       verbose=verbose)

        try:
            svn_rel_base = os.path.join(svn_base_url, rel_subdir)
            for _, release in list_projects(svn_rel_base, debug=debug,
                                            verbose=command_verbose):
                if not switch_subproject(release, git_wrkspc, svn_wrkspc,
                                         svn_rel_base,
                                         save_snapshot=save_snapshot,
                                         command_verbose=command_verbose,
                                         debug=debug, verbose=verbose):
                    continue

                compare_loudly(release, svn_wrkspc, git_wrkspc,
                               ignored=ignored, pause_on_error=pause_on_error,
                               debug=debug, verbose=verbose)

                if num_to_process is not None:
                    num_to_process -= 1
                    if num_to_process <= 0:
                        break
        except SVNConnectException:
            pass


def compare_loudly(release, svn_wrkspc, git_wrkspc, ignored=None,
                   pause_on_error=False, debug=False, verbose=False):
    # compare Git and SVN workspaces
    proj_counts = compare_workspaces(release, svn_wrkspc, git_wrkspc,
                                     ignored=ignored, debug=debug,
                                     verbose=verbose)
    text = None
    if proj_counts is not None:
        for proj, count in sorted(proj_counts.items(), key=lambda x: x[0]):
            if proj in ignored:
                continue
            if text is None:
                text = ":"
            text += " %s*%d" % (proj, count)

    if text is None:
        print("** %s matches" % release)
    else:
        print("!! MISMATCH for %s%s" % (release, text))
        __print_revisions("pdaq", svn_wrkspc, debug=debug, verbose=verbose)
        if pause_on_error:
            print(":: repodiff %s %s" % (git_wrkspc, svn_wrkspc))
            read_input("%s %% Hit Return to continue: " % svn_wrkspc)


def compare_workspaces(release, svn_wrkspc, git_wrkspc, ignored=None,
                       debug=False, verbose=False):
    """
    If workspaces are the same, return True
    If workspaces differ, print a summary of the differences and return False
    """
    if verbose:
        print("Compare %s Git and Subversion" % (release, ))
    treecmp = CompareTrees(svn_wrkspc, git_wrkspc,
                           ignore_empty_directories=True)
    if not treecmp.is_modified:
        return None

    counts = None
    for name, count in sorted(treecmp.modified_trees.items(),
                              key=lambda x: x[1]):
        if ignored is not None and name in ignored:
            continue

        if counts is None:
            counts = {}
        counts[name] = count

    return counts

def list_directory(topdir, title=None):
    if not os.path.isdir(topdir):
        if title is None:
            tstr = ""
        else:
            tstr = " (%s)" % (title, )
        print("!!! %s does not exist%s!!!" % (topdir, tstr), file=sys.stderr)
        return

    if title is None:
        title = topdir

    print("/// %s \\\\\\" % (title, ))
    subdirs = []
    for entry in sorted(os.listdir(topdir)):
        path = os.path.join(topdir, entry)
        if os.path.isdir(path):
            subdirs.append(path)
            continue
        print("\t%s" % (path, ))

    for subdir in sorted(subdirs):
        print("\t%s/" % (subdir, ))


def list_projects(base_url, debug=False, verbose=False):
    list_gen = svn_list(base_url, list_verbose=True, debug=debug,
                        verbose=verbose)
    for _, _, svn_date, filename in \
      sorted(list_gen, key=lambda x: (x[2], x[3])):
        if not filename.endswith("/") or filename == "./":
            continue

        if filename.find("_rc") > 0 or filename.find("-RC") > 0 or \
          filename.find("_debug") > 0 or filename.endswith("dbg/"):
            continue

        yield svn_date, filename[:-1]


def switch_subproject(release, git_wrkspc, svn_wrkspc, svn_rel_base,
                      save_snapshot=False, command_verbose=False, debug=False,
                      verbose=False):
    # switch Git sandbox to next release
    if verbose:
        print("-- switch Git to %s" % (release, ))
    try:
        git_checkout(branch_name=release, sandbox_dir=git_wrkspc, debug=debug,
                     verbose=command_verbose)
        git_submodule_update(initialize=True, sandbox_dir=git_wrkspc,
                             debug=debug, verbose=command_verbose)
    except GitBadPathspecException:
        print("ERROR: No Git branch for %s" % (release, ))
        return False

    # clean Git repo after update
    __delete_untracked(git_wrkspc, debug=debug, verbose=command_verbose)

    if save_snapshot:
        __status_snapshot(git_wrkspc, release, suffix="check", debug=debug,
                          verbose=command_verbose)

    # switch SVN sandbox to next release
    if verbose:
        print("-- switch SVN to %s" % (release, ))
    rel_url = os.path.join(svn_rel_base, release)
    for _ in svn_switch(rel_url, sandbox_dir=svn_wrkspc, debug=debug,
                        verbose=command_verbose):
        pass

    return True


def main():
    parser = argparse.ArgumentParser()
    add_arguments(parser)
    args = parser.parse_args()

    # if no organization was specified, try the current username
    if args.organization is not None:
        organization = args.organization
    else:
        organization = getpass.getuser()

    # build SVN and Git URLs
    svn_base_url = "http://code.icecube.wisc.edu/daq/%s/%s/" % \
      ("meta-projects" if args.svn_project == "pdaq" else "projects",
       args.svn_project)

    if args.use_github:
        git_url = "git@github.com:%s/%s.git" % (organization, args.svn_project)
    elif args.local_repo_path is None:
        raise SystemExit("Please use \"--local-repo=<path>\" to specify"
                         " the path for the local repo")
    elif not os.path.exists(args.local_repo_path):
        raise SystemExit("Local repo \"%s\" does not exist" %
                         (args.local_repo_path, ))
    else:
        git_url = "file://%s/%s.git" % (args.local_repo_path, args.svn_project)

    rel_subdir = "releases"  # pDAQ uses 'releases' subdir instead of 'tags'

    ignored = ("config", "cluster-config", "daq-moni-tool", "fabric-common",
               "pdaq-user")

    compare_all(args.svn_project, svn_base_url, git_url, ignored=ignored,
                num_to_process=args.num_to_process, pause_on_error=args.pause,
                rel_subdir=rel_subdir, save_snapshot=args.snapshot,
                command_verbose=args.command_verbose, debug=args.debug,
                verbose=args.verbose)


if __name__ == "__main__":
    main()
