#!/usr/bin/env python

import select
import subprocess
import sys


class CommandException(Exception):
    "General exception for CommandRunner"


def __finish_proc(proc, cmdname, saved_output, returncode_handler,
                  verbose=False):
    # wait for subprocess to finish
    proc.wait()

    if proc.returncode != 0 and returncode_handler is not None:
        returncode_handler(cmdname, proc.returncode, saved_output,
                           verbose=verbose)


def __init_and_run(cmd_args, working_directory=None, debug=False,
                   dry_run=False):
    if dry_run:
        print("%s" % " ".join(cmd_args))
        return None

    if debug:
        print("CMD: %s" % " ".join(cmd_args))
    proc = subprocess.Popen(cmd_args, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, close_fds=True,
                            cwd=working_directory)

    return proc


def __stderr_handler(cmdname, line, verbose=False):
    if verbose:
        print("%s!! %s" % (cmdname, line))

    raise CommandException("%s failed: %s" % (cmdname, line))


def __stdout_handler(cmdname, line, saved_output, verbose):
    line = line.rstrip().decode("utf-8")

    if verbose:
        print("%s>> %s" % (cmdname, line, ))
    else:
        saved_output.append(line)

    return line


def default_returncode_handler(cmdname, returncode, saved_output,
                               verbose=False):
    if not verbose:
        print("Output from '%s'" % cmdname, file=sys.stderr)
        for line in saved_output:
            print(">> %s" % line, file=sys.stderr)
        raise CommandException("%s failed with returncode %d" %
                               (cmdname, returncode))


def __process_output(cmdname, proc, stderr_handler, returncode_handler,
                     verbose):
    proc_out = proc.stdout.fileno()
    proc_err = proc.stderr.fileno()

    saved_output = []
    saw_error = False
    while proc_out is not None or proc_err is not None:
        reads = []
        if proc_out is not None:
            reads.append(proc_out)
        if proc_err is not None:
            reads.append(proc_err)

        try:
            ret = select.select(reads, [], [])
        except select.error:
            # ignore a single interrupt
            if saw_error:
                break
            saw_error = True
            continue

        # check all file handles with new data
        for fno in ret[0]:
            # deal with stderr
            if proc_err is not None and fno == proc_err:
                line = proc.stderr.readline()
                if len(line) == 0:
                    proc_err = None
                else:
                    stderr_handler(cmdname, line.strip().decode("utf-8"),
                                   verbose=verbose)
                continue

            if proc_out is not None and fno == proc_out:
                line = proc.stdout.readline()
                if len(line) == 0:
                    proc_out = None
                else:
                    yield __stdout_handler(cmdname, line, saved_output,
                                           verbose)
                continue

            raise CommandException("Unknown %s file handle #%s" %
                                   (cmdname, fno))

    __finish_proc(proc, cmdname, saved_output, returncode_handler, verbose)


def run_command(cmd_args, cmdname=None, working_directory=None,
                stderr_handler=__stderr_handler,
                returncode_handler=default_returncode_handler, debug=False,
                dry_run=False, verbose=False):
    if cmdname is None:
        cmdname = cmd_args[1].upper()

    proc = __init_and_run(cmd_args, working_directory=working_directory,
                          debug=debug, dry_run=dry_run)
    if dry_run:
        return

    for _ in __process_output(cmdname, proc, stderr_handler,
                              returncode_handler, verbose):
        pass


def run_generator(cmd_args, cmdname=None, working_directory=None,
                  stderr_handler=__stderr_handler,
                  returncode_handler=default_returncode_handler, debug=False,
                  dry_run=False, verbose=False):
    if cmdname is None:
        cmdname = cmd_args[1].upper()

    proc = __init_and_run(cmd_args, working_directory=working_directory,
                          debug=debug, dry_run=dry_run)
    if dry_run:
        return

    for line in __process_output(cmdname, proc, stderr_handler,
                                 returncode_handler, verbose):
        yield line
