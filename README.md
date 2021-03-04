# svn2git_tools
Scripts and supporting code for moving the IceCube Subversion repositories
to GitHub

Main scripts:
`convert_svn_to_git.py`
: Upload Subversion repo (and, optionally, Mantis issues) to GitHub.
: This script can also be used to convert the SVN repo to a local Git repo,
: ignoring the Mantis data

`move_issues.py`
: Upload Mantis issues to GitHub

Useful bits for non-Cubers:
`decorators.py`
: This currently contains a `@classproperty` decorator which converts a class
: method into a class property, so `val = class.some_value()` can be written
: as `val = class.property`

`dictobject.py`
: A dictionary whose elements can be addressed as either `dict["key"]` or
: `dict.key` (and `dict[key] = value` or `dict.key = value`)

`git.py`
: Python wrapper around some common `git` commands

`github_util.py`
: Python implementations of some common GitHub operations
: (e.g. create/destroy GitHub repository, create issues with built-in waits
: to avoid GitHub temporary bans)

`i3helper.py`
: A few helper functions and classes.  The most intersting are probably:
:
: `Comparable`, a mix-in class which implements the standard Python comparison
: functions (`__lt__`, `__eq__`, `__gt__`, etc.) based on the value returned
: by the extending class's `compare_key()` function.
:
: `TemporaryDirectory`, a context manager which creates a temporary directory
: on entry, and destroys the temporary directory on exit.  This is useful in
: unit tests or as a home for scratch files which should be removed when the
: program ends.  Example: ```
:     with TemporaryDirectory() as tmpdir:
:         ...do stuff inside "tmpdir"..
:     ...tmpdir no longer exists...

`mysqldump.py`
: Read in a MySQL dump file and return a dictionary of table names mapped
: to table objects.  The table objects contain a table's name and column names,
: along with a list of DataRows, which are essentially dictionaries mapping
: column names to column values.

`profile_code.py`
: A decorator which profiles the code it's decorating.
: ```
: from profile_code import profile
: ...
: @profile(output_file="/tmp/profile.out", strip_dirs=True, save_stats=True)
: def method_to_profile():
:    ...
: ```

`svn.py`
: Python wrapper around some common `svn` commands
