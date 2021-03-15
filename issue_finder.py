#!/usr/bin/env python

import re


class CommitFinder(object):
    """
    Find SVN revision numbers embedded in Mantis issues
    """
    COMMIT_PAT = re.compile(r"^(?:.*)rev(?:ision)? #?(\d+)(\D.*)?\s*$")
    RNUM_PAT = re.compile(r"^r([0-9]+)\s+\|\s+\S+\s+\|\s+\d+-\d+-\d+.*$")
    NUMCOMM_PAT = re.compile(r"^(\d+) commit fixes this\s*$")
    SVNREV_PAT = re.compile(r"^(?:.*)(?:SVN revision # |New Revision: |"
                            r"Checkin #? ?|checked in .* as |commit )"
                            r"(\d+)(\D.*)?\s*$")
    FIXED_PAT = re.compile(r"^(?:.*)[Ff]ixed .*in"
                           r"(?: at| checkin| rev:| -r| under)?"
                           r" (\d+)(\D.*)?\s*$")
    CHKIN_PAT = re.compile(r"^(?:.*)check(?: |-|)in\s+#?(\d+)(\D.*)?\s*$")
    COMMIT2_PAT = re.compile(r"^(?:.*)[Cc]ommit\s+#?(\d+)(\D.*)?\s*$")

    @classmethod
    def find_all(cls, issue):
        commits = None
        for note in issue.notes:
            fixed = note.text.decode("utf-8", "ignore")
            for line in fixed.split("\n"):
                commit = cls.find_in_text(line)
                if commit is not None:
                    if commits is None:
                        commits = []
                    commits.append(commit)

        return commits


    @classmethod
    def find_in_text(cls, line):
        for pattern in (cls.COMMIT_PAT, cls.RNUM_PAT, cls.NUMCOMM_PAT,
                        cls.SVNREV_PAT, cls.FIXED_PAT, cls.CHKIN_PAT,
                        cls.COMMIT2_PAT):
            mtch = pattern.match(line)
            if mtch is not None:
                return int(mtch.group(1))
        if line.startswith("Fix in CnCServer r112"):
            return 112
        if line.startswith("Fixed as part of 3295"):
            return 3295
        return None


class IssueFinder(object):
    """
    Find Mantis issue numbers embedded in SVN commits
    """
    SIMPLE_PAT = re.compile(r"^:?(Issue|Issur|Isseu)\s*:?"
                            r"\s*#?(\d+)(?:\:\s+)?(.*)$", flags=re.IGNORECASE)
    EMBED_PAT = re.compile(r"^(|.*\s|.*(?:\())\s*(?:I?ssue|fix)\s*:?"
                           r"\s*#?:?\s*(\d+)(?:\:\s+)?(.*)$",
                           flags=re.IGNORECASE)
    NUMBER_PAT = re.compile(r"^#?(\d+)(?:\:\s+)?([^-\.\d].*)?$")

    # these patterns match a single problematic pDAQ log entry
    MANTIS_PAT = re.compile(r"^Reference\s+mantis\s+#(\d+)(.*\S)?\s*$")
    ISSUES_PAT = re.compile(r"^(.*)\s+-\s+this\s+should\s+address\s+issues"
                            r"\s+(\d+)\s+/\s+(\d+)\.\s*$")

    @classmethod
    def __add(cls, lines, idx, fixed_line, issues, issue,
              extra_issue=None, fix_lines=False):
        if fix_lines:
            lines[idx] = fixed_line

        if issue not in issues:
            issues.append(issue)
        if extra_issue is not None and extra_issue not in issues:
            issues.append(extra_issue)

    @classmethod
    def find(cls, lines, fix_lines=False):
        issues = []

        match_simple = True
        for idx, line in enumerate(lines):
            if match_simple:
                mtch = cls.SIMPLE_PAT.match(lines[0])
                if mtch is not None:
                    issue = int(mtch.group(2))
                    line = mtch.group(3).strip()

                    cls.__add(lines, idx, line, issues, issue,
                              fix_lines=fix_lines)

                    continue

            if match_simple and idx > 0:
                break
            match_simple = False

            mtch = cls.NUMBER_PAT.match(line)
            if mtch is not None:
                issue = int(mtch.group(1))
                line = mtch.group(2)

                cls.__add(lines, idx, line, issues, issue, fix_lines=fix_lines)

                continue

            mtch = cls.EMBED_PAT.match(line)
            if mtch is not None:
                issue = int(mtch.group(2))
                if mtch.group(1) is None:
                    line = mtch.group(3).strip()
                elif mtch.group(3) is None:
                    line = mtch.group(1).strip()
                else:
                    line = mtch.group(1).strip() + " " + mtch.group(3).strip()

                cls.__add(lines, idx, line, issues, issue, fix_lines=fix_lines)

                continue

            mtch = cls.MANTIS_PAT.match(line)
            if mtch is not None:
                issue = int(mtch.group(1))
                line = mtch.group(2)
                if line is not None:
                    line = line.strip()

                cls.__add(lines, idx, line, issues, issue, fix_lines=fix_lines)

                continue

            mtch = cls.ISSUES_PAT.match(line)
            if mtch is not None:
                line = mtch.group(1).strip()
                issue1 = int(mtch.group(2))
                issue2 = int(mtch.group(3))

                cls.__add(lines, idx, line, issues, issue1, extra_issue=issue2,
                          fix_lines=fix_lines)

                continue

        return issues


if __name__ == "__main__":
    pass
