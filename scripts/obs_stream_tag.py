#!/usr/bin/env python3
"""Classify + ARRIVAL-TIMESTAMP each line of the observer stream.

Replaces obs_stream_tag.awk, which could not stamp anything: macOS ships BWK awk
(version 20200816) with NO strftime and NO systime, and there is no gawk on this
host -- measured, not assumed. The arranger asked for the WALL-CLOCK SECOND of
the next foreign launcher line, and the foreign lines carry no timestamp of their
own, so without stamping arrival here the best I could give was a bracket off my
adjacent line. A duty the instrument cannot observe is not a duty, it is a
decoration -- so the instrument gets extended first.

Attribution is DERIVED from each line's own text, never asserted by position. An
earlier version blanket-tagged every line with this observer's id, which
LAUNDERED a sibling writer's output into apparently-attributable instrument
output. Three verdicts:
  obs-v11              - matches a prefix this observer actually emits
  FOREIGN-IN-OBS-STREAM - it does not; another writer shares this fd
  SPLICED-MID-LINE     - begins as ours and does NOT end as ours, so a mid-line
                         interleave corrupted it. A page can be split that way,
                         so it must be loud rather than silently wrong.

Units are printed explicitly in BOTH zones. A bare stamp invites the
foreign-timezone error that has already cost this lane a four-hour boundary slip
and a mislabelled birth-vs-mtime pass.
"""
import sys, re, datetime

OURS = re.compile(
    r'^(WATCH-v11|HOURLY |NEW STASH |\*\*\* |SUBSCRIBER |COND 4 UNDETERMINED'
    r'|STASH RELOCATED|FAILURE BURST |DAY BOUNDARY|CURSOR REGRESSED|CONFLICT DIVERGENCE)'
)

def main():
    for raw in sys.stdin:
        line = raw.rstrip('\n')
        u = datetime.datetime.now(datetime.timezone.utc)
        l = datetime.datetime.now().astimezone()
        stamp = 'arrived=%sZ/%s' % (u.strftime('%H:%M:%S'), l.strftime('%H:%M:%S %Z'))
        if OURS.match(line):
            if line.startswith('WATCH-v11') and not line.endswith('from this lane'):
                tag = '[SPLICED-MID-LINE obs-v11 %s]' % stamp
            else:
                tag = '[obs-v11 %s]' % stamp
        else:
            tag = '[FOREIGN-IN-OBS-STREAM not-mine %s]' % stamp
        print('%s %s' % (tag, line), flush=True)

if __name__ == '__main__':
    main()
