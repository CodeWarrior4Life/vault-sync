#!/usr/bin/env python3
"""Classify + ARRIVAL-TIMESTAMP each line of the observer stream.

Replaces obs_stream_tag.awk, which could not stamp anything: macOS ships BWK awk
(version 20200816) with NO strftime and NO systime, and there is no gawk on this
host -- measured, not assumed. A duty the instrument cannot observe is not a duty,
it is a decoration -- so the instrument gets extended first.

Attribution is DERIVED from each line's own text, never asserted by position. An
earlier version blanket-tagged every line with this observer's id, which LAUNDERED
a sibling writer's output into apparently-attributable instrument output. PREFIX
LIST MAINTENANCE IS LOAD-BEARING: emits were added all session without updating
this regex, so the observer's OWN alarm lines were tagged not-mine -- the
laundering defect inverted, and actively misleading because the arranger reads
these tags. Broadened to prefix families so a new emit cannot be disowned again.

TKT-60693542 (p2, owner nexus-obs-2, filed 2026-09-13 12:44 EDT). THE DEFECT THIS
FIXES, and it is the worst kind a classifier can have: FOR A FULL SESSION THIS
TAGGER EXPLAINED AWAY ITS OWN INSTRUMENT'S MALFUNCTION. The observer's alarm
strings contained an unescaped backtick pair around `pitboss`, so sh
command-substituted them and injected that command's output into the middle of
every alarm. This tagger labelled the injected lines FOREIGN-IN-OBS-STREAM
"not-mine" and the truncated head SPLICED-MID-LINE, which reads as another lane
sharing the fd. TWO SEATS DISMISSED IT ON THAT BASIS. A tag that explains away
your own malfunction is worse than no tag: no tag leaves a mystery, a wrong tag
closes the question.

FOUR VERDICTS NOW:
  obs-v11              - matches a prefix this observer actually emits
  OWN-EMIT-CORRUPTED   - OUR OWN emit, corrupted. Either it carries the signature
                         of injected foreign output, or it is a known-family head
                         whose terminator is missing (a cut line). NEVER "foreign".
  FOREIGN-IN-OBS-STREAM - genuinely another writer sharing this fd
  (inherited)          - a FOREIGN-shaped line arriving within INHERIT_MS of an
                         OWN-EMIT-CORRUPTED line inherits that verdict, because
                         injected output IS the corruption, not a bystander.

WHY INHERITANCE RUNS FORWARD ONLY: this is a streaming filter -- a line already
printed cannot be retagged. The corrupted head arrives BEFORE the injected body,
so the body is what we can still attribute. That is precisely the shape of the
2026-09-13 16:2x armed-line splice, which the fixture replays.

Units are printed explicitly in BOTH zones. A bare stamp invites the
foreign-timezone error that has already cost this lane a four-hour boundary slip
and a mislabelled birth-vs-mtime pass.
"""
import sys, re, datetime, time

OURS = re.compile(
    r'^(WATCH-v11|HOURLY |NEW STASH |\*\*\* |SUBSCRIBER |COND 1 |COND 4 '
    r'|STASH RELOCATED|STASHES RELOCATED|FAILURE BURST |DAY BOUNDARY|CURSOR REGRESSED'
    r'|CONFLICT DIVERGENCE|WINDOW VIEWS DIVERGE|PRE-BREACH WATCH|ARRANGER RUNG)'
)

# Signature of output injected by command substitution inside our own emit strings.
# Derived from the MEASURED 2026-09-13 corruption, not guessed: these are the lines
# `pitboss` with no args prints. Keep this list additive -- a marker that stops
# matching degrades to the inheritance path, never to "foreign".
INJECTED = re.compile(
    r'(open: project |resolve: fuzzy ->|one-flow open|session \(Req |'
    r'conductor: registration=|kitten @ --to|provenance events:|Traceback \(most recent)'
)

# Known family terminators. A head that matches the family but lacks its terminator
# was CUT. Absent an entry a family simply is not checked this way -- its splice is
# still caught, one line later, by inheritance.
TERMINATORS = {'WATCH-v11': 'from this lane'}

INHERIT_MS = int(__import__('os').environ.get('TAG_INHERIT_MS', '1500'))


def classify(line, now_ms, state):
    """Return (verdict, why). Pure, so the fixture can drive it directly."""
    if OURS.match(line):
        if INJECTED.search(line):
            state['corrupt_until'] = now_ms + INHERIT_MS
            return 'OWN-EMIT-CORRUPTED', 'our emit CARRIES injected foreign output (command substitution in the emit string)'
        for fam, term in TERMINATORS.items():
            if line.startswith(fam) and not line.endswith(term):
                state['corrupt_until'] = now_ms + INHERIT_MS
                return 'OWN-EMIT-CORRUPTED', 'our %s emit is CUT (terminator %r missing) -- a mid-line interleave, not a foreign line' % (fam, term)
        return 'obs-v11', ''
    if now_ms <= state.get('corrupt_until', 0) and INJECTED.search(line):
        return 'OWN-EMIT-CORRUPTED', 'inherited: injected output within %dms of our corrupted emit -- this IS the corruption, not a bystander' % INHERIT_MS
    return 'FOREIGN-IN-OBS-STREAM', ''


def main():
    state = {'corrupt_until': 0}
    for raw in sys.stdin:
        line = raw.rstrip('\n')
        now_ms = int(time.time() * 1000)
        u = datetime.datetime.now(datetime.timezone.utc)
        l = datetime.datetime.now().astimezone()
        stamp = 'arrived=%sZ/%s' % (u.strftime('%H:%M:%S'), l.strftime('%H:%M:%S %Z'))
        verdict, why = classify(line, now_ms, state)
        if verdict == 'FOREIGN-IN-OBS-STREAM':
            tag = '[FOREIGN-IN-OBS-STREAM not-mine %s]' % stamp
        elif verdict == 'OWN-EMIT-CORRUPTED':
            tag = '[OWN-EMIT-CORRUPTED %s] 🔴 %s |' % (stamp, why)
        else:
            tag = '[obs-v11 %s]' % stamp
        print('%s %s' % (tag, line), flush=True)


if __name__ == '__main__':
    main()
