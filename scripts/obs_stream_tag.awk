# Classify every line of the observer stream by DERIVING attribution from the
# line's own text. Written 2026-09-13 after MEASURING foreign output (a
# lattice-pitboss-cc spawn) entering this observer's stdout on EVERY monitor arm.
#
# WHY NOT A BLANKET TAG: the first version stamped every line "[obs-v11]",
# which LAUNDERED foreign lines into apparently-attributable instrument output --
# a label asserting ownership it had not established. Attribution is derived
# here, never asserted, which is the same rule that governs this instrument's
# state labels.
#
# Three verdicts, and the third is the one that matters:
#   OBS       - matches a prefix this observer actually emits
#   FOREIGN   - does not; another writer shares this fd
#   SPLICED   - starts as ours but does NOT end as ours => a mid-line interleave
#               corrupted it. A page can be split this way, so it must be LOUD.
BEGIN { ours = "^(WATCH-v11|HOURLY |NEW STASH |\\*\\*\\* |SUBSCRIBER |COND 4 UNDETERMINED|STASH RELOCATED|FAILURE BURST |DAY BOUNDARY|CURSOR REGRESSED|CONFLICT DIVERGENCE)" }
{
  line = $0
  if (line ~ ours) {
    # A line of ours must end the way our emitters end: a known terminator, or
    # at minimum not with another writer's text. We can only check the banner
    # deterministically, so that is what we check.
    if (line ~ /^WATCH-v11/ && line !~ /from this lane$/)
      printf "[SPLICED-MID-LINE obs-v11] %s\n", line
    else
      printf "[obs-v11] %s\n", line
  } else {
    printf "[FOREIGN-IN-OBS-STREAM not-mine] %s\n", line
  }
  fflush()
}
