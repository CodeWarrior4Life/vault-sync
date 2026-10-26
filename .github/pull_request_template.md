## Summary

## Checklist
- [ ] New dependency: extend `src-tauri/clippy.toml` `disallowed-methods` with its file-writing APIs, and add a `[reviewed_deps]` line in `src-tauri/state_io_exemptions.toml` (CS-2 gate 1 / C6).
- [ ] New `allow(clippy::disallowed_methods)` or `.display(<arg>)` site outside `state_io`: listed in `state_io_exemptions.toml` with a reason (CS-2 gate 2 / C7).
