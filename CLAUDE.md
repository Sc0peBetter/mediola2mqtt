# CLAUDE.md

Guidance for Claude Code when working on this repository.

## Project overview

`mediola2mqtt` is a Home Assistant add-on / standalone bridge that exposes a
Mediola AIO Gateway over MQTT. The runtime is a single Python script
(`mediola2mqtt.py`) plus the HA add-on manifest (`config.json`) and packaging
files (`PKGBUILD`, `Dockerfile`, systemd unit, etc.).

Tests live under `tests/` and run with the stdlib `unittest` runner:

```
python3 -m unittest discover -s tests
```

## Versioning task — REQUIRED on every code change

The HA add-on version lives in `config.json` (`"version": "X.Y.Z"`). The
add-on store and existing installations only pick up changes when this
number is bumped, so **whenever you modify code that ships to users you
MUST bump the version in `config.json` as part of the same commit / PR**.

Files whose changes require a version bump:

- `mediola2mqtt.py`
- `mediolamanager.py`
- `Dockerfile`
- `run.sh`
- `mediola2mqtt.service`
- `mediola2mqtt.sysusers`
- `mediola2mqtt.yaml.example` (only if the schema changes — additions of
  optional keys still warrant a bump so the example reaches users)
- `config.json` schema changes themselves (independently of the version
  string)

Files that do **not** require a version bump on their own:

- `README.md`, `CLAUDE.md`, other docs
- `tests/**`
- `.gitignore`, `.github/**`, repo metadata
- `*.ui` Qt designer files used only by the manager tool

### How to bump

Use semantic versioning against the current value in `config.json`:

- **PATCH** (`1.0.1` → `1.0.2`) — bug fixes, internal refactors, performance
  improvements, log-message changes, defensive guards.
- **MINOR** (`1.0.x` → `1.1.0`) — new optional config keys, new device-type
  support, new MQTT topics, anything user-visible that is backward compatible.
- **MAJOR** (`1.x.y` → `2.0.0`) — removal/rename of config keys or topics,
  changes to the discovery payload that break existing HA entities, anything
  that needs migration steps documented.

Reset lower components when bumping higher ones (`1.0.5` → `1.1.0`, not
`1.1.5`).

### Commit message convention

When a change requires a bump, follow the existing style:

```
Bump add-on version to <new version>
```

Either as the sole commit message of a dedicated bump commit, or — preferred
for small changes — as a trailer in the same commit that made the change:

```
Fix DNS cache TTL handling

Bump add-on version to 1.0.2
```

### Workflow checklist

Before completing any task that touches the files listed above:

1. Read the current `"version"` value in `config.json`.
2. Decide patch / minor / major based on the rules above. If you are unsure,
   choose **patch** and call out the choice in your summary so the user can
   override.
3. Edit `config.json` to the new version in the same change set.
4. Mention the bump explicitly in your final summary so the user can verify.

If the user explicitly says "do not bump the version", skip the bump but
note the skipped step in your summary.

## Other repository conventions

- Run the test suite (`python3 -m unittest discover -s tests`) before
  declaring a code change complete.
- Prefer editing existing files over adding new ones; if you must add a
  test file, place it under `tests/` and use the `tests/_helper.py`
  fixture loader so the module-level config bootstrap keeps working.
- Do **not** create pull requests unless the user explicitly asks for one.
- Development branch for the current task is set by the harness; commit
  and push there rather than to `master`.
