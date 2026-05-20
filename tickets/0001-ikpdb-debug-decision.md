# Ticket 0001 — Decide the fate of `ikpdb_debug`

**Status:** open
**Created:** 2026-05-20
**Type:** decision (evolve vs remove)
**Raised by:** IMQ SSOT generation, Discovery phase (Open Question Q1)

## Context

`ikpdb_debug` is a boolean field on `imq.message`. When a task raises an
exception, the worker checks the flag and, if set, drops into a post-mortem
`ikp3db` (Inouk Python Debugger) session:

```python
# models/worker.py:391-395
if raised and message_obj.ikpdb_debug:
    try:
        import ikp3db; ikp3db.post_mortem(exc_info[2])
    except ImportError:
        _logger.critical("ImportError: Failed to import 'ikp3db'.")
```

**Operator statement (2026-05-20):** the feature is **not used anymore**.

The legacy IMQ documentation (`InoukMessageQueue_aka_IMQ.md`, section
"Reste à faire") already flagged it as a known TODO:

> Revoir la mise en oeuvre de `ikpdb_debug` (via un attribut)
> — Un flag sur le processor qui est recopié sur le message
> — TODO: Rework API pour encoder `ikpdb_debug`

So the mechanism was always considered incomplete: today the flag only
exists on `imq.message` and there is no clean API path to set it at
enqueue time.

## Decision needed

Choose one:

### Option A — Remove
- Delete the `ikpdb_debug` field from `imq.message`.
- Delete the post-mortem code path in `models/worker.py:391-395`.
- Drop any view/field references.
- Rationale: not used, `ikp3db` is a niche dependency, dead code.

### Option B — Evolve
- Add `ikpdb_debug` on `imq.message_processor`, copied onto the message
  at enqueue time (per the legacy TODO).
- Provide a first-class API path (`@processor(..., ikpdb_debug=True)` or a
  `_imq_ikpdb_debug` enqueue kwarg) instead of relying on a raw DB field.
- Document it in the README.
- Rationale: remote post-mortem debugging of async tasks is genuinely
  useful; the current implementation is just unfinished.

## Impact on the IMQ SSOT

Pending this decision, the SSOT classifies `ikpdb_debug` as
**`Status: deprecated`** (capability "post-mortem debugging" — operator
decision at Gate 2: not used). If Option B is chosen, it should be
re-promoted to `wip` then `shipped` once delivered.

## References

- `parts/inouk_addons/inouk_message_queue/models/worker.py:391-395`
- `parts/inouk_addons/inouk_message_queue/models/message.py` (`ikpdb_debug` field)
- `InoukMessageQueue_aka_IMQ.md` § "Reste à faire"
