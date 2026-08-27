# IMQ Multi-Tenancy Support

IMQ exposes a `company_id` field on the message graph (`imq.message`,
`imq.message_processing`, `imq.message_processing_log`) to let consumer
addons implement tenant-scoped access control.

**IMQ itself enforces no ACL on `company_id`.** The existing record rules
(`ir_rule__imq_message_for_user`, `ir_rule__imq_message_for_admin`) are
unchanged. Consumers that want company-scoped visibility must add their
own rules — see `muppy_manganese/security/ir_rules__imq.xml` for an example.

## Semantics

- **`imq.message.company_id`** is set at `create()` from
  `requesting_user_id.company_id`. It represents *the tenant for whom this
  work was submitted* — a snapshot frozen at submission time.
- If `requesting_user_id` is not set (system message), `company_id` is
  `NULL`. There is **no fallback** to `user_id.company_id` at `create()`.
  Rationale: a fallback could leak system messages into a tenant's view if
  the system user happens to be assigned to that tenant's company.
- **At enqueue, `_imq_requesting_user_id` carries three cases** (resolved
  in `api_pgsql.py`):
    - **an id** — that user requested the work. The escalated-path form:
      the message is filed under that user and their company.
    - **omitted** — the message is attributed to the enqueuing user
      (`user_id`). This is the default, and it is almost always right: the
      person whose action enqueued the work is the person the message is
      about.
    - **`False`, explicitly** — a **system message**: `requesting_user_id`
      stays `NULL`, so `company_id` derives to `NULL` and the message is
      invisible to every tenant-scoped rule by construction. This is the
      form for platform-internal findings a tenant must never be shown
      (first consumer: `muppy_keyring`'s vault-divergence dispatcher).
      Omitting the kwarg does **not** produce a system message — the
      omission fallback above swallows it.
- `imq.message_processing.company_id` and
  `imq.message_processing_log.company_id` are stored related fields on
  `message_id.company_id` — they always mirror their parent message's
  tenancy. Caller-supplied values in `create()` vals are ignored by
  Odoo's stored-related compute, so V9 (cross-tenant injection) is
  closed by construction at the processing/log level too.
- The field is **frozen**: `imq.message.company_id` is set once at
  create from `requesting_user_id.company_id` and never recomputed. Even
  if the user later changes company, historical messages keep their
  original tenancy attribution.

## Security: tenancy is derived, never declared

`create()` strips any caller-supplied `company_id` from `vals` (with a
warning log) and replaces it with the derived value. This prevents a
**cross-tenant injection** attack, where a user in company X could
otherwise create a message tagged for company Y by passing
`company_id=Y` to `create()`.

If your code legitimately needs to set `company_id` (rare — e.g., a
migration script), use raw SQL or `sudo()` outside the ORM `create()`
path. There is no first-class API to override the derivation.

## Example: tenant-scoped record rule for a consumer

```xml
<record id="my_tenant_admin_rule" model="ir.rule">
    <field name="name">imq.message: tenant admin sees company messages</field>
    <field name="model_id" ref="inouk_message_queue.model_imq_message"/>
    <field name="groups" eval="[(4, ref('my_module.tenant_admin_group'))]"/>
    <field name="domain_force">[('company_id', '=', company_id)]</field>
    <field name="perm_read" eval="True"/>
    <field name="perm_write" eval="True"/>
    <field name="perm_create" eval="False"/>
    <field name="perm_unlink" eval="False"/>
</record>
```

Pair this with an override of `write()` on `imq.message` if you want to
prevent your tenant admins from mutating sensitive fields (e.g.,
`company_id`, `state`) — see `muppy_manganese/models/imq_message__manganese.py`.
