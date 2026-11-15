# Troubleshooting

Read the failing node's output first: `client.run(...)["errors"]` names the node and carries Snowflake's message.

| What you see | Cause | Fix |
|---|---|---|
| `AUTHZ_OBJECT_NOT_IN_FLOW_CONTEXT` | The object is not selected in the flow Context | Add it with `set_context` (all objects of the flow in one call: the call replaces the selection) |
| A role error, or `Role '...' was not activated` | No role selected for this flow, or the user was not granted that role | `lf.py role <flow> <ROLE>`; `lf.py context <flow>` lists the roles available |
| Everything worked yesterday, role errors today | The service restarted: role selections are kept in memory | Select the role again |
| `Model '...' is not served by the LLM proxy` or `Language Model` / `Agent` fail | No LLM proxy, or the model is not in its list | `lf.py models`; pick a listed model, or replace the node with `AI Complete` or a `Cortex Agent` |
| `SQL Statement failed: ...` | Snowflake refused the statement | The message is Snowflake's. Check names are fully qualified and the role's privileges |
| `Snowflake Tables failed`, `Snowflake Views failed`, `Stage Files failed` | Same, for those nodes | Same |
| `Cortex Analyst failed: Cortex Analyst refused the request ...` | Wrong semantic view name, or the role cannot use it | Check the name and the grants; the message carries the Analyst reason |
| Cortex Analyst returns `"rows": []` with `"answered_with_sql": false` | The question cannot be answered from the semantic view; `interpretation` holds what Analyst said | Rephrase the question, or handle the no-data case in the flow |
| `Postgres: running statements on Snowflake Postgres is not available` | The Postgres node has no connection to the instance | Use `SQL Statement` on Snowflake tables |
| `Knowledge ingest: no text received` | Nothing was linked into the Knowledge node | Link a text, a document or rows |
| A `CALL` receives its arguments in the wrong order | Old image: bind order was arbitrary before the 2026-11-14 fix | Update the image; values follow the order the links were created |
| An AI function or a search gets a blob of JSON with `sql` and `active_role` in it | The `message` output of `SQL Statement` was linked | Link the `rows` output instead |
| `No node type '...'` | Wrong component type | `lf.py nodes <text>` |
| `... has no field '...'` / `... has no output '...'` | Wrong field or port name | `lf.py node <Type>` |
| The canvas shows "Some connections were removed" | The flow was saved without the canvas repair | Save through `Client.save` (it applies the repair) |
| A run stops around 90 seconds with a connection error | A single long HTTP response was cut by the Snowflake ingress | Use `client.run`, which polls |
| A trigger never fires | It is `Paused`, or the flow was never run once to create the stream and task, or the trigger setup script of the deployment was not run | Set `status_mode` to `Active`, run the flow once, check `SHOW TASKS` in the schema |
| A Cortex Agent node calls an agent with a 403 | The agent is not in the Context, or the role lacks `USAGE` on it | Add it under `cortex_agents`; grant usage |
| A Cortex Agent with flow tools answers before the flow is done | Tool descriptions do not say "call again while RUNNING", or the agent's time budget is too short | See pattern 4 in `patterns.md` |

## Checks that save time

- `lf.py check` before anything else.
- After `save`, run once with a trivial input and read **every** node output, not only the last one.
- When an answer looks wrong, look at what the node received: the prompt node's `prompt` output shows the exact text.
- Model-chosen values (a severity, a classification, wording) can change between runs. Test for facts, not sentences.

## Limits worth telling the user

- Tested on Snowflake on AWS only, with synthetic data. Unofficial project, best-effort replies.
- `Language Model` and `Agent` depend on the separate LLM proxy.
- The `Postgres` node cannot run statements.
- A Cortex Agent request ends after 15 minutes unless the caller uses a background run.
- A trigger task waits for the flow to finish: runs started by the same trigger do not overlap.
