---
name: native-snowflake-langflow
description: Build, run, debug and explain flows on Native Snowflake Langflow (the Langflow visual editor running inside Snowflake on Snowpark Container Services). Use when the user wants to create or change a Langflow flow that uses Snowflake nodes (Cortex Agent, Cortex Analyst, Cortex Search, AI functions, Decision, SQL Statement, triggers, memory), choose a design pattern (parallel helpers, sequential pipeline, data engineering, a Cortex Agent that hands work to a flow), set a flow's role and Context, run a flow and read node outputs, import or export flow files, or diagnose a failing node. Triggers: langflow, snowflake langflow, flow, canvas, cortex agent node, table change trigger, flow context, lf.py.
---

# Native Snowflake Langflow

Native Snowflake Langflow is Langflow 1.13 running as a service inside a Snowflake account, with extra nodes
that call Snowflake (Cortex Agents, Cortex Analyst, Cortex Search, AI functions, SQL, tables, stages, triggers).
Flows can be drawn in the browser or built as code with the client in `scripts/lf.py`. This skill covers the
second way, and what you need to know to advise on the first.

It is an unofficial personal project, not a Snowflake product. Say so if the user asks about support.

## Three rules that explain most failures

1. **Role.** Every Snowflake call runs under the role selected for the flow. The selection lives in the
   server memory: after a service restart, select it again (`lf.py role <flow> <ROLE>`).
2. **Context.** Each flow has its own list of databases, schemas and objects it may use. A node can touch an
   object only if the role allows it **and** it is in the flow Context. Anything else is refused with
   `AUTHZ_OBJECT_NOT_IN_FLOW_CONTEXT`. Set the Context right after creating a flow.
3. **LLM proxy.** The standard Langflow nodes `Language Model` and `Agent` need a separate LLM proxy service.
   `lf.py check` lists the models it serves; an empty list means those two nodes will not work. Nodes that
   call Snowflake directly (`Cortex Agent`, `AI Complete`, `AI Function`, `AI Redact`, `Decision`, `Cortex
   Analyst`, `Cortex Search`, `SQL Statement`) work without it. Prefer them when the user has no proxy.

## Connect

The client reads three environment variables. Ask the user to set them in their shell; never ask them to paste
the values into the conversation and never write them to a file.

| Variable | What it is |
|---|---|
| `LF_URL` | Base URL of the service (the public endpoint of the SPCS service) |
| `LF_API_KEY` | Langflow API key, created in the Langflow UI under Settings |
| `LF_SPCS_TOKEN` | Snowflake programmatic access token, needed to pass the Snowflake ingress |

```bash
python3 scripts/lf.py check          # reachable? which projects? which proxy models?
```

## Work in this order

1. **Discover, do not guess.** Node type names, field names and output ports must be exact.
   `lf.py nodes snowflake` lists the Snowflake nodes; `lf.py node <Type>` prints fields, what each field
   accepts and the output ports. `references/nodes.md` has the same information for the common nodes.
2. **Check the Snowflake objects exist** (tables, semantic views, search services, agents, procedures) with
   the user's SQL tool before wiring them. The client does not create Snowflake objects.
3. **Pick a pattern** from `references/patterns.md` and adapt it. Keep flows small: one job per node.
4. **Build and save** with `Graph` and `Client.save(...)`, passing the Context in the same call.
5. **Select the role**, **run**, and read every node's output: `client.run(flow)` returns them by node name.
6. **Fix and rerun.** `references/troubleshooting.md` maps error texts to causes.
7. **Hand over.** Tell the user the flow name so they can open it in the canvas, or `lf.py export` it.

```python
import sys; sys.path.insert(0, "scripts")
from lf import Client, Graph, context_body

c = Client()
g = Graph(c, tag="DEMO")
ask = g.add("ChatInput", {"input_value": "Chiller C7 is leaking coolant. What do we do?"})
redact = g.add("SnowflakeAIRedact", {"redact_mode": "redact", "source_mode": "Text Input"}, name="Remove personal data", x=450)
g.link(ask, "message", redact, "input_text")
agent = g.add("SnowflakeCortexAgent", {"database": "MY_DB", "schema_name": "MY_SCHEMA", "cortex_agent": "MY_AGENT",
                                       "resume_thread": False,
                                       "thread_continuity_mode": "Select Specific Thread / Start Fresh Thread"},
              name="Cortex Agent", x=900)
g.link(redact, "message", agent, "user_message")
out = g.add("ChatOutput", x=1350)
g.link(agent, "response", out, "input_value")

flow_id = c.save("My first flow", g, "Redact, then ask a Cortex Agent",
                 context_body({"MY_DB.MY_SCHEMA": {"cortex_agents": ["MY_AGENT"]}}))
c.select_role(flow_id, "MY_ROLE")
result = c.run(flow_id, "Chiller C7 is leaking coolant. What do we do?")
print(result["errors"] or result["nodes"]["Chat Output"]["outputs"]["message"])
```

## Things to get right

- **Names.** `Graph.add` takes the component type (`SnowflakeCortexAgent`), not the label on the canvas
  (`Cortex Agent`). `client.run` reports nodes by their display name, which you set with `name=`.
- **Object dropdowns** (database, schema, table, agent, search service, semantic view) accept the name as a
  string. The object must also be in the Context.
- **Prompts.** `g.prompt("... {tickets} ...")` creates one input per `{variable}`; link into it by variable
  name, and read it from the `prompt` output.
- **SQL Statement.** Write fully qualified names. Use `?` placeholders and link values into `bind_values`;
  with several links, values fill the `?` in the order the links were created. Pass results on through the
  `rows` output (just the rows as JSON). `SELECT` and `CALL` are supported; use a stored procedure for writes.
- **Writing rows.** `SnowflakeTables` with `operation_mode="INSERT / MERGE (Write)"` inserts the rows linked
  into `rows_to_write`: one object or a list of objects keyed by column name, as JSON, Table or Message
  (JSON text). Empty input, an unknown column or a wrong type fail the node. For anything beyond plain
  inserts (merge, update, idempotent loads) call a stored procedure.
- **Tools for an Agent.** Add the node with `tool_mode=True` and link its `component_as_tool` output to the
  Agent's `tools` field.
- **Models.** For `LanguageModelComponent` and `Agent`, set `model` to `c.model_value("<name>")` with a name
  from `lf.py models`. `SnowflakeAIComplete` takes the model name as a plain string and needs no proxy.
- **Failures are loud.** A node whose Snowflake call fails stops the flow with the Snowflake message; it never
  returns placeholder data. If a node is green and its rows are empty, the result really is empty.
- **Cortex Analyst** returns the whole API record as JSON (`interpretation`, `generated_sql`, `rows`). When the
  question cannot be answered from the semantic view, `rows` is empty and `answered_with_sql` is false.
- **Do not use the `Postgres` node**: it cannot run statements.
- **Decision** answers `choice`, `score` or `noul` (a yes probability) questions. The `summary` output is one
  readable line per question; `high_confidence_route` / `low_confidence_route` run only the chosen branch.
- **Triggers** create Snowflake objects (stream, task, alert) the first time the flow runs. Save a trigger
  with `status_mode` `Paused` unless the user asked for it to be live, and say what it will create.
- **Long runs.** `client.run` polls for events, because the Snowflake ingress cuts a single response after
  about 90 seconds. Do not replace it with one long HTTP call.
- **Do not edit flows other people own** unless asked. `Client.save` replaces a flow that has the same name.

## Patterns (details and code in `references/patterns.md`)

| The user wants | Pattern | Needs the proxy |
|---|---|---|
| Several specialists, one final answer | Parallel helpers, then a Cortex Agent | Only if helpers are `Language Model` / `Agent` |
| Clean, classify, then answer | Sequential pipeline | No |
| Turn raw rows into clean rows, no chat | Data engineering: trigger, SQL, AI function, Decision, procedure | No |
| An agent that needs a long, multi-step job | The agent calls two tools: start the flow, then get its result | No |
| Send a case down one of two paths | Decision routing | No |
| An assistant that remembers | `Agent Memory` / `Message History` | Depends on the model node |

The repository also ships these as importable flows with a setup script that creates the sample objects:
`examples/maintenance/` (`lf.py import <file>`).

## Files in this skill

| File | Use it for |
|---|---|
| `scripts/lf.py` | The client and command line tool (standard library only) |
| `scripts/flow_canvas_repair.py` | Used by `lf.py` so that flows built as code open correctly in the canvas |
| `references/nodes.md` | Field names and output ports of the common nodes |
| `references/patterns.md` | The design patterns with working code |
| `references/troubleshooting.md` | Error messages, causes and fixes |
