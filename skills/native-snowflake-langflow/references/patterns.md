# Design patterns

Five patterns with code that was run against a live service. They use the sample objects of the repository
(`examples/maintenance/00_setup.sql` creates them in `SNOWFLAKE_LANGFLOW.EXAMPLES`); replace names with the
user's own objects. The same flows ship as importable files in `examples/maintenance/`.

Every snippet starts from this setup:

```python
import json, sys
sys.path.insert(0, "scripts")
from lf import Client, Graph, context_body

c = Client()
DB, SCH = "SNOWFLAKE_LANGFLOW", "EXAMPLES"
E = f"{DB}.{SCH}"
ROLE = "MY_ROLE"                      # a role that can use the objects below
src = {"database": DB, "schema_name": SCH}
advisor = {**src, "cortex_agent": "MAINTENANCE_ADVISOR", "resume_thread": False,
           "thread_continuity_mode": "Select Specific Thread / Start Fresh Thread"}
sql = {**src, "max_rows": 50, "output_format": "JSON Object (OBJECT_CONSTRUCT)"}
CONTEXT = context_body({E: {
    "tables": ["SERVICE_TICKETS", "SERVICE_DOCS", "TICKET_INBOX", "REVIEW_RUNS"],
    "semantic_views": ["TICKETS_ANALYTICS"], "cortex_search_services": ["DOC_SEARCH"],
    "cortex_agents": ["MAINTENANCE_ADVISOR", "MAINTENANCE_PLANNER"], "functions": ["NORMALIZE_SITE"],
    "procedures": ["LOAD_TICKET", "START_SITE_REVIEW", "GET_SITE_REVIEW", "CLAIM_SITE_REVIEW", "COMPLETE_SITE_REVIEW"]}})
```

## 1. Parallel helpers, one Cortex Agent

Use it when a question needs several kinds of evidence. Each helper does one job; a Cortex Agent writes the answer.
The two helpers show the two ways to give a model data: **run the data node first and hand over the result**
(`Language Model`), or **give the node as a tool and let the model decide** (`Agent`). Needs the LLM proxy.

```python
g = Graph(c, "P1")
ask = g.add("ChatInput", {"input_value": "Chiller C7 at MILANO_PLANT is leaking coolant. What should the technician do?"}, y=300)
# Helper A: Cortex Analyst runs first, a Language Model reads the result
q = g.prompt("How many tickets exist for the site and the asset named in this report, by status and severity?\n\nReport: {report}",
             name="Question for Cortex Analyst", x=500, y=-650)
g.link(ask, "message", q, "report")
analyst = g.add("SnowflakeCortexAnalyst", {**src, "source_mode": "Semantic View", "semantic_source": f"{E}.TICKETS_ANALYTICS",
                                          "execute_generated_sql": True}, name="Cortex Analyst", x=500, y=-250)
g.link(q, "prompt", analyst, "question")
facts = g.prompt("Technician report:\n{report}\n\nTicket figures:\n{figures}", name="Report and figures", x=1000, y=-250)
g.link(ask, "message", facts, "report")
g.link(analyst, "message", facts, "figures")
helper_a = g.add("LanguageModelComponent", {
    "model": c.model_value("claude-haiku-5-5"), "temperature": 0.1,
    "system_message": "You are a ticket analyst. In at most 3 short lines say what the figures tell about this fault. Use only the figures given."},
    name="Helper A", x=1500, y=-250)
g.link(facts, "prompt", helper_a, "input_value")
# Helper B: an Agent that calls Cortex Search as a tool
search = g.add("SnowflakeCortexSearch", {**src, "search_service": f"{E}.DOC_SEARCH", "columns_csv": "TITLE,CHUNK", "limit": 3},
               tool_mode=True, name="Cortex Search (tool)", x=1000, y=900)
helper_b = g.add("Agent", {
    "model": c.model_value("openai-gpt-6-luna"), "n_messages": 1, "add_calculator_tool": False, "add_current_date_tool": False,
    "system_prompt": "You are a procedure specialist. Use the search tool to find the procedure that covers the fault. Answer in at most 4 short lines: its title and its steps."},
    name="Helper B", x=1500, y=600)
g.link(ask, "message", helper_b, "input_value")
g.link(search, "component_as_tool", helper_b, "tools")
# The Cortex Agent gets both
brief = g.prompt("Technician report:\n{report}\n\nNotes from the ticket analyst:\n{tickets}\n\nNotes from the procedure specialist:\n{procedure}\n\nWrite the final advice.",
                 name="Brief", x=2050, y=300)
g.link(ask, "message", brief, "report")
g.link(helper_a, "text_output", brief, "tickets")
g.link(helper_b, "response", brief, "procedure")
final = g.add("SnowflakeCortexAgent", advisor, name="Cortex Agent", x=2550, y=300)
g.link(brief, "prompt", final, "user_message")
out = g.add("ChatOutput", x=3050, y=300)
g.link(final, "response", out, "input_value")

flow = c.save("PATTERN_1_Parallel_Helpers", g, "Two helpers, one Cortex Agent", CONTEXT)
c.select_role(flow, ROLE)
r = c.run(flow, "Chiller C7 at MILANO_PLANT is leaking coolant. What should the technician do?")
assert not r["errors"], r["errors"]
print(r["nodes"]["Chat Output"]["outputs"]["message"])
```

## 2. Sequential pipeline

Use it when each step prepares the input of the next. No proxy needed.

```python
g = Graph(c, "P2")
ask = g.add("ChatInput", {"input_value": "Hi, this is Marco Bianchi (marco.bianchi@example.com) from the Milan plant. Chiller C7 is leaking coolant."})
redact = g.add("SnowflakeAIRedact", {"redact_mode": "redact", "source_mode": "Text Input", "pii_categories": "ALL"},
               name="AI Redact", x=450)
g.link(ask, "message", redact, "input_text")
kind = g.add("SnowflakeAIFunction", {"ai_function": "AI_CLASSIFY", "return_as_json": False,
                                     "categories_or_schema": "cooling,hydraulic,electrical,mechanical,safety"},
             name="AI_CLASSIFY", x=900)
g.link(redact, "message", kind, "input_text")
brief = g.prompt("Technician report (personal data removed):\n{report}\n\nKind of fault: {kind}", name="Brief", x=1350)
g.link(redact, "message", brief, "report")
g.link(kind, "message", brief, "kind")
agent = g.add("SnowflakeCortexAgent", advisor, name="Cortex Agent", x=1800)
g.link(brief, "prompt", agent, "user_message")
out = g.add("ChatOutput", x=2250)
g.link(agent, "response", out, "input_value")

flow = c.save("PATTERN_2_Sequential", g, "Redact, classify, answer", CONTEXT)
c.select_role(flow, ROLE)
r = c.run(flow, "Hi, this is Marco Bianchi (marco.bianchi@example.com) from the Milan plant. Chiller C7 is leaking coolant.")
assert not r["errors"], r["errors"]
assert "Bianchi" not in r["nodes"]["AI Redact"]["outputs"]["message"]
```

## 3. Data engineering: raw rows in, clean rows out

Use it for work without a chat: a trigger starts the flow, SQL reads, AI functions and a Decision enrich, a stored
procedure writes. Rules that keep it safe:

- Reads are `SELECT`, writes go through one stored procedure that also marks the input row as done, so a rerun
  never writes twice.
- Link the `rows` output of `SQL Statement`, not `message`.
- Several links into `bind_values` fill the `?` in the order the links are created.
- The trigger is saved `Paused`; the first run still creates its stream and suspended task.

```python
g = Graph(c, "P3")
trig = g.add("SnowflakeTableChangeTrigger", {"database": DB, "schema": SCH, "source_object": "TICKET_INBOX",
                                             "trigger_on": "New Rows Only (Insert)", "status_mode": "Paused"},
             name="Trigger")
nxt = g.add("SnowflakeSQLStatement", {**sql, "sql_statement":
            f"SELECT INBOX_ID, RAW_TEXT FROM {E}.TICKET_INBOX WHERE PROCESSED_AT IS NULL AND ? IS NOT NULL ORDER BY RECEIVED_AT LIMIT 1"},
            name="Next message", x=500)
g.link(trig, "trigger_message_out", nxt, "bind_values")       # the link makes the trigger start this node
extract = g.add("SnowflakeAIFunction", {"ai_function": "AI_EXTRACT", "return_as_json": True, "categories_or_schema": json.dumps({
    "site": "Which city or site does RAW_TEXT mention?",
    "asset": "Which machine does RAW_TEXT mention? Answer with one code: PRESS_A12, CONVEYOR_B3, CHILLER_C7, ROBOT_ARM_D2 or PACKER_E5",
    "problem": "Describe the fault reported in RAW_TEXT in one short sentence"})}, name="AI_EXTRACT", x=1000, y=-350)
g.link(nxt, "rows", extract, "input_text")
severity = g.add("SnowflakeDecision", {"model": "snowflake-decision-fast", "include_none_of_these": False, "questions_json": json.dumps({
    "severity": {"type": "choice", "instructions": "How severe is the fault described in this message?",
                 "criteria": {"critical": "production is stopped or people are at risk",
                              "high": "a safety device or a key machine misbehaves but production still runs",
                              "medium": "a fault that needs repeated manual work or will get worse",
                              "low": "a minor annoyance, no hurry"}}})}, name="Decision", x=1000, y=350)
g.link(nxt, "rows", severity, "state_input")
load = g.add("SnowflakeSQLStatement", {**sql, "sql_statement": f"CALL {E}.LOAD_TICKET(?, ?, ?)"}, name="Load ticket", x=1550)
g.link(nxt, "rows", load, "bind_values")            # first ?
g.link(extract, "message", load, "bind_values")     # second ?
g.link(severity, "summary", load, "bind_values")    # third ?
out = g.add("TextOutput", x=2050)
g.link(load, "rows", out, "input_value")

flow = c.save("PATTERN_3_Data_Engineering", g, "Inbox rows become tickets", CONTEXT)
c.select_role(flow, ROLE)
r = c.run(flow)
assert not r["errors"], r["errors"]
print(r["nodes"]["Load ticket"]["outputs"]["rows"])
```

## 4. A Cortex Agent hands a long job to a flow

Use it when someone talks to a Cortex Agent (in Snowflake Intelligence, in an app) and the job behind the answer
is a multi-step workflow that can take minutes. An agent cannot wait minutes inside one tool call, so it gets two
quick tools backed by stored procedures:

| Tool | Does | Returns |
|---|---|---|
| start | Inserts a request row | A run id, within seconds |
| get | Waits up to about 50 seconds for the result | `RUNNING` (call again), `DONE` with the result, `FAILED`, `NOT_FOUND` |

The flow starts on the new row through an **active** `Table Change Trigger`, claims the request, does the work and
ends with a procedure that stores the result. Nothing leaves Snowflake. The setup script has all five objects
(`REVIEW_RUNS`, `START_SITE_REVIEW`, `GET_SITE_REVIEW`, `CLAIM_SITE_REVIEW`, `COMPLETE_SITE_REVIEW`) and the agent
`MAINTENANCE_PLANNER`; copy them and rename.

What makes the agent wait is text: the tool descriptions and the agent instructions must say that *start* does
not return the answer and that *get* must be called again while it answers `RUNNING`. Set the agent's time budget
above the longest expected run.

```python
g = Graph(c, "P4")
trig = g.add("SnowflakeTableChangeTrigger", {"database": DB, "schema": SCH, "source_object": "REVIEW_RUNS",
                                             "trigger_on": "New Rows Only (Insert)", "status_mode": "Paused"},  # "Active" to go live
             name="Trigger")
claim = g.add("SnowflakeSQLStatement", {**sql, "sql_statement": f"CALL {E}.CLAIM_SITE_REVIEW()"}, name="Claim", x=500)
g.link(trig, "trigger_message_out", claim, "bind_values")
tickets = g.add("SnowflakeSQLStatement", {**sql, "sql_statement":
                "SELECT TICKET_ID, ASSET, SEVERITY, STATUS, NOTE "
                f"FROM {E}.SERVICE_TICKETS WHERE STATUS <> 'closed' AND SITE = REGEXP_SUBSTR(?, '[A-Z]+_(PLANT|DEPOT)')"},
                name="Open tickets", x=1000)
g.link(claim, "rows", tickets, "bind_values")
query = g.prompt("Maintenance procedures, safety bulletins and quality procedures that cover these faults:\n{tickets}",
                 name="What to search", x=1500, y=-400)
g.link(tickets, "rows", query, "tickets")
docs = g.add("SnowflakeCortexSearch", {**src, "search_service": f"{E}.DOC_SEARCH", "columns_csv": "TITLE,CHUNK", "limit": 8},
             name="Procedures", x=2000, y=-400)
g.link(query, "prompt", docs, "query")
brief = g.prompt("Write the weekly maintenance plan of the site in this request. Use only the facts below. Maximum 20 lines.\n\n"
                 "Request:\n{request}\n\nOpen tickets:\n{tickets}\n\nProcedures:\n{procedures}", name="Brief", x=2500)
g.link(claim, "rows", brief, "request")
g.link(tickets, "rows", brief, "tickets")
g.link(docs, "message", brief, "procedures")
writer = g.add("SnowflakeAIComplete", {"model": "claude-sonnet-5-5", "max_tokens": 2000}, name="Writer", x=3000)
g.link(brief, "prompt", writer, "prompt")
done = g.add("SnowflakeSQLStatement", {**sql, "sql_statement": f"CALL {E}.COMPLETE_SITE_REVIEW(?, ?)"}, name="Store the plan", x=3500)
g.link(claim, "rows", done, "bind_values")
g.link(writer, "message", done, "bind_values")
out = g.add("TextOutput", x=4000)
g.link(done, "rows", out, "input_value")

flow = c.save("PATTERN_4_Flow_Behind_An_Agent", g, "The workflow behind an agent tool", CONTEXT)
c.select_role(flow, ROLE)
r = c.run(flow)          # with no pending request it claims nothing and stores nothing
assert not r["errors"], r["errors"]
```

With the trigger `Active`, run the flow once to create the stream and the task; from then on a call to the start
procedure (by the agent or by hand) starts it. Runs started by the trigger use the role selected in that first run.

## 5. Decision routing

Use it to send a case down one of two paths. `Decision` returns a confidence; the node's two route outputs run
only the branch that matches `confidence_threshold`. No proxy needed.

```python
g = Graph(c, "P5")
ask = g.add("ChatInput", {"input_value": "The E5 packer makes a light rattling noise. No hurry."})
route = g.add("SnowflakeDecision", {"model": "snowflake-decision-fast", "include_none_of_these": False,
                                    "confidence_threshold": 0.6, "questions_json": json.dumps({
    "urgent": {"type": "noul", "instructions": "Does this report describe a stopped line or a risk for people?"}})},
    name="Urgent?", x=450)
g.link(ask, "message", route, "state_input")
yes = g.add("TypeConverterComponent", {"output_type": "Message"}, name="Urgent branch", x=900, y=-250)
g.link(route, "high_confidence_route", yes, "input_data")
no = g.add("TypeConverterComponent", {"output_type": "Message"}, name="Routine branch", x=900, y=250)
g.link(route, "low_confidence_route", no, "input_data")
out_yes = g.add("TextOutput", name="Urgent output", x=1350, y=-250)
g.link(yes, "message_output", out_yes, "input_value")
out_no = g.add("TextOutput", name="Routine output", x=1350, y=250)
g.link(no, "message_output", out_no, "input_value")

flow = c.save("PATTERN_5_Decision_Routing", g, "One of two branches runs", CONTEXT)
c.select_role(flow, ROLE)
r = c.run(flow, "The E5 packer makes a light rattling noise. No hurry.")
assert not r["errors"], r["errors"]
print([n for n in r["nodes"] if "output" in n])      # only the branch that ran: ['Routine output']
```

Put the real work of each branch (a Cortex Agent, a SQL call, a notification) after the converter.

## Memory

`Message History` keeps the turns of a conversation and `Agent Memory` saves and recalls facts in Snowflake tables.
They are described in the repository README (section 5) and are not covered by tested code in this skill: read
`lf.py node MessageHistory` and `lf.py node SnowflakeAgentMemory`, and build a small flow to try them first.
