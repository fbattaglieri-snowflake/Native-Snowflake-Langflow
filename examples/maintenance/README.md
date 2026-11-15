# Four starter examples

Four small flows on the same sample data: a maintenance team with tickets, procedures and a Cortex Agent.
One SQL script creates everything they need, so you can rebuild them in your own account.

| # | Example | Pattern | Needs the LLM proxy |
|---|---|---|---|
| 1 | [Two helpers, one Cortex Agent](#1-two-helpers-one-cortex-agent) | Parallel | Yes |
| 2 | [Redact, classify, answer](#2-redact-classify-answer) | Sequential | No |
| 3 | [Inbox to tickets](#3-inbox-to-tickets) | Data engineering | No |
| 4 | [A Cortex Agent hands a long job to a workflow](#4-a-cortex-agent-hands-a-long-job-to-a-workflow) | Agent tool that starts a flow and waits for it | No |

All data is synthetic.

## Set up, once

1. **Create the objects.** Open [`00_setup.sql`](00_setup.sql), replace `<WAREHOUSE>`, and run the whole script.
   It creates the schema `SNOWFLAKE_LANGFLOW.EXAMPLES`. Running it again puts the data back to its initial state.
2. **Import the flows.** In Langflow, upload the four `.json` files of this folder (drag them onto the flow list).
3. **Pick a role.** Open a flow and select a role that can use the schema. The flow file carries its Context
   (the schema `EXAMPLES` with its objects), so there is nothing to tick; open `[ Context ]` to see it.

If your Langflow database is not called `SNOWFLAKE_LANGFLOW`, replace that name in the script and in the four
`.json` files before you start.

What the script creates:

| Object | Kind | Used by |
|---|---|---|
| `SERVICE_TICKETS` | Table, 30 tickets | 1, 2, 3, 4 |
| `SERVICE_DOCS` | Table, 8 maintenance procedures | 1, 2, 4 |
| `TICKET_INBOX` | Table, 4 raw messages | 3 |
| `TICKETS_ANALYTICS` | Semantic view on the tickets | 1, 2, 4 |
| `DOC_SEARCH` | Cortex Search service on the procedures | 1, 2, 4 |
| `MAINTENANCE_ADVISOR` | Cortex Agent with the two objects above as tools | 1, 2 |
| `NORMALIZE_SITE` | SQL function | 3 |
| `LOAD_TICKET` | Stored procedure | 3 |
| `REVIEW_RUNS` | Table, requests and results of the workflow | 4 |
| `START_SITE_REVIEW`, `GET_SITE_REVIEW` | Stored procedures, the two tools of the agent | 4 |
| `CLAIM_SITE_REVIEW`, `COMPLETE_SITE_REVIEW` | Stored procedures, first and last step of the flow | 4 |
| `MAINTENANCE_PLANNER` | Cortex Agent that hands the job to the workflow | 4 |

---

## 1. Two helpers, one Cortex Agent

File: [`1_two_helpers_one_cortex_agent.json`](1_two_helpers_one_cortex_agent.json)

Two helpers work on the same question at the same time, each in its own way and on its own model. A Cortex Agent
reads both results and writes the answer.

```mermaid
flowchart LR
    Q([Chat Input]) --> AN[Cortex Analyst<br/>ticket figures]
    AN --> A[Helper A<br/>Language Model · claude-haiku-5-5]
    Q --> B[Helper B<br/>Agent · openai-gpt-6-luna]
    S[Cortex Search<br/>procedures] -. tool .-> B
    A --> CA[Cortex Agent<br/>MAINTENANCE_ADVISOR]
    B --> CA
    CA --> O([Chat Output])
```

| | Helper A | Helper B |
|---|---|---|
| Node | `Language Model` | `Agent` |
| How it gets data | The flow runs Cortex Analyst first and hands over the result | It decides by itself when to call Cortex Search |
| Job | Say what the ticket history tells | Find the procedure and its steps |

**Ask:** `Chiller C7 at MILANO_PLANT is leaking coolant and the line is stopped. What should the technician do?`

**You should see:** a short advice with severity, likely cause, ordered steps, the number of tickets for that
chiller in Milan, and the title "Chiller coolant leak procedure".

The two model names come from the LLM proxy. If your proxy serves other models, pick two of yours in the nodes.

## 2. Redact, classify, answer

File: [`2_sequential_redact_classify_agent.json`](2_sequential_redact_classify_agent.json)

One step after the other. Each node does one thing and passes its result on.

```mermaid
flowchart LR
    Q([Chat Input]) --> R[1 · AI Redact<br/>personal data out]
    R --> C[2 · AI Function<br/>AI_CLASSIFY the fault]
    C --> CA[3 · Cortex Agent<br/>MAINTENANCE_ADVISOR]
    CA --> O([Chat Output])
```

**Ask:** `Hi, this is Marco Bianchi (marco.bianchi@example.com, +39 333 1234567) from the Milan plant. Chiller C7 is leaking coolant and line 2 is stopped. What do we do?`

**You should see:**

| Step | Result |
|---|---|
| AI Redact | `Hi, this is [NAME] ([EMAIL], [PHONE_NUMBER]) from the Milan plant. ...` |
| AI_CLASSIFY | `cooling` |
| Cortex Agent | The advice, written without ever seeing the name, the email or the phone number |

## 3. Inbox to tickets

File: [`3_data_engineering_inbox_to_tickets.json`](3_data_engineering_inbox_to_tickets.json)

No chat. Raw messages land in a table; each run turns the oldest one into a clean ticket.

```mermaid
flowchart LR
    T([Table Change Trigger<br/>TICKET_INBOX]) --> N[1 · SQL Statement<br/>next message]
    N --> X[2 · AI Function<br/>AI_EXTRACT site, asset, problem]
    N --> D[3 · Decision<br/>how severe?]
    N --> L
    X --> L[4 · SQL Statement<br/>CALL LOAD_TICKET]
    D --> L
    L --> R[5 · SQL Statement<br/>open tickets per site]
    R --> O([Text Output])
```

| Step | Node | What it does |
|---|---|---|
| 1 | `SQL Statement` (SELECT) | Reads the oldest message that has not been processed |
| 2 | `AI Function` | `AI_EXTRACT` reads site, asset and problem from the free text |
| 3 | `Decision` | Chooses the severity: low, medium, high or critical |
| 4 | `SQL Statement` (CALL) | The procedure `LOAD_TICKET` writes the ticket and marks the message as processed. It uses the function `NORMALIZE_SITE` to turn "Milan plant" into `MILANO_PLANT` |
| 5 | `SQL Statement` (SELECT) | Counts open and urgent tickets per site |

**Run it:** open the flow and run the last node (`Text Output`). Run it four times: one message per run.

**You should see** four new rows in `SERVICE_TICKETS`:

| Message | Site | Asset | Severity |
|---|---|---|---|
| "Milan plant, night shift: chiller C7 is losing coolant again and line 2 is stopped..." | `MILANO_PLANT` | `CHILLER_C7` | critical |
| "...at the Bologna depot the E5 packer makes a light rattling noise. No hurry..." | `BOLOGNA_DEPOT` | `PACKER_E5` | low |
| "TORINO - the light curtain on press A12 did not react twice this morning..." | `TORINO_PLANT` | `PRESS_A12` | high |
| "Napoli depot: conveyor B3 belt drifts to the left after every restart..." | `NAPOLI_DEPOT` | `CONVEYOR_B3` | medium |

```sql
SELECT TICKET_ID, SITE, ASSET, SEVERITY, NOTE
FROM SNOWFLAKE_LANGFLOW.EXAMPLES.SERVICE_TICKETS
WHERE SOURCE = 'ticket_inbox_flow';
```

A fifth run finds nothing to do and says so. Severity is chosen by a model, so it can differ by one level.

**The trigger.** The `Table Change Trigger` is saved as *Paused*: the first run creates its stream and its
suspended task in the schema (the role needs `CREATE STREAM` and `CREATE TASK` there; the role that ran the setup
script has them), and nothing runs on its own. Set it to *Active* to start the flow when new rows arrive in
`TICKET_INBOX`; that needs the trigger setup of the deployment guide (`sql/03_triggers_service_function.sql`).
This example was tested by running it from the canvas; example 4 is the one tested through an active trigger.

**Bind values.** Step 4 has three nodes connected to `Bind Values`: they fill the three `?` of the `CALL` in the
order the connections were drawn (message, extraction, decision).

**Rows Only.** The `SQL Statement` nodes pass their result on through the `Rows Only` output: just the rows as
JSON, without the SQL text and the role that the `JSON Message` output also carries.

## 4. A Cortex Agent hands a long job to a workflow

File: [`4_workflow_behind_a_cortex_agent.json`](4_workflow_behind_a_cortex_agent.json)

Someone asks the Cortex Agent `MAINTENANCE_PLANNER` for the weekly maintenance plan of a site. One agent is not
enough for that job: it has fixed steps in a fixed order, it uses several services, and it can take longer than an
agent is allowed to wait inside one tool call. So the agent hands the job to a flow and waits for the result.

```mermaid
sequenceDiagram
    participant U as User
    participant A as Cortex Agent<br/>MAINTENANCE_PLANNER
    participant T as Table<br/>REVIEW_RUNS
    participant F as Langflow flow
    U->>A: Plan for the Torino plant?
    A->>T: tool start_site_review → new row, PENDING
    T-->>A: review_run_id (seconds)
    T->>F: Table Change Trigger starts the flow
    A->>T: tool get_site_review (waits up to ~50 s)
    T-->>A: RUNNING → the agent calls it again
    F->>T: plan stored, DONE
    A->>T: tool get_site_review
    T-->>A: DONE + plan
    A->>U: The plan
```

The "webhook" is a row in a table. Nothing leaves Snowflake: no HTTP call, no network rule.

What the flow does:

```mermaid
flowchart LR
    T([Table Change Trigger<br/>REVIEW_RUNS]) --> C[1 · SQL Statement<br/>CALL CLAIM_SITE_REVIEW]
    C --> K[2 · SQL Statement<br/>open tickets of the site]
    K --> S[3 · Cortex Search<br/>procedures]
    C --> N[4 · Cortex Analyst<br/>critical tickets per asset]
    K --> D[5 · Decision<br/>planned line stop?]
    S --> W[6 · AI Complete<br/>writes the plan]
    N --> W
    D --> W
    K --> W
    W --> E[7 · SQL Statement<br/>CALL COMPLETE_SITE_REVIEW]
    E --> O([Text Output])
```

The two tools of the agent are two stored procedures:

| Tool | Procedure | Returns | Takes |
|---|---|---|---|
| `start_site_review` | `START_SITE_REVIEW(site)` | A `review_run_id`, not the plan | Seconds |
| `get_site_review` | `GET_SITE_REVIEW(review_run_id)` | `RUNNING` (call again), `DONE` with the plan, `FAILED` or `NOT_FOUND` | Up to about 50 seconds per call |

The tool descriptions and the agent instructions say that the first tool does not return the plan and that the
second must be called again while it answers `RUNNING`. That text is what makes the agent wait.

**Arm the trigger, once.** Open the flow and run the last node (`Text Output`). This first run finds no request
and creates the stream and the task that start the flow. It needs the trigger setup of the deployment guide
(`sql/03_triggers_service_function.sql`) and a role that can create streams and tasks in the schema. Later runs
started by the trigger use the role that was selected in this first run.

**Ask the agent.** In Snowsight open the agent `MAINTENANCE_PLANNER` and ask, or run:

```sql
SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
  'SNOWFLAKE_LANGFLOW.EXAMPLES.MAINTENANCE_PLANNER',
  $${"messages":[{"role":"user","content":[{"type":"text","text":"Prepare this week's maintenance plan for the Torino plant."}]}]}$$);
```

**You should see:** the agent calls `start_site_review`, then `get_site_review` once or more, then answers with a
plan: whether a line stop is needed, one item per open ticket with the procedure to follow, and a line on
preventive maintenance. Each request is a row in `REVIEW_RUNS`:

```sql
SELECT RUN_ID, SITE, STATUS, REQUESTED_AT, STARTED_AT, COMPLETED_AT
FROM SNOWFLAKE_LANGFLOW.EXAMPLES.REVIEW_RUNS ORDER BY REQUESTED_AT;
```

Measured on the test account: the flow started 6 to 9 seconds after the request and ran for 16 to 20 seconds; the
whole answer took about 45 seconds. With the trigger held back for 80 seconds, the agent received `RUNNING`, called
`get_site_review` again and answered after about 2 minutes.

**Limits to know.**

- A Cortex Agent request ends after 15 minutes unless the caller uses a background run. `GET_SITE_REVIEW` reports
  `FAILED` when a review is still unfinished after 15 minutes.
- The agent budget is set to 600 seconds in the setup script. Raise it for longer workflows.
- The task waits for the flow to finish and handles one batch of requests at a time: requests that arrive while a
  flow is running start after it.
- If the flow fails, the request stays `RUNNING` until the 15 minutes pass.
- The agent was tested through `DATA_AGENT_RUN` in SQL, not in the Snowsight chat.

## Start again

Run `00_setup.sql` again: sample data, search service, agents, functions and procedures are rebuilt. The tables
`TICKET_INBOX` and `REVIEW_RUNS` are emptied, not replaced, so the streams of the two triggers stay valid.
