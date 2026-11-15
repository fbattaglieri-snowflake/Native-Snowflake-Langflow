-- Starting objects for the four maintenance examples (examples/maintenance/README.md).
-- Run it once, top to bottom, with a role that can create objects in the database. It is safe to run again:
-- it rebuilds every object and puts the sample data back to its initial state.
--
-- Before running, replace:
--   <WAREHOUSE>           a warehouse the role can use (Cortex Search indexing, Cortex Analyst queries, agent tools)
--   SNOWFLAKE_LANGFLOW    only if your Langflow database has another name. The three flow files use the same
--                         name, so replace it there too before importing them.
--
-- Creates, in schema SNOWFLAKE_LANGFLOW.EXAMPLES:
--   SERVICE_TICKETS     table, 30 field service tickets
--   SERVICE_DOCS        table, 8 maintenance procedures
--   TICKET_INBOX        table, 4 raw messages waiting to become tickets (example 3)
--   TICKETS_ANALYTICS   semantic view on SERVICE_TICKETS (Cortex Analyst)
--   DOC_SEARCH          Cortex Search service on SERVICE_DOCS
--   MAINTENANCE_ADVISOR Cortex Agent that uses the two objects above
--   NORMALIZE_SITE      SQL function (example 3)
--   LOAD_TICKET         stored procedure (example 3)
--   REVIEW_RUNS         table, requests and results of the site review workflow (example 4)
--   START_SITE_REVIEW, GET_SITE_REVIEW          procedures, the two tools of the agent (example 4)
--   CLAIM_SITE_REVIEW, COMPLETE_SITE_REVIEW     procedures, first and last step of the flow (example 4)
--   MAINTENANCE_PLANNER Cortex Agent that hands the job to the workflow (example 4)

CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_LANGFLOW.EXAMPLES
  COMMENT = 'Sample objects for the Native Snowflake Langflow examples';
USE SCHEMA SNOWFLAKE_LANGFLOW.EXAMPLES;

-- 1. Tables and sample data -------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SERVICE_TICKETS (
  TICKET_ID       VARCHAR DEFAULT UUID_STRING(),
  SITE            VARCHAR,
  ASSET           VARCHAR,
  SEVERITY        VARCHAR,   -- low, medium, high, critical
  STATUS          VARCHAR,   -- open, in_progress, closed
  SENTIMENT_SCORE FLOAT,     -- -1 very negative .. 1 very positive
  NOTE            VARCHAR,
  SOURCE          VARCHAR,   -- seed, or the flow that wrote the row
  CREATED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO SERVICE_TICKETS (TICKET_ID, SITE, ASSET, SEVERITY, STATUS, SENTIMENT_SCORE, NOTE, SOURCE, CREATED_AT) VALUES
  ('T-0001', 'MILANO_PLANT', 'PRESS_A12', 'high', 'closed', -0.6, 'Hydraulic pressure drop during night shift, seal replaced.', 'seed', '2026-08-01 07:15:00'),
  ('T-0002', 'MILANO_PLANT', 'CONVEYOR_B3', 'medium', 'closed', -0.2, 'Belt misalignment, retensioned.', 'seed', '2026-08-02 09:40:00'),
  ('T-0003', 'MILANO_PLANT', 'CHILLER_C7', 'critical', 'open', -0.9, 'Coolant leak, line stopped, waiting for spare part.', 'seed', '2026-08-03 06:05:00'),
  ('T-0004', 'MILANO_PLANT', 'ROBOT_ARM_D2', 'low', 'closed', 0.3, 'Routine calibration, within tolerance.', 'seed', '2026-08-04 11:20:00'),
  ('T-0005', 'MILANO_PLANT', 'PACKER_E5', 'medium', 'in_progress', -0.3, 'Label applicator jams intermittently.', 'seed', '2026-08-05 14:00:00'),
  ('T-0006', 'MILANO_PLANT', 'PRESS_A12', 'low', 'closed', 0.4, 'Lubrication cycle completed, no findings.', 'seed', '2026-08-06 08:10:00'),
  ('T-0007', 'MILANO_PLANT', 'CHILLER_C7', 'high', 'in_progress', -0.7, 'Temperature drift above threshold after restart.', 'seed', '2026-08-07 16:35:00'),
  ('T-0008', 'TORINO_PLANT', 'PRESS_A12', 'medium', 'closed', -0.1, 'Noise on downstroke, bushing replaced.', 'seed', '2026-08-01 10:05:00'),
  ('T-0009', 'TORINO_PLANT', 'CONVEYOR_B3', 'critical', 'open', -0.95, 'Drive motor burnt, production line halted.', 'seed', '2026-08-02 05:50:00'),
  ('T-0010', 'TORINO_PLANT', 'ROBOT_ARM_D2', 'low', 'closed', 0.5, 'Firmware updated, cycle time improved.', 'seed', '2026-08-03 13:25:00'),
  ('T-0011', 'TORINO_PLANT', 'PACKER_E5', 'high', 'in_progress', -0.65, 'Carton feeder repeatedly out of sync.', 'seed', '2026-08-04 15:45:00'),
  ('T-0012', 'TORINO_PLANT', 'CHILLER_C7', 'medium', 'closed', -0.25, 'Filter clogged, replaced during planned stop.', 'seed', '2026-08-05 07:30:00'),
  ('T-0013', 'TORINO_PLANT', 'PRESS_A12', 'low', 'closed', 0.2, 'Operator training request, closed with checklist.', 'seed', '2026-08-06 12:15:00'),
  ('T-0014', 'TORINO_PLANT', 'CONVEYOR_B3', 'medium', 'open', -0.4, 'Roller bearing noise increasing week over week.', 'seed', '2026-08-07 09:55:00'),
  ('T-0015', 'BOLOGNA_DEPOT', 'PACKER_E5', 'low', 'closed', 0.6, 'Preventive check, all parameters nominal.', 'seed', '2026-08-01 08:45:00'),
  ('T-0016', 'BOLOGNA_DEPOT', 'ROBOT_ARM_D2', 'high', 'open', -0.75, 'Gripper drops packages above 12 kg.', 'seed', '2026-08-02 14:10:00'),
  ('T-0017', 'BOLOGNA_DEPOT', 'CHILLER_C7', 'low', 'closed', 0.1, 'Condensate drain cleaned.', 'seed', '2026-08-03 10:30:00'),
  ('T-0018', 'BOLOGNA_DEPOT', 'PRESS_A12', 'medium', 'in_progress', -0.35, 'Cycle time degraded by eight percent.', 'seed', '2026-08-04 17:05:00'),
  ('T-0019', 'BOLOGNA_DEPOT', 'CONVEYOR_B3', 'low', 'closed', 0.45, 'Emergency stop test passed.', 'seed', '2026-08-05 11:50:00'),
  ('T-0020', 'BOLOGNA_DEPOT', 'PACKER_E5', 'critical', 'open', -0.85, 'Sealing bar overheating, fire risk flagged.', 'seed', '2026-08-06 06:20:00'),
  ('T-0021', 'NAPOLI_DEPOT', 'CHILLER_C7', 'medium', 'closed', -0.15, 'Refrigerant topped up after slow loss.', 'seed', '2026-08-01 15:35:00'),
  ('T-0022', 'NAPOLI_DEPOT', 'PRESS_A12', 'high', 'open', -0.7, 'Safety light curtain intermittently unresponsive.', 'seed', '2026-08-02 08:25:00'),
  ('T-0023', 'NAPOLI_DEPOT', 'ROBOT_ARM_D2', 'low', 'closed', 0.35, 'Cable chain inspection, no wear.', 'seed', '2026-08-03 12:40:00'),
  ('T-0024', 'NAPOLI_DEPOT', 'CONVEYOR_B3', 'medium', 'in_progress', -0.3, 'Speed sensor reading unstable at low speed.', 'seed', '2026-08-04 09:15:00'),
  ('T-0025', 'NAPOLI_DEPOT', 'PACKER_E5', 'low', 'closed', 0.55, 'Spare parts restocked, no downtime.', 'seed', '2026-08-05 16:00:00'),
  ('T-0026', 'NAPOLI_DEPOT', 'CHILLER_C7', 'high', 'open', -0.8, 'Compressor short cycling under full load.', 'seed', '2026-08-06 07:45:00'),
  ('T-0027', 'NAPOLI_DEPOT', 'PRESS_A12', 'low', 'closed', 0.25, 'Guard panel realigned.', 'seed', '2026-08-07 13:05:00'),
  ('T-0028', 'MILANO_PLANT', 'CONVEYOR_B3', 'high', 'open', -0.65, 'Product jam at transfer point twice per shift.', 'seed', '2026-08-08 10:20:00'),
  ('T-0029', 'TORINO_PLANT', 'ROBOT_ARM_D2', 'medium', 'in_progress', -0.2, 'Vision system misreads reflective labels.', 'seed', '2026-08-08 11:40:00'),
  ('T-0030', 'BOLOGNA_DEPOT', 'CHILLER_C7', 'critical', 'open', -0.9, 'Cold room above target for six hours, stock at risk.', 'seed', '2026-08-08 05:30:00');

CREATE OR REPLACE TABLE SERVICE_DOCS (
  DOC_ID VARCHAR,
  TITLE  VARCHAR,
  CHUNK  VARCHAR,
  SOURCE VARCHAR
);

INSERT INTO SERVICE_DOCS (DOC_ID, TITLE, CHUNK, SOURCE) VALUES
  ('D-001', 'Chiller coolant leak procedure', 'When a chiller reports a coolant leak, isolate the unit, depressurise the circuit and inspect the shaft seal first: it accounts for most leaks on the C7 family. Replace the seal kit, refill with the approved refrigerant and run a thirty minute leak test before returning the unit to production. Escalate to the OEM if pressure drops more than 0.2 bar during the test.', 'maintenance_manual'),
  ('D-002', 'Conveyor belt misalignment', 'Belt misalignment on the B3 conveyor is corrected by retensioning the belt to 320 newton and squaring the tail pulley. Check for material build-up on the return rollers before adjusting tension: build-up reproduces the fault within a shift. Record the tension value in the maintenance log.', 'maintenance_manual'),
  ('D-003', 'Press hydraulic pressure loss', 'A pressure drop on the A12 press is usually the rod seal or the relief valve. Measure static pressure with the pump off. If pressure holds, the pump is fine and the relief valve is the suspect. Replace the seal kit every 4000 operating hours as preventive maintenance.', 'maintenance_manual'),
  ('D-004', 'Robot gripper payload limits', 'The D2 robot arm gripper is rated for 10 kg continuous and 12 kg peak. Dropped packages above 12 kg are expected behaviour, not a fault: reduce the payload or fit the high force gripper. Vacuum loss below 0.6 bar triggers the drop protection.', 'maintenance_manual'),
  ('D-005', 'Packer sealing bar overheating', 'Overheating of the E5 sealing bar is a safety stop condition. Cut power at the local isolator, let the bar cool, then check the thermocouple and the solid state relay: a welded relay keeps the heater energised and is the most frequent root cause. Do not restart production until the relay has been replaced and the temperature curve verified.', 'safety_bulletin'),
  ('D-006', 'Safety light curtain unresponsive', 'An intermittently unresponsive light curtain must be treated as a line stop. Verify alignment, clean the lenses and check the muting circuit wiring. Any curtain fault older than 24 hours has to be reported to the safety officer under the plant safety procedure.', 'safety_bulletin'),
  ('D-007', 'Cold room temperature excursion', 'A cold room above target temperature for more than two hours puts stock at risk and starts the quality hold procedure. Log the excursion, move perishable stock to the backup room and have quality assurance release or scrap the affected batches.', 'quality_procedure'),
  ('D-008', 'Preventive maintenance intervals', 'Preventive maintenance intervals: press A12 every 4000 hours, conveyor B3 every 2000 hours, chiller C7 every 1500 hours, robot arm D2 every 3000 hours, packer E5 every 2500 hours. Intervals are halved for assets with two or more critical tickets in the last quarter.', 'maintenance_manual');

-- Raw messages as they arrive from the field: free text, no structure. Example 3 turns them into tickets.
-- Not replaced on a rerun, so the stream of the flow trigger stays valid: only the rows are reset.
CREATE TABLE IF NOT EXISTS TICKET_INBOX (
  INBOX_ID     NUMBER,
  RAW_TEXT     VARCHAR,
  RECEIVED_AT  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  PROCESSED_AT TIMESTAMP_NTZ,
  TICKET_ID    VARCHAR
);
DELETE FROM TICKET_INBOX;

INSERT INTO TICKET_INBOX (INBOX_ID, RAW_TEXT, RECEIVED_AT) VALUES
  (1, 'Milan plant, night shift: chiller C7 is losing coolant again and line 2 is stopped. Need someone now.', '2026-09-01 02:10:00'),
  (2, 'Hello, at the Bologna depot the E5 packer makes a light rattling noise. No hurry, it still works fine.', '2026-09-01 08:30:00'),
  (3, 'TORINO - the light curtain on press A12 did not react twice this morning. Operators are worried.', '2026-09-01 09:05:00'),
  (4, 'Napoli depot: conveyor B3 belt drifts to the left after every restart, we keep adjusting it by hand.', '2026-09-01 11:40:00');

-- 2. Semantic view for Cortex Analyst --------------------------------------------------------------------
CREATE OR REPLACE SEMANTIC VIEW TICKETS_ANALYTICS
  TABLES (
    TICKETS AS SNOWFLAKE_LANGFLOW.EXAMPLES.SERVICE_TICKETS PRIMARY KEY (TICKET_ID)
      WITH SYNONYMS = ('tickets', 'service tickets', 'work orders') COMMENT = 'One row per field service ticket'
  )
  FACTS (
    TICKETS.SENTIMENT AS SENTIMENT_SCORE COMMENT = 'Sentiment of the ticket note, -1 very negative to 1 very positive'
  )
  DIMENSIONS (
    TICKETS.TICKET_ID AS TICKET_ID WITH SYNONYMS = ('ticket number', 'ticket code') COMMENT = 'Ticket identifier',
    TICKETS.SITE AS SITE WITH SYNONYMS = ('plant', 'location', 'depot') COMMENT = 'Site that raised the ticket: MILANO_PLANT, TORINO_PLANT, BOLOGNA_DEPOT, NAPOLI_DEPOT',
    TICKETS.ASSET AS ASSET WITH SYNONYMS = ('machine', 'equipment') COMMENT = 'Asset the ticket refers to: PRESS_A12, CONVEYOR_B3, CHILLER_C7, ROBOT_ARM_D2, PACKER_E5',
    TICKETS.SEVERITY AS SEVERITY WITH SYNONYMS = ('priority', 'criticality') COMMENT = 'Severity: low, medium, high or critical',
    TICKETS.STATUS AS STATUS WITH SYNONYMS = ('state') COMMENT = 'Status: open, in_progress or closed',
    TICKETS.SOURCE AS SOURCE WITH SYNONYMS = ('origin', 'writer') COMMENT = 'System that wrote the row',
    TICKETS.CREATED_DATE AS TO_DATE(CREATED_AT) WITH SYNONYMS = ('date', 'day', 'creation date') COMMENT = 'Date the ticket was created'
  )
  METRICS (
    TICKETS.TICKET_COUNT AS COUNT(tickets.ticket_id) WITH SYNONYMS = ('number of tickets', 'how many tickets', 'volume') COMMENT = 'Number of tickets',
    TICKETS.AVG_SENTIMENT AS AVG(tickets.sentiment) WITH SYNONYMS = ('average sentiment', 'mean sentiment') COMMENT = 'Average sentiment score'
  )
  COMMENT = 'Field service tickets of the maintenance examples';

-- 3. Cortex Search on the procedures ---------------------------------------------------------------------
-- The procedures never change, so the long TARGET_LAG keeps the indexing cost close to zero.
CREATE OR REPLACE CORTEX SEARCH SERVICE DOC_SEARCH
  ON CHUNK
  ATTRIBUTES DOC_ID, TITLE, SOURCE
  WAREHOUSE = <WAREHOUSE>
  TARGET_LAG = '7 days'
  EMBEDDING_MODEL = 'snowflake-arctic-embed-m-v1.5'
  AS SELECT DOC_ID, TITLE, CHUNK, SOURCE FROM SNOWFLAKE_LANGFLOW.EXAMPLES.SERVICE_DOCS;

-- 4. Cortex Agent ---------------------------------------------------------------------------------------
-- The same agent you could build in Snowsight (AI & ML > Agents) with these two tools.
CREATE OR REPLACE AGENT MAINTENANCE_ADVISOR
  COMMENT = 'Advises field technicians: procedures from DOC_SEARCH, ticket figures from TICKETS_ANALYTICS'
  FROM SPECIFICATION
$$
{
  "models": {
    "orchestration": "auto"
  },
  "orchestration": {
    "budget": {
      "seconds": 90,
      "tokens": 24000
    }
  },
  "instructions": {
    "system": "You are the Maintenance Advisor of a manufacturing company. You advise field technicians on a reported fault.",
    "orchestration": "For every fault report call maintenance_procedures first and base the advice on the procedure you find. Call ticket_history only when the report names a site or an asset, to say how many similar tickets exist. Never answer from general knowledge when a procedure exists. When the message already contains notes from other assistants, check them against your tools and keep only what is supported.",
    "response": "Answer in the language of the report, in at most 6 short lines: severity (low, medium, high or critical), likely cause, the steps to take in order, and the title of the procedure you used. If no procedure covers the fault, say so."
  },
  "tools": [
    {
      "tool_spec": {
        "type": "cortex_search",
        "name": "maintenance_procedures",
        "description": "Maintenance procedures and runbooks for plant assets: presses, conveyors, chillers, robots, packers, light curtains, cold rooms."
      }
    },
    {
      "tool_spec": {
        "type": "cortex_analyst_text_to_sql",
        "name": "ticket_history",
        "description": "Field service tickets by site, asset, severity and status."
      }
    }
  ],
  "tool_resources": {
    "maintenance_procedures": {
      "search_service": "SNOWFLAKE_LANGFLOW.EXAMPLES.DOC_SEARCH",
      "id_column": "DOC_ID",
      "title_column": "TITLE",
      "max_results": 4
    },
    "ticket_history": {
      "execution_environment": {
        "type": "warehouse",
        "warehouse": "<WAREHOUSE>"
      },
      "semantic_view": "SNOWFLAKE_LANGFLOW.EXAMPLES.TICKETS_ANALYTICS"
    }
  }
}
$$;

-- 5. Function and procedure for example 3 ----------------------------------------------------------------
-- Turns a site written by hand ("Milan plant", "TORINO", "Napoli depot") into the site code used in the tickets.
CREATE OR REPLACE FUNCTION NORMALIZE_SITE(SITE_TEXT VARCHAR)
RETURNS VARCHAR
COMMENT = 'Maps a free-text site name to the site code of SERVICE_TICKETS'
AS
$$
  CASE
    WHEN SITE_TEXT ILIKE '%milan%'   THEN 'MILANO_PLANT'
    WHEN SITE_TEXT ILIKE ANY ('%torino%', '%turin%') THEN 'TORINO_PLANT'
    WHEN SITE_TEXT ILIKE '%bologna%' THEN 'BOLOGNA_DEPOT'
    WHEN SITE_TEXT ILIKE ANY ('%napoli%', '%naples%') THEN 'NAPOLI_DEPOT'
    ELSE 'UNKNOWN_SITE'
  END
$$;

-- Writes one ticket from one inbox message and marks the message as processed.
-- Each argument is the text output of a node, passed as it is:
--   INBOX_ROW  SQL Statement "next message"     contains INBOX_ID and RAW_TEXT
--   EXTRACTED  AI Function AI_EXTRACT           contains site, asset, problem
--   DECISION   Decision, output Summary         contains the chosen severity
CREATE OR REPLACE PROCEDURE LOAD_TICKET(INBOX_ROW VARCHAR, EXTRACTED VARCHAR, DECISION VARCHAR)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
COMMENT = 'Example 3: stores the enriched ticket and marks the inbox message as processed'
EXECUTE AS OWNER
AS
$$
  const T = 'SNOWFLAKE_LANGFLOW.EXAMPLES.';
  const run = (sqlText, binds) => snowflake.execute({ sqlText: sqlText, binds: binds || [] });
  // The nodes send text: take the first JSON object found in it.
  const firstJson = (text) => {
    const s = String(text || '');
    const start = s.indexOf('{');
    if (start < 0) return {};
    let depth = 0;
    for (let i = start; i < s.length; i++) {
      if (s[i] === '{') depth++;
      if (s[i] === '}') {
        depth--;
        if (depth === 0) {
          try { return JSON.parse(s.slice(start, i + 1)); } catch (err) { return {}; }
        }
      }
    }
    return {};
  };
  // The values sit at different depths (SQL Statement: rows[0].JSON_ROW as JSON text, AI_EXTRACT: result.response):
  // look for the key at any depth, opening JSON text on the way.
  const find = (obj, key) => {
    if (typeof obj === 'string') {
      if (!/^\s*[\[{]/.test(obj)) return undefined;
      try { obj = JSON.parse(obj); } catch (err) { return undefined; }
    }
    if (obj === null || typeof obj !== 'object') return undefined;
    for (const k of Object.keys(obj)) {
      const simple = obj[k] !== null ? typeof obj[k] !== 'object' : false;
      if (simple ? k.toLowerCase() === key : false) return String(obj[k]);
    }
    for (const k of Object.keys(obj)) {
      const found = find(obj[k], key);
      if (found !== undefined) return found;
    }
    return undefined;
  };

  const inbox = firstJson(INBOX_ROW);
  const inboxId = Number(find(inbox, 'inbox_id'));
  if (!(inboxId >= 1)) return { loaded: false, reason: 'no unprocessed message in TICKET_INBOX' };
  const rawText = find(inbox, 'raw_text') || '';

  // AI_EXTRACT answers "None" when it finds nothing: treat it as missing.
  const extracted = firstJson(EXTRACTED);
  const answer = (key) => {
    const value = String(find(extracted, key) || '').trim();
    return /^(none|null|n\/a|unknown)?$/i.test(value) ? '' : value;
  };
  const siteText = answer('site') || rawText;
  const asset = (answer('asset') || 'UNKNOWN_ASSET').toUpperCase().replace(/[^A-Z0-9]+/g, '_').replace(/^_|_$/g, '');
  const problem = answer('problem') || rawText;

  const match = String(DECISION || '').toLowerCase().match(/\b(critical|high|medium|low)\b/);
  const severity = match ? match[1] : 'medium';

  const done = run('SELECT TICKET_ID FROM ' + T + 'TICKET_INBOX WHERE INBOX_ID = ? AND PROCESSED_AT IS NOT NULL', [inboxId]);
  if (done.next()) return { loaded: false, reason: 'message already processed', inbox_id: inboxId, ticket_id: done.getColumnValue(1) };

  const ticketId = 'T-IN-' + String(inboxId).padStart(4, '0');
  run('INSERT INTO ' + T + 'SERVICE_TICKETS (TICKET_ID, SITE, ASSET, SEVERITY, STATUS, NOTE, SOURCE) '
    + 'SELECT ?, ' + T + "NORMALIZE_SITE(?), ?, ?, 'open', ?, 'ticket_inbox_flow'",
    [ticketId, siteText, asset, severity, problem.slice(0, 500)]);
  run('UPDATE ' + T + 'TICKET_INBOX SET PROCESSED_AT = CURRENT_TIMESTAMP(), TICKET_ID = ? WHERE INBOX_ID = ?', [ticketId, inboxId]);
  const site = run('SELECT SITE FROM ' + T + 'SERVICE_TICKETS WHERE TICKET_ID = ?', [ticketId]);
  site.next();
  return { loaded: true, inbox_id: inboxId, ticket_id: ticketId, site: site.getColumnValue(1), asset: asset, severity: severity };
$$;

-- 6. Example 4: a Cortex Agent that hands a long job to a workflow ---------------------------------------
-- The agent cannot wait minutes inside one tool call. It gets two quick tools instead:
--   START_SITE_REVIEW  writes a request row and returns its id          (seconds)
--   GET_SITE_REVIEW    waits up to ~50 s and returns RUNNING or the plan (called again while RUNNING)
-- The flow starts when the request row appears (Table Change Trigger) and ends by calling COMPLETE_SITE_REVIEW.
CREATE TABLE IF NOT EXISTS REVIEW_RUNS (
  RUN_ID       VARCHAR,
  SITE         VARCHAR,
  STATUS       VARCHAR,   -- PENDING, RUNNING, DONE, FAILED
  RESULT       VARCHAR,
  REQUESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  STARTED_AT   TIMESTAMP_NTZ,
  COMPLETED_AT TIMESTAMP_NTZ
);
DELETE FROM REVIEW_RUNS;

-- Tool 1 of the agent.
CREATE OR REPLACE PROCEDURE START_SITE_REVIEW(SITE VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Example 4: queues a site review for the workflow and returns its id'
EXECUTE AS OWNER
AS
$$
DECLARE
  site_code VARCHAR;
  new_id VARCHAR;
BEGIN
  site_code := (SELECT SNOWFLAKE_LANGFLOW.EXAMPLES.NORMALIZE_SITE(:SITE));
  IF (site_code = 'UNKNOWN_SITE') THEN
    RETURN '{"status": "FAILED", "error": "Unknown site. Valid sites: MILANO_PLANT, TORINO_PLANT, BOLOGNA_DEPOT, NAPOLI_DEPOT"}';
  END IF;
  new_id := 'REV-' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISSFF3');
  INSERT INTO SNOWFLAKE_LANGFLOW.EXAMPLES.REVIEW_RUNS (RUN_ID, SITE, STATUS) VALUES (:new_id, :site_code, 'PENDING');
  RETURN OBJECT_CONSTRUCT('status', 'RUNNING', 'review_run_id', new_id, 'site', site_code,
                          'next_step', 'Call get_site_review with this review_run_id')::VARCHAR;
END;
$$;

-- Tool 2 of the agent. A review still unfinished after 15 minutes is reported as FAILED.
CREATE OR REPLACE PROCEDURE GET_SITE_REVIEW(REVIEW_RUN_ID VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Example 4: waits up to about 50 seconds for a site review and returns its status or its plan'
EXECUTE AS OWNER
AS
$$
DECLARE
  st VARCHAR;
  res VARCHAR;
  age NUMBER;
  i INTEGER DEFAULT 0;
BEGIN
  WHILE (i < 10) DO
    SELECT MAX(STATUS), MAX(RESULT), MAX(DATEDIFF('second', REQUESTED_AT, CURRENT_TIMESTAMP()))
      INTO :st, :res, :age
      FROM SNOWFLAKE_LANGFLOW.EXAMPLES.REVIEW_RUNS WHERE RUN_ID = TRIM(:REVIEW_RUN_ID);
    IF (st IS NULL) THEN
      RETURN '{"status": "NOT_FOUND", "error": "No review with this id"}';
    ELSEIF (st = 'DONE') THEN
      RETURN OBJECT_CONSTRUCT('status', 'DONE', 'review_run_id', TRIM(REVIEW_RUN_ID), 'plan', res)::VARCHAR;
    ELSEIF (st = 'FAILED') THEN
      RETURN OBJECT_CONSTRUCT('status', 'FAILED', 'error', res)::VARCHAR;
    ELSEIF (age > 900) THEN
      RETURN '{"status": "FAILED", "error": "The workflow did not finish within 15 minutes"}';
    END IF;
    CALL SYSTEM$WAIT(5);
    i := i + 1;
  END WHILE;
  RETURN OBJECT_CONSTRUCT('status', 'RUNNING', 'review_run_id', TRIM(REVIEW_RUN_ID),
                          'next_step', 'Not finished yet: call get_site_review again with the same review_run_id')::VARCHAR;
END;
$$;

-- First step of the flow: takes the oldest waiting request and marks it RUNNING.
CREATE OR REPLACE PROCEDURE CLAIM_SITE_REVIEW()
RETURNS VARIANT
LANGUAGE SQL
COMMENT = 'Example 4: the flow takes the oldest pending site review'
EXECUTE AS OWNER
AS
$$
DECLARE
  claimed_id VARCHAR;
  claimed_site VARCHAR;
BEGIN
  SELECT MAX(RUN_ID) INTO :claimed_id FROM (
    SELECT RUN_ID FROM SNOWFLAKE_LANGFLOW.EXAMPLES.REVIEW_RUNS WHERE STATUS = 'PENDING' ORDER BY REQUESTED_AT LIMIT 1);
  IF (claimed_id IS NULL) THEN
    RETURN OBJECT_CONSTRUCT('claimed', FALSE, 'reason', 'no pending site review');
  END IF;
  UPDATE SNOWFLAKE_LANGFLOW.EXAMPLES.REVIEW_RUNS SET STATUS = 'RUNNING', STARTED_AT = CURRENT_TIMESTAMP()
   WHERE RUN_ID = :claimed_id;
  SELECT MAX(SITE) INTO :claimed_site FROM SNOWFLAKE_LANGFLOW.EXAMPLES.REVIEW_RUNS WHERE RUN_ID = :claimed_id;
  RETURN OBJECT_CONSTRUCT('claimed', TRUE, 'review_run_id', claimed_id, 'site', claimed_site);
END;
$$;

-- Last step of the flow: stores the plan. CLAIMED is the text output of the first step (it contains the id).
CREATE OR REPLACE PROCEDURE COMPLETE_SITE_REVIEW(CLAIMED VARCHAR, PLAN VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
COMMENT = 'Example 4: the flow stores the finished plan of a site review'
EXECUTE AS OWNER
AS
$$
DECLARE
  done_id VARCHAR;
  n INTEGER;
BEGIN
  done_id := REGEXP_SUBSTR(:CLAIMED, 'REV-[0-9]+');
  IF (done_id IS NULL) THEN
    RETURN OBJECT_CONSTRUCT('stored', FALSE, 'reason', 'no site review was claimed in this run');
  END IF;
  UPDATE SNOWFLAKE_LANGFLOW.EXAMPLES.REVIEW_RUNS
     SET STATUS = IFF(TRIM(COALESCE(:PLAN, '')) = '', 'FAILED', 'DONE'),
         RESULT = IFF(TRIM(COALESCE(:PLAN, '')) = '', 'The workflow produced no plan', :PLAN),
         COMPLETED_AT = CURRENT_TIMESTAMP()
   WHERE RUN_ID = :done_id;
  n := SQLROWCOUNT;
  RETURN OBJECT_CONSTRUCT('stored', n = 1, 'review_run_id', done_id);
END;
$$;

CREATE OR REPLACE AGENT MAINTENANCE_PLANNER
  COMMENT = 'Example 4: delivers a site maintenance plan prepared by a Langflow workflow (start + get tools)'
  FROM SPECIFICATION
$$
{
  "models": {
    "orchestration": "auto"
  },
  "orchestration": {
    "budget": {
      "seconds": 600,
      "tokens": 60000
    }
  },
  "instructions": {
    "system": "You are the Maintenance Planner of a manufacturing company. You deliver the weekly maintenance plan of a site.",
    "orchestration": "A site plan is prepared by a separate workflow that takes a few minutes. Call start_site_review once with the site, then call get_site_review with the review_run_id it returns. While get_site_review answers RUNNING, call get_site_review again with the same review_run_id; do not call start_site_review a second time. When it answers DONE, present the plan. If it answers FAILED or NOT_FOUND, say so and stop. Never write a plan yourself.",
    "response": "Present the plan as returned by the workflow, in the language of the request, and add the review run id at the end."
  },
  "tools": [
    {
      "tool_spec": {
        "type": "generic",
        "name": "start_site_review",
        "description": "Starts the maintenance review workflow for one site and returns a review_run_id within seconds. It does NOT return the plan. Valid sites: MILANO_PLANT, TORINO_PLANT, BOLOGNA_DEPOT, NAPOLI_DEPOT.",
        "input_schema": {
          "type": "object",
          "properties": {
            "site": {
              "type": "string",
              "description": "Site name or code, for example MILANO_PLANT or Milan"
            }
          },
          "required": [
            "site"
          ]
        }
      }
    },
    {
      "tool_spec": {
        "type": "generic",
        "name": "get_site_review",
        "description": "Waits up to about 50 seconds for the review started by start_site_review and returns its status: RUNNING (call this tool again with the same id), DONE (with the plan), FAILED or NOT_FOUND.",
        "input_schema": {
          "type": "object",
          "properties": {
            "review_run_id": {
              "type": "string",
              "description": "The review_run_id returned by start_site_review, for example REV-20260901101500123"
            }
          },
          "required": [
            "review_run_id"
          ]
        }
      }
    }
  ],
  "tool_resources": {
    "start_site_review": {
      "type": "procedure",
      "identifier": "SNOWFLAKE_LANGFLOW.EXAMPLES.START_SITE_REVIEW",
      "execution_environment": {
        "type": "warehouse",
        "warehouse": "<WAREHOUSE>",
        "query_timeout": 60
      }
    },
    "get_site_review": {
      "type": "procedure",
      "identifier": "SNOWFLAKE_LANGFLOW.EXAMPLES.GET_SITE_REVIEW",
      "execution_environment": {
        "type": "warehouse",
        "warehouse": "<WAREHOUSE>",
        "query_timeout": 120
      }
    }
  }
}
$$;

-- 7. Access for the role you select in Langflow ------------------------------------------------------------
-- Skip this block when that role owns the objects. Otherwise replace <LANGFLOW_ROLE> and remove the comment marks.
-- GRANT USAGE ON SCHEMA SNOWFLAKE_LANGFLOW.EXAMPLES TO ROLE <LANGFLOW_ROLE>;
-- GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA SNOWFLAKE_LANGFLOW.EXAMPLES TO ROLE <LANGFLOW_ROLE>;
-- GRANT SELECT ON SEMANTIC VIEW SNOWFLAKE_LANGFLOW.EXAMPLES.TICKETS_ANALYTICS TO ROLE <LANGFLOW_ROLE>;
-- GRANT USAGE ON CORTEX SEARCH SERVICE SNOWFLAKE_LANGFLOW.EXAMPLES.DOC_SEARCH TO ROLE <LANGFLOW_ROLE>;
-- GRANT USAGE ON AGENT SNOWFLAKE_LANGFLOW.EXAMPLES.MAINTENANCE_ADVISOR TO ROLE <LANGFLOW_ROLE>;
-- GRANT USAGE ON FUNCTION SNOWFLAKE_LANGFLOW.EXAMPLES.NORMALIZE_SITE(VARCHAR) TO ROLE <LANGFLOW_ROLE>;
-- GRANT USAGE ON PROCEDURE SNOWFLAKE_LANGFLOW.EXAMPLES.LOAD_TICKET(VARCHAR, VARCHAR, VARCHAR) TO ROLE <LANGFLOW_ROLE>;
-- GRANT USAGE ON AGENT SNOWFLAKE_LANGFLOW.EXAMPLES.MAINTENANCE_PLANNER TO ROLE <LANGFLOW_ROLE>;
-- GRANT USAGE ON PROCEDURE SNOWFLAKE_LANGFLOW.EXAMPLES.START_SITE_REVIEW(VARCHAR) TO ROLE <LANGFLOW_ROLE>;
-- GRANT USAGE ON PROCEDURE SNOWFLAKE_LANGFLOW.EXAMPLES.GET_SITE_REVIEW(VARCHAR) TO ROLE <LANGFLOW_ROLE>;
-- GRANT USAGE ON PROCEDURE SNOWFLAKE_LANGFLOW.EXAMPLES.CLAIM_SITE_REVIEW() TO ROLE <LANGFLOW_ROLE>;
-- GRANT USAGE ON PROCEDURE SNOWFLAKE_LANGFLOW.EXAMPLES.COMPLETE_SITE_REVIEW(VARCHAR, VARCHAR) TO ROLE <LANGFLOW_ROLE>;
-- GRANT CREATE STREAM, CREATE TASK ON SCHEMA SNOWFLAKE_LANGFLOW.EXAMPLES TO ROLE <LANGFLOW_ROLE>;  -- triggers of examples 3 and 4

-- 8. Check ---------------------------------------------------------------------------------------------
SELECT (SELECT COUNT(*) FROM SERVICE_TICKETS) AS TICKETS,
       (SELECT COUNT(*) FROM SERVICE_DOCS) AS PROCEDURES,
       (SELECT COUNT(*) FROM TICKET_INBOX WHERE PROCESSED_AT IS NULL) AS INBOX_WAITING;
-- Expected: 30, 8, 4
