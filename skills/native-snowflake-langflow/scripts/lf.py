#!/usr/bin/env python3
"""Small client for Native Snowflake Langflow: discover nodes, build flows as code, set the Context, run, export.

Standard library only. Use it as a command (`python3 lf.py check`) or as a module (`from lf import Client, Graph`).

Connection, from environment variables (never write these values into files or chat):
  LF_URL          base URL of the Langflow service, e.g. https://<endpoint>.snowflakecomputing.app
  LF_API_KEY      Langflow API key (Langflow UI > Settings > Langflow API Keys)
  LF_SPCS_TOKEN   Snowflake token for the SPCS public endpoint (a programmatic access token); omit when the
                  URL is reachable without Snowflake ingress authentication
  LF_PROJECT_ID   optional: Langflow project (folder) for new flows; default is the first project
"""
from __future__ import annotations

import copy
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))
from flow_canvas_repair import flatten_catalog, repair_flow_canvas  # noqa: E402

OBJECT_KINDS = ("tables", "views", "iceberg_tables", "stages", "semantic_views", "cortex_search_services",
                "cortex_agents", "functions", "procedures")


class LangflowError(RuntimeError):
    pass


class Client:
    def __init__(self, url: str | None = None, api_key: str | None = None, spcs_token: str | None = None):
        self.url = (url or os.environ.get("LF_URL") or "").rstrip("/")
        self.api_key = api_key or os.environ.get("LF_API_KEY") or ""
        self.spcs_token = spcs_token or os.environ.get("LF_SPCS_TOKEN") or ""
        if not self.url or not self.api_key:
            raise LangflowError("Set LF_URL and LF_API_KEY (and LF_SPCS_TOKEN for an SPCS public endpoint).")
        self._catalog: dict[str, Any] | None = None
        self._models: dict[str, Any] | None = None

    # ---------------------------------------------------------------- HTTP
    def _headers(self, accept: str = "application/json") -> dict[str, str]:
        h = {"x-api-key": self.api_key, "Content-Type": "application/json", "Accept": accept}
        if self.spcs_token:
            h["X-SF-SPCS-Authorization"] = f'Snowflake Token="{self.spcs_token}"'
        return h

    def req(self, method: str, path: str, body: Any = None, timeout: int = 300) -> tuple[int, Any]:
        data = json.dumps(body).encode() if body is not None else None
        r = urllib.request.Request(self.url + path, data=data, method=method, headers=self._headers())
        try:
            with urllib.request.urlopen(r, timeout=timeout) as resp:
                status, text = resp.status, resp.read().decode()
        except urllib.error.HTTPError as e:
            status, text = e.code, e.read().decode()
        try:
            return status, json.loads(text)
        except ValueError:
            return status, text

    def ok(self, method: str, path: str, body: Any = None, timeout: int = 300) -> Any:
        status, out = self.req(method, path, body, timeout)
        if status not in (200, 201):
            raise LangflowError(f"{method} {path} -> HTTP {status}: {str(out)[:600]}")
        return out

    # ---------------------------------------------------------------- discovery
    def catalog(self) -> dict[str, Any]:
        """Every node installed on the server, by component type (e.g. 'SnowflakeCortexAgent')."""
        if self._catalog is None:
            self._catalog = flatten_catalog(self.ok("GET", "/api/v1/all"))
        return self._catalog

    def models(self) -> dict[str, Any]:
        """Models served by the LLM proxy (empty when the proxy is not configured)."""
        if self._models is None:
            status, out = self.req("GET", "/api/v1/snowflake/language-models")
            self._models = {m["name"]: m for m in (out.get("models") or [])} if status == 200 and isinstance(out, dict) else {}
        return self._models

    def model_value(self, name: str) -> list[dict[str, Any]]:
        """Value for the `model` field of Language Model and Agent."""
        if name not in self.models():
            raise LangflowError(f"Model '{name}' is not served by the LLM proxy. Available: {sorted(self.models())}")
        m = self.models()[name]
        return [{"name": m["name"], "provider": m["provider"], "icon": "Snowflake", "metadata": m["metadata"]}]

    def describe(self, component: str) -> dict[str, Any]:
        node = self.catalog().get(component)
        if not (isinstance(node, dict) and "template" in node):
            close = [k for k in self.catalog() if component.lower() in k.lower()]
            raise LangflowError(f"No node type '{component}'. Similar: {close[:12]}")
        fields = {}
        for name, f in node["template"].items():
            if not isinstance(f, dict) or name == "code":
                continue
            fields[name] = {"value": f.get("value"), "options": f.get("options"), "accepts": f.get("input_types"),
                            "shown": f.get("show", True), "advanced": f.get("advanced", False),
                            "list": bool(f.get("list")), "info": f.get("info")}
        return {"type": component, "display_name": node.get("display_name"), "description": node.get("description"),
                "outputs": [{"name": o["name"], "types": o["types"], "label": o.get("display_name")} for o in node["outputs"]],
                "fields": fields}

    def projects(self) -> list[dict[str, Any]]:
        out = self.ok("GET", "/api/v1/projects/")
        return out if isinstance(out, list) else []

    def project_id(self) -> str:
        if os.environ.get("LF_PROJECT_ID"):
            return os.environ["LF_PROJECT_ID"]
        projects = self.projects()
        if not projects:
            raise LangflowError("No Langflow project found.")
        return projects[0]["id"]

    # ---------------------------------------------------------------- flows
    def flows(self) -> list[dict[str, Any]]:
        return self.ok("GET", "/api/v1/flows/?get_all=true&header_flows=true")

    def flow_id(self, name_or_id: str) -> str:
        for f in self.flows():
            if name_or_id in (f["id"], f["name"]):
                return f["id"]
        raise LangflowError(f"No flow named or identified by '{name_or_id}'.")

    def get_flow(self, name_or_id: str) -> dict[str, Any]:
        return self.ok("GET", f"/api/v1/flows/{self.flow_id(name_or_id)}")

    def save(self, name: str, graph: "Graph | dict[str, Any]", description: str = "",
             context: dict[str, Any] | None = None) -> str:
        """Create the flow, or replace the flow with the same name. Returns the flow id."""
        data = graph.data() if isinstance(graph, Graph) else graph
        repair_flow_canvas(data, self.catalog())
        body = {"name": name, "description": description, "data": data, "folder_id": self.project_id()}
        existing = {f["name"]: f["id"] for f in self.flows()}
        if name in existing:
            out = self.ok("PATCH", f"/api/v1/flows/{existing[name]}", body)
        else:
            out = self.ok("POST", "/api/v1/flows/", body)
        if context is not None:
            self.set_context(out["id"], context)
        return out["id"]

    def export_flow(self, name_or_id: str, path: str) -> None:
        f = self.get_flow(name_or_id)
        out = {"name": f["name"], "description": f.get("description"), "data": f["data"], "is_component": False}
        Path(path).write_text(json.dumps(out, indent=1) + "\n")

    def import_flow(self, path: str, name: str | None = None) -> str:
        body = json.loads(Path(path).read_text())
        return self.save(name or body["name"], body["data"], body.get("description") or "")

    def delete_flow(self, name_or_id: str) -> None:
        self.ok("DELETE", f"/api/v1/flows/{self.flow_id(name_or_id)}")

    # ---------------------------------------------------------------- role and Context
    def get_context(self, flow: str) -> dict[str, Any]:
        fid = self.flow_id(flow)
        return self.ok("GET", f"/api/v1/snowflake/context?flow_id={fid}")

    def set_context(self, flow: str, context: dict[str, Any]) -> dict[str, Any]:
        """`context` is the output of context_body({...})."""
        fid = self.flow_id(flow)
        return self.ok("POST", f"/api/v1/snowflake/context?flow_id={fid}", {"flow_id": fid, **context})

    def select_role(self, flow: str, role: str) -> str:
        """Role sessions live in the server memory: select the role again after a service restart."""
        fid = self.flow_id(flow)
        out = self.ok("POST", f"/api/v1/snowflake/role?flow_id={fid}", {"target_role": role, "flow_id": fid})
        if out.get("active_role") != role:
            raise LangflowError(f"Role '{role}' was not activated: {str(out)[:300]}")
        return out["active_role"]

    # ---------------------------------------------------------------- run
    def run(self, flow: str, text: str = "", session: str | None = None, timeout: int = 900) -> dict[str, Any]:
        """Run the flow as the canvas does. Returns {"nodes": {name: {"ok", "outputs"}}, "errors": [...], "seconds"}.

        Events are read by polling: a single long streaming response is cut by the Snowflake ingress (about 90 s).
        """
        fid = self.flow_id(flow)
        t0 = time.time()
        job = self.ok("POST", f"/api/v1/build/{fid}/flow",
                      {"inputs": {"input_value": text, "session": session or f"lf-{int(t0)}"}}, timeout=120)
        url = f"{self.url}/api/v1/build/{job['job_id']}/events?event_delivery=polling"
        chunks, ended = [], False
        while not ended and time.time() - t0 < timeout:
            request = urllib.request.Request(url, headers=self._headers("application/x-ndjson"))
            with urllib.request.urlopen(request, timeout=80) as resp:
                body = resp.read().decode()
            if body.strip():
                chunks.append(body)
                ended = '"event": "end"' in body or '"event":"end"' in body
            else:
                time.sleep(2)
        names = {n["id"]: n["data"]["node"].get("display_name") or n["id"]
                 for n in self.ok("GET", f"/api/v1/flows/{fid}")["data"]["nodes"]}
        nodes: dict[str, Any] = {}
        errors: list[str] = []
        for line in "\n".join(chunks).splitlines():
            if not line.strip():
                continue
            event = json.loads(line)
            data = event.get("data") or {}
            if event.get("event") == "end_vertex":
                bd = data.get("build_data") or {}
                outs = {}
                for port, v in ((bd.get("data") or {}).get("outputs") or {}).items():
                    m = v.get("message") if isinstance(v, dict) else v
                    outs[port] = m.get("text") if isinstance(m, dict) and "text" in m else m
                key = names.get(bd.get("id"), bd.get("id"))
                entry = nodes.setdefault(key, {"ok": True, "outputs": {}})
                entry["ok"] = entry["ok"] and bool(bd.get("valid"))
                entry["outputs"] = outs or entry["outputs"]
                if not bd.get("valid"):
                    errors.append(f"{key}: {json.dumps(bd.get('params') or bd.get('data'), default=str)[:900]}")
            elif event.get("event") == "error":
                errors.append(json.dumps(data, default=str)[:900])
        if not ended:
            errors.append(f"No end event within {timeout} s")
        return {"nodes": nodes, "errors": errors, "seconds": round(time.time() - t0, 1)}


def context_body(selection: dict[str, dict[str, list[str]]]) -> dict[str, Any]:
    """Per-flow Context from {"DB.SCHEMA": {"tables": [...], "cortex_agents": [...], ...}} (see OBJECT_KINDS)."""
    dbs: list[str] = []
    schemas: dict[str, list[str]] = {}
    for pair, kinds in selection.items():
        unknown = set(kinds) - set(OBJECT_KINDS)
        if unknown:
            raise LangflowError(f"Unknown object kinds {sorted(unknown)}. Use: {OBJECT_KINDS}")
        db, sch = pair.split(".")
        if db not in dbs:
            dbs.append(db)
        schemas.setdefault(db, []).append(sch)
    first = dbs[0]
    return {"database": first, "schema": schemas[first][0], "databases": dbs, "schemas_by_db": schemas,
            "full_databases": [], "full_schemas": [], "objects_by_db_schema": selection, "selected_objects": selection}


class Graph:
    """Build a flow as code. Nodes are copied from the server catalog, so they carry the installed component."""

    def __init__(self, client: Client, tag: str = "N"):
        self.client, self.tag, self.nodes, self.edges, self.n = client, tag, [], [], 0

    def add(self, component: str, values: dict[str, Any] | None = None, *, name: str | None = None,
            tool_mode: bool = False, x: int = 0, y: int = 0, show: dict[str, bool] | None = None) -> str:
        """Add a node. `values` sets fields by name (see `lf.py node <Type>`). Returns the node id."""
        self.client.describe(component)  # raises with similar names when the type does not exist
        self.n += 1
        nid = f"{component.replace(' ', '')}-{self.tag}{self.n:02d}"
        node = copy.deepcopy(self.client.catalog()[component])
        for key, value in (values or {}).items():
            if key not in node["template"]:
                raise LangflowError(f"{component} has no field '{key}'. Fields: {sorted(k for k in node['template'] if k != 'code')}")
            field = node["template"][key]
            field["value"] = value
            if isinstance(field.get("options"), list) and isinstance(value, str) and value not in field["options"]:
                field["options"] = [*field["options"], value]
        for key, visible in (show or {}).items():
            node["template"][key]["show"] = visible
        if name:
            node["display_name"] = name
        if tool_mode:
            node["tool_mode"] = True
            node["outputs"] = [{"name": "component_as_tool", "display_name": "Toolset", "method": "to_toolkit",
                                "types": ["Tool"], "selected": "Tool", "cache": True, "tool_mode": True,
                                "allows_loop": False, "group_outputs": False, "value": "__UNDEFINED__"}]
        self.nodes.append({"id": nid, "type": "genericNode", "position": {"x": x, "y": y},
                           "data": {"id": nid, "type": component, "node": node}})
        return nid

    def prompt(self, text: str, *, name: str | None = None, x: int = 0, y: int = 0) -> str:
        """Prompt Template. Every {variable} in `text` becomes an input field you can link to."""
        self.n += 1
        nid = f"PromptTemplate-{self.tag}{self.n:02d}"
        out = self.client.ok("POST", "/api/v1/validate/prompt", {
            "name": "template", "template": text, "custom_fields": {},
            "frontend_node": copy.deepcopy(self.client.catalog()["Prompt Template"])})
        node = out["frontend_node"]
        node["template"]["template"]["value"] = text
        if name:
            node["display_name"] = name
        self.nodes.append({"id": nid, "type": "genericNode", "position": {"x": x, "y": y},
                           "data": {"id": nid, "type": "Prompt Template", "node": node}})
        return nid

    def _node(self, nid: str) -> dict[str, Any]:
        return next(n for n in self.nodes if n["id"] == nid)

    def link(self, source: str, output: str, target: str, field: str) -> None:
        """Connect output port `output` of `source` to input `field` of `target`."""
        sn, tn = self._node(source), self._node(target)
        outputs = sn["data"]["node"]["outputs"]
        port = next((o for o in outputs if o["name"] == output), None)
        if port is None:
            raise LangflowError(f"{sn['data']['type']} has no output '{output}'. Outputs: {[o['name'] for o in outputs]}")
        if field not in tn["data"]["node"]["template"]:
            raise LangflowError(f"{tn['data']['type']} has no field '{field}'.")
        f = tn["data"]["node"]["template"][field]
        sh = {"dataType": sn["data"]["type"], "id": source, "name": output, "output_types": port["types"]}
        th = {"fieldName": field, "id": target, "inputTypes": f.get("input_types") or port["types"],
              "type": f.get("type", "other")}
        enc = lambda d: json.dumps(d, separators=(",", ":")).replace('"', "\u0153")  # noqa: E731
        self.edges.append({"source": source, "target": target, "sourceHandle": enc(sh), "targetHandle": enc(th),
                           "data": {"sourceHandle": sh, "targetHandle": th},
                           "id": f"xy-edge__{source}{output}-{target}{field}"})

    def data(self) -> dict[str, Any]:
        return {"nodes": self.nodes, "edges": self.edges, "viewport": {"x": 0, "y": 0, "zoom": 0.5}}


# -------------------------------------------------------------------- command line
def _print(obj: Any) -> None:
    print(json.dumps(obj, indent=2, default=str))


USAGE = """usage: lf.py <command> [arguments]
  check                          connection, version, projects, models of the LLM proxy
  nodes [text]                   node types installed (filter by text); Snowflake nodes start with 'Snowflake'
  node <Type>                    fields, accepted inputs and output ports of one node type
  models                         models served by the LLM proxy
  flows                          flows on the server
  context <flow>                 role, databases, schemas and objects selected for a flow
  set-context <flow> <json>      e.g. '{"DB.SCHEMA": {"tables": ["T1"], "cortex_agents": ["A1"]}}' (or @file.json)
  role <flow> <ROLE>             select the Snowflake role for a flow
  run <flow> [text]              run a flow and print every node output
  export <flow> <file.json>      save a flow to a file
  import <file.json> [name]      create or replace a flow from a file
  delete <flow>                  delete a flow
<flow> is a flow name or id."""


def main(argv: list[str]) -> int:
    if not argv or argv[0] in ("-h", "--help", "help"):
        print(USAGE)
        return 0
    cmd, args = argv[0], argv[1:]
    try:
        c = Client()
        if cmd == "check":
            status, version = c.req("GET", "/api/v1/version")
            _print({"url_reachable": status == 200, "langflow_version": version.get("version") if isinstance(version, dict) else None,
                    "node_types": len(c.catalog()), "snowflake_node_types": sum(k.startswith("Snowflake") for k in c.catalog()),
                    "projects": [{"id": p["id"], "name": p["name"]} for p in c.projects()],
                    "llm_proxy_models": sorted(c.models()) or "none: Language Model and Agent nodes will not work"})
        elif cmd == "nodes":
            text = (args[0] if args else "").lower()
            for key, node in sorted(c.catalog().items()):
                if not (isinstance(node, dict) and "template" in node):
                    continue
                if text in key.lower() or text in str(node.get("display_name", "")).lower():
                    print(f"{key:44} {node.get('display_name')}")
        elif cmd == "node":
            _print(c.describe(args[0]))
        elif cmd == "models":
            _print({name: m.get("metadata", {}).get("capabilities") for name, m in sorted(c.models().items())})
        elif cmd == "flows":
            for f in c.flows():
                print(f"{f['id']}  {f['name']}")
        elif cmd == "context":
            ctx = c.get_context(args[0])
            _print({k: ctx.get(k) for k in ("flow_id", "active_role", "available_roles", "selected_context")})
        elif cmd == "set-context":
            raw = Path(args[1][1:]).read_text() if args[1].startswith("@") else args[1]
            out = c.set_context(args[0], context_body(json.loads(raw)))
            _print({"ok": out.get("ok"), "message": out.get("message")})
        elif cmd == "role":
            print(c.select_role(args[0], args[1]))
        elif cmd == "run":
            out = c.run(args[0], args[1] if len(args) > 1 else "")
            _print(out)
            return 1 if out["errors"] else 0
        elif cmd == "export":
            c.export_flow(args[0], args[1])
            print(f"written {args[1]}")
        elif cmd == "import":
            print(c.import_flow(args[0], args[1] if len(args) > 1 else None))
        elif cmd == "delete":
            c.delete_flow(args[0])
            print("deleted")
        else:
            print(USAGE)
            return 2
    except IndexError:
        print(USAGE)
        return 2
    except LangflowError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
