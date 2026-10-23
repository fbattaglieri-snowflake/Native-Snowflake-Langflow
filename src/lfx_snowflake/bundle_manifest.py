"""Snowflake Bundle Manifest, UI Branding Contract, and Invariant Category Registry."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

UPSTREAM_BASELINE_TAG = "v1.12.5"
BUNDLE_ID = "snowflake"
BUNDLE_PACKAGE = "lfx_snowflake"
BUNDLE_VERSION = "0.1.0"

# Official Snowflake branding rules (REQ-01, W2-R2-14)
SNOWFLAKE_BRANDING_CONTRACT: dict[str, Any] = {
    "ui_style_policy": "PRESERVE_LANGFLOW_NATIVE_COMPONENTS_TYPOGRAPHY_SPACING_THEMES",
    "official_snowflake_blue": "#29B5E8",
    "topbar_logo": {
        "placement": ["home_topbar_top_left", "flow_editor_topbar_top_left"],
        "variant": "official_snowflake_black_unmodified",
        "recolor_allowed": False,
        "preserve_aspect_ratio": True,
        "asset_id": "snowflake-logo-black-official",
    },
    "removed_toolbar_links": ["GitHub", "Discord"],
    "removed_profile_menu_links": ["GitHub", "Discord", "X", "Twitter"],
    "profile_menu_additions": [
        {
            "id": "snowflake_documentation",
            "label": "Snowflake Documentation",
            "url": "https://docs.snowflake.com/",
            "icon_variant": "official_snowflake_color",
            "coexists_with_langflow_docs": True,
        }
    ],
    "role_selector": {
        "placement": ["home_topbar", "flow_editor_topbar"],
        "component_style": "langflow_native_radix_select",
        "inspiration": "snowsight_role_switcher",
        "shows_active_role": True,
        "shows_context_epoch": True,
    },
}

# Explicit 4-code error / denial taxonomy (ADR-004, REQ-02)
ERROR_TAXONOMY_CONTRACT: dict[str, dict[str, Any]] = {
    "AUTHZ_ROLE_DENIED": {
        "http_status": 403,
        "banner_variant": "destructive",
        "it_message_template": "Il ruolo attivo ({active_role}) non consente questa operazione",
        "disclose_object_existence": False,
        "audit_required": True,
    },
    "OBJECT_NOT_FOUND_OR_UNAUTHORIZED": {
        "http_status": 403,
        "banner_variant": "warning",
        "it_message_template": (
            "Oggetto non trovato o non autorizzato per il ruolo attivo ({active_role}): "
            "does not exist or not authorized"
        ),
        "disclose_object_existence": False,
        "audit_required": True,
    },
    "UNSUPPORTED_CAPABILITY": {
        "http_status": 422,
        "banner_variant": "info",
        "it_message_template": "Combinazione di capability non supportata o non qualificata: {detail}",
        "disclose_object_existence": False,
        "audit_required": False,
    },
    "TECHNICAL_EXECUTION_ERROR": {
        "http_status": 502,
        "banner_variant": "error",
        "it_message_template": "Errore tecnico di esecuzione o comunicazione upstream: {detail}",
        "disclose_object_existence": False,
        "audit_required": False,
    },
}

# 100% Invariant Categories and Components (REQ-11, REQ-09, REQ-12, REQ-13, G-A6)
INVARIANT_CATEGORIES_MANIFEST: dict[str, Any] = {
    "categories": {
        "Input & Output": {
            "invariant": True,
            "components": ["Chat Input", "Chat Output", "Text Input", "Text Output", "Webhook"],
            "visual_or_functional_changes_allowed": False,
        },
        "Prompt Template": {
            "invariant": True,
            "components": ["Prompt", "Prompt Template"],
            "visual_or_functional_changes_allowed": False,
        },
        "Processing": {
            "invariant": True,
            "components": [
                "Data Operations",
                "Dynamic Create Data",
                "Parser",
                "Split Text",
                "Type Convert",
            ],
            "visual_or_functional_changes_allowed": False,
        },
        "Flow Control": {
            "invariant": True,
            "components": [
                {"name": "Human Input", "beta_badge": False},
                {"name": "If-Else", "beta_badge": False},
                {"name": "Listen", "beta_badge": True},
                {"name": "Loop", "beta_badge": False},
                {"name": "Notify", "beta_badge": True},
                {"name": "Run Flow", "beta_badge": True},
            ],
            "preserve_beta_badges": ["Listen", "Notify", "Run Flow"],
            "visual_or_functional_changes_allowed": False,
        },
    },
    "standalone_invariant_components": {
        "LLM Selector": {
            "category": "LLM Operations",
            "invariant": True,
            "visual_or_functional_changes_allowed": False,
        },
        "Calculator": {
            "category": "Utilities",
            "invariant": True,
            "visual_or_functional_changes_allowed": False,
        },
        "Current Date": {
            "category": "Utilities",
            "invariant": True,
            "visual_or_functional_changes_allowed": False,
        },
    },
    "invariant_settings_tabs": [
        "General",
        "MCP Servers",
        "Langflow API Keys",
        "Langflow MCP Client",
        "Shortcuts",
    ],
    "discover_more_components_policy": {
        "modified_bundles": ["ToolGuard / Policies (ALTK)"],
        "other_bundles_invariant": True,
        "preserve_beta_badge_on_policies": True,
    },
}

# Settings extension contract (REQ-04)
SETTINGS_CONTRACT: dict[str, Any] = {
    "Model Providers": {
        "providers": [
            {
                "id": "snowflake_proxy_responses",
                "display_name": "Snowflake Proxy - Responses",
                "protocol": "responses",
                "sdk_base_path": "/responses/v1",
                "inference_path": "/responses/v1/responses",
                "models_discovery_path": "/responses/v1/models",
                "icon": "snowflake-official",
                "required_options": {
                    "use_responses_api": True,
                    "store": False,
                    "include": ["reasoning.encrypted_content"],
                    "response_stream": "ordered-items",
                },
            },
            {
                "id": "snowflake_proxy_messages",
                "display_name": "Snowflake Proxy - Messages",
                "protocol": "messages",
                "sdk_base_path": "/messages",
                "inference_path": "/messages/v1/messages",
                "models_discovery_path": "/messages/v1/models",
                "icon": "snowflake-official",
                "required_options": {
                    "preserve_thinking_signature": True,
                    "preserve_redacted_thinking": True,
                },
            },
            {
                "id": "snowflake_proxy_chat",
                "display_name": "Snowflake Proxy - Chat",
                "protocol": "chat",
                "sdk_base_path": "/chat/v1",
                "inference_path": "/chat/v1/chat/completions",
                "models_discovery_path": "/chat/v1/models",
                "icon": "snowflake-official",
                "required_options": {
                    "stream_include_usage": True,
                    "deduplicate_tool_call_ids": True,
                },
            },
            {
                "id": "snowflake_ollama_hosted",
                "display_name": "Ollama (Snowflake Hosted)",
                "protocol": "ollama",
                "sdk_base_path": "<OLLAMA_BASE_URL>",
                "inference_path": "/api/chat",
                "models_discovery_path": "/api/tags",
                "icon": "ollama-snowflake",
                "required_options": {
                    "separate_generative_and_embedding_catalogs": True,
                },
            },
        ],
        "preserve_existing_providers": True,
    },
    "DB Providers": {
        "backends": [
            {
                "id": "snowflake_standard_vector",
                "display_name": "Snowflake Standard Table (VECTOR)",
                "max_dimension": 4096,
                "supports_create_from_ui": True,
                "required_create_privilege": "CREATE TABLE ON SCHEMA",
            },
            {
                "id": "snowflake_hybrid_vector",
                "display_name": "Snowflake Hybrid Table (VECTOR)",
                "max_dimension": 4096,
                "supports_create_from_ui": True,
                "required_create_privilege": "CREATE HYBRID TABLE ON SCHEMA",
                "constraints": {
                    "requires_primary_key": True,
                    "vector_cannot_be_pk_or_secondary_index": True,
                },
            },
            {
                "id": "snowflake_cortex_search",
                "display_name": "Snowflake Cortex Search",
                "execution_rights": "owners_rights",
                "requires_explicit_isolation_filters": True,
                "modes": ["managed_embeddings", "precomputed_vectors"],
                "supports_create_from_ui": True,
                "required_create_privilege": "CREATE CORTEX SEARCH SERVICE ON SCHEMA",
            },
            {
                "id": "snowflake_postgres_pgvector",
                "display_name": "Snowflake Postgres (pgvector)",
                "port": 5432,
                "sslmode": "verify-ca",
                "connect_timeout_sec": 10,
                "separate_postgres_auth": True,
                "supports_create_from_ui": True,
            },
        ]
    },
    "Messages": {
        "supported_protocols": ["chat", "responses", "messages"],
        "actions": ["inspect", "edit", "delete", "branch_on_signed_block_mutation"],
        "signed_blocks": ["thinking.signature", "redacted_thinking", "reasoning.encrypted_content"],
    },
    "Global Variables": {
        "system_variables": [
            {
                "name": "SNOWFLAKE_ACTIVE_ROLE",
                "read_only": True,
                "authoritative_source": "backend_session_context",
                "can_override_backend_authz": False,
            },
            {
                "name": "SNOWFLAKE_DEFAULT_WAREHOUSE",
                "read_only": False,
                "role_filtered": True,
                "can_override_backend_authz": False,
            },
            {
                "name": "SNOWFLAKE_DEFAULT_DATABASE",
                "read_only": False,
                "role_filtered": True,
                "can_override_backend_authz": False,
            },
            {
                "name": "SNOWFLAKE_DEFAULT_SCHEMA",
                "read_only": False,
                "role_filtered": True,
                "can_override_backend_authz": False,
            },
        ]
    },
}

# New Flow Import Modes (REQ-03)
NEW_FLOW_IMPORT_MODES: dict[str, dict[str, Any]] = {
    "upload_from_computer": {
        "id": "upload_from_computer",
        "label": "Upload from computer",
        "validates_json_schema": True,
        "redacts_embedded_secrets": True,
        "executes_custom_code_on_import": False,
    },
    "add_from_stage": {
        "id": "add_from_stage",
        "label": "Add from stage",
        "requires_stage_read_privilege": True,
        "stage_uri_pattern": "@<DATABASE>.<SCHEMA>.<STAGE_NAME>/<subpath>",
        "validates_subpath_traversal": True,
        "redacts_embedded_secrets": True,
        "executes_custom_code_on_import": False,
    },
    "add_from_git": {
        "id": "add_from_git",
        "label": "Add from Git",
        "requires_eai_and_secret_access": True,
        "records_commit_provenance": True,
        "redacts_embedded_secrets": True,
        "executes_custom_code_on_import": False,
    },
}


@dataclass(frozen=True)
class BundleComponentSpec:
    """Declarative specification for a Snowflake bundle component."""

    component_id: str
    display_name: str
    category: str
    release_phase: str
    icon: str = "Snowflake"
    beta: bool = False
    description: str = ""
    required_privileges: tuple[str, ...] = field(default_factory=tuple)


SNOWFLAKE_BUNDLE_COMPONENTS: tuple[BundleComponentSpec, ...] = (
    # Data Sources (6)
    BundleComponentSpec(
        component_id="snowflake_tables",
        display_name="Tables",
        category="Data Sources",
        release_phase="W2-R2",
        description="Role-governed hierarchical Tables component (Standard, Hybrid, Interactive, Dynamic) with input ports and Read/Write Tool Mode.",
        required_privileges=("USAGE ON DATABASE", "USAGE ON SCHEMA", "SELECT/INSERT/UPDATE ON TABLE"),
    ),
    BundleComponentSpec(
        component_id="snowflake_views",
        display_name="Views",
        category="Data Sources",
        release_phase="W2-R2",
        description="Role-governed hierarchical Views component (Standard View, Secure View, Materialized View) with input ports and Read Tool Mode.",
        required_privileges=("USAGE ON DATABASE", "USAGE ON SCHEMA", "SELECT ON VIEW"),
    ),
    BundleComponentSpec(
        component_id="snowflake_iceberg_tables",
        display_name="Iceberg Tables",
        category="Data Sources",
        release_phase="W2-R2",
        description="Role-governed hierarchical Iceberg Tables component (Snowflake-managed & Externally-managed) with input ports and Read/Write Tool Mode.",
        required_privileges=("USAGE ON DATABASE", "USAGE ON SCHEMA", "SELECT ON ICEBERG TABLE"),
    ),
    BundleComponentSpec(
        component_id="snowflake_postgres",
        display_name="Postgres",
        category="Data Sources",
        release_phase="W2-R2",
        description="Role-governed Snowflake Postgres component (Relational, JSONB, pgvector) with input ports and Read/Write Tool Mode.",
        required_privileges=("USAGE ON POSTGRES INSTANCE",),
    ),
    BundleComponentSpec(
        component_id="snowflake_cortex_search",
        display_name="Cortex Search",
        category="Data Sources",
        release_phase="W2-R2",
        description="Query Cortex Search Services in Data Sources with attribute filter JSON, scoring config, and Agent Tool Mode.",
        required_privileges=("USAGE ON CORTEX SEARCH SERVICE",),
    ),
    BundleComponentSpec(
        component_id="snowflake_cortex_analyst",
        display_name="Cortex Analyst",
        category="Data Sources",
        release_phase="W2-R2",
        description="Dedicated Cortex Analyst (/api/v2/cortex/analyst/message) semantic SQL generator, governed read-only SQL executor, and Agent Tool Mode.",
        required_privileges=("USAGE ON SEMANTIC VIEW", "SELECT ON SOURCE TABLES"),
    ),
    # Models & Agents (5)
    BundleComponentSpec(
        component_id="snowflake_standard_chat",
        display_name="Proxy - Chat",
        category="Models & Agents",
        release_phase="W2-R2",
        description="Snowflake Proxy Standard Chat (/chat/v1) with model-aware reasoning_effort support.",
        required_privileges=("USAGE ON SERVICE",),
    ),
    BundleComponentSpec(
        component_id="snowflake_standard_responses",
        display_name="Proxy - Responses",
        category="Models & Agents",
        release_phase="W2-R2",
        description="Snowflake Proxy Standard Responses (/responses/v1) with reasoning effort and encrypted reasoning item continuity.",
        required_privileges=("USAGE ON SERVICE",),
    ),
    BundleComponentSpec(
        component_id="snowflake_standard_messages",
        display_name="Proxy - Messages",
        category="Models & Agents",
        release_phase="W2-R2",
        description="Snowflake Proxy Standard Messages (/messages) with Extended Thinking budget_tokens support.",
        required_privileges=("USAGE ON SERVICE",),
    ),
    BundleComponentSpec(
        component_id="snowflake_private_ollama",
        display_name="Private - Ollama",
        category="Models & Agents",
        release_phase="W2-R2",
        description="Snowflake-hosted Private Ollama provider on SPCS.",
        required_privileges=("USAGE ON SERVICE",),
    ),
    BundleComponentSpec(
        component_id="snowflake_cortex_agent_a2a",
        display_name="Cortex Agent & A2A",
        category="Models & Agents",
        release_phase="W2-R2",
        description="Cascading Database -> Schema -> Cortex Agent & internal Langflow A2A client with thread continuity.",
        required_privileges=("USAGE ON AGENT",),
    ),
    # LLM Operations (8)
    BundleComponentSpec(
        component_id="snowflake_object_provisioner_llm",
        display_name="Object Provisioner",
        category="LLM Operations",
        release_phase="W2-R2",
        description="Governed hierarchical Database -> Schema -> Object provisioner (Table, View, Stage, Snowflake Postgres) in LLM Operations.",
        required_privileges=("CREATE TABLE/STAGE/VIEW ON SCHEMA",),
    ),
    BundleComponentSpec(
        component_id="snowflake_stored_procedure",
        display_name="Stored Procedure",
        category="LLM Operations",
        release_phase="W2-R2",
        description="Execute, create, or chain Snowflake Stored Procedures (SQL/Python) with dynamic code editor and graphical child procedure ports.",
        required_privileges=("USAGE/CREATE PROCEDURE ON SCHEMA",),
    ),
    BundleComponentSpec(
        component_id="snowflake_function_udf",
        display_name="Function (UDF / UDTF)",
        category="LLM Operations",
        release_phase="W2-R2",
        description="Execute or create Snowflake Scalar UDFs and Tabular UDTFs with dynamic code editor and stage imports.",
        required_privileges=("USAGE/CREATE FUNCTION ON SCHEMA",),
    ),
    BundleComponentSpec(
        component_id="snowflake_ai_complete",
        display_name="AI Complete",
        category="LLM Operations",
        release_phase="W2-R2",
        description="Dedicated AI_COMPLETE component with multimodal stage files, model_parameters, and structured JSON schema output.",
        required_privileges=("CORTEX AI_COMPLETE",),
    ),
    BundleComponentSpec(
        component_id="snowflake_ai_function",
        display_name="AI Function",
        category="LLM Operations",
        release_phase="W2-R2",
        description="Task-specific Cortex AI_* SQL component with dynamic per-function input adaptation.",
        required_privileges=("EXPLAIN_PRIVILEGES PRECHECK", "AI_COUNT_TOKENS"),
    ),
    BundleComponentSpec(
        component_id="snowflake_sql_statement",
        display_name="SQL Statement",
        category="LLM Operations",
        release_phase="W2-R2",
        description="Role-governed parameterized SQL Statement executor returning DataFrame or OBJECT_CONSTRUCT JSON.",
        required_privileges=("USAGE ON DATABASE/SCHEMA", "SELECT/DML ON OBJECT"),
    ),
    BundleComponentSpec(
        component_id="snowflake_llm_node",
        display_name="Operations",
        category="LLM Operations",
        release_phase="W2-R2",
        description="Unified context-bound Snowflake Operations node in LLM Operations.",
        required_privileges=("USAGE ON DATABASE/SCHEMA",),
    ),
    BundleComponentSpec(
        component_id="snowflake_decision",
        display_name="Decision",
        category="LLM Operations",
        release_phase="W2-R2",
        description="Structured AI_COMPLETE decision router with capability-filtered model catalog and Cortex SystemOne API node.",
        required_privileges=("CORTEX AI_COMPLETE",),
    ),
    # Files & Knowledge (6)
    BundleComponentSpec(
        component_id="snowflake_object_provisioner_fk",
        display_name="Object Provisioner",
        category="Files & Knowledge",
        release_phase="W2-R2",
        description="Governed hierarchical Database -> Schema -> Object provisioner (Table, View, Stage, Snowflake Postgres) in Files & Knowledge.",
        required_privileges=("CREATE TABLE/STAGE/VIEW ON SCHEMA",),
    ),
    BundleComponentSpec(
        component_id="snowflake_stage_file_system",
        display_name="Stage File System",
        category="Files & Knowledge",
        release_phase="W2-R2",
        description="Role-governed Snowflake Stage browser and governed stage creator (Internal Named Stage and External Stage).",
        required_privileges=("USAGE ON STAGE", "READ/WRITE ON STAGE"),
    ),
    BundleComponentSpec(
        component_id="snowflake_cortex_search_parse_document",
        display_name="Cortex Search 1: Parse Document",
        category="Files & Knowledge",
        release_phase="W2-R2",
        description="Step 1 of Cortex Search indexing pipeline: AI_PARSE_DOCUMENT on Stage with LAYOUT/OCR, page_split, and LIMIT 1 preview.",
        required_privileges=("READ ON STAGE", "CREATE TABLE ON SCHEMA"),
    ),
    BundleComponentSpec(
        component_id="snowflake_cortex_search_split_text",
        display_name="Cortex Search 2: Split Text",
        category="Files & Knowledge",
        release_phase="W2-R2",
        description="Step 2 of Cortex Search indexing pipeline: SPLIT_TEXT_RECURSIVE_CHARACTER / LangChain UDF chunking (<512 tokens) with histogram & preview.",
        required_privileges=("SELECT/INSERT ON TABLE",),
    ),
    BundleComponentSpec(
        component_id="snowflake_cortex_search_service_creator",
        display_name="Cortex Search 3: Search Service",
        category="Files & Knowledge",
        release_phase="W2-R2",
        description="Step 3 of Cortex Search indexing pipeline: CREATE OR REPLACE CORTEX SEARCH SERVICE, status inspection, and Suspend/Resume.",
        required_privileges=("CREATE CORTEX SEARCH SERVICE ON SCHEMA",),
    ),
    BundleComponentSpec(
        component_id="snowflake_cortex_search_stream_task",
        display_name="Cortex Search 4: Stream + Task",
        category="Files & Knowledge",
        release_phase="W2-R2",
        description="Step 4 of Cortex Search indexing pipeline: DOC_STREAM + DOC_INGEST_TASK with atomic _DOC_STREAM_BATCH temp table.",
        required_privileges=("CREATE STREAM ON STAGE", "CREATE TASK ON SCHEMA"),
    ),
    # Utilities (1)
    BundleComponentSpec(
        component_id="snowflake_toolguard_policies",
        display_name="ToolGuard",
        category="Utilities",
        release_phase="W2-R2",
        beta=True,
        description="Snowflake ToolGuard wrapping connected Tools with CURRENT_ROLE() RBAC, Cortex Guard, AI_REDACT PII masking, and Proxy/Ollama LLM policy evaluation.",
        required_privileges=("USAGE ON SERVICE",),
    ),
)


def verify_invariant_category_integrity(category_snapshot: dict[str, Any] | None = None) -> dict[str, Any]:
    """Verify that invariant categories, Beta badges, and standalone components match the contract."""
    manifest = category_snapshot or INVARIANT_CATEGORIES_MANIFEST
    categories = manifest.get("categories", {})
    required_categories = ("Input & Output", "Prompt Template", "Processing", "Flow Control")
    missing_categories = [c for c in required_categories if c not in categories]

    flow_control = categories.get("Flow Control", {})
    beta_components = {
        item["name"]
        for item in flow_control.get("components", [])
        if isinstance(item, dict) and item.get("beta_badge") is True
    }
    expected_beta = {"Listen", "Notify", "Run Flow"}
    missing_beta = sorted(expected_beta - beta_components)

    standalone = manifest.get("standalone_invariant_components", {})
    required_standalone = ("LLM Selector", "Calculator", "Current Date")
    missing_standalone = [c for c in required_standalone if c not in standalone]

    passed = not missing_categories and not missing_beta and not missing_standalone
    return {
        "passed": passed,
        "upstream_baseline": UPSTREAM_BASELINE_TAG,
        "missing_categories": missing_categories,
        "missing_beta_badges": missing_beta,
        "missing_standalone_components": missing_standalone,
        "discover_other_bundles_invariant": bool(
            manifest.get("discover_more_components_policy", {}).get("other_bundles_invariant", False)
        ),
    }


def get_bundle_manifest() -> dict[str, Any]:
    """Return the complete serializable bundle manifest for Langflow loader and UI introspection."""
    return {
        "bundle_id": BUNDLE_ID,
        "bundle_package": BUNDLE_PACKAGE,
        "bundle_version": BUNDLE_VERSION,
        "upstream_baseline_tag": UPSTREAM_BASELINE_TAG,
        "branding": SNOWFLAKE_BRANDING_CONTRACT,
        "error_taxonomy": ERROR_TAXONOMY_CONTRACT,
        "invariant_categories": INVARIANT_CATEGORIES_MANIFEST,
        "settings": SETTINGS_CONTRACT,
        "new_flow_import_modes": NEW_FLOW_IMPORT_MODES,
        "components": [
            {
                "component_id": c.component_id,
                "display_name": c.display_name,
                "category": c.category,
                "release_phase": c.release_phase,
                "icon": c.icon,
                "beta": c.beta,
                "description": c.description,
                "required_privileges": list(c.required_privileges),
            }
            for c in SNOWFLAKE_BUNDLE_COMPONENTS
        ],
    }
