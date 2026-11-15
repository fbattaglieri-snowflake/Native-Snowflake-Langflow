"""The agent skill shipped in skills/ stays consistent with the product."""
import ast
import pathlib
import re

ROOT = pathlib.Path(__file__).parent.parent
SKILL = ROOT / "skills" / "native-snowflake-langflow"


def test_skill_has_frontmatter_with_name_and_description():
    text = (SKILL / "SKILL.md").read_text()
    front = re.match(r"---\n(.*?)\n---\n", text, re.S)
    assert front, "SKILL.md must start with YAML frontmatter"
    assert re.search(r"^name: native-snowflake-langflow$", front.group(1), re.M)
    assert re.search(r"^description: .{80,}", front.group(1), re.M)


def test_canvas_repair_copy_matches_the_product_module():
    assert (SKILL / "scripts" / "flow_canvas_repair.py").read_text() == (
        ROOT / "src" / "lfx_snowflake" / "flow_canvas_repair.py").read_text()


def test_client_is_standard_library_only_and_holds_no_endpoint():
    src = (SKILL / "scripts" / "lf.py").read_text()
    tree = ast.parse(src)
    roots = {(n.module or "").split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)} | {
        a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    allowed = {"__future__", "copy", "json", "os", "sys", "time", "urllib", "pathlib", "typing", "flow_canvas_repair"}
    assert roots <= allowed, roots - allowed
    assert "snowflakecomputing.app" not in src.replace("<endpoint>.snowflakecomputing.app", "")


def test_every_file_named_in_the_skill_exists():
    text = (SKILL / "SKILL.md").read_text()
    for rel in set(re.findall(r"`((?:scripts|references)/[\w./-]+)`", text)):
        assert (SKILL / rel).exists(), rel
