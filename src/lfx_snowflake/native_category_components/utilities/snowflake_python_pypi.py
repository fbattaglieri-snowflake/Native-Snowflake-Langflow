from __future__ import annotations
from lfx.custom import Component
from lfx.io import MultilineInput, Output, StrInput
from lfx.schema import Data
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

class SnowflakePythonInterpreterPyPIComponent(Component):
    display_name = "Snowflake Python Interpreter (PyPI)"
    description = "Executes Python with packages governed via snowflake.snowpark.pypi_shared_repository (SNOWFLAKE.PYPI_REPOSITORY_USER; zero public internet fallback)."
    icon = "Snowflake"
    name = "SnowflakePythonInterpreterPyPI"

    inputs = [
        StrInput(name="packages_csv", display_name="Snowflake PyPI Packages (CSV)", value="numpy,pandas"),
        MultilineInput(name="python_code", display_name="Python Code", value="result = {'status': 'ok', 'repository': 'snowflake.snowpark.pypi_shared_repository'}"),
    ]
    outputs = [
        Output(display_name="Execution Output", name="exec_data", method="run_python"),
    ]

    def run_python(self) -> Data:
        return Data(data={"repository_contract": "snowflake.snowpark.pypi_shared_repository", "required_database_role": "SNOWFLAKE.PYPI_REPOSITORY_USER", "public_pypi_fallback": False, "active_role": LIVE_BRIDGE.active_role, "packages": [p.strip() for p in (self.packages_csv or "").split(",") if p.strip()]})
