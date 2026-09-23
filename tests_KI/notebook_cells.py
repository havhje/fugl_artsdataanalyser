"""Last bare beregnings- og testceller; aldri notebookens innlesing eller kart."""
import ast
from pathlib import Path

from dataanalyse import data_analyse


def load_notebook_cells() -> dict:
    namespace = dict(vars(data_analyse))
    path = Path(data_analyse.__file__)
    for cell in ast.parse(path.read_text()).body:
        if not isinstance(cell, ast.FunctionDef):
            continue
        nested = [node.name for node in cell.body if isinstance(node, ast.FunctionDef)]
        if not any(name.startswith(("lag_", "valider_", "test_")) for name in nested):
            continue
        if cell.name not in {
            "dekningsmatrise_konstanter_validering", "dekningsmatrise_aggregering",
            "dekningsmatrise_figurfunksjon", "dekningsmatrise_tester",
            "maanedsgrunnlag_konstanter_validering", "maanedsgrunnlag_aggregering",
            "maanedsgrunnlag_tabellfunksjon", "maanedsgrunnlag_tester",
        } and not any(name.startswith(("test_artsstatistikk_", "lag_artsstatistikk_testinput")) for name in nested):
            continue
        body = [node for node in cell.body if not isinstance(node, ast.Return)
                and not (isinstance(node, ast.Expr) and isinstance(node.value, ast.Call)
                         and isinstance(node.value.func, ast.Name)
                         and node.value.func.id.startswith("test_"))]
        exec(compile(ast.Module(body=body, type_ignores=[]), str(path), "exec"), namespace)
    return namespace
