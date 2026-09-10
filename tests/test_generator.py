from pathlib import Path
def test_project_structure():
    assert Path("src/generators/generate_data.py").exists()
def test_sql_exists():
    assert Path("sql/analytics_queries.sql").exists()
