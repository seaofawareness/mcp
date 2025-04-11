"""Tests for pandas code interpreter functionality."""

import os
import pandas as pd
import pytest
from awslabs.syntheticdata_mcp_server.pandas_interpreter import (
    check_referential_integrity,
    execute_pandas_code,
)


def test_execute_pandas_code_success(temp_dir: str, sample_pandas_code: str) -> None:
    """Test successful execution of pandas code."""
    result = execute_pandas_code(sample_pandas_code, temp_dir)

    assert result['success'] is True
    assert len(result['saved_files']) == 3
    assert 'customers_df.csv' in os.listdir(temp_dir)
    assert 'orders_df.csv' in os.listdir(temp_dir)
    assert 'addresses_df.csv' in os.listdir(temp_dir)

    # Verify file contents
    customers_df = pd.read_csv(os.path.join(temp_dir, 'customers_df.csv'))
    assert len(customers_df) == 3
    assert list(customers_df.columns) == ['customer_id', 'name', 'email', 'city']

    orders_df = pd.read_csv(os.path.join(temp_dir, 'orders_df.csv'))
    assert len(orders_df) == 4
    assert list(orders_df.columns) == ['order_id', 'customer_id', 'amount', 'status']


def test_execute_pandas_code_no_dataframes(temp_dir: str) -> None:
    """Test execution with code that doesn't create any DataFrames."""
    code = """
    x = 1
    y = 2
    result = x + y
    """
    result = execute_pandas_code(code, temp_dir)

    assert result['success'] is False
    assert result['message'] == 'No DataFrames found in the code'
    assert not os.listdir(temp_dir)


def test_execute_pandas_code_syntax_error(temp_dir: str) -> None:
    """Test handling of syntax errors in pandas code."""
    code = """
    # This code has a syntax error
    customers_df = pd.DataFrame({
        'id': [1, 2, 3]
        'name': ['A', 'B', 'C']  # Missing comma
    })
    """
    result = execute_pandas_code(code, temp_dir)

    assert result['success'] is False
    assert 'error' in result
    assert 'SyntaxError' in str(result['error'])


def test_check_referential_integrity(sample_pandas_code: str, temp_dir: str) -> None:
    """Test referential integrity checking."""
    # Execute code to create DataFrames
    result = execute_pandas_code(sample_pandas_code, temp_dir)
    assert result['success'] is True

    # Load DataFrames
    customers_df = pd.read_csv(os.path.join(temp_dir, 'customers_df.csv'))
    orders_df = pd.read_csv(os.path.join(temp_dir, 'orders_df.csv'))
    addresses_df = pd.read_csv(os.path.join(temp_dir, 'addresses_df.csv'))

    dataframes = {
        'customers': customers_df,
        'orders': orders_df,
        'addresses': addresses_df
    }

    # Check integrity
    issues = check_referential_integrity(dataframes)

    # Verify referential integrity issues
    ref_integrity_issues = [i for i in issues if i['type'] == 'referential_integrity']
    assert len(ref_integrity_issues) > 0
    assert any(
        i['source_table'] == 'orders' and
        i['target_table'] == 'customers' and
        i['column'] == 'customer_id'
        for i in ref_integrity_issues
    )

    # Verify functional dependency issues
    func_dep_issues = [i for i in issues if i['type'] == 'functional_dependency']
    assert len(func_dep_issues) > 0
    assert any(
        i['table'] == 'addresses' and
        i['determinant'] == 'city' and
        i['dependent'] == 'zip_code'
        for i in func_dep_issues
    )


def test_execute_pandas_code_directory_creation(temp_dir: str, sample_pandas_code: str) -> None:
    """Test that output directory is created if it doesn't exist."""
    output_dir = os.path.join(temp_dir, 'output')
    result = execute_pandas_code(sample_pandas_code, output_dir)

    assert result['success'] is True
    assert os.path.exists(output_dir)
    assert len(os.listdir(output_dir)) == 3


def test_execute_pandas_code_invalid_directory(sample_pandas_code: str) -> None:
    """Test handling of invalid output directory."""
    invalid_dir = '/nonexistent/directory'
    result = execute_pandas_code(sample_pandas_code, invalid_dir)

    assert result['success'] is False
    assert 'error' in result
    assert 'No such file or directory' in str(result['error'])


@pytest.mark.parametrize('code,expected_error', [
    ('import os; os.system("echo hack")', 'NameError'),  # Security: No access to os
    ('import sys; sys.exit(1)', 'NameError'),  # Security: No access to sys
    ('__import__("os")', 'NameError'),  # Security: No dynamic imports
])
def test_execute_pandas_code_security(temp_dir: str, code: str, expected_error: str) -> None:
    """Test that code execution is properly sandboxed."""
    result = execute_pandas_code(code, temp_dir)

    assert result['success'] is False
    assert 'error' in result
    assert expected_error in str(result['error'])
