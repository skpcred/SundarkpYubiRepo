#!/usr/bin/env python

import logging
import os
import json
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from dataclasses import dataclass
from dotenv import load_dotenv
from typing import Dict, List, Any, Optional
from mcp.server.fastmcp import FastMCP, Context
from legion_query_runner import QueryRunner
import sys
import argparse

# Load environment variables
load_dotenv()
logger = logging.getLogger("server")

# Initialize the Legion Query Runner
def init_query_runner():

    # Use command line arguments for direct execution
    parser = argparse.ArgumentParser(description='Legion MCP Server')
    parser.add_argument('--db-type', required=False, help='Database type (e.g., mysql, postgresql)')
    parser.add_argument('--db-config', required=False, help='JSON string containing database configuration')

    args = parser.parse_args()
    db_type = args.db_type
    db_config_str = args.db_config
    # Only parse args if we're not in MCP CLI mode
    if not db_type:
        db_type = os.getenv("DB_TYPE", "pg")
    if not db_config_str:
        db_config_str = os.getenv("DB_CONFIG", "")
    db_config = json.loads(db_config_str)

    if not db_type or not db_config:
        raise ValueError("Database type and configuration are required")

    print(f"Initializing query runner for {db_type} database...")
        
    return QueryRunner(db_type=db_type, configuration=db_config)

try:
    query_runner = init_query_runner()
except Exception as e:
    print(f"Error initializing query runner: {str(e)}")
    print("\nUsage:")
    print("1. For MCP CLI mode:")
    print("   Set environment variables: DB_TYPE, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME")
    print("   Then run: mcp install mcp_server.py")
    print("   Or: mcp dev mcp_server.py")
    print("\n2. For direct execution:")
    print("   python mcp_server.py --db-type <db_type> --db-config '<json_config>'")
    print("   Example: python mcp_server.py --db-type mysql --db-config '{\"host\":\"localhost\",\"port\":3306,\"user\":\"root\",\"password\":\"pass\",\"database\":\"test\"}'")
    sys.exit(1)

# Define database context that will be available to all handlers
@dataclass
class DbContext:
    query_runner: QueryRunner
    last_query: Optional[str] = None
    last_result: Optional[Dict[str, Any]] = None
    query_history: List[str] = None
    
    def __post_init__(self):
        if self.query_history is None:
            self.query_history = []

# Server lifespan manager
@asynccontextmanager
async def db_lifespan(server: FastMCP) -> AsyncIterator[DbContext]:
    """Initialize database connection on startup and provide context to handlers"""
    
    # Initialize context
    db_context = DbContext(
        query_runner=query_runner,
    )
    
    try:
        # Test connection on startup
        query_runner.test_connection()
        yield db_context
    finally:
        # Cleanup could happen here if needed
        pass

# Pass lifespan to server
mcp = FastMCP("Legion Database Access", lifespan=db_lifespan)

# Define resources
@mcp.resource("schema://all")
def get_schema() -> str:
    """Get the database schema"""
    try:
        schema = query_runner.get_schema()
        return json.dumps(schema)
    except Exception as e:
        return f"Error getting schema: {str(e)}"

@mcp.tool()
def get_query_history(ctx: Context) -> str:
    """Get the recent query history"""
    db_context = ctx.request_context.lifespan_context
    
    if db_context.query_history:
        history_list = "\n".join([f"- {query}" for query in db_context.query_history])
        return f"Query history:\n{history_list}"
    else:
        return "No queries have been executed yet."

def _execute_and_get_results(query: str, ctx: Context) -> Dict[str, Any]:
    """Helper function to execute query and get formatted results"""
    db_context = ctx.request_context.lifespan_context
    
    # Execute query
    result = query_runner.run_query(query)
    
    # Update query history in the context
    db_context.last_query = query
    db_context.last_result = result
    db_context.query_history.append(query)
    
    # Extract column info
    columns = result.get('columns', [])
    column_names = [col.get('friendly_name', col.get('name', '')) for col in columns]
    
    # Extract row data
    rows = result.get('rows', [])
    row_count = len(rows)
    
    # Process rows - each row is a dictionary with column names as keys
    processed_rows = []
    for row_dict in rows:
        # Create a row with values in the same order as column_names
        processed_row = [row_dict.get(col.get('name', '')) for col in columns]
        processed_rows.append(processed_row)
    
    return {
        'column_names': column_names,
        'columns': columns,
        'rows': processed_rows,
        'raw_rows': rows,
        'row_count': row_count
    }

# Define tools
@mcp.tool()
def execute_query(query: str, ctx: Context) -> str:
    """Execute a SQL query and return results as a markdown table"""
    try:
        result = _execute_and_get_results(query, ctx)
        
        # Build a markdown table for output
        header = " | ".join(result['column_names'])
        separator = " | ".join(["---"] * len(result['column_names']))
        
        table_rows = []
        for row in result['rows'][:10]:  # Limit to first 10 rows for display
            table_rows.append(" | ".join(str(cell) for cell in row))
        
        result_table = f"{header}\n{separator}\n" + "\n".join(table_rows)
        
        if result['row_count'] > 10:
            result_table += f"\n\n... and {result['row_count'] - 10} more rows (total: {result['row_count']})"
            
        return result_table
    except Exception as e:
        return f"Error executing query: {str(e)}"

@mcp.tool()
def execute_query_json(query: str, ctx: Context) -> str:
    """Execute a SQL query and return results as JSON"""
    try:
        result = _execute_and_get_results(query, ctx)
        
        # Create a more compact representation for JSON output
        output = {
            'columns': result['column_names'],
            'rows': result['raw_rows'],  # Return the original row dictionaries for JSON output
            'row_count': result['row_count']
        }
        return json.dumps(output, indent=2)
    except Exception as e:
        return f"Error executing query: {str(e)}"

@mcp.tool()
def get_table_columns(table_name: str) -> str:
    """Get column names for a specific table"""
    try:
        columns = query_runner.get_table_columns(table_name)
        return json.dumps(columns)
    except Exception as e:
        return f"Error getting columns for table {table_name}: {str(e)}"

@mcp.tool()
def get_table_types(table_name: str) -> str:
    """Get column types for a specific table"""
    try:
        types = query_runner.get_table_types(table_name)
        return json.dumps(types)
    except Exception as e:
        return f"Error getting types for table {table_name}: {str(e)}"

# Define prompts
@mcp.prompt()
def sql_query() -> str:
    """Create an SQL query against the database"""
    return "Please help me write a SQL query for the following question:\n\n"

@mcp.prompt()
def explain_query(query: str) -> str:
    """Explain what a SQL query does"""
    return f"Can you explain what the following SQL query does?\n\n```sql\n{query}\n```"

@mcp.prompt()
def optimize_query(query: str) -> str:
    """Optimize a SQL query for better performance"""
    return f"Can you optimize the following SQL query for better performance?\n\n```sql\n{query}\n```"

def main():
    print("Starting Legion MCP server...")
    mcp.run()

if __name__ == "__main__":
    main() 