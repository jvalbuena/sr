import os
from crewai import Agent, Task, Crew
from crewai_tools import tool
from langchain_community.utilities.sql_database import SQLDatabase
from langchain_community.tools.sql_database.tool import (
    QuerySQLDataBaseTool,
    QuerySQLCheckerTool,
    ListSQLDatabaseTool
)

class PostgreSQLCRUDAgent:
    def __init__(self):
        self.db = self._connect_to_db()
        self.tools = self._create_tools()
        self.agent = self._create_agent()

    def _connect_to_db(self):
        """Create PostgreSQL connection using environment variables"""
        db_uri = (
            f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}"
            f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}"
        )
        return SQLDatabase.from_uri(db_uri)

    def _create_tools(self):
        """Create tools with access to the database connection via closure"""
        db = self.db

        @tool("Execute SQL")
        def execute_sql(sql_query: str) -> str:
            """Execute a SQL query against PostgreSQL database. Use for CRUD operations."""
            try:
                return str(QuerySQLDataBaseTool(db=db).run(sql_query))
            except Exception as e:
                return f"Execution failed: {str(e)}"

        @tool("Validate SQL")
        def check_sql(sql_query: str) -> str:
            """Validate SQL queries for correctness and safety before execution."""
            try:
                return QuerySQLCheckerTool(db=db).run({"query": sql_query})
            except Exception as e:
                return f"Validation failed: {str(e)}"

        @tool("Describe Table")
        def describe_table(table_name: str) -> str:
            """Get table schema and metadata for CRUD operations."""
            try:
                return db.get_table_info(table_names=[table_name])
            except Exception as e:
                return f"Table description failed: {str(e)}"

        @tool("List Tables")
        def list_tables(query: str = "") -> str:
            """List all available tables in the database."""
            try:
                return ListSQLDatabaseTool(db=db).run(query)
            except Exception as e:
                return f"Table listing failed: {str(e)}"

        return [execute_sql, check_sql, describe_table, list_tables]

    def _create_agent(self):
        """Create CRUD-focused agent with PostgreSQL expertise"""
        return Agent(
            role="Senior PostgreSQL Database Engineer",
            goal="Perform safe and efficient CRUD operations on PostgreSQL",
            backstory=(
                "Expert in PostgreSQL operations with strong focus on data integrity "
                "and performance. Specializes in database operations with proper validation."
            ),
            tools=self.tools,
            verbose=True,
            allow_delegation=False
        )

    def run_operation(self, operation_description: str):
        """Execute a database operation through the agent"""
        task = Task(
            description=operation_description,
            expected_output="Accurate results based on the requested operation",
            agent=self.agent
        )
        crew = Crew(
            agents=[self.agent],
            tasks=[task],
            verbose=2
        )
        return crew.kickoff()

if __name__ == "__main__":
    os.environ["PG_USER"] = "user_name"  # Replace with valid username
    os.environ["PG_PASSWORD"] = "your_password"  # Replace
    os.environ["PG_HOST"] = "localhost" # DB hostname
    os.environ["PG_PORT"] = "5432" # DB Port
    os.environ["PG_DATABASE"] = "postgres"  # DB Name

    crud_agent = PostgreSQLCRUDAgent()
    
    # List tables operation
    list_tables_result = crud_agent.run_operation(
        "List all available tables in the database"
    )
    print("Tables in database:", list_tables_result)
    
    # List missions (assuming you have a 'blah' table)
    if "blah" in list_tables_result.lower():
        blah = crud_agent.run_operation(
            "List all items with their status and creation date"
        )
        print("Items:", blah)
    else:
        print("No items table found in the database")
