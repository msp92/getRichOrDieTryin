import pytest
import psycopg2


class TestDatabaseConnection:
    """Integration tests for database connectivity."""

    def test_connection(self, db_config):
        """Test basic connection to the RDS database."""
        try:
            with psycopg2.connect(**db_config) as conn:
                assert conn.closed == 0, "Connection should be open"
        except psycopg2.Error as e:
            pytest.fail(f"Failed to connect to database: {e}")

    def test_simple_query(self, db_config):
        """Test executing a simple query on the RDS database."""
        try:
            with psycopg2.connect(**db_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    result = cur.fetchone()
                    assert result[0] == 1, "Query should return 1"
        except psycopg2.Error as e:
            pytest.fail(f"Failed to execute query: {e}")
