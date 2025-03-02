import os
import subprocess
import pytest


class TestEnvironment:
    """System tests for environment setup."""

    @pytest.mark.docker
    def test_docker_available(self):
        """Test if Docker is available and working on the host."""
        try:
            result = subprocess.run(
                ["docker", "--version"], capture_output=True, text=True, check=True
            )
            assert (
                "Docker version" in result.stdout
            ), "Docker not installed or not working"
        except subprocess.CalledProcessError as e:
            pytest.fail(f"Docker check failed: {e}")


class TestEnvVariables:
    """System tests for environment variables."""

    def test_required_vars(self):
        """Test if critical environment variables are loaded."""
        required_vars = ["DB_NAME", "DB_USER", "DB_PASSWORD", "DB_HOST"]
        for var in required_vars:
            assert os.getenv(var) is not None, f"Environment variable {var} is not set"
