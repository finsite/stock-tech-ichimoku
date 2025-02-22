import pytest


# Example placeholder functions to test
# Replace these with imports from your actual project modules
def example_function(value):
    """A placeholder function for demonstration."""
    return value * 2


def another_function(value):
    """Another placeholder function for demonstration."""
    if value < 0:
        raise ValueError("Value must be non-negative")
    return value + 10


# Example tests
def test_example_function():
    """Test for the example_function."""
    assert example_function(2) == 4
    assert example_function(0) == 0
    assert example_function(-1) == -2


def test_another_function():
    """Test for the another_function."""
    assert another_function(5) == 15
    assert another_function(0) == 10

    # Test for exceptions
    with pytest.raises(ValueError):
        another_function(-1)


# Add more tests as needed
