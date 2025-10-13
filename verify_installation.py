#!/usr/bin/env python
"""Verification script to test polyspark installation."""

import sys
from dataclasses import dataclass
from typing import List, Optional


def check_polyspark():
    """Check if polyspark is installed and importable."""
    try:
        import polyspark
        print("✓ polyspark is installed")
        print(f"  Version: {polyspark.__version__}")
        return True
    except ImportError:
        print("✗ polyspark is not installed")
        print("  Install with: pip install polyspark")
        return False


def check_pyspark():
    """Check if PySpark is installed."""
    from polyspark.protocols import is_pyspark_available
    
    if is_pyspark_available():
        import pyspark
        print("✓ PySpark is installed")
        print(f"  Version: {pyspark.__version__}")
        return True
    else:
        print("ℹ PySpark is not installed (optional)")
        print("  Install with: pip install pyspark")
        return False


def test_basic_functionality():
    """Test basic polyspark functionality."""
    print("\nTesting basic functionality...")
    
    try:
        from polyspark import SparkFactory
        
        @dataclass
        class TestModel:
            id: int
            name: str
            active: bool
        
        class TestFactory(SparkFactory[TestModel]):
            __model__ = TestModel
        
        # Test build_dicts (works without PySpark)
        dicts = TestFactory.build_dicts(size=5)
        assert len(dicts) == 5
        assert all(isinstance(d, dict) for d in dicts)
        assert all(set(d.keys()) == {"id", "name", "active"} for d in dicts)
        
        print("✓ build_dicts() works correctly")
        return True
        
    except Exception as e:
        print(f"✗ Basic functionality test failed: {e}")
        return False


def test_with_pyspark():
    """Test PySpark integration if available."""
    from polyspark.protocols import is_pyspark_available
    
    if not is_pyspark_available():
        print("\nSkipping PySpark tests (PySpark not installed)")
        return True
    
    print("\nTesting PySpark integration...")
    
    try:
        from pyspark.sql import SparkSession
        from polyspark import SparkFactory, build_spark_dataframe
        
        @dataclass
        class User:
            id: int
            username: str
            email: str
        
        class UserFactory(SparkFactory[User]):
            __model__ = User
        
        # Create SparkSession
        spark = SparkSession.builder \
            .appName("polyspark-verify") \
            .master("local[1]") \
            .config("spark.ui.enabled", "false") \
            .getOrCreate()
        
        # Test build_dataframe
        df = UserFactory.build_dataframe(spark, size=10)
        assert df.count() == 10
        assert set(df.columns) == {"id", "username", "email"}
        
        print("✓ build_dataframe() works correctly")
        
        # Test convenience function
        df2 = build_spark_dataframe(User, spark, size=5)
        assert df2.count() == 5
        
        print("✓ build_spark_dataframe() works correctly")
        
        spark.stop()
        return True
        
    except Exception as e:
        print(f"✗ PySpark integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_complex_types():
    """Test complex type support."""
    print("\nTesting complex types...")
    
    try:
        from polyspark import SparkFactory
        
        @dataclass
        class Address:
            street: str
            city: str
        
        @dataclass
        class Person:
            id: int
            name: str
            address: Address  # Nested
            tags: List[str]  # Array
            nickname: Optional[str]  # Optional
        
        class PersonFactory(SparkFactory[Person]):
            __model__ = Person
        
        dicts = PersonFactory.build_dicts(size=3)
        assert len(dicts) == 3
        
        # Check nested structure
        for person in dicts:
            assert "address" in person
            assert isinstance(person["address"], dict)
            assert "street" in person["address"]
            assert "city" in person["address"]
            assert isinstance(person["tags"], list)
        
        print("✓ Complex types work correctly")
        return True
        
    except Exception as e:
        print(f"✗ Complex types test failed: {e}")
        return False


def main():
    """Run all verification checks."""
    print("=" * 60)
    print("Polyspark Installation Verification")
    print("=" * 60)
    
    results = []
    
    # Check installations
    results.append(check_polyspark())
    if not results[-1]:
        print("\nCannot continue without polyspark installed.")
        sys.exit(1)
    
    check_pyspark()  # Optional, so don't fail on this
    
    # Run tests
    results.append(test_basic_functionality())
    results.append(test_complex_types())
    results.append(test_with_pyspark())
    
    # Summary
    print("\n" + "=" * 60)
    if all(results):
        print("✓ All tests passed!")
        print("Polyspark is ready to use.")
        print("\nNext steps:")
        print("  - Check out examples/: python examples/basic_usage.py")
        print("  - Read the docs: README.md")
        print("  - Run tests: pytest")
    else:
        print("✗ Some tests failed.")
        print("Please check the errors above.")
        sys.exit(1)
    print("=" * 60)


if __name__ == "__main__":
    main()

