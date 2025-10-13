"""SparkFactory class for generating PySpark DataFrames."""

from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Optional, Type, TypeVar, Union

from polyfactory.factories import DataclassFactory
from pydantic import BaseModel

from polyspark.exceptions import PySparkNotAvailableError
from polyspark.protocols import (
    DataFrameProtocol,
    SparkSessionProtocol,
    StructTypeProtocol,
    is_pyspark_available,
)
from polyspark.schema import infer_schema

T = TypeVar("T")


class SparkFactory(DataclassFactory[T]):
    """Factory for generating PySpark DataFrames from models.
    
    This factory extends polyfactory's DataclassFactory to support generating
    PySpark DataFrames instead of model instances. It works with dataclasses,
    Pydantic models, and TypedDicts.
    
    Example:
        ```python
        from dataclasses import dataclass
        from polyspark import SparkFactory
        from pyspark.sql import SparkSession
        
        @dataclass
        class User:
            id: int
            name: str
            email: str
        
        class UserFactory(SparkFactory[User]):
            __model__ = User
        
        spark = SparkSession.builder.getOrCreate()
        df = UserFactory.build_dataframe(spark, size=100)
        df.show()
        ```
    """
    
    @classmethod
    def build_dataframe(
        cls,
        spark: SparkSessionProtocol,
        size: int = 10,
        schema: Optional[Union[StructTypeProtocol, List[str]]] = None,
        **kwargs: Any,
    ) -> DataFrameProtocol:
        """Build a PySpark DataFrame with generated data.
        
        Args:
            spark: SparkSession instance to create the DataFrame.
            size: Number of rows to generate.
            schema: Optional explicit schema. Can be:
                   - PySpark StructType: Used as-is
                   - List[str]: Column names to include (infers types from model)
                   - None: Infers full schema from model
            **kwargs: Additional keyword arguments passed to the factory.
            
        Returns:
            A PySpark DataFrame with generated data.
            
        Raises:
            PySparkNotAvailableError: If PySpark is not installed.
        """
        if not is_pyspark_available():
            raise PySparkNotAvailableError(
                "PySpark is required to build DataFrames. "
                "Install it with: pip install pyspark\n"
                "Or use build_dicts() to generate data without PySpark."
            )
        
        # Generate data as list of dictionaries
        data = cls.build_dicts(size=size, **kwargs)
        
        # Infer schema if needed
        inferred_schema = infer_schema(cls.__model__, schema)
        
        # Create DataFrame
        df = spark.createDataFrame(data, schema=inferred_schema)
        return df
    
    @classmethod
    def build_dicts(
        cls,
        size: int = 10,
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        """Build a list of dictionaries with generated data.
        
        This method doesn't require PySpark and can be used to generate
        data that can be converted to a DataFrame later.
        
        Args:
            size: Number of records to generate.
            **kwargs: Additional keyword arguments passed to the factory.
            
        Returns:
            A list of dictionaries with generated data.
        """
        instances = cls.batch(size=size, **kwargs)
        
        # Convert instances to dictionaries
        dicts = []
        for instance in instances:
            if is_dataclass(instance):
                dicts.append(asdict(instance))
            elif isinstance(instance, BaseModel):
                dicts.append(instance.model_dump())
            elif isinstance(instance, dict):
                dicts.append(instance)
            else:
                # Try to convert to dict
                try:
                    dicts.append(dict(instance))
                except (TypeError, ValueError):
                    dicts.append(instance.__dict__)
        
        return dicts
    
    @classmethod
    def create_dataframe_from_dicts(
        cls,
        spark: SparkSessionProtocol,
        data: List[Dict[str, Any]],
        schema: Optional[Union[StructTypeProtocol, List[str]]] = None,
    ) -> DataFrameProtocol:
        """Convert pre-generated dictionary data to a PySpark DataFrame.
        
        Useful when you've generated data with build_dicts() and want to
        convert it to a DataFrame later.
        
        Args:
            spark: SparkSession instance to create the DataFrame.
            data: List of dictionaries to convert.
            schema: Optional explicit schema.
            
        Returns:
            A PySpark DataFrame.
            
        Raises:
            PySparkNotAvailableError: If PySpark is not installed.
        """
        if not is_pyspark_available():
            raise PySparkNotAvailableError()
        
        inferred_schema = infer_schema(cls.__model__, schema)
        return spark.createDataFrame(data, schema=inferred_schema)


def build_spark_dataframe(
    model: Type[T],
    spark: SparkSessionProtocol,
    size: int = 10,
    schema: Optional[Union[StructTypeProtocol, List[str]]] = None,
    **kwargs: Any,
) -> DataFrameProtocol:
    """Convenience function to build a DataFrame without creating a factory class.
    
    Args:
        model: The model type (dataclass, Pydantic, TypedDict).
        spark: SparkSession instance.
        size: Number of rows to generate.
        schema: Optional explicit schema.
        **kwargs: Additional keyword arguments for data generation.
        
    Returns:
        A PySpark DataFrame with generated data.
        
    Example:
        ```python
        from dataclasses import dataclass
        from polyspark import build_spark_dataframe
        from pyspark.sql import SparkSession
        
        @dataclass
        class Product:
            id: int
            name: str
            price: float
        
        spark = SparkSession.builder.getOrCreate()
        df = build_spark_dataframe(Product, spark, size=50)
        df.show()
        ```
    """
    # Create a dynamic factory class
    factory_class = type(
        f"{model.__name__}Factory",
        (SparkFactory,),
        {"__model__": model}
    )
    
    return factory_class.build_dataframe(spark, size=size, schema=schema, **kwargs)

