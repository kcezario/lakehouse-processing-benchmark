from app.transformation.pandas_transformer import PandasTransformer
from app.transformation.python_transformer import PythonTransformer
from app.transformation.spark_transformer import SparkTransformer


def get_transformer(engine_name: str):
    engine = engine_name.lower().strip()

    transformers = {
        "pandas": PandasTransformer,
        "python": PythonTransformer,
        "spark": SparkTransformer,
    }

    if engine == "trino":
        raise ValueError("Trino is not supported as a transformation engine")

    if engine not in transformers:
        supported = ", ".join(sorted(transformers))
        raise ValueError(
            f"Invalid transformation engine: {engine}. "
            f"Supported: {supported}"
        )

    return transformers[engine]()