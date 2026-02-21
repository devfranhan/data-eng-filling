from typing import Union, List, Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, count, when, isnan, mean, stddev, lit
)
import datetime

def check_data_quality(
    df: DataFrame,
    table_name: str,
    primary_key: Union[str, List[str]],
    max_null_percentage: float = 0.1,
    expected_schema: Optional[Dict[str, str]] = None,
    expected_values: Optional[Dict[str, List[Any]]] = None,
    priority_fields: Optional[List[str]] = None,
    dq_table_name: str = "monitoramento.data_quality_logs",
    spark_session: Optional[SparkSession] = None
) -> None:
    """
    Valida a qualidade dos dados e já envia o registro para a tabela monitoramento.data_quality_logs (Delta).
    """

    if spark_session is None:
        spark_session = df.sparkSession

    if isinstance(primary_key, str):
        primary_key = [primary_key]

    total_rows = df.count()
    total_columns = len(df.columns)

    # Duplicidade e nulos na PK
    duplicate_pk_count = df.groupBy(primary_key).count().filter(col("count") > 1).count()
    null_pk_count = df.filter(sum([col(pk).isNull().cast("int") for pk in primary_key]) > 0).count()

    # Nulos por coluna
    nulls_per_column = df.select([
        (count(when(col(c).isNull() | isnan(c), c)) / total_rows).alias(c) for c in df.columns
    ])
    null_percent_dict = nulls_per_column.first().asDict()
    null_percent_total = round(sum(null_percent_dict.values()) / total_columns, 4)
    top_null_columns = sorted(null_percent_dict.items(), key=lambda x: x[1], reverse=True)[:5]
    top_null_columns_names = [x[0] for x in top_null_columns]

    # Constantes
    constant_columns = [c for c in df.columns if df.select(c).distinct().count() == 1]

    # Outliers (z-score simples)
    numeric_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() in ['int', 'double', 'float', 'bigint']]
    outlier_columns = []
    for col_name in numeric_cols:
        stats = df.select(mean(col(col_name)).alias('mean'), stddev(col(col_name)).alias('std')).first()
        if stats and stats['std'] is not None and stats['std'] > 0:
            mean_val, std_val = stats['mean'], stats['std']
            threshold = 3 * std_val
            count_outliers = df.filter((col(col_name) > mean_val + threshold) | (col(col_name) < mean_val - threshold)).count()
            if count_outliers > 0:
                outlier_columns.append(col_name)

    # Schema esperado
    schema_mismatch = False
    if expected_schema:
        for col_name, expected_type in expected_schema.items():
            if col_name not in df.columns or df.schema[col_name].simpleString() != expected_type:
                schema_mismatch = True
                break

    # Valores inesperados
    unexpected_values = []
    if expected_values:
        for col_name, allowed_values in expected_values.items():
            if col_name in df.columns:
                invalid_count = df.filter(~col(col_name).isin(allowed_values)).count()
                if invalid_count > 0:
                    unexpected_values.append(col_name)

    # Linhas duplicadas completas
    duplicate_rows = df.groupBy(df.columns).count().filter("count > 1").count()

    # Tamanho estimado (MB)
    size_in_mb = round(df.rdd.map(lambda r: len(str(r))).mean() * total_rows / (1024 * 1024), 2)

    # Nulos em campos prioritários
    null_priority_fields = []
    if priority_fields:
        for field in priority_fields:
            if field in df.columns:
                null_count = df.filter(col(field).isNull() | isnan(col(field))).count()
                if null_count > 0:
                    null_priority_fields.append(field)

    # Status final
    status = "OK"
    if duplicate_pk_count > 0 or null_pk_count > 0 or null_percent_total > max_null_percentage or null_priority_fields:
        status = "WARNING"

    # Registro final
    registro = [{
        "table_name": table_name,
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "total_rows": total_rows,
        "total_columns": total_columns,
        "duplicate_pk_count": duplicate_pk_count,
        "null_pk_count": null_pk_count,
        "null_percentage_total": null_percent_total * 100,
        "top_null_columns": ", ".join(top_null_columns_names),
        "constant_columns": ", ".join(constant_columns),
        "outlier_columns": ", ".join(outlier_columns),
        "duplicate_rows": duplicate_rows,
        "unexpected_values_columns": ", ".join(unexpected_values),
        "null_priority_fields": ", ".join(null_priority_fields),
        "schema_mismatch": schema_mismatch,
        "size_mb_estimate": size_in_mb,
        "status": status
    }]

    # Cria e envia para a tabela Delta
    result_df = spark_session.createDataFrame(registro)
    result_df.write.mode("append").format("delta").saveAsTable(dq_table_name)
