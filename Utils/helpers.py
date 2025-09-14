
import logging

from google.cloud import bigquery
import pandas as pd



logging.basicConfig(
level=logging.INFO,
format="%(asctime)s - %(levelname)s - %(message)s")

def warning(msg: str):
    """
    Ecris le message passé en en warning 
    """
    logging.warning(msg)

def error(msg: str):
    """
    Ecris le message passé en en error 
    """
    logging.error(msg)

def info(msg: str):
    """
    Ecris le message passé en en info 
    """
    logging.info(msg)

def debug(msg: str):
    """
    Ecris le message passé en en debug 
    """
    logging.debug(msg)

def infer_bq_type_from_value(value, df=None, col=None, checked_rows=100):
    from google.cloud import bigquery
    
    # Cas None
    if value is None:
        return bigquery.SchemaField(str(col), "STRING", mode="NULLABLE")
    
    # Cas dict
    if isinstance(value, dict):
        fields = []
        for k, v in value.items():
            fields.append(infer_bq_type_from_value(v, df=None, col=k))
        return bigquery.SchemaField(str(col), "RECORD", mode="REPEATED", fields=fields)

    # Cas liste
    if isinstance(value, list):
        if len(value) == 0 and df is not None and col is not None:
            # Chercher un autre exemple non vide dans le DataFrame
            df_no_na = df[col].dropna()
            for sample in df_no_na.head(df_no_na.shape[0]):
                if isinstance(sample, list) and len(sample) > 0:
                    return infer_bq_type_from_value(sample[0], df=df, col=col)                   
            # Aucun exemple trouvé → fallback
            return bigquery.SchemaField(col, "STRING", mode="REPEATED")
        
        # Liste non vide
        if len(value) > 0:
            sample_type = infer_bq_type_from_value(value[0], df=df, col=col)
            return bigquery.SchemaField(str(col), sample_type.field_type, mode="REPEATED", fields=sample_type.fields)

        return bigquery.SchemaField(str(col), "STRING", mode="REPEATED")

    # Cas scalaires
    if isinstance(value, int):
        return bigquery.SchemaField(str(col), "INTEGER", mode="NULLABLE")
    if isinstance(value, float):
        return bigquery.SchemaField(str(col), "FLOAT", mode="NULLABLE")
    if isinstance(value, bool):
        return bigquery.SchemaField(str(col), "BOOLEAN", mode="NULLABLE")
    
    # Fallback
    return bigquery.SchemaField(str(col), "STRING", mode="NULLABLE")


def generate_bq_schema_from_df(df, override_schema: list[bigquery.SchemaField] = []) -> list:
    """
    Génère un schéma BigQuery à partir d'un DataFrame Pandas.
    - Si une colonne est dans overrides -> on prend le SchemaField fourni tel quel.
    - Sinon -> on infère automatiquement (supporte RECORD / REPEATED).

    Args:
        df(pd.DataFrame): Le dataFrame à analyser
        override_schema(list): Le schema surchargé
    
    Return:
        list
    """
    schema = []
    overrides = {field.name: field for field in override_schema} if override_schema else {} 

    for col in df.columns:
        if col in overrides:
            schema.append(overrides[col])
        else:
            sample_val = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if sample_val is not None:
                inferred_field = infer_bq_type_from_value(sample_val, df, col)
                schema.append(bigquery.SchemaField(
                    col, inferred_field.field_type,
                    mode=inferred_field.mode,
                    fields=inferred_field.fields
                ))
            else:
                schema.append(bigquery.SchemaField(col, "STRING"))

    # Ajouter aussi les overrides pour colonnes absentes du DF
    for col, field in overrides.items():
        if col not in df.columns:
            schema.append(field)

    return schema
