from sqlalchemy.engine import Engine
from sqlalchemy import inspect

def generate_schema_description(engine: Engine) -> str:
    """
    Introspects the database to generate a text description of the schema.
    """
    inspector = inspect(engine)
    schema_text = []
    
    for table_name in inspector.get_table_names():
        columns = inspector.get_columns(table_name)
        pk = inspector.get_pk_constraint(table_name)
        fks = inspector.get_foreign_keys(table_name)
        
        schema_text.append(f"Table: {table_name}")
        schema_text.append("  Columns:")
        for col in columns:
            col_str = f"    - {col['name']} ({col['type']})"
            if col['name'] in pk['constrained_columns']:
                col_str += " [PK]"
            schema_text.append(col_str)
            
        if fks:
            schema_text.append("  Foreign Keys:")
            for fk in fks:
                schema_text.append(f"    - {fk['constrained_columns']} -> {fk['referred_table']}.{fk['referred_columns']}")
        
        schema_text.append("") # Empty line between tables
        
    return "\n".join(schema_text)
