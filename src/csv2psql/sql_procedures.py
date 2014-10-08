from textwrap import dedent


class SqlProcedure:
    def __init__(self, col_name, fn_name, procedure_str):
        self.col_name = col_name
        self.fn_name = fn_name
        self.procedure_str = procedure_str


modified_time_procedure = SqlProcedure(
    "modified_time",
    "update_modified_time_column", dedent("""
    CREATE OR REPLACE FUNCTION update_modified_time_column()
    RETURNS TRIGGER AS $$
    BEGIN
       NEW.modified_time = CURRENT_TIMESTAMP;
       RETURN NEW;
    END;
    $$ language 'plpgsql';
    """))