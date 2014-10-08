from sql_procedures import *

_modified_time_trigger_str = """
CREATE TRIGGER {trigger_name} BEFORE UPDATE
ON {table_name} FOR EACH ROW EXECUTE PROCEDURE
%s();
""" % modified_time_procedure.fn_name


def modified_time_trigger(table_name, trigger_name=None):
    if trigger_name is None:
        trigger_name = "update_%s_modified_time" % table_name
    return _modified_time_trigger_str.format(
        table_name=table_name,
        trigger_name=trigger_name
    )