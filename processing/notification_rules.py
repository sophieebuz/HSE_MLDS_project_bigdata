from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType, StringType


@udf(returnType=BooleanType())
def tickets_appeared(tickets_before, tickets_after):
    before_cond = tickets_before is None or tickets_before == 0
    after_cond = tickets_after is not None and tickets_after > 0

    return before_cond and after_cond


@udf(returnType=StringType())
def tickets_appeared_message(tickets_appeared):
    return 'Появились билеты на спектакль' if tickets_appeared else None


@udf(returnType=BooleanType())
def few_tickets(tickets_before, tickets_after, min_tickets=10):
    before_cond = tickets_before is not None and tickets_before > min_tickets
    after_cond = tickets_after is not None and tickets_after <= min_tickets

    return before_cond and after_cond


@udf(returnType=StringType())
def few_tickets_message(few_tickets, tickets_num):
    return f'Осталось мало билетов: {tickets_num}' if few_tickets else None
