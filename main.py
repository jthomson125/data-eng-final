import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery
import logging


def run():
    opt = PipelineOptions(
        temp_location="gs://york_temp_files/tmp",
        project="york-cdf-start",
        region="us-central1",
        staging_location="gs://york_temp_files/staging",
        job_name="james-thomson-final-job",
        save_main_session=True
    )

    views_schema = {
        'fields': [
            {'name': 'cust_tier_code', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'sku', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'total_no_of_product_views', 'type': 'INTEGER', 'mode': 'REQUIRED'}
        ]
    }

    sales_schema = {
        'fields': [
            {'name': 'cust_tier_code', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'sku', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'total_sales_amount', 'type': 'FLOAT', 'mode': 'REQUIRED'}
        ]
    }

    views_table = bigquery.TableReference(
        projectId="york-cdf-start",
        datasetId="final_james_thomson",
        tableId="cust_tier_code-sku-total_no_of_product_views"
    )

    sales_table = bigquery.TableReference(
        projectId="york-cdf-start",
        datasetId="final_james_thomson",
        tableId="cust_tier_code-sku-total_sales_amount"
    )

    with beam.Pipeline(runner="DataflowRunner", options=opt) as pipeline:
        data1 = pipeline | "ReadFromBigQuery1" >> beam.io.ReadFromBigQuery(
            query=
            """
            SELECT customers.CUST_TIER_CODE AS cust_tier_code, product_views.SKU AS sku, COUNT(DISTINCT(product_views.EVENT_TM)) AS total_no_of_product_views
            FROM `york-cdf-start.final_input_data.customers` AS customers
            JOIN `york-cdf-start.final_input_data.product_views` AS product_views ON (customers.CUSTOMER_ID = product_views.CUSTOMER_ID)
            GROUP BY customers.CUST_TIER_CODE, product_views.SKU
            """,
            use_standard_sql=True
        )

        data2 = pipeline | "ReadFromBigQuery2" >> beam.io.ReadFromBigQuery(
            query=
            """
            SELECT customers.CUST_TIER_CODE AS cust_tier_code, orders.SKU AS sku, ROUND(SUM(orders.ORDER_AMT), 2) AS total_sales_amount
            FROM `york-cdf-start.final_input_data.customers` AS customers
            JOIN `york-cdf-start.final_input_data.orders` AS orders ON (customers.CUSTOMER_ID = orders.CUSTOMER_ID)
            GROUP BY customers.CUST_TIER_CODE, orders.SKU
            """,
            use_standard_sql=True
        )

        data1 | "Write" >> beam.io.WriteToBigQuery(
            views_table,
            schema=views_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            custom_gcs_temp_location="gs://york_temp_files/tmp"
        )

        data2 | "Write2" >> beam.io.WriteToBigQuery(
            sales_table,
            schema=sales_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            custom_gcs_temp_location="gs://york_temp_files/tmp"
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

    pass

# adding a comment to test the jenkins hook again
