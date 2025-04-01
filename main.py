# importing required libraries
import pyspark
from pyspark.sql import SQLContext, SparkSession
import pyspark.sql.functions as f
from pyspark.sql.window import Window
import boto3
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from pyspark.sql.types import IntegerType, StringType, DateType, DoubleType, DateType, StructType, StructField, TimestampType, BooleanType

# creating spark session
spark = SparkSession.builder \
    .appName("zeus-new-disbs-format") \
    .config("spark.sql.debug.maxToStringFields", "1000") \
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
    .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
    .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .enableHiveSupport() \
    .getOrCreate()


current_date = datetime.today().date()
date_filter = current_date - timedelta(days=2)

current_date_str = current_date.strftime("%Y-%m-%d")


previous_days_successful_schema = StructType([
    StructField('S.No', IntegerType(), True),
    StructField('partner_name_short', StringType(), True),
    StructField('loan_id', IntegerType(), True),
    StructField('borrower_id', IntegerType(), True),
    StructField('disbursement_account_id', IntegerType(), True),
    StructField('borrower_record_exists', BooleanType(), True),
    StructField('disbursement_accounts_record_exists', BooleanType(), True),
    StructField('products_record_exists', BooleanType(), True),
    StructField('rs_record_exists', BooleanType(), True),
    StructField('External Id', StringType(), True),
    StructField('Entity', StringType(), True),
    StructField('Applicant Type', StringType(), True),
    StructField('Asset Class', StringType(), True),
    StructField('first_name', StringType(), True),
    StructField('middle_name', StringType(), True),
    StructField('last_name', StringType(), True),
    StructField('age', IntegerType(), True),
    StructField('gender', StringType(), True),
    StructField('DOB', StringType(), True),
    StructField('PAN', StringType(), True),
    StructField('Aadhar', StringType(), True),
    StructField('Voter ID', StringType(), True),
    StructField('Passport Number', StringType(), True),
    StructField('Driving License', StringType(), True),
    StructField('Address', StringType(), True),
    StructField('Pin Code', StringType(), True),
    StructField('Mobile Number', StringType(), True),
    StructField('Email Address', StringType(), True),
    StructField('City', StringType(), True),
    StructField('State', StringType(), True),
    StructField('Submitted On', StringType(), True),
    StructField('Activation Date', StringType(), True),
    StructField('Beneficiary Name', StringType(), True),
    StructField('Beneficiary Account No', StringType(), True),
    StructField('Account Type', StringType(), True),
    StructField('IFSC Code', StringType(), True),
    StructField('MICR Code', StringType(), True),
    StructField('Swift Code', StringType(), True),
    StructField('Branch', StringType(), True),
    StructField('Loan Submitted On', StringType(), True),
    StructField('agreement signed date', StringType(), True),
    StructField('Disbursed Date', StringType(), True),
    StructField('Principal', DoubleType(), True),
    StructField('Loan Term', IntegerType(), True),
    StructField('First Repayment On', StringType(), True),
    StructField('Due Day', IntegerType(), True),
    StructField('Interest Rate', DoubleType(), True),
    StructField('Processing Fee', DoubleType(), True),
    StructField('Nach Charges', DoubleType(), True),
    StructField('DCC Charges', DoubleType(), True),
    StructField('Stamp Duty', DoubleType(), True),
    StructField('Documentation Charges', DoubleType(), True),
    StructField('Other Charges', DoubleType(), True),
    StructField('Insurance Charges', DoubleType(), True),
    StructField('loans_vcl_updated_at', TimestampType(), True),
    StructField('products_vcl_updated_at', TimestampType(), True),
    StructField('borrowers_vcl_updated_at', TimestampType(), True),
    StructField('disbursement_accounts_vcl_updated_at', TimestampType(), True),
    StructField('max_etl_updated_at_from_tbls', TimestampType(), True),
    StructField('pipeline_run_date', StringType(), True)])


previous_days_successful = spark.read.schema(previous_days_successful_schema).parquet(
    f"s3://snowbergsandbox/zeus_disb_format/successful_list_new/", InferSchema=True).alias("pds")

previous_day_unsuccessful = spark.read.parquet(
    "s3://snowbergsandbox/zeus_disb_format/unsuccessful_list/")

fl = spark.read.parquet("s3://snowbergdatalanding/stg_vcl_colending_yuserve/fact_loans", InferSchema=True).filter(
    (f.to_date(f.col('vcl_updated_at'), 'yyyy-MM-dd') == date_filter) | (f.col('id').isin([row["loan_id"] for row in previous_day_unsuccessful.select("loan_id").collect()]))).select("*",
                                                                                                                                                                                      f.to_date(
                                                                                                                                                                                          f.col("client_disbursed_date"), 'yyyy-MM-dd').alias("cdd_new"),
                                                                                                                                                                                      f.to_date(
                                                                                                                                                                                          f.col("first_repayment_date"), 'yyyy-MM-dd').alias("frd_new"),
                                                                                                                                                                                      f.to_date(
                                                                                                                                                                                          f.col("agreement_signed_date"), 'yyyy-MM-dd').alias("asd_new"),
                                                                                                                                                                                      f.to_date(
                                                                                                                                                                                          f.col("disbursed_date"), 'yyyy-MM-dd').alias("dd_new"),
                                                                                                                                                                                      ).alias("fl")

today_updated_loans_df = fl.join(previous_days_successful, previous_days_successful.loan_id ==
                                 fl.id, "left_anti").select(f.col("fl.*")).alias("fl")

fb = spark.read.parquet("s3://snowbergdatalanding/stg_vcl_colending_yuserve/fact_borrowers", InferSchema=True).select("*",
                                                                                                                      f.to_date(f.col("date_of_birth"), 'yyyy-MM-dd').alias("dob_new")).alias("fb")

fpro = spark.read.parquet("s3://snowbergdatalanding/stg_vcl_colending_yuserve/fact_products/", InferSchema=True, pathGlobfilter="*.parquet").filter(
    (f.col("is_deleted") == False) & (f.col("client_namespace_internal").isin(['true', 'whizdm', 'smartcoin', 'moneyboxx', 'axio', 'flexiloan']))).alias("fpro")

fda = spark.read.parquet("s3://snowbergdatalanding/stg_vcl_colending_yuserve/fact_disbursement_accounts/", InferSchema=True).filter(
    (f.col("is_deleted") == False) & (f.col("accountable_type") == 'VivColendingApi::Loan') & (f.col("account_type") == 0)).alias("fda")

frs = spark.read.parquet("s3://snowbergdatalanding/stg_vcl_colending_yuserve/fact_repayment_schedules/",
                         InferSchema=True, pathGlobfilter="*.parquet").filter(f.col("is_deleted") == False).alias("frs")

cutoff_date = '2025-03-21'

print(previous_day_unsuccessful.count())
print(today_updated_loans_df.count())

print(today_updated_loans_df.filter(fl.status.isin(5, 6, 7, 15)).count())

today_updated_loans_df = today_updated_loans_df.filter(
    fl.dd_new >= cutoff_date)

window_spec = Window.orderBy(f.col("cdd_new").asc())

# list to ignore invalid values
invalid_values = ['na', 'NA', 'NULL', 'null', '-', '', 'others', 'Select']

# reading state name mapping
state_name_mapping_df = spark.read.option("InferSchema", True).option("header", True).csv(
    "s3://snowbergdatalanding/state_name_mapping/mapping_state_names.csv").alias("snm")

# taking distinct rows
snm = state_name_mapping_df.select(f.col("State_Derv"), f.col(
    "State"), f.col("Zone")).distinct().alias("snm")


last_name_expr = f.when(
    f.length(f.trim(f.col("fb.last_name"))) > 0, f.col("fb.last_name")
).when(
    (f.split(f.col("fb.first_name"), " ").getItem(1).isNotNull()) &
    (f.split(f.col("fb.first_name"), " ").getItem(1) != ""),
    f.split(f.col("fb.first_name"), " ").getItem(1)
).when(
    (f.col("fb.middle_name").isNotNull()) & (f.col("fb.middle_name") != ""),
    f.col("fb.middle_name")
).otherwise(None)

rs_window_spec = Window.partitionBy("loan_id").orderBy("due_date")

rs_df_cte_1 = frs.withColumn("row_num", f.row_number().over(rs_window_spec))

rs_cte_2a = rs_df_cte_1.filter(f.col("row_num") == 2)\
    .select(f.col("loan_id"), f.col("due_date").alias("second_due_date"), f.dayofmonth("due_date").alias("day_of_month"))

rs_cte_2b = rs_df_cte_1.filter(f.col("row_num") == 3)\
    .select(f.col("loan_id"), f.col("due_date").alias("third_due_date"), f.dayofmonth("due_date").alias("day_of_month"))

rs_cte_2c = rs_cte_2a.alias("a").join(rs_cte_2b.alias("b"), on=["loan_id"], how="inner")\
    .select(
        f.col("a.loan_id"),
        f.col("a.day_of_month").alias("day_of_month_rs_cte_2a"),
        f.col("b.day_of_month").alias("day_of_month_rs_cte_2b")
)

rs_cte_2 = rs_cte_2c.withColumn(
    "day_of_month",
    f.when((f.col("day_of_month_rs_cte_2a").isin(28, 29, 30, 31)) & f.col("day_of_month_rs_cte_2b").isin(28, 29),
           f.col("day_of_month_rs_cte_2a"))
    .otherwise(f.col("day_of_month_rs_cte_2b"))
).select("loan_id", "day_of_month").alias("frs_final")


joins = today_updated_loans_df.alias("fl").join(fpro, today_updated_loans_df.product_id == fpro.id, "inner")\
    .join(fda, fda.accountable_id == today_updated_loans_df.id, "left")\
    .join(fb, fb.id == today_updated_loans_df.borrower_id, "left")\
    .join(rs_cte_2, rs_cte_2.loan_id == today_updated_loans_df.id, "left")\
    .join(snm, f.trim(snm.State_Derv) == f.when((fb.permanent_state.isNull() | f.trim(fb.permanent_state).isin(invalid_values) | (f.length(f.trim(fb.permanent_state)) < 2)),
                                                f.when((fb.current_state.isNull() | f.trim(fb.current_state).isin(invalid_values) | (f.length(f.trim(fb.current_state)) < 2)),
                                                       f.when((fb.current_city.isNull() | f.trim(fb.current_city).isin(invalid_values) | (f.length(f.trim(fb.current_city)) < 2)),
                                                              f.trim(fb.permanent_city))
                                                       .otherwise(f.trim(fb.current_city)))
                                                .otherwise(f.trim(fb.current_state)))
          .otherwise(f.trim(fb.permanent_state)), "left")\
    .select(
    f.row_number().over(window_spec).alias("S.No"),
    fpro.client_namespace_internal.alias("partner_name_short"),
    today_updated_loans_df.id.alias("loan_id"),
    fb.id.alias("borrower_id"),
    fda.id.alias("disbursement_account_id"),
    f.when(fb.id.isNull(), False)
    .otherwise(True)
    .alias("borrower_record_exists"),
    f.when(fda.id.isNull(), False)
    .otherwise(True)
    .alias("disbursement_accounts_record_exists"),
    f.when(fpro.id.isNull(), False)
    .otherwise(True)
    .alias("products_record_exists"),
    f.when(rs_cte_2.loan_id.isNull(), False)
    .otherwise(True)
    .alias("rs_record_exists"),
    today_updated_loans_df.client_loan_id.alias("External Id"),
    f.lit("Vivriti Capital Limited").alias("Entity"),
    f.lit("PERSON").alias("Applicant Type"),
    fpro.name.alias("Asset Class"),
    f.regexp_replace(fb.first_name, '[^a-zA-Z0-9]\s', '').alias("first_name"),
    f.regexp_replace(
        fb.middle_name, '[^a-zA-Z0-9]\s', '').alias("middle_name"),
    f.when(last_name_expr.isNotNull(), f.regexp_replace(
        last_name_expr, "[^a-zA-Z0-9]\s", ""))
    .otherwise(None).alias("last_name"),
    f.round(f.coalesce(f.col("fb.age"),
                       f.floor(f.datediff(f.col("fl.cdd_new"),
                               f.col("fb.dob_new")) / 365)
                       ), 0).alias("age"),
    f.when(f.lower(fb.gender).isin(
        ['male', 'm', 'mele', 'male&nbsp;']), 'Male')
    .when(f.lower(fb.gender).isin(['female', 'femala', 'f', 'fe male', 'fmale']), 'Female')
    .when(f.lower(fb.gender).isin(['o', 'others', 'transgender']), 'Transgender')
    .otherwise(None)
    .alias("gender"),
    f.date_format(fb.dob_new, 'dd-MM-yyyy').alias("DOB"),
    f.upper(f.trim(fb.pan_number)).alias("PAN"),
    fb.aadhar_number.cast("string").alias("Aadhar"),
    f.regexp_replace(fb.voter_id_number, '[^a-zA-Z0-9]', '').alias("Voter ID"),
    f.regexp_replace(fb.passport_number,
                     '[^a-zA-Z0-9]', '').alias("Passport Number"),
    f.regexp_replace(fb.driving_license_number,
                     '[^a-zA-Z0-9]', '').alias("Driving License"),
    f.concat(f.lit("'"), f.coalesce(
        f.when((f.length(f.trim(fb.preferred_address)) > 1) & ~(
            fb.preferred_address.startswith("[B%")), fb.preferred_address)
        .when((f.length(f.trim(fb.permanent_address)) > 1) & ~(fb.permanent_address.startswith("[B%")), fb.permanent_address)
        .when((f.length(f.trim(fb.current_address)) > 1) & ~(fb.current_address.startswith("[B%")), fb.current_address)
        .when((f.length(f.trim(fb.mailing_address)) > 1) & ~(fb.mailing_address.startswith("[B%")), fb.mailing_address)))
    .substr(1, 200).alias("Address"),
    f.coalesce(fb.permanent_pincode, fb.current_pincode).cast(
        "string").alias("Pin Code"),
    f.regexp_replace(fb.mobile_number,
                     '[^0-9]', '').cast("string").alias("Mobile Number"),
    f.when(f.length(f.trim(fb.email)) > 4, fb.email).cast(
        "string").alias("Email Address"),
    f.regexp_replace(
        f.coalesce(
            f.when(f.length(f.trim(fb.permanent_city)) > 1, fb.permanent_city)
            .when(f.length(f.trim(fb.current_city)) > 1, fb.current_city)), '[^a-zA-Z0-9]\s', '')
    .alias("City"),
    f.when(~snm.State_Derv.isin(['zBlank', 'others']),
           f.when(snm.State.startswith("Jammu"), f.lit("Jammu and Kashmir"))
           .when(snm.State.like("%Madhya%"), f.lit('Madhya pradesh'))
           .when((snm.State.startswith('Dadra') | snm.State.startswith('Daman & Diu')), f.lit("Dadra and Nagar Haveli and Daman and Diu"))
           .when(snm.State.startswith("Andaman"), f.lit("Andaman and Nicobar Islands"))
           .when(snm.State.startswith("Arunachal"), f.lit("Arunachal pradesh"))
           .when(snm.State.startswith("Uttar Pradesh"), f.lit("Uttar pradesh"))
           .when(snm.State.startswith("Chhattis"), f.lit("Chattisgarh"))
           .otherwise(snm.State))
    .alias("State"),
    f.date_format(today_updated_loans_df.cdd_new,
                  'dd-MM-yyyy').alias("Submitted On"),
    f.date_format(today_updated_loans_df.cdd_new,
                  'dd-MM-yyyy').alias("Activation Date"),
    f.regexp_replace(f.coalesce(fda.account_name, f.concat(f.trim(fb.first_name), f.lit(" "), f.trim(
        fb.middle_name), f.lit(" "), f.trim(fb.last_name))), '[^a-zA-Z0-9]\s', '').substr(1, 50).alias("Beneficiary Name"),
    f.regexp_replace(fda.account_no, '[^a-zA-Z0-9]',
                     '').cast("string").alias("Beneficiary Account No"),
    f.when(fda.bank_account_type.isNotNull(), fda.bank_account_type)
    .otherwise(f.lit(None)).cast("string").alias("Account Type"),
    f.when(f.length(f.trim(fda.ifsc_code)) > 2, f.trim(fda.ifsc_code))
    .when(f.length(f.trim(fb.ifsc_code)) > 2, f.trim(fb.ifsc_code))
    .otherwise(f.lit(None)).alias("IFSC Code"),
    fda.bank_branch_micr.alias("MICR Code"),
    f.lit(None).cast("string").alias("Swift Code"),
    fb.bank_branch_name.alias("Branch"),
    f.date_format(today_updated_loans_df.cdd_new,
                  'dd-MM-yyyy').alias("Loan Submitted On"),
    f.date_format(today_updated_loans_df.asd_new,
                  'dd-MM-yyyy').alias("agreement signed date"),
    f.date_format(today_updated_loans_df.cdd_new,
                  'dd-MM-yyyy').alias("Disbursed Date"),
    today_updated_loans_df.principal_amount.alias("Principal"),
    today_updated_loans_df.tenure.alias("Loan Term"),
    f.date_format(today_updated_loans_df.frd_new,
                  'dd-MM-yyyy').alias("First Repayment On"),
    #     f.day(fl.first_repayment_date).alias("Due Day"),
    rs_cte_2.day_of_month.alias("Due Day"),
    f.round(today_updated_loans_df.interest_rate, 15).alias("Interest Rate"),
    f.coalesce(today_updated_loans_df.processing_fee,
               f.lit(0)).alias("Processing Fee"),
    f.coalesce(today_updated_loans_df.nach_charges,
               f.lit(0)).alias("Nach Charges"),
    f.coalesce(today_updated_loans_df.dcc_charges,
               f.lit(0)).alias("DCC Charges"),
    f.coalesce(today_updated_loans_df.stamp_duty,
               f.lit(0)).alias("Stamp Duty"),
    f.coalesce(today_updated_loans_df.documentation_charges,
               f.lit(0)).alias("Documentation Charges"),
    f.coalesce(today_updated_loans_df.other_charges,
               f.lit(0)).alias("Other Charges"),
    f.coalesce(today_updated_loans_df.insurance_charges,
               f.lit(0)).alias("Insurance Charges"),
    today_updated_loans_df.etl_updated_at.alias("loans_vcl_updated_at"),
    fpro.etl_updated_at.alias("products_vcl_updated_at"),
    fb.etl_updated_at.alias("borrowers_vcl_updated_at"),
    fda.etl_updated_at.alias("disbursement_accounts_vcl_updated_at"),
    f.greatest(today_updated_loans_df.etl_updated_at, fpro.etl_updated_at,
               fb.etl_updated_at, fda.etl_updated_at).alias("max_etl_updated_at_from_tbls"),
    f.current_date().alias("pipeline_run_date")
).alias("joins")

unsuccessful_df = joins\
    .filter((f.col("borrower_record_exists") == False) |
            (f.col("disbursement_accounts_record_exists") == False) |
            (f.col("products_record_exists") == False) |
            (f.col("rs_record_exists") == False) |
            (f.length(f.trim(f.col("first_name"))) == 0) |
            (f.length(f.trim(f.col("last_name"))) == 0) |
            (f.col("DOB").isNull()) |
            (f.length(f.col("Address")) <= 1) |
            (f.length(f.col("Pin Code")) <= 1) |
            (f.length(f.col("Mobile Number")) < 10) |
            (f.length(f.col("City")) <= 1) |
            (f.length(f.col("State")) <= 1) |
            (f.col("Submitted On").isNull()) |
            (f.col("Activation Date").isNull()) |
            (f.length(f.col("Beneficiary Account No")) <= 1) |
            (f.col("Submitted On").isNull()) |
            (f.col("Principal").isNull()) |
            (f.col("Loan Term").isNull()) |
            (f.col("First Repayment On").isNull()) |
            (f.col("Interest Rate").isNull())
            )\
    .select(f.col("partner_name_short"), f.col("Asset Class").alias("product"), f.col("loan_id"))

unsuccessful_df.coalesce(1).write.mode("overwrite").parquet(
    "s3://snowbergsandbox/zeus_disb_format/unsuccessful_list/")

successful_df = joins.join(unsuccessful_df, joins.loan_id ==
                           unsuccessful_df.loan_id, "left_anti").select("joins.*")


successful_df.coalesce(1).write.mode("overwrite").parquet(
    f"s3://snowbergsandbox/zeus_disb_format/successful_list_new/{current_date}/")
