"""This is my solution to the take home problem 

When run from here aka the terminal, ensure the run_type arg = "python"
When run from a jupyter notebook, set it to "jupyter"

In production, run type wouldn't be a factor since it would only ever run from jupyter or python not both
I was using my local neovim setup/pylint for a better LSP/linting experience which is why I have duplicates

When run with verbose = True, timing/debug messages will be printed
Always, logs will be dumped in ./logs
Images will be dumped in ./img
Output text will be dumped in .out
"""
import logging
import os
import calendar
from pathlib import Path
from zipfile import ZipFile
from shutil import  rmtree
import uuid
import concurrent.futures
import time
import datetime
from typing import Tuple
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, sum, date_trunc, regexp_replace
from pyspark.sql.functions import col, substring, udf, max, explode, split
from pyspark.sql.functions import from_json, to_json
from pyspark.sql.types import LongType, StringType, IntegerType, TimestampType, BooleanType
from pyspark.sql.types import StructType

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib


def init_logging() -> Tuple[logging.Logger, str]:
    """Instantiates the python logger and gets a uuid for this run

    Returns
    logger (logging.Logger): The configured logger
    u (str(UUID)): The uuid as a string
    """
    u = str(uuid.uuid4())
    log_file = f"./logs/{u}.log"
    logging.basicConfig(filename=log_file,
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.DEBUG)

    logging.info("Starting Tiny Town Police Department Analytics Engine")
    logger = logging.getLogger(__name__)
    return logger, u


def get_spark_session() -> SparkSession:
    """Retrieves or creates an active Spark Session for Delta operations
    
    Returns:
        spark (SparkSession): the active Spark Session
    """
    builder = SparkSession \
        .builder \
        .appName('takehome')
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def stage_data() -> None:
    """Unzips the .zip and splits up the datasources into sub-dirs in the ttpd_data dir

    Does Not Return: Could also be accomplished with a bash using find and subprocess
    """
    if os.path.isdir("./ttpd_data"):
        rmtree("./ttpd_data")
    with ZipFile("./ttpd_data.zip", "r") as z:
        z.extractall(path="./")
    

def create_empty_df() -> DataFrame:
    """Returns an empty dataframe

    Returns:
        df (DataFrame): an empty dataframe
    """
    return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema = StructType([]))


def load_people_df(people_dir:str="./ttpd_data") -> DataFrame:
    """Loads the peoples datasource from pipe delimited csvs into a dataframe.

    Parameters:
        people_dir (str): people data directory. Added for future-proofing. Hardcoded for now

    Returns:
        people_df (DataFrame): either the people dataframe or an empty one
    """
    ck = Path(people_dir).glob("*.csv")
    if not list(ck):
        return create_empty_df()

    people_schema = StructType() \
            .add("id", StringType(),True) \
            .add("first_name",StringType(),True) \
            .add("last_name",StringType(),True) \
            .add("sex",StringType(),True) \
            .add("address",StringType(),True) \
            .add("phone_number",StringType(),True) \
            .add("profession",StringType(),True) \
            .add("company",StringType(),True) \
            .add("date_of_birth",StringType(),True)

    people_df = spark.read.format("csv") \
            .option("delimiter", "|") \
            .option("header", "true") \
            .option("mode", "PERMISSIVE") \
            .schema(people_schema) \
            .load(people_dir+"/*.csv")

    return people_df


def load_speeding_df(speeding_dir:str="./ttpd_data") -> DataFrame:
    """Loads the speeding datasource from json into a dataframe

    Parameters:
        speeding_dir (str): speeding data directory. Added for future-proofing. Hardcoded for now
    Returns:
        speeding_df (DataFrame): either the speeding dataframe or an empty one
    """

    ck = Path(speeding_dir).glob("*.json")
    if not list(ck):
        return create_empty_df()
        
    speeding_schema = StructType() \
            .add("id", StringType(),True) \
            .add("ticket_time",TimestampType(),True) \
            .add("license_plate",StringType(),True) \
            .add("officer_id",StringType(),True) \
            .add("speed_limit",IntegerType(),True) \
            .add("recorded_mph_over_limit",IntegerType(),True) \
            .add("school_zone_ind",BooleanType(),True) \
            .add("work_zone_ind",BooleanType(),True)
    

    json_df = spark.read.format("json") \
            .option("mode", "PERMISSIVE") \
            .load(speeding_dir+"/*.json")
    df_with_json = json_df.withColumn("ticket_json", explode(col("speeding_tickets"))) \
            .withColumn("ticket_json_str", to_json(col("ticket_json"))) \
            .withColumn("json_data", from_json(col("ticket_json_str"), speeding_schema))

    speeding_df = df_with_json.select(col("json_data.*"))

    return speeding_df


def load_auto_df(auto_dir:str='./ttpd_data') -> DataFrame:
    """Loads the automobile datasource from xml to a dataframe. Loads it each file as one line in a dataframe, explodes each line into columns based on the "<automobile>" tag, filters out the starting tag info (<? xml-version), and then extracts each
    individual data col by splitting the start and ending tag; i.e. <person_id> </person_id>.

    Caveats:
        Excludes any row with a null column
        Could be more robust, but this works quite well for now

    Parameters:
        auto_dir (str): auto data directory. Added for future-proofing. Hardcoded for now
        columns (List[str]): the xml we want to parse out. defaults to None and is overwritten if so. Added for future-proofing and the dir changes

    Returns:
        auto_df (DataFrame): either the speeding dataframe or an empty one
    """

    ck = Path(auto_dir).glob("*.xml")
    if not list(ck):
        return create_empty_df()

    auto_schema = StructType() \
            .add("person_id", StringType(),True) \
            .add("license_plate",StringType(),True) \
            .add("vin",StringType(),True) \
            .add("color",StringType(),True) \
            .add("year",IntegerType(),True)

    xml_df = spark.read.text(auto_dir+"/*.xml", wholetext=True)
    df = xml_df.select(explode(split(xml_df['value'], '<automobile>'))).where("substring(col, 0,2) != '<?'")
    auto_df = df.select(split(split(df["col"], "<person_id>").getItem(1), "</person_id>").getItem(0).alias("person_id"),
              split(split(df["col"], "<license_plate>").getItem(1), "</license_plate>").getItem(0).alias("license_plate"),
              split(split(df["col"], "<vin>").getItem(1), "</vin>").getItem(0).alias("vin"),
              split(split(df["col"], "<color>").getItem(1), "</color>").getItem(0).alias("color"),
              split(split(df["col"], "<year>").getItem(1), "</year>").getItem(0).alias("year"),
             )
    
    auto_df = auto_df.where("person_id is not null or license_plate is not null or vin is not null or color is not null or year is not null")

    return auto_df


def load_data() -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Attempts to load each of the three datasets and panics if any one shows up empty

    Returns:
        people_df (DataFrame): the people dataset
        speeding_df (DataFrame): the speeding dataset
        auto_df (DataFrame): the automobiles dataset
    """

    # Making this data load happen concurrently PJS 5/7/2024
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        f_people = executor.submit(load_people_df)
        f_speeding = executor.submit(load_speeding_df)
        f_auto = executor.submit(load_auto_df)

        people_df = f_people.result()
        speeding_df = f_speeding.result()
        auto_df = f_auto.result()

    if people_df.isEmpty():
        raise Exception("People DataFrame is Empty!")
    if speeding_df.isEmpty():
        raise Exception("Speeding DataFrame is Empty!")
    if auto_df.isEmpty():
        raise Exception("Automobiles DataFrame is Empty!")

    return people_df, speeding_df, auto_df


def question_one(people_df:DataFrame, speeding_df:DataFrame) -> str:
    """Answers the first question

    Parameters:
        people_df (DataFrame): the people dataset
        speeding_df (DataFrame): the speeding dataset

    Returns:
        res (str): the analysis of question one
    """
    police_df = people_df.where("profession = 'Police Officer'")
    officer_grouped = speeding_df.groupBy("officer_id").agg(count("id").alias("ticket_count"))
    max_val = officer_grouped.agg(max("ticket_count")).collect()[0][0]
    res = officer_grouped.filter(col("ticket_count") == max_val)
    joined_df = res.join(police_df, res.officer_id == police_df.id, "inner").select("first_name", "last_name", "ticket_count")

    # naive get max val then get index of max val approach can return multiple rows if theres a tie for first
    # create a test case for this
    officers = []
    t_count = 0 
    for row in joined_df.collect():
        officers.append(f"{row['first_name']} {row['last_name']}")
        t_count = row['ticket_count']
    out_officers = ','.join(officers)

    return f"Officer(s) {out_officers} distributed the most speeding tickets: {t_count}"


def question_two(speeding_df:DataFrame) -> str:
    """Answers the second question

    Parameters:
        speeding_df (DataFrame): the speeding dataset

    Returns:
        stout (str): the analysis of question two
    """
    speeding_df = speeding_df.withColumn("yyyymm", regexp_replace(substring("ticket_time", 0,7), '-', ''))
    time_grouped = speeding_df.groupBy("yyyymm").agg(count("id").alias("ticket_count"))
    out = time_grouped.sort("ticket_count", ascending=False).take(3)
    stout= 'These are the top three months by total tickets written\n\t'
    for row in out:
        stout += f"{calendar.month_name[int(row['yyyymm'][4:6])]} {row['yyyymm'][:4]}: {row['ticket_count']} Tickets Written\n\t"

    return stout[:-2]


@udf(returnType=LongType())
def calc_ticket_cost(school_zone_ind:bool, work_zone_ind:bool) -> int:
    """A spark sql user defined function (udf) to calculate a tickets cost to the driver.

    Parameters:
        school_zone_ind (bool): 1 if school 0 if not
        work_zone_ind(bool): 1 if work 0 if not            

    Returns:
        cost (int): the resultng cost of the ticket
    """
    # this would be replaced with a call to some API in production, hardcoded for now
    ticket_config = {'base':30, 'school':60, 'work': 60, 'school+work':120}
    if not ticket_config:
        raise Exception ("can't access ticket price database")

    cost = ticket_config['base']
    if school_zone_ind:
        cost += ticket_config['school']
    if work_zone_ind:
        cost += ticket_config['work']
    if school_zone_ind and work_zone_ind:
        cost = ticket_config['school+work']

    return cost


def question_three(people_df:DataFrame, speeding_df:DataFrame, auto_df:DataFrame) -> str:
    """Answers the third question

    Parameters:
        people_df (DataFrame): the people dataset
        speeding_df (DataFrame): the speeding dataset
        auto_df (DataFrame): the automobiles dataset

    Returns:
        stout (str): the analysis of question three
    """
    speeding_df = speeding_df.withColumn('ticket_cost', calc_ticket_cost(col('school_zone_ind'), col('work_zone_ind')))
    all_joined = auto_df.join(speeding_df, auto_df.license_plate == speeding_df.license_plate, "inner") \
                .join(people_df, people_df.id == auto_df.person_id, "inner") \
                .select(auto_df.person_id, speeding_df.ticket_cost)
    person_grouped = all_joined.groupBy("person_id").agg(sum("ticket_cost").alias("total_ticketed_amount"))
    pg=person_grouped.alias("pg")
    res = pg.join(people_df, people_df.id == pg.person_id, "inner").orderBy("total_ticketed_amount", ascending=False)
    out=res.select("first_name", "last_name", "total_ticketed_amount").take(10)

    stout= 'These are the top ten most ticketed drivers by total ticket dollars levied\n\t'
    for row in out:
        stout += f"{row['first_name']} {row['last_name']}: ${row['total_ticketed_amount']}\n\t"

    return stout[:-2]


def bonus(speeding_df:DataFrame) -> str:
    """Answers the bonus question

    Parameters:
        speeding_df (DataFrame): the speeding dataset

    Returns:
        res_b (str): the analysis of the bonus question 
    """
    speeding_df = speeding_df.withColumn('ticket_cost', calc_ticket_cost('school_zone_ind', 'work_zone_ind'))
    speeding_df = speeding_df.withColumn("year", date_trunc("year", "ticket_time"))
    speeding_df = speeding_df.withColumn("month", date_trunc("month", "ticket_time"))
    yyyymm_grouped = speeding_df.groupBy("month").agg(count("id").alias("ticket_count"))
    yyyy_grouped = speeding_df.groupBy("year").agg(count("id").alias("ticket_count"))

    pd_yyyymm = yyyymm_grouped.toPandas()
    pd_yyyymm["month"] = pd.to_datetime(pd_yyyymm["month"])
    pd_yyyymm.set_index("month", inplace=True, drop=True)
    pd_yyyymm.sort_index(inplace=True)
    pd_yyyy = yyyy_grouped.toPandas()
    pd_yyyy["year"] = pd.to_datetime(pd_yyyy["year"])
    pd_yyyy.set_index("year", inplace=True, drop=True)
    pd_yyyy.sort_index(inplace=True)

    if run_type == "python" or not verbose:
        matplotlib.use('agg')
    fig, axs = plt.subplots(2, figsize=(30, 15))
    plt.ioff()
    axs[0].set(xlabel="Year and Month",
       ylabel="Total Tickets Written",
       title="Month over Month Speeding Tickets\nTiny Town Police Department 2020-2023")

    axs[1].set(xlabel="Year",
       ylabel="Total Tickets Written",
       title="Year over Year Speeding Tickets\nTiny Town Police Department 2020-2023")

    plt.setp(axs[0].get_xticklabels(), rotation=45)
    plt.setp(axs[1].get_xticklabels(), rotation=45)
    pd_yyyymm.plot.bar(ax=axs[0], y="ticket_count")
    pd_yyyy.plot.bar(ax=axs[1], y="ticket_count")
    plt.tight_layout()
    plt.savefig(f'./img/{u}.png', bbox_inches='tight')
    if verbose and run_type == "jupyter":
        plt.show()
    plt.close(fig)
    res = """
        Looking year-over-year, the number of tickets written increases over time.
         Drilling down and looking month-over-month, the number of tickets written follows a seasonal pattern.
         - There first is a slight spike at the beginning of the year, just in January.
         - Numbers dwindle as Winter gives way to Spring but begin to tick back up when the weather warms up. 
             - May is the start of the Summer increase, which see's its peak around July, then tapers down through September and fully wanes through the advent of Autumn.
         - The end of the year, as evidenced by the prior Top Three Months analysis, is where the lions share of tickets are written. 
             - December ranks in as either the highest or second highest month of each year by volume, potentially indicating the use of an annualized ticket quota system 
             - Maybe a scramble to meet it before the year completes that carries on into the start of the year, followed by a Spring lull.
             - The Summer Swell could correlate with a mid-year goals check-in or just increased number of motorists driving/speeding in warmer weather.
    """

    return res


def answer_questions(people_df:DataFrame, speeding_df:DataFrame, auto_df:DataFrame) -> Tuple[str,str,str,str]:
    """Entrypoint to answer all questions concurrently

    Parameters:
        people_df (DataFrame): the people dataset
        speeding_df (DataFrame): the speeding dataset
        auto_df (DataFrame): the automobiles dataset

    Returns:
        q1 (str): the analysis of question one
        q2 (str): the analysis of question two
        q3 (str): the analysis of question three
        b (str): the analysis of the bonus question 
    """

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        f_1 = executor.submit(question_one, people_df, speeding_df)
        f_2 = executor.submit(question_two, speeding_df)
        f_3 = executor.submit(question_three, people_df, speeding_df, auto_df)
        f_b = executor.submit(bonus, speeding_df)

        q1 = f_1.result()
        q2 = f_2.result()
        q3 = f_3.result()
        b = f_b.result()

    return q1, q2, q3, b


def main() -> bool:
    """The main application entrypoint

    Returns:
        (bool): true if successful, false if not
    """

    logger.info("Data Staging Start")
    t1 = time.perf_counter()
    try:
        stage_data()
    except Exception as e:
        logger.exception(e)
        t2 = time.perf_counter()
        s = f"Time Elapsed {t2 - t1:0.4f} seconds"
        msg = "Data Staging Failure, Killing App"
        return False
    else:
        t2 = time.perf_counter()
        s = f"Time Elapsed {t2 - t1:0.4f} seconds"
        msg = "Data Staging Success"
    finally:
        res = f"{msg}: {s}"
        logger.info(res)
        if verbose:
            print(res)


    logger.info("Data Load Start")
    t1 = time.perf_counter()
    try:
        people_df, speeding_df, auto_df = load_data()
    except Exception as e:
        logger.exception(e)
        t2 = time.perf_counter()
        s = f"Time Elapsed {t2 - t1:0.4f} seconds"
        msg = "Data Load Failure, Killing App"
        return False
    else:
        t2 = time.perf_counter()
        s = f"Time Elapsed {t2 - t1:0.4f} seconds"
        msg = "Data Load Success"
    finally:
        res = f"{msg}: {s}"
        logger.info(res)
        if verbose:
            print(res)
 

    logger.info("Questions Start")
    t1 = time.perf_counter()
    try:
        q1, q2, q3, b = answer_questions(people_df, speeding_df, auto_df)
    except Exception as e:
        logger.exception(e)
        t2 = time.perf_counter()
        s = f"Time Elapsed {t2 - t1:0.4f} seconds"
        msg = "Questions Failure, Killing App"
        return False
    else:
        t2 = time.perf_counter()
        s = f"Time Elapsed {t2 - t1:0.4f} seconds"
        msg = "Questions Success"
    finally:
        res = f"{msg}: {s}"
        logger.info(res)
        if verbose:
            print(res)

    spark.stop()

    output= f"""
    Tiny Town Police Department Ticketing Analysis:
    1. Which police officer was handed the most speeding tickets?
        {q1}
    2. What 3 months (year + month) had the most speeding tickets? 
        {q2}
    3. Using the ticket fee table below, who are the top 10 people who have spent the most money paying speeding tickets overall?
        {q3}
    Bonus: What overall month-by-month or year-by-year trends, if any, do you see?
        {b}
            """
    if verbose:
        print(output)
    with open(f"./out/{u}.txt", "w") as f:
        f.write(output)

    t1 = time.perf_counter()
    s = f"Time Elapsed {t1 - t0:0.4f} seconds"
    msg = "Successfully Completed"
    res = f"{msg}: {s}"
    logger.info(res)
    if verbose:
        print(res)

    return True


if __name__ == '__main__':
    global spark
    global u
    global verbose
    global run_type

    verbose = True
    run_type = "python"
    
    try:
        logger, u = init_logging()
    except Exception as e:
        print(e)
        sys.exit()

    t0 = time.perf_counter()
    logger.info("Time Start: %s", datetime.datetime.now())
    if verbose:
        print(f"Time Start: {datetime.datetime.now()}")


    logger.info("Spark Init Start")
    t1 = time.perf_counter()
    try:
        spark: SparkSession= get_spark_session()
    except Exception as e:
        logger.exception(e)
        t2 = time.perf_counter()
        s = f"Time Elapsed {t2 - t1:0.4f} seconds"
        msg = "Spark Init Failure, Killing App"
        sys.exit()
    else:
        t2 = time.perf_counter()
        s = f"Time Elapsed {t2 - t1:0.4f} seconds"
        msg = "Spark init success"
    finally:
        res = f"{msg}: {s}"
        logger.info(res)
        if verbose:
            print(res)

    res = main()

    logger.info("Time Stop: %s", datetime.datetime.now())
    if verbose:
        if res:
            print(f"Process finished successfully at {datetime.datetime.now()}")
        else: 
            print(f"Process failed at {datetime.datetime.now()}")

