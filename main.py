import traceback
import sys
import logging
import traceback
import sys
import logging
import os
import json
import xml.etree.ElementTree as ET 
import calendar
from pathlib import Path
from zipfile import ZipFile 
import shutil
import uuid

from delta import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import StructType

import pandas as pd
import matplotlib.pyplot as plt


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
    logger = logging.getLogger('urbanGUI')
    return logger, u


def get_spark_session() -> SparkSession:
    """Retrieves or creates an active Spark Session for Delta operations
    
    Returns:
        spark (SparkSession): the active Spark Session
    """
    builder = SparkSession \
        .builder \
        .appName('takehome') \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    return configure_spark_with_delta_pip(builder).getOrCreate()


def stage_data() -> None:
    """Unzips the .zip and splits up the datasources into sub-dirs in the ttpd_data dir

    Does Not Return: Could also be accomplished with a bash script using find and using subprocess.run()
    """
    if os.path.isdir("./ttpd_data"):
        shutil.rmtree("./ttpd_data")
    with ZipFile("./ttpd_data.zip", "r") as z:
        z.extractall(path="./")
    os.makedirs("./ttpd_data/people")    
    os.makedirs("./ttpd_data/speeding")
    os.makedirs("./ttpd_data/automobiles")
    source_map={".csv":"people", ".json":"speeding", ".xml":"automobiles"}    
    files = Path("./ttpd_data").glob("*")
    for file in files:
        f_base= os.path.basename(file)
        _, f_ext = os.path.splitext(file)
        if f_ext:
            new_dir = f"./ttpd_data/{source_map[f_ext]}/{f_base}"     
            shutil.move(file, new_dir)
    shutil.rmtree("./__MACOSX")


def load_people_df(spark:SparkSession, peopleDir:str=None) -> DataFrame:
    """Loads the peoples datasource from pipe delimited csvs into a dataframe.
    Hardcoded the landing dir for now but left open the possibility of having alternates

    Parameters:
        spark (sparkSession): sparkSession to use Spark
        peopleDir (str): people data directory. defaults to None and is overwritten if so. Added for future-proofing and the dir changes

    Returns:
        peopleDf (DataFrame): either the people dataframe or an empty one
    """
    if not peopleDir:        
        peopleDir = "./ttpd_data/people"
    if not os.path.exists(peopleDir):
        return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema = StructType([]))
    peopleDf = spark.read.csv(peopleDir, header=True, inferSchema=True, sep="|")
    
    return peopleDf


def load_speeding_df(spark:SparkSession, speedingDir=None) -> DataFrame:
    """Loads the speeding datasource from json into a dataframe

    Parameters:
        spark (sparkSession): sparkSession to use Spark
        speedingDir (str): speeding data directory. defaults to None and is overwritten if so. Added for future-proofing and the dir changes

    Returns:
        speedingDf (DataFrame): either the speeding dataframe or an empty one
    """
    if not speedingDir:        
        speedingDir = "./ttpd_data/speeding"
    if not os.path.exists(speedingDir):
        return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema = StructType([]))
        
    files = Path(speedingDir).glob("*.json")
    speedingData=[]
    for f in files:
        with open(f, 'r') as j:
            data=json.load(j)
            speedingData.append(json.dumps(data['speeding_tickets']))
    speedingDf=spark.read.json(spark.sparkContext.parallelize(speedingData))
    
    return speedingDf


def load_auto_df(spark:SparkSession, autoDir=None, columns:List[str]=None) -> DataFrame:
    """Loads the automobile datasource from xml to a dataframe. Parses each file 1 by 1 appending to a list of lists then creating a dataframe from it.
    was having trouble getting the databricks jar to work nicely to load an xml file directly into Spark. Whipped this up to handle things instead.

    Parameters:
        spark (sparkSession): sparkSession to use Spark
        autoDir (str): auto data directory. defaults to None and is overwritten if so. Added for future-proofing and the dir changes
        columns (List[str]): the xml we want to parse out. defaults to None and is overwritten if so. Added for future-proofing and the dir changes

    Inner Functions:
        parseXML(): do the xml parsing and return a list of lists

    Returns:
        autoDf (DataFrame): either the speeding dataframe or an empty one
    """
    def parseXML(xmlfile:str, columns:List[str]) -> List[List[str]]: 
        """An xml file parser, highly customized to this problemset.

        Parameters:
            xmlfile (str): The path of an xml file to be parsed
            columns (List[str]): A list of xml tags we want to extract and determinisitically enforce their existance

        Returns:
            data (List[List[str]]): A list of lists, each sublist containing one automobile tag from the xml
        """
        root = ET.parse(xmlfile) 
        data = []
        empty_line = {}
        for c in columns:
            empty_line[c] = None
        line = empty_line
        for child in root.iter():
            if child.tag == 'automobiles':
                continue
            if child.tag == 'automobile':
                # the first iteration will add a [None]. This check stops that
                if line["person_id"]:
                # if line[0]:
                    data.append(list(line.values()))
                line = empty_line
            else:
                if child.tag not in columns:
                    raise Exception("Malformed XML which will break parsing")
                line[child.tag] = child.text
                
        return data
        
    if not autoDir:        
        autoDir = './ttpd_data/automobiles'
    if not os.path.exists(autoDir):
        return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema = StructType([]))
    if not columns:        
        columns = ["person_id", "license_plate", "vin", "color", "year"]
        
    files = Path(autoDir).glob("*.xml")
    xmlData=[]
    for f in files:
        xmlData += parseXML(f, columns)
    autoDf = spark.createDataFrame(xmlData, columns)

    return autoDf


def load_data(spark:SparkSession) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Attempts to load each of the three datasets and panics if any one shows up empty

    Returns:
        peopleDf (DataFrame): the people dataset
        speedingDf (DataFrame): the speeding dataset
        autoDf (DataFrame): the automobiles dataset
    """
    peopleDf= load_people_df(spark)
    if peopleDf.isEmpty():
        raise Exception("People DataFrame is Empty!")
        
    speedingDf = load_speeding_df(spark)    
    if speedingDf.isEmpty():
        raise Exception("Speeding DataFrame is Empty!")
        
    autoDf = load_auto_df(spark)
    if autoDf.isEmpty():
        raise Exception("Automobiles DataFrame is Empty!")
        
    return peopleDf, speedingDf, autoDf


def question_one(peopleDf:DataFrame, speedingDf:DataFrame) -> str:
    """Answers the first question

    Parameters:
        peopleDf (DataFrame): the people dataset
        speedingDf (DataFrame): the speeding dataset

    Returns:
        (str): the analysis of question one
    """
    policeDf = peopleDf.where("profession = 'Police Officer'")
    officer_grouped = speedingDf.groupBy("officer_id").agg(count("id").alias("ticket_count"))
    max_val=officer_grouped.agg(max("ticket_count")).collect()[0][0]
    res= officer_grouped.filter(col("ticket_count") == max_val)
    joinedDF= res.join(policeDf, res.officer_id == policeDf.id, "inner").select("first_name", "last_name", "ticket_count")

    # naive get max val then get index of max val approach can return multiple rows if theres a tie for first
    # create a test case for this
    officers=[]
    for row in joinedDF.collect():
        officers.append(f"{row['first_name']} {row['last_name']}")
    out_officers = ','.join(officers)
    return f"Officer(s) {out_officers} distributed the most speeding tickets: {row['ticket_count']}"


def question_two(speedingDf:DataFrame) -> Tuple[str, DataFrame]:
    """Answers the second question

    Parameters:
        speedingDf (DataFrame): the speeding dataset

    Returns:
        stout (str): the analysis of question two
        speedingDf (DataFrame): speedingDf updated with a new col, yyyymm
    """
    speedingDf = speedingDf.withColumn("yyyymm", regexp_replace(substring("ticket_time", 0,7), '-', ''))
    time_grouped = speedingDf.groupBy("yyyymm").agg(count("id").alias("ticket_count"))
    out= time_grouped.sort("ticket_count", ascending=False).take(3)
    stout= 'These are the top three months by total tickets written\n\t'
    for row in out:
        stout += f"{calendar.month_name[int(row['yyyymm'][4:6])]} {row['yyyymm'][:4]}: {row['ticket_count']} Tickets Written\n\t"
    return stout[:-2], speedingDf


def question_three(peopleDf:DataFrame, speedingDf:DataFrame, autoDf:DataFrame) -> Tuple[str, DataFrame]:
    """Answers the third question

    Parameters:
        peopleDf (DataFrame): the people dataset
        speedingDf (DataFrame): the speeding dataset
        autoDf (DataFrame): the automobiles dataset

    Inner Functions:
        calc_ticket_cost(): a udf to calculate ticket cost

    Returns:
        stout (str): the analysis of question three
        speedingDf (DataFrame): speedingDf updated with a new col, ticket_cost
    """
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
        
    speedingDf = speedingDf.withColumn('ticket_cost', calc_ticket_cost('school_zone_ind', 'work_zone_ind'))
    all_joined = autoDf.join(speedingDf, autoDf.license_plate == speedingDf.license_plate, "inner") \
                .join(peopleDf, peopleDf.id == autoDf.person_id, "inner") \
                .select(autoDf.person_id, speedingDf.ticket_cost) 
    person_grouped = all_joined.groupBy("person_id").agg(sum("ticket_cost").alias("total_ticketed_amount"))
    pg=person_grouped.alias("pg")
    res = pg.join(peopleDf, peopleDf.id == pg.person_id, "inner").orderBy("total_ticketed_amount", ascending=False)
    out=res.select("first_name", "last_name", "total_ticketed_amount").take(10)
    
    stout= 'These are the top ten most ticketed drivers by total ticket dollars levied\n\t'
    for row in out:
        stout += f"{row['first_name']} {row['last_name']}: ${row['total_ticketed_amount']}\n\t"
    return stout[:-2], speedingDf


def bonus(speedingDf:DataFrame, u:str, verbose:bool=False) -> str:
    """Answers the bonus question

    Parameters:
        speedingDf (DataFrame): the speeding dataset
        u (str): the uuid of a given applicaiton run
        verbose (bool): toggle verbose execution

    Returns:
        res (str): the analysis of the bonus question 
    """
    speedingDf = speedingDf.withColumn("year", date_trunc("year", "ticket_time"))                                       
    speedingDf = speedingDf.withColumn("month", date_trunc("month", "ticket_time"))
    yyyymm_grouped = speedingDf.groupBy("month").agg(count("id").alias("ticket_count"))    
    yyyy_grouped = speedingDf.groupBy("year").agg(count("id").alias("ticket_count"))

    pd_yyyymm = yyyymm_grouped.toPandas()
    pd_yyyymm["month"] = pd.to_datetime(pd_yyyymm["month"])
    pd_yyyymm.set_index("month", inplace=True, drop=True)
    pd_yyyymm.sort_index(inplace=True)
    pd_yyyy = yyyy_grouped.toPandas()
    pd_yyyy["year"] = pd.to_datetime(pd_yyyy["year"])
    pd_yyyy.set_index("year", inplace=True, drop=True)
    pd_yyyy.sort_index(inplace=True)

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
    if verbose:
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


def main(verbose:bool=False) -> bool:
    """The main application entrypoint

    Parameters:
        verbose (bool): toggle verbose execution

    Returns:
        (bool): true if successful, false if not
    """
    try:
        logger, u = init_logging()
    except Exception:
        return False
        
    logger.info("Spark Init Start")
    try:
        spark: SparkSession= get_spark_session()
    except Exception:
        logger.error(traceback.print_exception(*sys.exc_info()))
        logger.info("Spark Init Failure, Killing App")
        return False
    logger.info("Spark init success")

    logger.info("Data Staging Start")
    try:
        stage_data()
    except Exception:
        traceback.print_exception(*sys.exc_info())
        logger.info("Data Staging Failure, Killing App")
        return False
    logger.info("Data Staging Success")
    
    logger.info("Data Load Start")
    try:
        peopleDf, speedingDf, autoDf = load_data(spark)
    except Exception:
        traceback.print_exception(*sys.exc_info())
        logger.info("Data Load Failure, Killing App")
        return False
    logger.info("Data Load Success")

    logger.info("Question One Start")
    try:
        q1 = question_one(peopleDf, speedingDf)
    except Exception:
        traceback.print_exception(*sys.exc_info())
        logger.info("Question One Failure, Killing App")
        return False
    logger.info("Question One Success")

    logger.info("Question Two Start")
    try:
        q2, speedingDf = question_two(speedingDf)
    except Exception:
        traceback.print_exception(*sys.exc_info())
        logger.info("Question Two Failure, Killing App")
        return False
    logger.info("Question Two Success")

    logger.info("Question Three Start")
    try:
        q3, speedingDf = question_three(peopleDf, speedingDf, autoDf)
    except Exception:
        traceback.print_exception(*sys.exc_info())
        logger.info("Question Three Failure, Killing App")
        return False
    logger.info("Question Three Success")

    try:
        b = bonus(speedingDf, u, verbose)
    except Exception:
        traceback.print_exception(*sys.exc_info())
        logger.info("Bonus Failure, Killing App")
        return False
    logger.info("Bonus Success")
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

    return True


if __name__ == '__main__':
    main(verbose=False)
