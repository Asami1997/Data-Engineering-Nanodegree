from pyspark.sql import functions as f
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import date_add as d_add

"""
all the functions here are going to be statis methods menaing that they
will be bound to the class itself and not an object so no need to create an object 
to use them.

"""
class Cleaner:
    
    @staticmethod
    def clean_194_dataset(df_194,i94port_valid):
        """
        This function will clean the 194 dataset by the performing the below
        
        1) typecasting the column names into the proper format
        2) rename the column names into understandable names
        
        param : 194 immigrartion dataset
        return : cleaned 194 immigration dataset
        """ 
        #typecasting
        df_194 = df_194.withColumn('cicid',f.col("cicid").cast("integer"))
        df_194 = df_194.withColumn('i94yr',f.col("i94yr").cast("integer"))
        df_194 = df_194.withColumn('i94mon',f.col("i94mon").cast("integer"))
        df_194 = df_194.withColumn('i94cit',f.col("i94cit").cast("integer"))
        df_194= df_194.withColumn('i94res',f.col("i94res").cast("integer"))
        df_194 = df_194.withColumn('biryear',f.col("biryear").cast("integer"))
        df_194 = df_194.withColumn('data_base_sas',f.to_date(f.lit('01/01/1960'),'MM/dd/yyyy'))
        print(df_194.printSchema())
        df_194 = df_194.withColumn('arrdate',f.expr("date_add(data_base_sas, arrdate)"))
        df_194 = df_194.withColumn('depdate',f.expr("date_add(data_base_sas, depdate)"))       
        
        #renaming
        df_194 = df_194.withColumnRenamed("i94port","port_code")
        df_194 = df_194.withColumnRenamed("i94addr","state_code")
        df_194 = df_194.withColumnRenamed("i94yr","year")
        df_194 = df_194.withColumnRenamed("i94mon","month")
        df_194 =df_194.withColumnRenamed("arrdate","arrival_date")
        df_194 = df_194.withColumnRenamed("depdate","departure_date")
        df_194 = df_194.withColumnRenamed("biryear","birthday_year")
        df_194 = df_194.withColumnRenamed("fltno","flight_number")
        
        df_194 = df_194.filter(df_194.port_code.isin(list(i94port_valid.keys())))
        
        
        
        return df_194
    
    @staticmethod
    def clean_temp_dataset(df_temp):
        
        """
        This function cleans the temperature data by the performing the below
        
        1) remove values with NA averages
        2) remove duplicates, keep first
        3) rename columns
        param : temeparature data
        
        """
       
        # remove NaN
        df_temp = df_temp.filter(df_temp.AverageTemperature != 'NaN')
    
        # remove duplicates across the city and country
        df_temp = df_temp.dropDuplicates(['City','Country'])
         
        df_temp = df_temp.withColumnRenamed("AverageTemperature","average_temperature")    
        
        return df_temp
                                 
    @staticmethod
    def clean_demographic_dataset(df_demo):
           
         df_demo = df_demo.filter(f.col('Average Household Size').isNull() == False)
         
         df_demo = df_demo.withColumnRenamed('State Code','state_code')    
         df_demo = df_demo.withColumnRenamed('Average Household Size','average_household_size')
         df_demo = df_demo.withColumnRenamed('Number of Veterans','number_of_veterans')
         df_demo = df_demo.withColumnRenamed('Total Population','total_population')
         df_demo = df_demo.withColumnRenamed('Female Population','female_population')
         df_demo = df_demo.withColumnRenamed('Male Population','male_population')
         df_demo = df_demo.withColumnRenamed('Median Age','median_age')
        
         return df_demo 