import findspark
findspark.init('/home/nikhil/spark-2.4.4-bin-hadoop2.7/')
import pyspark
import configparser

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import hour,year,month,weekofyear,dayofweek,dayofmonth,dayofyear
from pyspark.sql.types import TimestampType
from pyspark.sql import functions as F
from pyspark.sql.functions import udf,col



config=configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

def process_songs_data(spark,input_data,output_data):
	song_data=spark.read.json(input_data+'/song_data/*/*/*/*.json')
	songs_table=song_data.selectExpr('title','artist_id','year','duration').dropDuplicates().withColumn('songs_id',monotonically_increasing_id())
	songs_table.write.parquet(output_data+'songs.parquet',mode='overwrite')

	artist_table=song_data.selectExpr('artist_name as name','artist_location as location','artist_longitude as longitude','artist_longitude as longitude','artist_location as location').distinct().withColumn('artist_id',monotonically_increasing_id())
	artist_table.write.mode('overwrite').parquet('artists.parquet')




def process_log_data(spark,input_data,output_data):
	log_data=spark.read.json(input_data+'/log-data/*.json')

	user_table=log_data.selectExpr('UserId as users_ID','firstName as first_name','lastName as last_name','gender','level').dropDuplicates()
	user_table.write.mode('overwrite').parquet(output_data+'users.parquet')


	log_data=log_data.withColumn('time',F.to_timestamp(F.from_unixtime(col('ts')/1000)).cast('Timestamp'))

	time_table=log_data.dropDuplicates().withColumn('year',year(col('time'))).withColumn('hour',hour(col('time'))).withColumn('day',dayofmonth(col('time'))).withColumn('week',dayofweek(col('time'))).withColumn('month',dayofmonth(col('time'))).withColumn('weekofyear',weekofyear(col('time'))).select('time','weekofyear','year','hour','day','week','month')

	time_table.write.parquet(output_data+'time_table.parquet',mode='overwrite')
	song_data=spark.read.json(input_data+'/song_data/*/*/*/*.json')


	combined=song_data.withColumn('userIds',monotonically_increasing_id()).join(log_data,song_data.title==log_data.song)
	songplays=combined.selectExpr('time','userIds','level','song_id','artist_id','sessionId','location','userAgent','year').dropDuplicates()


	songplays.write.mode('overwrite').parquet(output_data+'songsplay.parquet')

















def main():
	"extract the data from the s3 and create tables defined on the facts and dimension table"

	spark=SparkSession.builder.appName('etl').getOrCreate()

	input_data="s3a://udacity-dend/"
	output_data="s3a://sparkify-data/"

	process_songs_data(spark,input_data,output_data)
	process_log_data(spark,input_data,output_data)






if __name__=='__main__':
	main()





