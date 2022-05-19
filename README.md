# Clinical_Trails_Analysis


dbutils.fs.ls ("dbfs:/FileStore/tables/")



dbutils.fs.head ('dbfs:/FileStore/tables/clinicaltrial_2019.csv')



dbutils.fs.head ("dbfs:/FileStore/tables/pharma.csv")



pharmaDF=spark.read.options(header=True,delimiter=',')\
    .csv('dbfs:/FileStore/tables/pharma.csv')



pharmaDF.show(10)



pharmaDF.select("Case_ID","Private_Litigation_Case_Title","Company","Parent_Company","Specific_Industry_of_Parent").show(1000)




ctraildf=spark.read.csv('dbfs:/FileStore/tables/clinicaltrial_2019.csv', header=True)



ctraildf.printSchema()


ctraildf2=spark.read.options(header=True,delimiter='|')\
    .csv('dbfs:/FileStore/tables/clinicaltrial_2019.csv')\
    .withColumnRenamed('Completion', 'Completion')


ctraildf2.show()




ctraildf2.distinct().count()



ctraildf2.printSchema()




from pyspark.sql.functions import col
ctraildf2.groupby("Type")\
         .count()\
         .withColumnRenamed ("count", "Count")\
         .orderBy(col("Count").desc())\
         .show()




ctraildf2.select('Conditions').show(5,truncate=False)



from pyspark.sql.functions import split,col,explode
ctraildf3 = ctraildf2.select(explode(split(col('conditions'), ","))).withColumnRenamed("col","Conditions")
ctraildf3.show(1000, truncate=False)


from pyspark.sql.functions import col
ctraildf3.groupby("Conditions")\
         .count()\
         .withColumnRenamed ("count", "Counted")\
         .orderBy(col("Counted").desc())\
         .show(5)



dbutils.fs.head ("dbfs:/FileStore/tables/mesh.csv")

-

meshDF=spark.read.options(header=True,delimiter=',')\
    .csv('dbfs:/FileStore/tables/mesh.csv')



meshDF.withColumnRenamed('term', 'Term').show()



from pyspark.sql.functions import split,col,explode
meshDF2 = meshDF.select(explode(split(col('term'), ",")), 'tree').withColumnRenamed("col","Term").withColumnRenamed("tree","Tree")
meshDF2.show(1000)



consolidatedDF = ctraildf3.join(meshDF,ctraildf3.Conditions ==  meshDF.term,"inner")



consolidatedDF.show(1000,truncate=False)



from pyspark.sql.functions import expr
consolidatedDF2=consolidatedDF.select(expr('LEFT(tree, 3)'),'Conditions').withColumnRenamed('left(Tree, 3)', 'MeshID')


consolidatedDF2.show(1000, truncate=False)



consolidatedDF2.groupBy("MeshID")\
               .count()\
               .withColumnRenamed('count', 'Count')\
               .orderBy(col("Count").desc())\
               .show(10,truncate=False)





ctraildf5=ctraildf2.select("Sponsor")



ctraildf5.show(100, truncate=False)



pharmaDF5=pharmaDF.select("Company","Parent_Company","Specific_Industry_of_Parent")



pharmaDF5.show(1000, truncate=False)

-

pharmaCtrailDF1 = ctraildf5.join(pharmaDF5,ctraildf5.Sponsor == pharmaDF5.Parent_Company,"left")



pharmaCtrailDF1.show(10000)



pharmaCtrailDF2=pharmaCtrailDF1.select('Sponsor','Parent_Company').withColumnRenamed('Parent_Company','Parent').withColumnRenamed('Sponsor','Sponsor').filter(col("Parent").isNull())



pharmaCtrailDF2.show(100000, truncate=False)



pharmaCtrailDF11=pharmaCtrailDF2.select('Sponsor')\
               .groupBy('Sponsor')\
               .count()\
               .withColumnRenamed ('count', 'Count')\
               .orderBy(col("Count").desc())


pharmaCtrailDF11.show(10, truncate=False)





ctraildf6=ctraildf2.select('Status','Completion')


ctraildf6.show()



ctraildf7=ctraildf6.select('Status','Completion').withColumnRenamed('Status','Status').withColumnRenamed('Completion','Completion').filter(ctraildf6.Status=='Completed')


ctraildf7.show(100)



ctraildf8=ctraildf7.select('Status','Completion').withColumnRenamed('Status','Status').withColumnRenamed('Completion','Completion').filter(ctraildf7.Completion!='null')



ctraildf8.show()



from pyspark.sql.functions import expr
ctraildf9=ctraildf8.select(expr('LEFT(Completion, 3)'),expr('RIGHT(Completion, 4)'))\
                   .withColumnRenamed('left(Completion, 3)','Month')\
                   .withColumnRenamed('right(Completion, 4)','Year')



ctraildf9.show()



ctraildf10=ctraildf9.select('Month','Year').withColumnRenamed('Month','Month').withColumnRenamed('Year','Year').filter(ctraildf9.Year=='2019')



ctraildf10.show()



ctraildf11=ctraildf10.groupBy('Month')\
         .count()\
         .withColumnRenamed('count','Count')\
         .orderBy('Month')



ctraildf11.show()



from pyspark.sql.functions import from_unixtime,unix_timestamp
ctraildf12=ctraildf11.withColumn("MonthInNumber",from_unixtime(unix_timestamp(col("Month"),'MMM'),'MM')).orderBy('MonthInNumber')



ctraildf12.show()



ctraildf13=ctraildf12.select('Month','Count')



ctraildf13.show()



import matplotlib.pyplot as mpltb

x = [val.Month for val in ctraildf13.select('Month').collect()]
y = [val.Count for val in ctraildf13.select('Count').collect()]


mpltb.plot(x, y)

mpltb.ylabel('Count')
mpltb.xlabel('Month')
mpltb.title('Trail completion by months')
mpltb.legend(['Count'], loc='upper left')

mpltb.show()



#Extra Work:Comparison between pharma and non pharma companies in case of providing sponsorship



pharmaCtrailDF1.show()



extraworkdf0001=pharmaCtrailDF1.select('Sponsor','Parent_Company')\
                               .withColumnRenamed('Parent_Company','Parent')\
                               .withColumnRenamed('Sponsor','Sponsor')\
                               .filter(col("Parent")\
                               .isNotNull())


extraworkdf0001.show(truncate=False)



extraworkdf0002=extraworkdf0001.select('Sponsor')\
               .groupBy('Sponsor')\
               .count()\
               .withColumnRenamed ('count', 'Count_by_Pharma')\
               .orderBy(col("Count").desc())\
               .limit(5)



extraworkdf0002.show(10, truncate=False)



import matplotlib.pyplot as mpltb; mpltb.rcdefaults()
import matplotlib.pyplot as mpltb

x = [val.Sponsor for val in extraworkdf0002.select('Sponsor').collect()]
y = [val.Count_by_Pharma for val in extraworkdf0002.select('Count_by_Pharma').collect()]


mpltb.bar(x, y)

mpltb.ylabel('No of Sponsored Trails')
mpltb.xlabel('Name of Sponsor')
mpltb.title('Top 5 sponsors from Pharmaceutical Industry')
mpltb.legend(['Count'], loc='upper center')

mpltb.show()



#Offences faced by companies in pharma industries



extraworkdf0003=pharmaDF.select("Company","Parent_Company","Specific_Industry_of_Parent","Primary_Offense")\
                        .withColumnRenamed ('Parent_Company','Parent_Company')\
                        .withColumnRenamed('Company','Company');



extraworkdf0003.show(10, truncate=False)



extraworkdf0004 = ctraildf5.join(extraworkdf0003,ctraildf5.Sponsor == extraworkdf0003.Parent_Company,"left")



extraworkdf0004.show(10, truncate=False)



extraworkdf0005=extraworkdf0004.select('Sponsor','Parent_Company','Primary_Offense')\
                               .withColumnRenamed ('Sponsor','Sponsor')\
                               .withColumnRenamed ('Parent_Company','Parent_Company')\
                               .withColumnRenamed ('Primary_Offense','Primary_Offense')\
                               .filter(col("Parent_Company")\
                               .isNotNull());



extraworkdf0005.show()



#Primary offences by companies from pharmaceuticals companies
extraworkdf0006=extraworkdf0005.select('Primary_Offense')\
               .groupBy('Primary_Offense')\
               .count()\
               .withColumnRenamed('count', 'Total')\
               .orderBy(col("Total").desc())\
               .limit(5);



extraworkdf0006.show(truncate=False)



import matplotlib.pyplot as plt

y = [val.Total for val in extraworkdf0006.select('Total').collect()]
mylabels = [val.Primary_Offense for val in extraworkdf0006.select('Primary_Offense').collect()]
myexplode = [0.05, 0.05, 0.05, 0,0]

plt.pie(y, labels = mylabels,explode = myexplode)
plt.show();

