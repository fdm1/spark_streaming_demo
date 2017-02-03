DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd $DIR

if [ ! -d docker-spark ]; then
  git clone https://github.com/gettyimages/docker-spark
fi

cp docker-spark/docker-compose.yml .

sed -i 's%- ./%- ./docker-spark/%' docker-compose.yml
sed -i '/\/docker-spark\/data/ a \    - ./course_scripts:/course_scripts' docker-compose.yml

cd course_scripts/SparkScala
mvn install
# mvn archetype:generate -B -DarchetypeCatalog=https://github.com/mbonaci/spark-archetype-scala/raw/master/archetype-catalog.xml -DarchetypeRepository=https://github.com/mbonaci/spark-archetype-scala/raw/master -DarchetypeGroupId=org.sia -DarchetypeArtifactId=spark-archetype-scala -DarchetypeVersion=0.9 -DgroupId=com.frankmassi -DartifactId=UdemySparkScala -Dversion=0.1 -Dpackage=org.sijaset
