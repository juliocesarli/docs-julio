import org.apache.spark.{SparkConf, SparkContext}

object ExercicioSentiment {
  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf()
    sparkConf.setAppName("Exercicio - Sentiment Analysis Dataset")
    sparkConf.setMaster("local")
    //definir o spark context
    val sc = new SparkContext(sparkConf)


    //carregar o arquivo csv
    val fileRdd = sc.textFile("dados/SentimentAnalysisDataset.csv")
    val first = fileRdd.first()
    val dados = fileRdd.filter(l=> l!=first)


    //fileRdd.take(10).foreach(println)
    //dados.take(10).foreach(println)

    val twit = dados.map(linha => ((linha.split(",")(3)),1))

    //separar cada elemento e filtrar por hashtag
    val twitFilter = dados.flatMap(l=>(l.split(" "))).filter(p=>p.startsWith("#")).filter(n=>n.length>2)

    val conta = twitFilter.map(wr=>(wr.replaceAll("\"","").toLowerCase,1)).reduceByKey((v1,v2)=>v1+v2)

    val contaSort = conta.sortBy(e=>e._2,false)


    contaSort.take(10).foreach(println)






  }
}
