import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    // Criar a sessão do Spark
    val spark = SparkSession.builder()
      .appName("Temperatura")
      .config("spark.master", "local")
      .getOrCreate()

    //carregando csv
    val dfTemperatura = spark.read
      .format("csv")
      .option("header", "true")      //A primeira linha do arquivo tem cabeçalho. Não começa diretamente nos dados
      .option("inferSchema", "true") // Garante que os tipos corretos sejam inferidos e não ler tudo como texto
      .load("temperatura_1000.csv")


    // renomeando todas as colunas
    val dfRenomeado = dfTemperatura
      .withColumnRenamed("ID", "id")
      .withColumnRenamed("Data", "data")
      .withColumnRenamed("Cidade", "cidade")
      .withColumnRenamed("Temperatura Máxima (°C)", "temperatura_maxima")
      .withColumnRenamed("Temperatura Mínima (°C)", "temperatura_minima")
      .withColumnRenamed("Precipitação (mm)", "precipitacao")
      .withColumnRenamed("Umidade (%)", "umidade")
      .withColumnRenamed("Velocidade do vento (km/h)", "velocidade_vento")

    // Mostrar todo o DataFrame com as colunas renomeadas
    dfRenomeado.show(dfRenomeado.count().toInt, truncate = false)

    // Relatórios
    //relatorios.Relatorio_1.show(dfRenomeado)
    //relatorios.Relatorio_2.show(dfRenomeado)
    //relatorios.Relatorio_3.show(dfRenomeado)
    //relatorios.Relatorio_4.show(dfRenomeado)
    //relatorios.Relatorio_5.show(dfRenomeado)
    //relatorios.Relatorio_6.show(dfRenomeado)
    //relatorios.Relatorio_7.show(dfRenomeado)
    //relatorios.Relatorio_8.show(dfRenomeado)
    //relatorios.Relatorio_9.show(dfRenomeado)
    relatorios.Relatorio_10.show(dfRenomeado)

    spark.stop()
  }
}