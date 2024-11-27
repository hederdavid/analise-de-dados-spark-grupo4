import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    // Criar a sessão do Spark
    val spark = SparkSession.builder()
      .appName("Consulta 10 - Distribuição de Umidade ao Longo do Ano")
      .config("spark.master", "local")
      .getOrCreate()

    // Ler o arquivo CSV
    val filePath = "caminho/para/temperatura_1000.csv"
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    // 1 - Temperatura média para todos os registros
    val tempMediaDF = df.withColumn(
      "Temperatura Média",
      (col("Temperatura Máxima (°C)") + col("Temperatura Mínima (°C)")) / 2
    )
    val mediaGeral = tempMediaDF.agg(avg("Temperatura Média").as("Média Geral")).collect()(0)(0)
    println(s"A temperatura média geral é: $mediaGeral%.2f°C")

    // 2 - Dias com grande variação de temperatura (> 15°C)
    val variacaoTemperaturaDF = df.withColumn(
      "Diferenca Temperatura",
      col("Temperatura Máxima (°C)") - col("Temperatura Mínima (°C)")
    )
    val diasComGrandeVariacao = variacaoTemperaturaDF.filter(
      col("Diferenca Temperatura") > 15
    ).select(
      "Data", "Cidade", "Temperatura Máxima (°C)", "Temperatura Mínima (°C)"
    )
    diasComGrandeVariacao.show()

    // 3 - Dias mais chuvosos (> 50 mm)
    val diasMaisChuvosos = df.filter(
      col("Precipitação (mm)") > 50
    ).select(
      "Data", "Cidade", "Precipitação (mm)"
    )
    diasMaisChuvosos.show()

    // 4 - Dias de alta umidade e alta temperatura (máxima > 30°C e umidade > 80%)
    val diasAltaUmidadeETemperatura = df.filter(
      col("Temperatura Máxima (°C)") > 30 &&
        col("Umidade (%)") > 80
    ).select(
      "Data", "Cidade", "Temperatura Máxima (°C)", "Umidade (%)"
    )
    diasAltaUmidadeETemperatura.show()

    // 5 - Média das temperaturas máximas
    val mediaTemperaturaMaxima = df.agg(avg("Temperatura Máxima (°C)").as("Média Temperatura Máxima")).collect()(0)(0)
    println(f"A média das temperaturas máximas é: $mediaTemperaturaMaxima%.2f°C")

    // 6 - Dias com vento mais intenso (> 20 km/h)
    val diasComVentoIntenso = df.filter(
      col("Velocidade do vento (km/h)") > 20
    ).select(
      "Data", "Cidade", "Velocidade do vento (km/h)"
    )
    diasComVentoIntenso.show()

    // 7 - Dias com precipitação acima da média
    val precipMedia = df.agg(avg("Precipitação (mm)").as("Precipitação Média")).collect()(0)(0)
    val diasComPrecipitacaoAcimaDaMedia = df.filter(
      col("Precipitação (mm)") > precipMedia
    ).select(
      "Data", "Cidade", "Precipitação (mm)"
    )
    diasComPrecipitacaoAcimaDaMedia.show()

    // 8 - Dias de baixa temperatura e vento forte (mínima < 10°C e vento > 15 km/h)
    val diasBaixaTemperaturaEVentoForte = df.filter(
      col("Temperatura Mínima (°C)") < 10 &&
        col("Velocidade do vento (km/h)") > 15
    ).select(
      "Data", "Cidade", "Temperatura Mínima (°C)", "Velocidade do vento (km/h)"
    )
    diasBaixaTemperaturaEVentoForte.show()

    // 9 - Diferença entre temperaturas máxima e mínima: Maiores variações por cidade
    val dfComDiferenca = df.withColumn(
      "Diferenca Temperatura",
      col("Temperatura Máxima (°C)") - col("Temperatura Mínima (°C)")
    )
    val variacaoPorCidade = dfComDiferenca.groupBy("Cidade")
      .agg(max("Diferenca Temperatura").as("Maior Variação de Temperatura"))
      .orderBy(desc("Maior Variação de Temperatura"))
    variacaoPorCidade.show()

    // 10 - Distribuição de umidade ao longo do ano (trimestres)
    val dfComData = df.withColumn("Data", to_date(col("Data"), "yyyy-MM-dd"))
    val dfComTrimestre = dfComData.withColumn("Trimestre",
      when(month(col("Data")).between(1, 3), lit(1))
        .when(month(col("Data")).between(4, 6), lit(2))
        .when(month(col("Data")).between(7, 9), lit(3))
        .otherwise(lit(4))
    )
    val umidadePorTrimestre = dfComTrimestre.groupBy("Trimestre")
      .agg(avg("Umidade (%)").as("Umidade Média"))
      .orderBy("Trimestre")
    umidadePorTrimestre.show()

    // Encerrar a sessão do Spark
    spark.stop()
  }
}