package relatorios

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Relatorio_10 {
  def show(dfRenomeado: DataFrame): Unit = {
    // 10 - Distribuição de umidade ao longo do ano (trimestres)
    val dfComData = dfRenomeado.withColumn("Data", to_date(col("data"), "yyyy-MM-dd"))

    // Adicionando coluna "Trimestre" baseado no mês
    val dfComTrimestre = dfComData.withColumn("Trimestre",
      when(month(col("Data")).between(1, 3), lit(1))
        .when(month(col("Data")).between(4, 6), lit(2))
        .when(month(col("Data")).between(7, 9), lit(3))
        .otherwise(lit(4))
    )

    // Agrupando por trimestre e calculando a umidade média
    val dfUmidadePorTrimestre = dfComTrimestre.groupBy("Trimestre")
      .agg(avg("umidade").as("Umidade Média"))
      .orderBy("Trimestre")

    // Exibindo a umidade média por trimestre
    dfUmidadePorTrimestre.show()
  }
}
