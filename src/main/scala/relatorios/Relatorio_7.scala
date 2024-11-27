package relatorios

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, avg}

object Relatorio_7 {
  def show(dfRenomeado: DataFrame): Unit = {
    // 7 - Dias com precipitação acima da média
    val dFprecipMedia = dfRenomeado.agg(avg("precipitacao").as("Precipitação Média")).collect()(0)(0)

    // Filtrando os dias com precipitação acima da média
    val dfDiasComPrecipitacaoAcimaDaMedia = dfRenomeado.filter(
      col("precipitacao") > dFprecipMedia
    ).select(
      "data", "cidade", "precipitacao"
    )

    // Exibindo os dias com precipitação acima da média
    dfDiasComPrecipitacaoAcimaDaMedia.show(false)
  }
}
