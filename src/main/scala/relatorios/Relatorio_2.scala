package relatorios

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object Relatorio_2 {
  def show(dfRenomeado: DataFrame): Unit = {
    // Calcular a diferença de temperatura entre a máxima e a mínima
    val dfVariacaoTemperatura = dfRenomeado.withColumn(
      "diferenca_temperatura",
      col("temperatura_maxima") - col("temperatura_minima")
    )

    // Filtrando os dias com grande variação de temperatura (> 15°C)
    val dfDiasComGrandeVariacao = dfVariacaoTemperatura.filter(
      col("diferenca_temperatura") > 15
    ).select(
      "data", "cidade", "temperatura_maxima", "temperatura_minima"
    )

    // Exibindo o resultado completo (sem truncamento)
    dfDiasComGrandeVariacao.show(false)
  }
}

