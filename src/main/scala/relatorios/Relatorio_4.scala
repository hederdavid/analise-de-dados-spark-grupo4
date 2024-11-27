package relatorios

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object Relatorio_4 {
  def show(dfRenomeado: DataFrame): Unit = {
    // 4 - Dias de alta umidade e alta temperatura (máxima > 30°C e umidade > 80%)
    val dfDiasAltaUmidadeETemperatura = dfRenomeado.filter(
      col("temperatura_maxima") > 30 &&
        col("umidade") > 80
    ).select(
      "data", "cidade", "temperatura_maxima", "umidade"
    )

    // Exibindo o resultado completo (sem truncamento)
    dfDiasAltaUmidadeETemperatura.show(false)
  }
}
