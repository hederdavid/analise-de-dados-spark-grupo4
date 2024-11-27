package relatorios

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object Relatorio_6 {
  def show(dfRenomeado: DataFrame): Unit = {
    // 6 - Dias com vento mais intenso (> 20 km/h)
    val dFDiasComVentoIntenso = dfRenomeado.filter(
      col("velocidade_vento") > 20
    ).select(
      "data", "cidade", "velocidade_vento"
    )

    // Exibindo os dias com vento mais intenso
    dFDiasComVentoIntenso.show(false)
  }
}
