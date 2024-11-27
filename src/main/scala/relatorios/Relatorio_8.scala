package relatorios

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object Relatorio_8 {
  def show(dfRenomeado: DataFrame): Unit = {
    // 8 - Dias de baixa temperatura e vento forte (mínima < 10°C e vento > 15 km/h)
    val dFDiasBaixaTemperaturaEVentoForte = dfRenomeado.filter(
      col("temperatura_minima") < 10 &&
        col("velocidade_vento") > 15
    ).select(
      "data", "cidade", "temperatura_minima", "velocidade_vento"
    )

    // Exibindo os dias de baixa temperatura e vento forte
    dFDiasBaixaTemperaturaEVentoForte.show(false)
  }
}
