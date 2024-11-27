package relatorios

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object Relatorio_3 {
  def show(dfRenomeado: DataFrame): Unit = {
    // 3 - Dias mais chuvosos (> 50 mm)
    val dfDiasMaisChuvosos = dfRenomeado.filter(
      col("precipitacao") > 50
    ).select(
      "data", "cidade", "precipitacao"
    )

    // Exibindo o resultado completo (sem truncamento)
    dfDiasMaisChuvosos.show(false)
  }
}
