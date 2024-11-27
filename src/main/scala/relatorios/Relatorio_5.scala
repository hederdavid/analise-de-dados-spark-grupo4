package relatorios

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.avg

object Relatorio_5 {
  def show(dfRenomeado: DataFrame): Unit = {
    // 5 - Média das temperaturas máximas
    val dfMediaTemperaturaMaxima = dfRenomeado
        .agg(avg("temperatura_maxima")
        .as("media_temperatura_maxima"))

    dfMediaTemperaturaMaxima.show(false)
  }
}
