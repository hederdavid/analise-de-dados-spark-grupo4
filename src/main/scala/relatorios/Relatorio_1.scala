package relatorios

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col}

object Relatorio_1 {
  def show (dfRenomeado: DataFrame): Unit = {
    // Calcule a temperatura média (média entre a máxima e a mínima) de todos os registros.
    val resultado = dfRenomeado
      .withColumn("media_temperatura", (col("temperatura_maxima") + col("temperatura_minima")) / 2)
      .agg(avg("media_temperatura").as("temperatura_media_geral"))

    // Exibindo o resultado completo (sem truncamento)
    resultado.show(false)
  }
}
