package relatorios

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Relatorio_9 {
  def show(dfRenomeado: DataFrame): Unit = {
    // 9 - Diferença entre temperaturas máxima e mínima: Maiores variações por cidade
    val dfComDiferenca = dfRenomeado.withColumn(
      "Diferenca Temperatura",
      col("temperatura_maxima") - col("temperatura_minima")
    )

    val dfVariacaoPorCidade = dfComDiferenca.groupBy("cidade")
      .agg(max("Diferenca Temperatura").as("Maior Variação de Temperatura"))
      .orderBy(desc("Maior Variação de Temperatura"))

    // Exibindo as maiores variações de temperatura por cidade
    dfVariacaoPorCidade.show()
  }
}
